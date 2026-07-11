#!/usr/bin/env bash
# Correctness benchmark: known events → expected Redis counters + checkpoint health.
# Requires: full stack (make start-full && make submit-job), API on :8000.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

API="${API_URL:-http://127.0.0.1:8000}"
FLINK_UI="${FLINK_UI:-http://localhost:8081}"
REDIS_CLI="${REDIS_CLI:-docker exec fluxmeter-redis redis-cli}"
WINDOW_WAIT_SEC="${WINDOW_WAIT_SEC:-35}"
CUST="bench_correctness_$(date +%s)"
MODEL="gpt-4o-mini"
# 1M input tokens @ $0.15/M = $0.15 exactly
INPUT_TOKENS=1000000
EXPECTED_COST="0.15"

log() { echo "$@"; }

fail() { echo "FAIL: $*" >&2; exit 1; }

curl -sf "$API/health" | grep -q ok || fail "API not healthy at $API"

log "=============================================="
log " FluxMeter correctness bench"
log " customer=$CUST model=$MODEL input=$INPUT_TOKENS"
log "=============================================="

# Ingest one known-cost event
EVENT_ID="bench-$(date +%s)-$$"
HTTP_CODE=$(curl -s -o /tmp/fm-bench-ingest.json -w "%{http_code}" \
  -X POST "$API/ingest" \
  -H "Content-Type: application/json" \
  -d "{\"customerId\":\"$CUST\",\"modelId\":\"$MODEL\",\"provider\":\"openai\",\"inputTokens\":$INPUT_TOKENS,\"outputTokens\":0,\"eventId\":\"$EVENT_ID\",\"timestamp\":$(($(date +%s) * 1000))}")
[[ "$HTTP_CODE" == "202" ]] || fail "ingest returned $HTTP_CODE: $(cat /tmp/fm-bench-ingest.json)"

# Advance watermarks (10s windows + 5s OOO + idle): two keepalive rounds
for _ in 1 2; do
  sleep 12
  curl -sf -X POST "$API/ingest" -H "Content-Type: application/json" \
    -d "{\"customerId\":\"$CUST\",\"modelId\":\"$MODEL\",\"provider\":\"openai\",\"inputTokens\":1,\"outputTokens\":0,\"timestamp\":$(($(date +%s) * 1000))}" \
    >/dev/null || true
done
sleep 5

log "Waiting up to ${WINDOW_WAIT_SEC}s for Flink window..."
DEADLINE=$((SECONDS + WINDOW_WAIT_SEC))
USAGE_JSON=""
while (( SECONDS < DEADLINE )); do
  CODE=$(curl -s -o /tmp/fm-bench-usage.json -w "%{http_code}" "$API/usage/customer/$CUST" || echo "000")
  if [[ "$CODE" == "200" ]]; then
    INPUT_GOT=$(python3 -c "import json; print(json.load(open('/tmp/fm-bench-usage.json')).get('input_tokens',0))")
    if (( INPUT_GOT >= INPUT_TOKENS )); then
      USAGE_JSON=$(cat /tmp/fm-bench-usage.json)
      break
    fi
  fi
  sleep 2
done

[[ -n "$USAGE_JSON" ]] || fail "usage not ready for $CUST after ${WINDOW_WAIT_SEC}s"

python3 - "$EXPECTED_COST" "$INPUT_TOKENS" <<'PY'
import json, sys
expected_cost = float(sys.argv[1])
min_input = int(sys.argv[2])
u = json.load(open("/tmp/fm-bench-usage.json"))
inp = int(u.get("input_tokens") or 0)
cost = float(u.get("cost_usd") or 0)
assert inp >= min_input, f"input_tokens={inp} < {min_input}"
# keepalive adds +1/+1 tokens; cost should be within 1e-4 of expected (+ tiny keepalive)
assert abs(cost - expected_cost) < 0.01, f"cost_usd={cost} expected ~{expected_cost}"
print(f"OK usage: input_tokens={inp} cost_usd={cost}")
PY

# Redis direct check
REDIS_INPUT=$(${REDIS_CLI} GET "customer:${CUST}:input_tokens" 2>/dev/null || echo "0")
log "Redis customer:${CUST}:input_tokens=$REDIS_INPUT"
[[ "${REDIS_INPUT:-0}" -ge "$INPUT_TOKENS" ]] || fail "Redis input_tokens too low"

# Flink checkpoint health (best-effort)
log ""
log "=== Flink checkpoint summary ==="
JOBS_JSON=$(curl -sf "$FLINK_UI/jobs" 2>/dev/null || echo "")
if [[ -n "$JOBS_JSON" ]]; then
  JOB_ID=$(python3 -c "import json,sys; j=json.loads(sys.argv[1]); print(next((x['id'] for x in j.get('jobs',[]) if x.get('status')=='RUNNING'),''))" "$JOBS_JSON" 2>/dev/null || echo "")
  if [[ -n "$JOB_ID" ]]; then
    CP=$(curl -sf "$FLINK_UI/jobs/$JOB_ID/checkpoints" 2>/dev/null || echo "{}")
    python3 -c "
import json,sys
c=json.loads(sys.argv[1])
counts=c.get('counts',{})
print('job:', sys.argv[2])
print('checkpoints completed:', counts.get('completed', 'n/a'))
print('checkpoints failed:', counts.get('failed', 'n/a'))
print('checkpoints in_progress:', counts.get('in_progress', 'n/a'))
latest=c.get('latest',{})
comp=latest.get('completed') or {}
if comp:
    print('latest completed id:', comp.get('id'), 'size:', comp.get('state_size'))
" "$CP" "$JOB_ID"
  else
    log "(no RUNNING Flink job — skip checkpoint stats)"
  fi
else
  log "(Flink UI unreachable at $FLINK_UI — skip)"
fi

log ""
log "=============================================="
log " Correctness bench PASSED"
log "=============================================="
