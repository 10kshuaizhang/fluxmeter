#!/usr/bin/env bash
# scripts/test-cold-store.sh — ADR-1 acceptance (A1,A2,A3,A4,A5,A6,A7)
# Seam: Kafka token-events → fluxmeter.raw_events / raw_events_dlq (Trusted Envelope)
set -euo pipefail

CLICKHOUSE_URL="${CLICKHOUSE_URL:-http://localhost:8123}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-fluxmeter-kafka}"
TOPIC="${TOPIC:-token-events}"
PREFIX="coldtest-$(date +%s)-$$"
PASS=0
FAIL=0

ch_query() {
  curl -sf "$CLICKHOUSE_URL" --data-binary "$1"
}

kafka_produce() {
  # Write payload to a temp file inside the container, then redirect into the producer.
  # Optional $2 = Kafka key so acceptance messages share a partition.
  # ponytail: console producer often idles after send; treat timeout 124/143 as OK.
  # Ceiling: silent drop if killed before broker ack. Upgrade: java LoadGenerator or kcat.
  local payload="$1"
  local key="${2:-coldstore}"
  local b64
  b64="$(printf '%s' "$payload" | base64 | tr -d '\n')"
  set +e
  docker exec "$KAFKA_CONTAINER" \
    env KEY="$key" B64="$b64" TOPIC="$TOPIC" \
    bash -c 'json=$(printf %s "$B64" | base64 -d)
      printf "%s:%s\n" "$KEY" "$json" > /tmp/coldstore-msg.txt
      timeout 12 /opt/kafka/bin/kafka-console-producer.sh \
        --bootstrap-server kafka:9092 \
        --topic "$TOPIC" \
        --property parse.key=true \
        --property key.separator=: \
        --producer-property linger.ms=0 \
        --producer-property acks=1 \
        --producer-property max.block.ms=8000 \
        < /tmp/coldstore-msg.txt' \
    >/dev/null
  local ec=$?
  set -e
  if [ "$ec" -eq 0 ] || [ "$ec" -eq 124 ] || [ "$ec" -eq 143 ]; then
    return 0
  fi
  return "$ec"
}

# Production-shaped Trusted Envelope (ADR-024)
envelope_event() {
  local eid="$1" cid="$2" input="${3:-10}" output="${4:-5}"
  local ts=$(( $(date +%s) * 1000 ))
  printf '%s' "{\"envelopeVersion\":1,\"source\":\"operator\",\"payload\":{\"eventId\":\"${eid}\",\"customerId\":\"${cid}\",\"modelId\":\"gpt-4o\",\"provider\":\"openai\",\"inputTokens\":${input},\"outputTokens\":${output},\"timestamp\":${ts}},\"auth\":{\"tenantId\":\"\",\"apiKeyId\":\"\",\"customerId\":\"${cid}\"},\"receipt\":{\"receivedAt\":${ts}}}"
}

wait_until() {
  local desc="$1" sql="$2" expect="$3" tries="${4:-30}"
  local got=""
  for _ in $(seq 1 "$tries"); do
    got="$(ch_query "$sql" 2>/dev/null || true)"
    got="$(echo -n "$got" | tr -d '[:space:]')"
    if [ "$got" = "$expect" ]; then
      echo "  OK  $desc (got=$got)"
      PASS=$((PASS + 1))
      return 0
    fi
    sleep 1
  done
  echo "  FAIL $desc (expected=$expect got=${got:-<empty>})"
  FAIL=$((FAIL + 1))
  return 1
}

echo "== ADR-1 cold store acceptance =="

ch_query "SELECT 1" >/dev/null
docker inspect "$KAFKA_CONTAINER" >/dev/null

# A7: no derived columns on raw_events
COLS="$(ch_query "SELECT name FROM system.columns WHERE database='fluxmeter' AND table='raw_events' FORMAT TSV" 2>/dev/null || true)"
for bad in balance held invoice_id cost_usd aggregated_total; do
  if echo "$COLS" | grep -qi "$bad"; then
    echo "  FAIL A7 column '$bad' present"; FAIL=$((FAIL + 1))
  else
    echo "  OK  A7 no column '$bad'"; PASS=$((PASS + 1))
  fi
done

# A1: N unique envelope events (retry once — console producer can drop under timeout)
N=5
CID="${PREFIX}-cust"
produce_a1() {
  for i in $(seq 1 "$N"); do
    kafka_produce "$(envelope_event "${PREFIX}-e${i}" "$CID")" "$CID" || true
    sleep 0.5
  done
}
produce_a1
if ! wait_until "A1 count FINAL == $N" \
  "SELECT count() FROM fluxmeter.raw_events FINAL WHERE customer_id='${CID}'" \
  "$N" 45; then
  produce_a1
  wait_until "A1 count FINAL == $N (retry)" \
    "SELECT count() FROM fluxmeter.raw_events FINAL WHERE customer_id='${CID}'" \
    "$N" 45 || true
fi

# A2: replay same eventId three times — count stays N
for _ in 1 2 3; do
  kafka_produce "$(envelope_event "${PREFIX}-e1" "$CID")" "$CID"
done
sleep 3
wait_until "A2 still $N after replay" \
  "SELECT count() FROM fluxmeter.raw_events FINAL WHERE customer_id='${CID}'" \
  "$N" 45 || true

# A4: customer + time range returns wide fields
ROW="$(ch_query "SELECT event_id, customer_id, model_id, input_tokens, output_tokens FROM fluxmeter.raw_events FINAL WHERE customer_id='${CID}' AND event_id='${PREFIX}-e1' FORMAT TSV" 2>/dev/null || true)"
if echo "$ROW" | grep -q "${PREFIX}-e1"; then
  echo "  OK  A4 wide row query"; PASS=$((PASS + 1))
else
  echo "  FAIL A4 wide row query (got=${ROW})"; FAIL=$((FAIL + 1))
fi

# A5: missing eventId → DLQ, not main
ts=$(( $(date +%s) * 1000 ))
DLQ_MISS_BEFORE="$(ch_query "SELECT count() FROM fluxmeter.raw_events_dlq WHERE error_reason='missing_event_id'" 2>/dev/null | tr -d '[:space:]' || true)"
kafka_produce "{\"envelopeVersion\":1,\"source\":\"operator\",\"payload\":{\"customerId\":\"${CID}-noid\",\"modelId\":\"gpt-4o\",\"provider\":\"openai\",\"inputTokens\":1,\"outputTokens\":1,\"timestamp\":${ts}},\"auth\":{},\"receipt\":{\"receivedAt\":${ts}}}" "${CID}-noid"
wait_until "A5 DLQ missing_event_id incremented" \
  "SELECT count() > ${DLQ_MISS_BEFORE:-0} FROM fluxmeter.raw_events_dlq WHERE error_reason='missing_event_id'" \
  "1" 90 || true
MAIN_NOID="$(ch_query "SELECT count() FROM fluxmeter.raw_events FINAL WHERE customer_id='${CID}-noid'" 2>/dev/null | tr -d '[:space:]' || true)"
if [ "${MAIN_NOID:-0}" = "0" ]; then
  echo "  OK  A5 missing eventId not in main"; PASS=$((PASS + 1))
else
  echo "  FAIL A5 missing eventId leaked to main count=${MAIN_NOID}"; FAIL=$((FAIL + 1))
fi

# A6: illegal JSON → DLQ parse_error
DLQ_PARSE_BEFORE="$(ch_query "SELECT count() FROM fluxmeter.raw_events_dlq WHERE error_reason='parse_error'" 2>/dev/null | tr -d '[:space:]' || true)"
kafka_produce "this is not json ${PREFIX}" "parse-bad"
wait_until "A6 parse_error → DLQ" \
  "SELECT count() > ${DLQ_PARSE_BEFORE:-0} FROM fluxmeter.raw_events_dlq WHERE error_reason='parse_error'" \
  "1" 90 || true

# A3: independent consumer group
ENGINE_FULL="$(ch_query "SELECT engine_full FROM system.tables WHERE database='fluxmeter' AND name='token_events_queue' FORMAT TSV" 2>/dev/null || true)"
if echo "$ENGINE_FULL" | grep -q "fluxmeter-cold-store"; then
  echo "  OK  A3 group fluxmeter-cold-store configured"
  PASS=$((PASS + 1))
else
  echo "  FAIL A3 expected kafka_group_name fluxmeter-cold-store in engine_full"
  FAIL=$((FAIL + 1))
fi
echo "  NOTE A3 runtime demo: stop Flink TMs, keep producing to Kafka, watch raw_events FINAL count increase."

echo "== results: PASS=$PASS FAIL=$FAIL =="
[ "$FAIL" -eq 0 ]
