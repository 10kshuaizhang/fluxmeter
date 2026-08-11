#!/usr/bin/env bash
# scripts/test-cold-store.sh — ADR-1 acceptance (A1,A2,A4,A5,A6,A7)
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
  # $1 = single-line JSON
  echo -n "$1" | docker exec -i "$KAFKA_CONTAINER" \
    /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:9092 \
    --topic "$TOPIC" >/dev/null
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

# Preconditions
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

# A1: N unique events
N=5
CID="${PREFIX}-cust"
for i in $(seq 1 "$N"); do
  ts=$(( $(date +%s) * 1000 ))
  kafka_produce "{\"eventId\":\"${PREFIX}-e${i}\",\"customerId\":\"${CID}\",\"modelId\":\"gpt-4o\",\"provider\":\"openai\",\"inputTokens\":10,\"outputTokens\":5,\"timestamp\":${ts}}"
done
wait_until "A1 count FINAL == $N" \
  "SELECT count() FROM fluxmeter.raw_events FINAL WHERE customer_id='${CID}'" \
  "$N" || true

# A2: replay same eventId three times — count stays N
for _ in 1 2 3; do
  ts=$(( $(date +%s) * 1000 ))
  kafka_produce "{\"eventId\":\"${PREFIX}-e1\",\"customerId\":\"${CID}\",\"modelId\":\"gpt-4o\",\"provider\":\"openai\",\"inputTokens\":10,\"outputTokens\":5,\"timestamp\":${ts}}"
done
sleep 3
wait_until "A2 still $N after replay" \
  "SELECT count() FROM fluxmeter.raw_events FINAL WHERE customer_id='${CID}'" \
  "$N" || true

# A4: customer + time range returns wide fields
ROW="$(ch_query "SELECT event_id, customer_id, model_id, input_tokens, output_tokens FROM fluxmeter.raw_events FINAL WHERE customer_id='${CID}' AND event_id='${PREFIX}-e1' FORMAT TSV" 2>/dev/null || true)"
if echo "$ROW" | grep -q "${PREFIX}-e1"; then
  echo "  OK  A4 wide row query"; PASS=$((PASS + 1))
else
  echo "  FAIL A4 wide row query (got=${ROW})"; FAIL=$((FAIL + 1))
fi

# A5: missing eventId → DLQ, not main
ts=$(( $(date +%s) * 1000 ))
kafka_produce "{\"customerId\":\"${CID}-noid\",\"modelId\":\"gpt-4o\",\"provider\":\"openai\",\"inputTokens\":1,\"outputTokens\":1,\"timestamp\":${ts}}"
sleep 3
MAIN_NOID="$(ch_query "SELECT count() FROM fluxmeter.raw_events FINAL WHERE customer_id='${CID}-noid'" 2>/dev/null | tr -d '[:space:]' || true)"
DLQ_MISS="$(ch_query "SELECT count() FROM fluxmeter.raw_events_dlq WHERE error_reason='missing_event_id' AND ingested_at > now() - INTERVAL 5 MINUTE" 2>/dev/null | tr -d '[:space:]' || true)"
if [ "${MAIN_NOID:-0}" = "0" ] && [ "${DLQ_MISS:-0}" != "0" ]; then
  echo "  OK  A5 missing eventId → DLQ"; PASS=$((PASS + 1))
else
  echo "  FAIL A5 main=${MAIN_NOID} dlq_missing=${DLQ_MISS}"; FAIL=$((FAIL + 1))
fi

# A6: illegal JSON → DLQ parse_error
kafka_produce "this is not json ${PREFIX}"
sleep 3
DLQ_PARSE="$(ch_query "SELECT count() FROM fluxmeter.raw_events_dlq WHERE error_reason='parse_error' AND ingested_at > now() - INTERVAL 5 MINUTE" 2>/dev/null | tr -d '[:space:]' || true)"
if [ "${DLQ_PARSE:-0}" != "0" ]; then
  echo "  OK  A6 parse_error → DLQ"; PASS=$((PASS + 1))
else
  echo "  FAIL A6 no parse_error DLQ rows"; FAIL=$((FAIL + 1))
fi

echo "== results: PASS=$PASS FAIL=$FAIL =="
[ "$FAIL" -eq 0 ]
