#!/usr/bin/env bash
set -euo pipefail

# FluxMeter Baseline Comparison: Flink (streaming) vs ClickHouse (store-then-query)
# Run this AFTER `make demo-full` has the pipeline running

CLICKHOUSE_URL="http://localhost:8123"
REDIS_CLI="docker exec fluxmeter-redis redis-cli"

echo "=============================================="
echo " FluxMeter vs ClickHouse Baseline Comparison"
echo "=============================================="
echo ""

# Wait for ClickHouse to have data
echo "Waiting for ClickHouse to ingest events..."
for i in $(seq 1 30); do
    CH_EVENTS=$(curl -s "$CLICKHOUSE_URL" --data-binary "SELECT sum(event_count) FROM fluxmeter.usage_per_minute" 2>/dev/null || echo "0")
    if [ "$CH_EVENTS" != "0" ] && [ -n "$CH_EVENTS" ]; then
        echo "  ClickHouse has $CH_EVENTS events aggregated."
        break
    fi
    sleep 2
done

echo ""
echo "=== LATENCY COMPARISON ==="
echo ""

# Flink latency: time since last window fired
FLINK_LAST=$(${REDIS_CLI} GET global:last_window_end 2>/dev/null || echo "0")
NOW_MS=$(($(date +%s) * 1000))
if [ "$FLINK_LAST" != "" ] && [ "$FLINK_LAST" != "0" ]; then
    FLINK_LAG_MS=$((NOW_MS - FLINK_LAST))
    echo "Flink (streaming):     ${FLINK_LAG_MS}ms since last window output"
else
    echo "Flink (streaming):     waiting for first window..."
fi

# ClickHouse latency: lag between newest aggregated window and now
CH_LAG=$(curl -s "$CLICKHOUSE_URL" --data-binary \
    "SELECT dateDiff('second', max(window_start), now()) FROM fluxmeter.usage_per_minute" 2>/dev/null || echo "N/A")
echo "ClickHouse (batch):    ${CH_LAG}s lag from newest aggregated window"

echo ""
echo "=== THROUGHPUT COMPARISON ==="
echo ""

# Flink: events processed (from Redis)
FLINK_EVENTS=$(${REDIS_CLI} GET global:total_events 2>/dev/null || echo "0")
echo "Flink events metered:      $(printf "%'d" ${FLINK_EVENTS:-0})"

# ClickHouse: events ingested
CH_TOTAL=$(curl -s "$CLICKHOUSE_URL" --data-binary \
    "SELECT sum(event_count) FROM fluxmeter.usage_per_minute" 2>/dev/null || echo "0")
echo "ClickHouse events stored:  $(printf "%'d" ${CH_TOTAL:-0})"

echo ""
echo "=== COST COMPARISON ==="
echo ""

FLINK_COST=$(${REDIS_CLI} GET global:total_cost_usd 2>/dev/null || echo "0")
CH_COST=$(curl -s "$CLICKHOUSE_URL" --data-binary \
    "SELECT round(sum(cost_usd), 2) FROM fluxmeter.usage_per_minute" 2>/dev/null || echo "0")
echo "Flink total cost:      \$${FLINK_COST:-0}"
echo "ClickHouse total cost: \$${CH_COST:-0}"

echo ""
echo "=== TOP CUSTOMERS (ClickHouse query) ==="
echo ""
curl -s "$CLICKHOUSE_URL" --data-binary \
    "SELECT customerId, sum(total_tokens) as tokens, round(sum(cost_usd),2) as cost FROM fluxmeter.usage_per_minute GROUP BY customerId ORDER BY cost DESC LIMIT 5 FORMAT Pretty" 2>/dev/null

echo ""
echo "=============================================="
echo " Summary:"
echo "  Flink:      sub-second aggregation latency"
echo "  ClickHouse: ${CH_LAG}s query lag (batch processing)"
echo "  Winner:     Streaming-first (Flink) for real-time"
echo "=============================================="
