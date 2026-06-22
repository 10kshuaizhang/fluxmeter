#!/usr/bin/env bash
set -euo pipefail

# Record a terminal demo of FluxMeter
# Prerequisites: `make build` already done, Docker running

echo "=== Recording FluxMeter Demo ==="
echo ""

# Clean slate
docker compose down -v 2>/dev/null || true
sleep 2

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  FluxMeter: Streaming metering engine for AI token billing"
echo "  1M+ events/sec | Sub-second aggregation | Apache Flink"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

echo "▶ Starting infrastructure (Kafka + Flink + Redis + ClickHouse)..."
docker compose up -d 2>&1 | grep -v "warning\|Pulling\|layer\|Wait\|Download\|Extract"
echo "  ✓ All services running"
echo ""

sleep 15

echo "▶ Submitting Flink job..."
JAR="$(ls -t build/libs/fluxmeter-*.jar 2>/dev/null | head -1)"
docker cp "$JAR" fluxmeter-jobmanager:/opt/flink/fluxmeter.jar >/dev/null
docker exec fluxmeter-jobmanager flink run -d -c io.fluxmeter.job.TokenUsageAggregator /opt/flink/fluxmeter.jar 2>&1 | grep "Job has been"
echo ""

sleep 3

echo "▶ Generating 500K token events/sec..."
echo ""
KAFKA_BROKERS=localhost:9094 NUM_CUSTOMERS=10000 NUM_THREADS=4 TARGET_EPS=500000 timeout 20 java -cp "$JAR" io.fluxmeter.generator.LoadGenerator 2>&1 | grep STATS
echo ""

# Flush windows
KAFKA_BROKERS=localhost:9094 NUM_CUSTOMERS=100 NUM_THREADS=1 TARGET_EPS=10000 timeout 15 java -cp "$JAR" io.fluxmeter.generator.LoadGenerator >/dev/null 2>&1

echo "▶ Real-time aggregation results (from Redis):"
echo ""
echo "  Events metered:  $(docker exec fluxmeter-redis redis-cli GET global:total_events)"
echo "  Tokens counted:  $(docker exec fluxmeter-redis redis-cli GET global:total_tokens)"
echo "  Cost calculated: \$$(docker exec fluxmeter-redis redis-cli GET global:total_cost_usd)"
echo ""

echo "▶ Baseline comparison: Flink vs ClickHouse"
echo ""
./baseline/benchmark.sh
echo ""

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  FluxMeter — github.com/10kshuaizhang/fluxmeter"
echo "  make demo  — try it yourself"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Cleanup
docker compose down 2>/dev/null || true
