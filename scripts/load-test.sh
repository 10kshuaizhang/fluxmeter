#!/usr/bin/env bash
# Staged load test: 10K → 50K → 100K → 500K → 1M eps (short bursts).
# Requires: docker-compose stack, Java 17, Flink job submitted.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

JAR="$(ls -t build/libs/fluxmeter-*.jar 2>/dev/null | head -1)"
if [[ -z "$JAR" ]]; then
  echo "ERROR: No JAR found. Run: make build"
  exit 1
fi

API="${API_URL:-http://localhost:8000}"
KAFKA_BROKERS="${KAFKA_BROKERS:-localhost:9094}"
NUM_CUSTOMERS="${NUM_CUSTOMERS:-10000}"
NUM_THREADS="${NUM_THREADS:-4}"
DURATION_SEC="${DURATION_SEC:-20}"
RESULTS_DIR="${RESULTS_DIR:-$ROOT/load-test-results}"
TIMESTAMP="$(date +%Y%m%d-%H%M%S)"

mkdir -p "$RESULTS_DIR"
SUMMARY="$RESULTS_DIR/run-$TIMESTAMP.txt"
LOG="$RESULTS_DIR/run-$TIMESTAMP.log"

log() { echo "$@" | tee -a "$SUMMARY"; }

check_health() {
  curl -sf "$API/health" | grep -q ok || { echo "API not healthy at $API"; exit 1; }
}

ensure_flink_job() {
  if docker exec fluxmeter-jobmanager flink list 2>/dev/null | grep -q RUNNING; then
    log "Flink job: RUNNING"
    return
  fi
  log "Submitting Flink job..."
  docker cp "$JAR" fluxmeter-jobmanager:/opt/flink/fluxmeter.jar
  docker exec fluxmeter-jobmanager flink run -d \
    -c io.fluxmeter.job.TokenUsageAggregator \
    /opt/flink/fluxmeter.jar
  sleep 8
  docker exec fluxmeter-jobmanager flink list 2>/dev/null | tee -a "$SUMMARY"
}

snapshot_metrics() {
  local label="$1"
  local global
  global="$(curl -sf "$API/usage/global" 2>/dev/null || echo '{}')"
  log "--- $label ---"
  log "  global: $global"
  docker stats --no-stream --format '{{.Name}}: CPU {{.CPUPerc}} MEM {{.MemUsage}}' \
    fluxmeter-taskmanager-1 fluxmeter-kafka fluxmeter-redis 2>/dev/null | tee -a "$SUMMARY" || true
}

run_tier() {
  local target_eps="$1"
  local duration="${2:-$DURATION_SEC}"
  local tier_log="$RESULTS_DIR/tier-${target_eps}-${TIMESTAMP}.log"

  log ""
  log "========== TIER ${target_eps} eps × ${duration}s =========="

  KAFKA_BROKERS="$KAFKA_BROKERS" \
  NUM_CUSTOMERS="$NUM_CUSTOMERS" \
  NUM_THREADS="$NUM_THREADS" \
  TARGET_EPS="$target_eps" \
  java -cp "$JAR" io.fluxmeter.generator.LoadGenerator >"$tier_log" 2>&1 &
  local pid=$!
  sleep "$duration"
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true

  # Extract peak and average EPS from STATS lines
  local peak avg count
  peak="$(grep '\[STATS\]' "$tier_log" | awk '{print $2}' | sort -n | tail -1 || echo "0")"
  avg="$(grep '\[STATS\]' "$tier_log" | awk '{print $2}' | sed 's/,//g' | awk '{s+=$1; n++} END {if(n>0) printf "%.0f", s/n; else print 0}')"
  count="$(grep '\[STATS\]' "$tier_log" | tail -1 | awk '{print $(NF-1)}' | tr -d ',' || echo "0")"

  log "  target_eps: $target_eps"
  log "  avg_eps:    $avg"
  log "  peak_eps:   $peak"
  log "  total_sent: $count"
  log "  log:        $tier_log"
  snapshot_metrics "after ${target_eps} eps"
}

log "FluxMeter Load Test — $TIMESTAMP"
log "JAR: $JAR"
log "Kafka: $KAFKA_BROKERS | customers=$NUM_CUSTOMERS threads=$NUM_THREADS"

check_health
snapshot_metrics "baseline"
ensure_flink_job

# Staged tiers (skip 1M if QUICK=1)
TIERS=(10000 50000 100000 500000)
if [[ "${QUICK:-}" != "1" ]]; then
  TIERS+=(1000000)
fi

for eps in "${TIERS[@]}"; do
  run_tier "$eps"
  sleep 3
done

log ""
log "========== DONE =========="
log "Full log: $LOG"
log "Summary:  $SUMMARY"
