#!/usr/bin/env bash
# Run all FluxMeter E2E / integration test suites in order.
# Usage: ./scripts/run-e2e-all.sh [--full-only|--lite-only|--unit-only]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

MODE="${1:-all}"
export PYTHONPATH="${ROOT}/api:${PYTHONPATH:-}"

pip install -q -r tests/requirements.txt

run_unit() {
  echo "=== Unit (no Docker) ==="
  pytest tests/test_auth_unit.py tests/test_billing_export.py tests/test_control_plane_models.py \
    tests/test_lite_aggregate_unit.py tests/test_rollup.py -v --timeout=60
  ./gradlew test -q
}

run_lite() {
  echo "=== Lite stack ==="
  docker compose down 2>/dev/null || true
  docker compose -f docker-compose.full.yml down 2>/dev/null || true
  docker compose up -d --build
  echo "Waiting for lite API..."
  for i in $(seq 1 30); do
    curl -sf http://localhost:8000/health | grep -q '"mode":"lite"' && break
    sleep 2
  done
  pytest tests/test_lite_aggregate_unit.py tests/test_lite_production.py tests/test_rollup.py -v --timeout=120
}

run_full() {
  echo "=== Full stack (Kafka + Flink) ==="
  docker compose down 2>/dev/null || true
  ./gradlew shadowJar -q
  # Prometheus image optional — core services only if pull fails
  if ! docker compose -f docker-compose.full.yml up -d --build kafka kafka-init redis jobmanager taskmanager-1 taskmanager-2 taskmanager-3 api grafana 2>/dev/null; then
    echo "WARN: full compose up failed; retrying without rebuild..."
    docker compose -f docker-compose.full.yml up -d kafka kafka-init redis jobmanager taskmanager-1 taskmanager-2 taskmanager-3 api grafana
  fi
  echo "Waiting for Flink cluster..."
  sleep 20
  FLINK_PARALLELISM="${FLINK_PARALLELISM:-8}" make submit-job
  for i in $(seq 1 30); do
    docker exec fluxmeter-jobmanager flink list 2>/dev/null | grep -q RUNNING && break
    sleep 3
  done
  if ! docker exec fluxmeter-jobmanager flink list 2>/dev/null | grep -q RUNNING; then
    echo "ERROR: Flink job not RUNNING — fix cluster before full E2E"
    exit 1
  fi
  pytest tests/test_integration.py tests/test_e2e_v2.py -v --timeout=300
}

run_saas() {
  echo "=== SaaS stack ==="
  docker compose down 2>/dev/null || true
  docker compose -f docker-compose.full.yml down 2>/dev/null || true
  export REDIS_PASSWORD="${REDIS_PASSWORD:-fluxmeter}"
  export CP_ADMIN_KEY="${CP_ADMIN_KEY:-cp_admin_test_key}"
  export FLUXMETER_API_KEY="${FLUXMETER_API_KEY:-test_api_key}"
  export FLUXMETER_ADMIN_KEY="${FLUXMETER_ADMIN_KEY:-test_admin_key}"
  docker compose -f docker-compose.saas.yml up -d --build
  sleep 8
  curl -sf http://localhost:8001/health
  pytest tests/test_control_plane.py tests/test_prod_overlay.py -v --timeout=60
}

case "$MODE" in
  --unit-only) run_unit ;;
  --lite-only) run_lite ;;
  --full-only) run_full ;;
  all)
    run_unit
    run_lite
    run_full
    run_saas
    ;;
  *) echo "Usage: $0 [--full-only|--lite-only|--unit-only|all]"; exit 1 ;;
esac

echo "=== All requested E2E suites passed ==="
