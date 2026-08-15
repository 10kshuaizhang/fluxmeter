#!/usr/bin/env bash
# Run all FluxMeter E2E / integration test suites in order.
# Usage: ./scripts/run-e2e-all.sh [--stack-only|--unit-only]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

MODE="${1:-all}"
export PYTHONPATH="${ROOT}/api:${PYTHONPATH:-}"

pip install -q -r tests/requirements.txt

UNIT_TESTS=(
  tests/test_auth_unit.py
  tests/test_billing_export.py
  tests/test_control_plane_models.py
  tests/test_tenant_keys.py
  tests/test_pricing_loader.py
  tests/test_pricing_validate.py
  tests/test_rerate_tier.py
  tests/test_phase2_billing.py
)

run_unit() {
  echo "=== Unit (no Docker) ==="
  pytest "${UNIT_TESTS[@]}" -v --timeout=60
  ./gradlew test -q
  if [ -d sdk/python/tests ]; then
    echo "=== Python SDK unit ==="
    (cd sdk/python && pip install -q -e ".[dev]" 2>/dev/null || pip install -q -e .)
    pytest sdk/python/tests -v --timeout=60
  fi
}

run_stack() {
  echo "=== HTTP -> Kafka -> Flink -> Redis stack ==="
  docker compose down 2>/dev/null || true
  ./gradlew shadowJar -q
  docker compose up -d --build
  echo "Waiting for the automatically submitted Flink job..."
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
  export REDIS_PASSWORD="${REDIS_PASSWORD:-fluxmeter}"
  export CP_ADMIN_KEY="${CP_ADMIN_KEY:-cp_admin_test_key}"
  export FLUXMETER_API_KEY="${FLUXMETER_API_KEY:-test_api_key}"
  export FLUXMETER_ADMIN_KEY="${FLUXMETER_ADMIN_KEY:-test_admin_key}"
  docker compose -f docker-compose.yml -f docker-compose.saas.yml up -d --build
  sleep 8
  curl -sf http://127.0.0.1:8001/health
  pytest tests/test_control_plane.py tests/test_prod_overlay.py -v --timeout=60
}

case "$MODE" in
  --unit-only) run_unit ;;
  --stack-only) run_stack ;;
  all)
    run_unit
    run_stack
    run_saas
    ;;
  *) echo "Usage: $0 [--stack-only|--unit-only|all]"; exit 1 ;;
esac

echo "=== All requested E2E suites passed ==="
