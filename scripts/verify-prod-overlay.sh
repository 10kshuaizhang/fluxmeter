#!/usr/bin/env bash
# Verify production overlay: auth enforced, Redis password, full integration suite.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

ENV_FILE="${ENV_FILE:-.env.prod}"
if [[ ! -f "$ENV_FILE" ]]; then
  echo "Creating $ENV_FILE from .env.prod.example (test defaults)..."
  cat > "$ENV_FILE" <<'EOF'
REDIS_PASSWORD=fluxmeter-test-redis
FLUXMETER_API_KEY=fluxmeter-test-read
FLUXMETER_ADMIN_KEY=fluxmeter-test-admin
GRAFANA_ADMIN_PASSWORD=fluxmeter-test-grafana
CLICKHOUSE_PASSWORD=fluxmeter-test-ch
EOF
fi

set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

echo "==> Building JAR..."
./gradlew shadowJar -q

echo "==> Pulling images (mirror fallback)..."
if ! docker pull redis:7-alpine >/dev/null 2>&1; then
  bash "$ROOT/scripts/pull-images-mirror.sh"
fi

echo "==> Starting stack with prod overlay..."
docker compose -f docker-compose.yml -f docker-compose.prod.yml down -v 2>/dev/null || true
docker compose --env-file "$ENV_FILE" -f docker-compose.yml -f docker-compose.prod.yml up -d --build

echo "==> Waiting for services (50s)..."
sleep 50

curl -sf http://localhost:8000/health | grep -q ok && echo "  API health: OK"

echo "==> Submitting Flink job..."
make submit-job

echo "==> Running prod auth tests..."
export FLUXMETER_API_KEY FLUXMETER_ADMIN_KEY
export FLUXMETER_AUTH_OPTIONAL=false
pip install -q httpx pytest pytest-timeout
pytest tests/test_prod_overlay.py -v --timeout=60

echo "==> Running full integration suite (with auth headers)..."
pytest tests/test_integration.py -v --timeout=300

echo ""
echo "==> Production overlay verification PASSED"
