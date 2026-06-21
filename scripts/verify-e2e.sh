#!/usr/bin/env bash
# Full-stack verification: build → deploy → submit Flink job → integration tests
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if ! command -v docker >/dev/null 2>&1; then
  echo "ERROR: docker not found. Install Docker Desktop and ensure it is running."
  exit 1
fi

echo "==> Building JAR..."
./gradlew shadowJar

echo "==> Pulling container images..."
if ! docker pull redis:7-alpine >/dev/null 2>&1; then
  echo "  Docker Hub unreachable — using mirror..."
  bash "$ROOT/scripts/pull-images-mirror.sh"
fi

echo "==> Restarting infrastructure..."
docker compose down -v
docker compose up -d --build

echo "==> Waiting for services (45s)..."
sleep 45

echo "==> Health checks..."
curl -sf http://localhost:8000/health | grep -q ok && echo "  API: OK" || { echo "  API: FAIL"; exit 1; }
curl -sf http://localhost:8081 >/dev/null && echo "  Flink: OK" || { echo "  Flink: FAIL"; exit 1; }

echo "==> Submitting Flink job..."
make submit-job
sleep 10

echo "==> Running integration tests..."
pip install -q -r tests/requirements.txt
pytest tests/test_integration.py -v --timeout=180
pytest tests/test_e2e_v2.py -v --timeout=300 -m v2

echo ""
echo "==> Full pipeline verification PASSED"
