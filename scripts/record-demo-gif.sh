#!/usr/bin/env bash
# Record README demo.gif via VHS (requires: docker, vhs, python3)
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

command -v vhs >/dev/null || { echo "Install vhs: https://github.com/charmbracelet/vhs"; exit 1; }

echo "==> Starting lite stack..."
docker compose up -d --build

echo "==> Waiting for API..."
for i in $(seq 1 30); do
  if curl -sf http://127.0.0.1:8000/health >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
curl -sf http://127.0.0.1:8000/health >/dev/null || { echo "API not ready"; exit 1; }

echo "==> Reset demo customer keys..."
docker exec fluxmeter-redis-lite redis-cli FLUSHDB >/dev/null

echo "==> Recording demo.tape → demo.gif ..."
vhs demo.tape

echo "==> Done: $(ls -lh demo.gif)"
