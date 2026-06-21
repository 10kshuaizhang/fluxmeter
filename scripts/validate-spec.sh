#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "==> Validating token-event-v1.json"
python3 -m json.tool spec/schema/token-event-v1.json > /dev/null

echo "==> Validating contrib pricing JSON"
python3 -m json.tool contrib/pricing/openai-2025-06.json > /dev/null

echo "==> Checking OpenAPI YAML exists"
test -f spec/openapi/openapi.yaml

echo "==> Checking semantic conventions"
test -f spec/schema/semantic-conventions.md

echo "All spec validations passed."
