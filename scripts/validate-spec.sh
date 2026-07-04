#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
OPENAPI="$ROOT/spec/openapi/openapi.yaml"

echo "==> Validating token-event-v1.json"
python3 -m json.tool spec/schema/token-event-v1.json > /dev/null

echo "==> Validating contrib pricing JSON"
python3 -m json.tool contrib/pricing/openai-2025-06.json > /dev/null

echo "==> Checking OpenAPI YAML exists"
test -f "$OPENAPI"

echo "==> Checking semantic conventions"
test -f spec/schema/semantic-conventions.md

echo "==> Checking OpenAPI 2.2.x completeness"
grep -q 'mode:' "$OPENAPI"
grep -q 'cost_usd' "$OPENAPI"
grep -q 'link-stripe' "$OPENAPI"
grep -q 'IngestBatchResponseLite' "$OPENAPI"

echo "All spec validations passed."
