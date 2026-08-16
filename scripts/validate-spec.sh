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

echo "==> Checking OpenAPI 4.4 single-path contract"
grep -q 'version: 4.4' "$OPENAPI"
grep -q '/ready:' "$OPENAPI"
grep -q 'cost_usd' "$OPENAPI"
grep -q 'link-stripe' "$OPENAPI"
grep -q '/admin/billing/{customer_id}/link:' "$OPENAPI"
grep -q '/pricing:' "$OPENAPI"
grep -q '/budget/{customer_id}/package:' "$OPENAPI"
grep -q '/usage/dim/' "$OPENAPI"
grep -q 'held_usd' "$OPENAPI"
grep -q 'IngestBatchResponse' "$OPENAPI"
! grep -q 'IngestBatchResponseLite' "$OPENAPI"
grep -q 'enum: \[accepted, partial, conflict, failed\]' "$OPENAPI"
grep -q '/usage/customer/{customer_id}/period/' "$OPENAPI"
grep -q '/usage/session/{session_id}' "$OPENAPI"
grep -q 'BucketUsage' "$OPENAPI"

echo "All spec validations passed."
