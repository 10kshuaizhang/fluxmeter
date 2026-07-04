# API Reference

**Overview & integration guides:** [fluxmeter.dev](https://fluxmeter.dev) · **OpenAPI:** [`spec/openapi/openapi.yaml`](../spec/openapi/openapi.yaml)

Base URL: `http://localhost:8000` (development) or your production endpoint.

Interactive docs: `GET /docs` (Swagger UI)

---

## Health

### `GET /health`

Check API and Redis connectivity.

**Response:** `200 OK`
```json
{"status": "ok", "mode": "full"}
```

`mode` is `lite` when `FLUXMETER_LITE_MODE=true` (API aggregates directly to Redis, no Flink).

---

## Authentication

Most endpoints require the `X-API-Key` header.

| Key | Env var | Access |
|-----|---------|--------|
| Global read | `FLUXMETER_API_KEY` | Usage, ingest (demo), check, pricing |
| Admin | `FLUXMETER_ADMIN_KEY` | Budget set/topup, rerate, reserve/reconcile, webhooks, pricing PUT |
| Customer-scoped | Created via `POST /admin/customers/{id}/api-keys` | Ingest/check/usage for **that customer only** |

Demo mode (`FLUXMETER_AUTH_OPTIONAL=true`, default in lite `docker-compose.yml` and full `docker-compose.full.yml`) allows unauthenticated access when keys are not configured. Production overlay (`docker-compose.prod.yml`, used with full stack) sets `FLUXMETER_AUTH_OPTIONAL=false`.

**Errors:** `401` invalid/missing key · `403` customer key does not match `customerId` in request

---

## Ingest

### `POST /ingest`

Ingest a single token usage event. Alternative to the Python SDK or direct Kafka producer.

**Auth:** API key or customer-scoped key (must match `customerId`)

**Request body:**
```json
{
  "customerId": "cust_123",
  "modelId": "gpt-4o",
  "provider": "openai",
  "inputTokens": 1250,
  "outputTokens": 847,
  "cacheReadTokens": 200,
  "cacheWriteTokens": 0,
  "reasoningTokens": 0,
  "embeddingTokens": 0,
  "eventId": "optional-uuid",
  "requestId": "chatcmpl-abc123",
  "spanId": "span_7f3a",
  "parentSpanId": "span_parent_42",
  "sessionId": "sess_123",
  "latencyMs": 1340,
  "environment": "production",
  "timestamp": 1718534400000
}
```

**Required fields:** `customerId`, `modelId`

**Auto-generated if omitted:** `eventId` (UUID), `timestamp` (current time)

**Response:** `202 Accepted`
```json
{"status": "accepted", "eventId": "2b14b730-4d7a-4985-a92f-c63a6f96d26f"}
```

---

### `POST /ingest/batch`

Ingest up to 1000 events in a single HTTP call.

**Request body:** Array of event objects (same schema as `/ingest`)
```json
[
  {"customerId": "cust_1", "modelId": "gpt-4o", "inputTokens": 500, "outputTokens": 150},
  {"customerId": "cust_2", "modelId": "claude-sonnet-4", "inputTokens": 2000, "outputTokens": 800}
]
```

**Response:** `202 Accepted`
```json
{
  "status": "accepted",
  "count": 2,
  "event_ids": ["uuid-1", "uuid-2"]
}
```

**Error:** `400` if batch exceeds 1000 events.

---

## Usage Queries

### `GET /usage/global`

Global aggregated usage across all customers.

**Response:** `200 OK`
```json
{
  "total_events": 116477687,
  "total_tokens": 183397816737,
  "input_tokens": 98234000000,
  "output_tokens": 72163816737,
  "total_cost_usd": 649392.81,
  "last_window_end": 1718534400000
}
```

---

### `GET /usage/customer/{customer_id}`

Per-customer usage breakdown.

**Response:** `200 OK`
```json
{
  "customer_id": "cust_42",
  "total_tokens": 6603839,
  "input_tokens": 4201000,
  "output_tokens": 2102839,
  "cache_read_tokens": 21180,
  "reasoning_tokens": 543556,
  "event_count": 7972,
  "cost_usd": 26.71
}
```

**Error:** `404` if customer has no usage data.

---

### `GET /usage/customer/{customer_id}/model/{model_id}`

Per-model usage for a specific customer.

**Response:** `200 OK`
```json
{
  "model_id": "gpt-4o",
  "total_tokens": 2002848,
  "input_tokens": 1200000,
  "output_tokens": 802848,
  "cost_usd": 9.99
}
```

**Error:** `404` if no usage for this customer/model combination.

---

### `GET /usage/span/{span_id}`

Aggregated cost and usage for an agent span (group of related LLM calls).

**Response:** `200 OK`
```json
{
  "span_id": "span_agent_42",
  "customer_id": "cust_1",
  "total_tokens": 18400,
  "call_count": 5,
  "cost_usd": 0.23,
  "duration_ms": 4200
}
```

**Error:** `404` if span not found (spans expire after 24 hours).

---

### `GET /usage/customer/{customer_id}/spans?limit=10`

Top N most expensive agent spans for a customer, sorted by cost descending.

**Query params:**
- `limit` (int, default 10): Number of spans to return

**Response:** `200 OK`
```json
[
  {"span_id": "span_agent_42", "cost_usd": 0.23},
  {"span_id": "span_agent_17", "cost_usd": 0.18}
]
```

Returns empty array if no spans found.

---

## Budget Management

### `GET /budget/{customer_id}`

Get current budget status.

**Response:** `200 OK`
```json
{
  "customer_id": "cust_42",
  "balance_usd": 23.41,
  "held_usd": 0.50,
  "effective_balance_usd": 22.91,
  "debt_usd": 0.0,
  "total_spent_usd": 26.59,
  "alert_threshold_usd": 5.0,
  "is_exhausted": false
}
```

| Field | Description |
|-------|-------------|
| `balance_usd` | Cash balance (only Flink Sink + topup/set mutate this) |
| `held_usd` | Sum of active streaming reserves |
| `effective_balance_usd` | `balance_usd - held_usd` (used by `/check`) |
| `debt_usd` | Overdraft recorded when window cost exceeds balance (balance floors at 0) |

**Error:** `404` if no budget configured for this customer.

---

### `POST /budget/{customer_id}`

Set or reset a customer's prepaid budget.

**Request body:**
```json
{
  "balance_usd": 50.0,
  "alert_threshold_usd": 5.0,
  "max_rpm": 100
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `balance_usd` | float | Yes | Prepaid balance |
| `alert_threshold_usd` | float | No | Alert when balance drops below this |
| `max_rpm` | int | No | Max requests per minute (rate limit) |

**Response:** `200 OK` — returns the budget status (same as GET).

---

### `POST /budget/{customer_id}/topup`

Add credits to a customer's balance.

**Query params:**
- `amount_usd` (float, required, must be > 0)

**Response:** `200 OK`
```json
{
  "customer_id": "cust_42",
  "new_balance_usd": 73.41,
  "added_usd": 50.0
}
```

**Error:** `400` if amount_usd <= 0. `404` if no budget configured.

---

## Guardrails

### `GET /budget/{customer_id}/check`

Pre-request guardrail gate. Call BEFORE every LLM request. Returns in <10ms.

Uses **effective balance** = `balance_usd - held_usd`. Active streaming reserves reduce what new calls can spend.

**Query params:**
- `estimated_cost_usd` (float, optional, default 0): Estimated cost of upcoming call

**Response:** `200 OK`
```json
{
  "allowed": true,
  "balance_usd": 23.41,
  "held_usd": 0.50,
  "effective_balance_usd": 22.91,
  "reason": "ok",
  "requests_this_minute": 5,
  "source": "redis"
}
```

**Possible `reason` values:**

| reason | meaning | action |
|--------|---------|--------|
| `ok` | All checks passed | Proceed with LLM call |
| `budget_exhausted` | Effective balance <= 0 | Reject request, return 402 to user |
| `insufficient_balance` | Effective balance < estimated_cost | Reject request |
| `rate_limited` | Exceeded max_rpm | Reject request, retry after 60s |
| `no_budget_configured` | No budget set (enforcement disabled) | Proceed |

`source` indicates which layer answered: `redis`, `cache`, or `policy` (fail-open/closed when Redis unavailable).

**Additional fields when rate limited:**
```json
{
  "allowed": false,
  "balance_usd": null,
  "reason": "rate_limited",
  "requests_this_minute": 100,
  "max_rpm": 100,
  "source": "redis"
}
```

---

### Streaming billing flow (Module 6)

Single-path deduction: **only the Flink Sink changes `balance_usd`**. Reserve/reconcile manage `held_usd` only.

```
Your app → GET /budget/cust_123/check (< 10ms)
           → allowed: true → proceed with LLM call
           → allowed: false → reject, return 402

Streaming (optional):
  POST /budget/cust_123/reserve  → holds estimate (balance unchanged)
  → LLM stream → POST /ingest
  → POST /budget/cust_123/reconcile → releases hold
  (Flink Sink deducts actual cost in 10s windows — single path, no double-charge)

After each LLM call:
Your app → POST /ingest {customerId, modelId, inputTokens, outputTokens}
           → Kafka → Flink (10s window) → Redis (atomic deduction) → Kafka alert / webhook
```

**Do not** expect `balance_usd` to drop on `reserve` or rise on `reconcile`. Actual cost is applied when the aggregation window closes in `BudgetEnforcerSink`.

---

### `POST /budget/{customer_id}/reserve`

Reserve budget **hold** for streaming responses. Increases `held_usd` only — does **not** deduct `balance_usd`.

**Auth:** Admin key

**Query params:**
- `estimated_cost_usd` (float, required, must be > 0)

**Response:** `200 OK`
```json
{
  "allowed": true,
  "balance_usd": 50.0,
  "held_usd": 0.50,
  "effective_balance_usd": 49.50,
  "reserved_usd": 0.50,
  "reason": "reserved"
}
```

If effective balance < estimate:
```json
{
  "allowed": false,
  "balance_usd": 50.0,
  "held_usd": 0.0,
  "effective_balance_usd": 0.30,
  "reason": "insufficient_balance"
}
```

---

### `POST /budget/{customer_id}/reconcile`

Release hold after streaming completes. Does **not** credit or debit `balance_usd` (Flink Sink already deducted actual usage).

**Auth:** Admin key

**Query params:**
- `reserved_usd` (float): Amount originally reserved (released from `held_usd`)
- `actual_usd` (float, optional): Actual cost for your logs; not used to adjust balance

**Response:** `200 OK`
```json
{
  "balance_usd": 49.92,
  "held_usd": 0.0,
  "released_usd": 0.50,
  "reserved_usd": 0.50,
  "actual_usd": 0.08
}
```

---

### `POST /budget/{customer_id}/webhook`

Configure HTTPS webhook for `BUDGET_LOW` and `BUDGET_EXHAUSTED` alerts. Delivered by the `webhook-worker` service (consumes `budget-alerts` Kafka topic).

**Auth:** Admin key

**Request body:**
```json
{
  "webhook_url": "https://your-app.com/hooks/fluxmeter",
  "webhook_secret": "optional-hmac-secret"
}
```

**Response:** `200 OK`
```json
{
  "customer_id": "cust_42",
  "webhook_url": "https://your-app.com/hooks/fluxmeter"
}
```

---

### `GET /budget/{customer_id}/webhook`

Return configured webhook URL for a customer.

**Auth:** Admin key

**Error:** `404` if webhook not configured.

---

## Pricing

Pricing is loaded from `config/pricing.json` (or classpath `pricing.json`). Flink uses `PRICING_FILE` env; API can hot-update Redis snapshot.

### `GET /pricing`

Return current pricing catalog (Redis snapshot if set, else file).

**Auth:** API key

**Response:** `200 OK` — JSON matching `config/pricing.json` schema (`models`, `defaults`, `prefix_models`, optional `tiers`).

---

### `PUT /admin/pricing`

Upload pricing JSON to Redis (`pricing:current`). Flink restart or file sync may be needed for engine to pick up changes immediately.

**Auth:** Admin key

**Request body:** Full pricing catalog JSON

**Response:** `200 OK`
```json
{"status": "updated", "version": "1"}
```

---

### `POST /admin/pricing/validate`

Validate pricing JSON structure without applying.

**Auth:** Admin key

**Response:** `200 OK`
```json
{"status": "valid", "models": 11}
```

---

## Admin

### `POST /admin/customers/{customer_id}/api-keys`

Create a customer-scoped API key for ingest and check.

**Auth:** Admin key

**Response:** `200 OK`
```json
{
  "key_id": "uuid",
  "api_key": "fm_live_...",
  "customer_id": "cust_42"
}
```

Store `api_key` securely — it is not shown again. Use header: `X-API-Key: fm_live_...`

---

### `DELETE /admin/api-keys/{key_id}`

Revoke a customer API key.

**Auth:** Admin key

**Response:** `200 OK`
```json
{"key_id": "uuid", "revoked": true}
```

---

### `GET /admin/reconciliation`

Last balance reconciliation snapshot from `jobs/reconcile_balances.py`.

**Auth:** Admin key

**Response:** `200 OK`
```json
{
  "timestamp": 1718534400000,
  "customers_scanned": 120,
  "drift_count": 0,
  "drifts": []
}
```

Returns `{"status": "no_data"}` if the reconciliation job has not run yet.

Formula: `balance_usd` should equal `initial_balance + total_topup - total_deducted` (see Redis keys `budget:{id}:total_deducted_usd`).

---

## Re-Rating

### `POST /rerate/preview`

Preview the cost adjustment for a pricing change without applying it.

**Request body:**
```json
{
  "model_id": "gpt-4o",
  "old_input_price": 2.50,
  "new_input_price": 2.50,
  "old_output_price": 10.00,
  "new_output_price": 5.00
}
```

Prices are per million tokens.

**Response:** `200 OK`
```json
{
  "model_id": "gpt-4o",
  "customers_affected": 847,
  "total_adjustment_usd": -4231.50,
  "adjustments": [
    {"customer_id": "cust_1", "input_tokens": 5000000, "output_tokens": 2000000, "adjustment_usd": -10.0},
    {"customer_id": "cust_2", "input_tokens": 3000000, "output_tokens": 1500000, "adjustment_usd": -7.5}
  ]
}
```

`adjustments` shows up to 50 customers (sorted by adjustment amount). Negative = credit (price decreased).

---

### `POST /rerate/apply`

Apply the pricing adjustment. Atomically updates all affected customer costs and budget balances.

**Request body:** Same as `/rerate/preview`

**Response:** `200 OK`
```json
{
  "model_id": "gpt-4o",
  "customers_adjusted": 847,
  "total_adjustment_usd": -4231.50,
  "status": "applied"
}
```

**Side effects:**
- Each customer's `cost_usd` adjusted
- Per-model `cost_usd` adjusted
- `global:total_cost_usd` adjusted
- If customer has budget: balance credited back on price decrease

---

## Budget Alerts (Kafka + Webhook)

FluxMeter emits alerts to the `budget-alerts` Kafka topic when a window closes and budget crosses thresholds. If `POST /budget/{id}/webhook` is configured, the `webhook-worker` also POSTs to your HTTPS URL (optional HMAC via `X-FluxMeter-Signature`).

### Alert schema

```json
{
  "type": "BUDGET_EXHAUSTED",
  "customerId": "cust_42",
  "remainingBalanceUsd": 0.0,
  "windowCostUsd": 0.96,
  "modelId": "o3-mini",
  "windowStart": 1718534460000,
  "windowEnd": 1718534470000,
  "timestamp": 1718534472000
}
```

When spend exceeds balance, `remainingBalanceUsd` is `0` and excess is recorded in `budget:{customerId}:debt_usd`.

### Alert types

| Type | Meaning | Action |
|------|---------|--------|
| `BUDGET_LOW` | Balance crossed alert threshold | Warn customer, prepare to deny |
| `BUDGET_EXHAUSTED` | Balance <= 0 | Block all new requests for this customer |

### Consumer example

```python
from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": "kafka:9092",
    "group.id": "my-app-budget-handler",
    "auto.offset.reset": "latest",
})
consumer.subscribe(["budget-alerts"])

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    alert = json.loads(msg.value())
    if alert["type"] == "BUDGET_EXHAUSTED":
        block_customer(alert["customerId"])
    elif alert["type"] == "BUDGET_LOW":
        warn_customer(alert["customerId"], alert["remainingBalanceUsd"])
```

---

## Error Responses

All error responses follow this format:

```json
{"detail": "Error description"}
```

| Status | Meaning |
|--------|---------|
| 400 | Invalid input (negative amount, batch too large) |
| 401 | Invalid or missing API key |
| 403 | Customer API key not authorized for this `customerId` |
| 404 | Resource not found (customer, budget, span) |
| 500 | Internal error (Redis down, Kafka unreachable) |

---

## Rate Limits

The API itself has no built-in rate limiting. The `/budget/{id}/check` endpoint implements per-customer rate limiting (configurable via `max_rpm`). The API server itself should be protected by your infrastructure (API gateway, load balancer rate limits).

---

## SDK Reference

### Python SDK

```bash
pip install fluxmeter
```

```python
from fluxmeter import FluxMeter

meter = FluxMeter(
    kafka_brokers="localhost:9094",
    topic="token-events",
    environment="production",
    wal_enabled=True,
    wal_path="~/.fluxmeter/wal",
)
```

| Method | Description |
|--------|-------------|
| `meter.track(customer_id, model_id, **kwargs)` | Track any LLM call |
| `meter.track_openai(customer_id, response)` | Auto-extract from OpenAI response |
| `meter.track_anthropic(customer_id, response)` | Auto-extract from Anthropic response |
| `meter.wrap_stream(stream, customer_id, model_id)` | Streaming response wrapper |
| `meter.flush()` | Flush pending events (auto on exit) |

### `track()` parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `customer_id` | str | Yes | Customer identifier |
| `model_id` | str | Yes | Model name (e.g. "gpt-4o") |
| `provider` | str | No | "openai", "anthropic", "google" (default: "openai") |
| `input_tokens` | int | No | Prompt/input tokens |
| `output_tokens` | int | No | Completion/output tokens |
| `cache_read_tokens` | int | No | Cached prompt tokens |
| `cache_write_tokens` | int | No | Cache write tokens |
| `reasoning_tokens` | int | No | Reasoning tokens (o1/o3) |
| `embedding_tokens` | int | No | Embedding tokens |
| `request_id` | str | No | Provider request ID |
| `span_id` | str | No | Observability span ID |
| `parent_span_id` | str | No | Parent span (agent attribution) |
| `session_id` | str | No | Conversation session ID |
| `latency_ms` | int | No | Provider response time |
| `environment` | str | No | "production", "staging" |
| `metadata` | dict | No | Arbitrary key-value pairs |
