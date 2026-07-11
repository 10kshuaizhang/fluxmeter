# Intelligence API

**Base URL:** `http://localhost:8000` (development) or your production endpoint.

Prescriptive monetization endpoints — root cause, unit economics, what-if simulation, and revenue overlay ingest. Reads native FluxMeter usage/cost rollups from Redis; optional revenue from OpenMeter overlay or manual admin POST.

**Auth:** Most endpoints require `X-API-Key` (`FLUXMETER_API_KEY`). Admin endpoints require `FLUXMETER_ADMIN_KEY`. See [api-reference.md](api-reference.md#authentication).

**Period format:** `YYYY-MM` (UTC calendar month), matching billing query periods.

---

## `GET /intelligence/root-cause`

Decompose spend change between two months by model, customer, and attribution dimensions.

**Query parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `period` | yes | Current month (`YYYY-MM`) |
| `baseline_period` | yes | Comparison month (`YYYY-MM`) |
| `scope` | no | `global` (default) or `customer:{customer_id}` |

```bash
curl -s "http://localhost:8000/intelligence/root-cause?period=2026-07&baseline_period=2026-06&scope=global" \
  -H "X-API-Key: $FLUXMETER_API_KEY" | jq .
```

**Example response:**

```json
{
  "period": "2026-07",
  "baseline_period": "2026-06",
  "total_cost_usd": 140.0,
  "baseline_cost_usd": 100.0,
  "delta_usd": 40.0,
  "delta_pct": 40.0,
  "summary": "Cost +40.0% vs 2026-06. Top driver: model gpt-4o (72% of change).",
  "contributors": [
    {
      "dimension": "model",
      "key": "gpt-4o",
      "current_cost_usd": 100.0,
      "baseline_cost_usd": 60.0,
      "delta_usd": 40.0,
      "delta_pct": 66.7,
      "share_of_total_delta_pct": 100.0
    }
  ]
}
```

---

## `GET /intelligence/unit-economics`

Per-customer revenue vs cost, margin, and rule-based recommendations.

Requires revenue data per customer (OpenMeter overlay or `POST /intelligence/revenue/{customer_id}`). Customers without revenue return `status: unknown_revenue`.

**Query parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `period` | yes | Month to analyze (`YYYY-MM`) |

```bash
curl -s "http://localhost:8000/intelligence/unit-economics?period=2026-07" \
  -H "X-API-Key: $FLUXMETER_API_KEY" | jq .
```

**Example response:**

```json
[
  {
    "customer_id": "cust_a",
    "period": "2026-07",
    "revenue_usd": 500.0,
    "cost_usd": 620.0,
    "margin_usd": -120.0,
    "margin_pct": -24.0,
    "status": "loss",
    "recommendation": "Customer losing money — suggest plan upgrade or usage cap"
  },
  {
    "customer_id": "cust_b",
    "period": "2026-07",
    "revenue_usd": null,
    "cost_usd": 45.0,
    "margin_usd": null,
    "margin_pct": null,
    "status": "unknown_revenue",
    "recommendation": "Connect revenue (OpenMeter overlay or POST /intelligence/revenue)"
  }
]
```

---

## `POST /intelligence/simulate`

What-if scenario simulation. Pure pricing math — no Redis writes.

**Scenario types:**

| `scenario` | Required fields |
|------------|-----------------|
| `model_switch` | `input_tokens`, `output_tokens`, `from_model`, `to_model`; optional `monthly_occurrences` (default 1) |
| `prompt_reduction` | `cost_usd`, `input_reduction_pct` |
| `token_grant` | `cost_usd`, `grant_tokens`, `signup_lift_pct`, `avg_revenue_per_customer_usd`, `customer_count` |

### Model switch (e.g. GPT-4o → Claude)

```bash
curl -s -X POST "http://localhost:8000/intelligence/simulate" \
  -H "X-API-Key: $FLUXMETER_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "scenario": "model_switch",
    "input_tokens": 1000000,
    "output_tokens": 500000,
    "from_model": "gpt-4o",
    "to_model": "claude-sonnet-4",
    "monthly_occurrences": 1
  }' | jq .
```

### Prompt reduction

```bash
curl -s -X POST "http://localhost:8000/intelligence/simulate" \
  -H "X-API-Key: $FLUXMETER_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "scenario": "prompt_reduction",
    "cost_usd": 800.0,
    "input_reduction_pct": 20
  }' | jq .
```

### Token promo (signup grant)

```bash
curl -s -X POST "http://localhost:8000/intelligence/simulate" \
  -H "X-API-Key: $FLUXMETER_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "scenario": "token_grant",
    "cost_usd": 50.0,
    "grant_tokens": 100000,
    "signup_lift_pct": 5,
    "avg_revenue_per_customer_usd": 29.0,
    "customer_count": 200
  }' | jq .
```

**Example response (model_switch):**

```json
{
  "scenario": "model_switch",
  "annual_savings_usd": 1234.56,
  "annual_profit_delta_usd": null,
  "notes": "Switch gpt-4o → claude-sonnet-4: $102.88/mo → $0.00/mo (+$102.88/mo)"
}
```

Returns `400` if required fields for the chosen scenario are missing.

---

## `POST /intelligence/revenue/{customer_id}` (admin)

Manually set revenue for a customer/period when no billing overlay is connected.

**Auth:** Admin key (`FLUXMETER_ADMIN_KEY`)

**Request body:**

```json
{
  "period": "2026-07",
  "revenue_usd": 500.0
}
```

```bash
curl -s -X POST "http://localhost:8000/intelligence/revenue/cust_a" \
  -H "X-API-Key: $FLUXMETER_ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{"period": "2026-07", "revenue_usd": 500.0}' | jq .
```

**Response:** `200 OK`

```json
{"status": "ok"}
```

---

## `POST /intelligence/import/openmeter` (admin)

Import revenue events from an OpenMeter export payload. Non-revenue events are counted in `ignored_rows`.

**Auth:** Admin key

**Query parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `period` | no* | Month (`YYYY-MM`); required if omitted from body |

\* `period` must appear in the JSON body or as a query parameter.

**Request body:**

```json
{
  "period": "2026-07",
  "events": [
    {
      "subject": "revenue",
      "customerId": "cust_a",
      "value": 500.0
    }
  ]
}
```

```bash
curl -s -X POST "http://localhost:8000/intelligence/import/openmeter?period=2026-07" \
  -H "X-API-Key: $FLUXMETER_ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "events": [
      {"subject": "revenue", "customerId": "cust_a", "value": 500.0},
      {"subject": "usage", "customerId": "cust_a", "value": 1200}
    ]
  }' | jq .
```

**Response:** `200 OK`

```json
{
  "revenue_rows": 1,
  "ignored_rows": 1
}
```

---

## `GET /intelligence/summary`

Prescriptive one-pager for Finance/CEO: root-cause headline plus list of unprofitable customers.

**Query parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `period` | yes | Current month (`YYYY-MM`) |
| `baseline_period` | yes | Comparison month (`YYYY-MM`) |

```bash
curl -s "http://localhost:8000/intelligence/summary?period=2026-07&baseline_period=2026-06" \
  -H "X-API-Key: $FLUXMETER_API_KEY" | jq .
```

**Example response:**

```json
{
  "headline": "Cost +40.0% vs 2026-06. Top driver: model gpt-4o (72% of change). 1 customer(s) unprofitable in 2026-07.",
  "root_cause_summary": "Cost +40.0% vs 2026-06. Top driver: model gpt-4o (72% of change).",
  "loss_customers": [
    {
      "customer_id": "cust_a",
      "period": "2026-07",
      "revenue_usd": 500.0,
      "cost_usd": 620.0,
      "margin_usd": -120.0,
      "margin_pct": -24.0,
      "status": "loss",
      "recommendation": "Customer losing money — suggest plan upgrade or usage cap"
    }
  ]
}
```

---

## Phase 6 — Intelligence v1.0 (3.1.0)

### `GET /intelligence/pricing-recommendations`

Rule-based pricing actions with annual ROI (price increase, model switch).

```bash
curl -s "http://localhost:8000/intelligence/pricing-recommendations?period=2026-07" \
  -H "X-API-Key: $FLUXMETER_API_KEY" | jq .
```

### `GET /intelligence/profitability`

Cross-customer margin overview, product breakdown, multi-month trend.

```bash
curl -s "http://localhost:8000/intelligence/profitability?period=2026-07&months=3" \
  -H "X-API-Key: $FLUXMETER_API_KEY" | jq .
```

### `GET /intelligence/forecast`

Linear spend forecast vs budget (`on_track` | `at_risk` | `over_budget` | `no_budget`).

```bash
curl -s "http://localhost:8000/intelligence/forecast?period=2026-07&scope=global" \
  -H "X-API-Key: $FLUXMETER_API_KEY" | jq .
```

### `GET /intelligence/report`

Finance/CEO export — `format=json` (default) or `format=markdown`.

```bash
curl -s "http://localhost:8000/intelligence/report?period=2026-07&baseline_period=2026-06&format=markdown" \
  -H "X-API-Key: $FLUXMETER_API_KEY"
```

### `POST /intelligence/alerts/webhook` (admin)

Configure webhook for anomaly alerts (`INTEL_COST_SPIKE`, `INTEL_MARGIN_LOSS`, `INTEL_FORECAST_RISK`).

```bash
curl -s -X POST "http://localhost:8000/intelligence/alerts/webhook" \
  -H "X-API-Key: $FLUXMETER_ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{"url":"https://hooks.example.com/intel","secret":"whsec_..."}'
```

Or set env: `FLUXMETER_INTEL_WEBHOOK_URL`, `FLUXMETER_INTEL_WEBHOOK_SECRET`. Background worker polls every 300s.

---

## Data sources

| Source | Provides |
|--------|----------|
| FluxMeter native (Lite/Full ingest) | Usage, cost rollups, model/dim attribution |
| OpenMeter overlay | Revenue per customer via `/import/openmeter` |
| Manual admin POST | Revenue per customer via `/revenue/{customer_id}` |

See also: [api-reference.md](api-reference.md) · [OpenAPI spec](../spec/openapi/openapi.yaml)
