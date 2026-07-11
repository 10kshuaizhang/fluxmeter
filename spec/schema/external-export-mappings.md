# External export mappings — FluxMeter → invoice SoR

Stable field mapping from FluxMeter usage deltas to partner ingest APIs (v2.8).

**Canonical event schema:** [`token-event-v1.json`](token-event-v1.json)

## Aggregated export cycle

Built-in export (`api/billing_export.py`) sends **deltas since last report**, not raw per-event streams.

| FluxMeter Redis source | Exported field |
|------------------------|----------------|
| `customer:{id}:event_count` − `billing:{id}:last_reported_events` | `new_events` |
| `customer:{id}:input_tokens` − `last_reported_input_tokens` | `new_input_tokens` |
| `customer:{id}:output_tokens` − `last_reported_output_tokens` | `new_output_tokens` |
| `customer:{id}:cost_usd` − `last_reported_cost_usd` | `new_cost_usd` |

Idempotency key: `fluxmeter-{customer_id}-{period}-{mode}` where `period` is `YYYYMMDDHH` (hourly) or `YYYY-MM` (monthly).

---

## Stripe Billing Meters

| FluxMeter | Stripe MeterEvent |
|-----------|-------------------|
| `billing:{id}:stripe_customer_id` | `payload.stripe_customer_id` |
| `new_events` (events mode) | `payload.value` |
| `new_cost_usd × 100` (cost mode) | `payload.value` (USD cents) |
| `STRIPE_METER_NAME` / `STRIPE_COST_METER_NAME` | `event_name` |
| export timestamp | `timestamp` (unix seconds) |

```json
{
  "event_name": "token_events_processed",
  "payload": {
    "stripe_customer_id": "cus_abc",
    "value": "150"
  },
  "timestamp": 1700000000
}
```

---

## Metronome ingest

Endpoint: `POST https://api.metronome.com/v1/ingest`

| FluxMeter | Metronome field |
|-----------|-----------------|
| `billing:{id}:metronome_customer_id` | `customer_id` |
| `METRONOME_BILLABLE_METRIC` | `event_type` |
| idempotency key | `transaction_id` |
| ISO8601 export time | `timestamp` |

**Events mode** (`STRIPE_EXPORT_MODE=events`):

```json
{
  "customer_id": "mtr_uuid",
  "event_type": "token_usage",
  "timestamp": "2026-07-01T12:00:00+00:00",
  "transaction_id": "fluxmeter-cust_1-2026070112-events",
  "properties": {
    "input_tokens": 5000,
    "output_tokens": 1200,
    "event_count": 42
  }
}
```

**Cost mode:**

```json
{
  "properties": {
    "total_cost_usd": 12.34
  }
}
```

---

## Orb ingest

Endpoint: `POST https://api.withorb.com/v1/ingest`

| FluxMeter | Orb field |
|-----------|-----------|
| `billing:{id}:orb_customer_id` | `external_customer_id` |
| `ORB_EVENT_NAME` | `event_name` |
| idempotency key + suffix | `idempotency_key` |
| ISO8601 export time | `timestamp` |

**Events mode** — one event per token type with non-zero delta:

```json
{
  "events": [
    {
      "idempotency_key": "fluxmeter-cust_1-2026070112-events-input",
      "external_customer_id": "orb_ext_id",
      "event_name": "token_usage",
      "timestamp": "2026-07-01T12:00:00+00:00",
      "properties": { "token_type": "input", "tokens": 5000 }
    },
    {
      "idempotency_key": "fluxmeter-cust_1-2026070112-events-output",
      "external_customer_id": "orb_ext_id",
      "event_name": "token_usage",
      "timestamp": "2026-07-01T12:00:00+00:00",
      "properties": { "token_type": "output", "tokens": 1200 }
    }
  ]
}
```

**Cost mode:**

```json
{
  "properties": { "cost_usd_cents": 1234 }
}
```

---

## Raw token-event → partner (manual / streaming)

For per-event streaming (not built into v2.8 export loop):

| token-event-v1 field | Metronome property | Orb property |
|----------------------|-------------------|--------------|
| `customerId` | map via link table → `customer_id` | → `external_customer_id` |
| `inputTokens` | `input_tokens` | `properties.tokens` (token_type=input) |
| `outputTokens` | `output_tokens` | `properties.tokens` (token_type=output) |
| `eventId` | `transaction_id` | `idempotency_key` |
| `timestamp` | `timestamp` (ISO8601) | `timestamp` |
| `modelId` | optional `properties.model_id` | optional `properties.model_id` |
| `parentSpanId` | optional `properties.parent_span_id` | optional |
| `sessionId` | optional `properties.session_id` | optional |
| `metadata.*` | passthrough string properties | passthrough |

Cost at event level: compute with FluxMeter `pricing.json` before export, or let FluxMeter aggregate and use **cost mode** delta export.

---

## Versioning

Mapping version follows API release **2.8.0**. Breaking changes to payload shapes require a spec bump and changelog entry.
