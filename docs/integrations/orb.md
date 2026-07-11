# Orb integration

[Orb](https://withorb.com) handles complex pricing (tiered, volume, package) and invoicing. FluxMeter handles **real-time enforcement** Orb cannot do at sub-second latency.

## Architecture

```
App → FluxMeter (/check, /reserve, /ingest) → Redis counters
              ↓ hourly export
         Orb POST /v1/ingest (idempotent) → Invoice
```

## Prerequisites

- FluxMeter API
- Orb API key
- Orb customer IDs mapped to FluxMeter customers

## 1. Configure export

```bash
export BILLING_EXPORT_TARGETS=orb
export ORB_API_KEY=...
export ORB_EVENT_NAME=token_usage
export STRIPE_EXPORT_MODE=events    # or cost
export BILLING_EXPORT_PERIOD=hourly
```

## 2. Link customers

```bash
curl -X POST "http://localhost:8000/admin/billing/cust_123/link" \
  -H "X-API-Key: $FLUXMETER_ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{"platform": "orb", "external_customer_id": "orb_external_cust_id"}'
```

## 3. Event shapes

### Events mode (`STRIPE_EXPORT_MODE=events`)

FluxMeter sends **two Orb events per cycle** when both deltas are non-zero:

```json
{
  "events": [
    {
      "idempotency_key": "fluxmeter-cust_123-2026070112-events-input",
      "external_customer_id": "orb_external_cust_id",
      "event_name": "token_usage",
      "timestamp": "2026-07-01T12:00:00+00:00",
      "properties": {"token_type": "input", "tokens": 5000}
    },
    {
      "idempotency_key": "fluxmeter-cust_123-2026070112-events-output",
      "external_customer_id": "orb_external_cust_id",
      "event_name": "token_usage",
      "timestamp": "2026-07-01T12:00:00+00:00",
      "properties": {"token_type": "output", "tokens": 1200}
    }
  ]
}
```

Configure Orb billable metrics to sum `properties.tokens` filtered by `token_type`.

### Cost mode (`STRIPE_EXPORT_MODE=cost`)

Single event with pre-computed FluxMeter cost:

```json
{
  "properties": {"cost_usd_cents": 150}
}
```

Use when FluxMeter tiers are authoritative and Orb should invoice the exported USD total.

## 4. Idempotency

Built-in export uses stable keys per hour (or month):

`fluxmeter-{customer_id}-{YYYYMMDDHH}-{mode}-input|output|cost`

Re-running the same cycle does not double-count in Orb.

## 5. Pre-computed cost strategy (recommended for tiered AI)

1. FluxMeter applies tiers at ingest (`pricing.json`)
2. Set `STRIPE_EXPORT_MODE=cost`
3. Orb invoices on `cost_usd_cents` — no duplicate tier engine needed
4. After tier catalog change: replay events locally, then wait for next export cycle

## Division of responsibility

| FluxMeter | Orb |
|-----------|-----|
| `/check` deny before LLM call | Contract & plan definitions |
| Streaming `/reserve` holds | Dunning, tax, portal |
| Agent span/session caps | Complex list/ramp pricing on exported dims |
| Normalized token events | Invoice PDF / payment |

See [integrations.md](../integrations.md) · [external-export-mappings.md](../../spec/schema/external-export-mappings.md)
