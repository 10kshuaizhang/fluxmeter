# Metronome integration

[Metronome](https://metronome.com) is an invoice SoR often paired with Stripe Billing. FluxMeter is the **runtime SoR** — enforce budgets on the hot path, export normalized usage deltas to Metronome for invoicing.

## Architecture

```
App → FluxMeter (check → reserve → ingest) → Redis
              ↓ hourly export (built-in)
         Metronome POST /v1/ingest → Stripe invoice (via Metronome)
```

## Prerequisites

- FluxMeter API with Redis
- Metronome account + API token
- Billable metric configured in Metronome (default event type: `token_usage`)

## 1. Configure Metronome metric

In Metronome, define a billable metric matching export mode:

| `STRIPE_EXPORT_MODE` | Properties sent |
|----------------------|-----------------|
| `events` (default) | `input_tokens`, `output_tokens`, `event_count` |
| `cost` | `total_cost_usd` (pre-computed by FluxMeter tiers) |

Set env:

```bash
export BILLING_EXPORT_TARGETS=metronome
export METRONOME_API_TOKEN=...
export METRONOME_BILLABLE_METRIC=token_usage
export STRIPE_EXPORT_MODE=events   # shared mode flag for all exporters
export BILLING_EXPORT_PERIOD=hourly
```

## 2. Link customers

Map FluxMeter `customer_id` → Metronome `customer_id`:

```bash
curl -X POST "http://localhost:8000/admin/billing/cust_123/link" \
  -H "X-API-Key: $FLUXMETER_ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{"platform": "metronome", "external_customer_id": "YOUR_METRONOME_CUSTOMER_UUID"}'
```

## 3. Built-in export vs manual script

**Built-in (recommended):** The API background loop calls Metronome every `BILLING_EXPORT_INTERVAL` seconds with idempotent `transaction_id` = `fluxmeter-{customer}-{period}-{mode}`.

**Manual fallback:** Poll FluxMeter and POST yourself:

```python
import httpx
from datetime import datetime, timezone

usage = httpx.get(f"{FLUXMETER_API}/usage/customer/{customer_id}").json()
httpx.post(
    "https://api.metronome.com/v1/ingest",
    headers={"Authorization": f"Bearer {METRONOME_TOKEN}"},
    json=[{
        "customer_id": metronome_customer_id,
        "event_type": "token_usage",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "transaction_id": f"fluxmeter-{customer_id}-manual",
        "properties": {
            "input_tokens": usage["input_tokens"],
            "output_tokens": usage["output_tokens"],
            "total_cost_usd": usage["cost_usd"],
        },
    }],
)
```

Use built-in export for production; manual scripts for one-off backfills.

## Tier pricing division

If FluxMeter applies volume/graduated tiers locally, sync **`cost` mode** (`STRIPE_EXPORT_MODE=cost`) so Metronome receives pre-computed USD. Metronome does not need to re-implement your tier breakpoints.

If Metronome owns tier logic, sync **raw token counts** (`events` mode) and disable tier repricing on the FluxMeter invoice path.

## Verify

1. Ingest test events via `POST /ingest`
2. Wait one export interval
3. Metronome → Usage → confirm ingest records with matching `transaction_id`

See [external-export-mappings.md](../../spec/schema/external-export-mappings.md) for field-level mapping.
