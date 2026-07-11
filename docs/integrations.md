# Integrations

FluxMeter handles real-time token metering. Your billing platform handles invoicing, subscriptions, and payment collection.

**Overview:** [fluxmeter.dev](https://fluxmeter.dev)

## Architecture

```
[Your App] → [FluxMeter: real-time metering] → [Billing Platform: invoicing]
                    ↓                                      ↓
         Sub-second aggregation              Monthly invoice, payment collection
         Budget enforcement                  Revenue recognition
         Per-model cost breakdown            Customer portal
```

FluxMeter produces the **usage data**. Your billing platform consumes it for **invoicing**.

### First-class partner recipes (v2.8)

| Platform | Built-in export | Recipe |
|----------|-----------------|--------|
| **Stripe** Meters | Yes (`STRIPE_API_KEY`) | [integrations/stripe.md](integrations/stripe.md) |
| **Metronome** | Yes (`METRONOME_API_TOKEN`) | [integrations/metronome.md](integrations/metronome.md) |
| **Orb** | Yes (`ORB_API_KEY`) | [integrations/orb.md](integrations/orb.md) |

Enable targets: `BILLING_EXPORT_TARGETS=stripe,metronome,orb` (default: `stripe`).

Link customers: `POST /admin/billing/{customer_id}/link` with `platform` + `external_customer_id`.

### Query endpoints for customer-facing billing (v2.6.1)

| Endpoint | Typical use |
|----------|-------------|
| `GET /usage/customer/{id}/period/{YYYY-MM}` | Monthly invoice line (token resellers) |
| `GET /usage/customer/{id}/day/{YYYY-MM-DD}` | Daily usage dashboard |
| `GET /usage/customer/{id}/model/{model}` | Per-model breakdown (lifetime cumulative) |
| `GET /usage/span/{id}` | One agent task / run (`parentSpanId` on ingest) |
| `GET /usage/session/{id}` | Conversation or project total (lite + `sessionId`) |
| `GET /usage/dim/{dim_key}/{dim_value}` | Feature / room slice (v2.8, whitelist dims) |

Buckets accumulate from deploy forward; they do not backfill history. For lifetime totals, use `GET /usage/customer/{id}`.

---

## Lago (open source)

Lago uses events + billable metrics. FluxMeter aggregates in real-time; Lago invoices monthly.

```python
import httpx
from datetime import datetime

def sync_to_lago(customer_id: str):
    usage = httpx.get(f"{FLUXMETER_API}/usage/customer/{customer_id}").json()
    httpx.post(
        f"{LAGO_API_URL}/events",
        headers={"Authorization": f"Bearer {LAGO_API_KEY}"},
        json={
            "event": {
                "transaction_id": f"fluxmeter-{customer_id}-{datetime.now().isoformat()}",
                "external_customer_id": customer_id,
                "code": "token_usage",
                "properties": {
                    "total_tokens": usage["total_tokens"],
                    "input_tokens": usage["input_tokens"],
                    "output_tokens": usage["output_tokens"],
                    "cost_usd": usage["cost_usd"],
                },
            }
        },
    )
```

---

## OpenMeter

OpenMeter uses CloudEvents. Push aggregated usage periodically:

```python
def sync_to_openmeter(customer_id: str):
    usage = httpx.get(f"{FLUXMETER_API}/usage/customer/{customer_id}").json()
    event = {
        "specversion": "1.0",
        "type": "ai.token.usage",
        "source": "fluxmeter",
        "id": f"fluxmeter-{customer_id}-{int(datetime.now().timestamp())}",
        "time": datetime.now(timezone.utc).isoformat(),
        "subject": customer_id,
        "data": {"total_tokens": usage["total_tokens"], "cost_usd": usage["cost_usd"]},
    }
    httpx.post(f"{OPENMETER_API}/api/v1/events", headers={...}, json=event)
```

---

## Zuora

Zuora uses Usage Records on subscriptions. Sync monthly quantity (e.g. tokens / 1M) via `POST /v1/usage`. FluxMeter handles real-time enforcement; Zuora handles the invoice.

---

## Generic Webhook Integration

For any billing system not listed above, use a periodic job that reads from FluxMeter API:

```python
for customer_id in get_active_customers():
    usage = httpx.get(f"{FLUXMETER_API}/usage/customer/{customer_id}").json()
    httpx.post(YOUR_BILLING_WEBHOOK, json={"customer_id": customer_id, "usage": usage})
```

Or configure per-customer budget webhooks on FluxMeter (`POST /budget/{id}/webhook`) for real-time alerts.

---

## Re-rating and tiered pricing

FluxMeter v2.4+ ships **flat**, **volume**, and **graduated** pricing. Retroactive re-rating (`POST /rerate/preview`) is a **flat-model** shortcut.

Volume/graduated changes require **reprocessing raw events**. After replay, sync corrected totals to Metronome/Orb/Stripe (`cost` mode) or raw tokens (`events` mode).

See [runbooks/dlq-replay.md](runbooks/dlq-replay.md).

---

## Integration Pattern Summary

| Platform | Sync | FluxMeter provides | Platform provides |
|----------|------|-------------------|-------------------|
| Stripe / Metronome / Orb | Built-in hourly | Usage deltas | Invoicing, payment |
| Lago | Manual / cron | Usage events | Invoicing, payment |
| OpenMeter | Manual / cron | CloudEvents | Usage aggregation |
| Zuora | Monthly | Usage quantity | Subscriptions |

**FluxMeter's unique value:** real-time budget enforcement, agent span attribution, and sub-second guardrails.

Field mappings: [spec/schema/external-export-mappings.md](../spec/schema/external-export-mappings.md)
