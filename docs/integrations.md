# Integrations

FluxMeter handles real-time token metering. Your billing platform handles invoicing, subscriptions, and payment collection. This guide shows how to connect them.

**Overview:** [fluxmeter.dev](https://fluxmeter.dev) · repo detail in this doc

## Architecture

```
[Your App] → [FluxMeter: real-time metering] → [Billing Platform: invoicing]
                    ↓                                      ↓
         Sub-second aggregation              Monthly invoice, payment collection
         Budget enforcement                  Revenue recognition
         Per-model cost breakdown            Customer portal
```

FluxMeter produces the **usage data**. Your billing platform consumes it for **invoicing**.

### Query endpoints for customer-facing billing (v2.6.1)

| Endpoint | Typical use |
|----------|-------------|
| `GET /usage/customer/{id}/period/{YYYY-MM}` | Monthly invoice line (token resellers) |
| `GET /usage/customer/{id}/day/{YYYY-MM-DD}` | Daily usage dashboard |
| `GET /usage/customer/{id}/model/{model}` | Per-model breakdown (lifetime cumulative) |
| `GET /usage/span/{id}` | One agent task / run (`parentSpanId` on ingest) |
| `GET /usage/session/{id}` | Conversation or project total (lite + `sessionId`) |

Buckets accumulate from deploy forward; they do not backfill history. For lifetime totals, use `GET /usage/customer/{id}`.

---

## Lago (open source)

Lago uses events + billable metrics. FluxMeter aggregates in real-time; Lago invoices monthly.

### Setup

```python
import httpx
from datetime import datetime

LAGO_API_URL = "https://api.getlago.com/api/v1"
LAGO_API_KEY = "your-lago-api-key"
FLUXMETER_API = "http://localhost:8000"

# Create a billable metric in Lago for token usage
# (do this once via Lago dashboard or API)
# Metric: "token_usage", aggregation: SUM, field: "total_tokens"
```

### Sync usage to Lago (periodic job, e.g. every hour)

```python
def sync_to_lago(customer_id: str):
    """Push FluxMeter usage to Lago as a billing event."""
    # Get current usage from FluxMeter
    usage = httpx.get(f"{FLUXMETER_API}/usage/customer/{customer_id}").json()

    # Send to Lago as an event
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

### Why both?

- FluxMeter: sub-second budget enforcement, per-request guardrails, real-time dashboard
- Lago: monthly invoicing, payment collection, tax, revenue recognition

---

## OpenMeter

OpenMeter uses CloudEvents. FluxMeter can push aggregated usage as CloudEvents.

### Setup

```python
import httpx
from datetime import datetime, timezone

OPENMETER_API = "https://openmeter.cloud"
OPENMETER_API_KEY = "your-api-key"
FLUXMETER_API = "http://localhost:8000"
```

### Sync usage to OpenMeter

```python
def sync_to_openmeter(customer_id: str):
    """Push FluxMeter usage to OpenMeter as CloudEvents."""
    usage = httpx.get(f"{FLUXMETER_API}/usage/customer/{customer_id}").json()

    event = {
        "specversion": "1.0",
        "type": "ai.token.usage",
        "source": "fluxmeter",
        "id": f"fluxmeter-{customer_id}-{int(datetime.now().timestamp())}",
        "time": datetime.now(timezone.utc).isoformat(),
        "subject": customer_id,
        "data": {
            "total_tokens": usage["total_tokens"],
            "model": "all",  # or sync per-model
            "cost_usd": usage["cost_usd"],
        },
    }

    httpx.post(
        f"{OPENMETER_API}/api/v1/events",
        headers={
            "Authorization": f"Bearer {OPENMETER_API_KEY}",
            "Content-Type": "application/cloudevents+json",
        },
        json=event,
    )
```

### When to use OpenMeter alongside FluxMeter

- OpenMeter: if you already use it for non-AI metering (API calls, storage, etc.)
- FluxMeter adds: sub-second enforcement, multi-model normalization, agent span attribution

---

## Orb

Orb uses usage events with a specific schema. Sync from FluxMeter periodically.

### Setup

```python
import httpx

ORB_API = "https://api.withorb.com/v1"
ORB_API_KEY = "your-orb-api-key"
FLUXMETER_API = "http://localhost:8000"
```

### Sync usage to Orb

```python
def sync_to_orb(customer_id: str, orb_customer_id: str):
    """Push FluxMeter usage to Orb for invoicing."""
    usage = httpx.get(f"{FLUXMETER_API}/usage/customer/{customer_id}").json()

    # Orb expects idempotency_key for dedup
    httpx.post(
        f"{ORB_API}/ingest",
        headers={"Authorization": f"Bearer {ORB_API_KEY}"},
        json={
            "events": [
                {
                    "idempotency_key": f"fluxmeter-{customer_id}-input-{usage['event_count']}",
                    "external_customer_id": orb_customer_id,
                    "event_name": "token_usage",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "properties": {
                        "token_type": "input",
                        "tokens": usage["input_tokens"],
                    },
                },
                {
                    "idempotency_key": f"fluxmeter-{customer_id}-output-{usage['event_count']}",
                    "external_customer_id": orb_customer_id,
                    "event_name": "token_usage",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "properties": {
                        "token_type": "output",
                        "tokens": usage["output_tokens"],
                    },
                },
            ]
        },
    )
```

### Why FluxMeter + Orb?

- Orb: handles complex pricing models (tiered, volume, package), invoicing, dunning
- FluxMeter: handles real-time enforcement that Orb can't do (sub-second budget check, agent kill signals)
- Sync hourly from FluxMeter → Orb for invoicing; FluxMeter handles real-time guardrails

---

## Metronome (Stripe Billing)

Metronome uses usage events. Similar pattern to Orb.

### Sync usage to Metronome

```python
import httpx

METRONOME_API = "https://api.metronome.com/v1"
METRONOME_TOKEN = "your-token"
FLUXMETER_API = "http://localhost:8000"


def sync_to_metronome(customer_id: str, metronome_customer_id: str):
    """Push FluxMeter usage to Metronome for Stripe invoicing."""
    usage = httpx.get(f"{FLUXMETER_API}/usage/customer/{customer_id}").json()

    httpx.post(
        f"{METRONOME_API}/ingest",
        headers={"Authorization": f"Bearer {METRONOME_TOKEN}"},
        json=[
            {
                "customer_id": metronome_customer_id,
                "event_type": "token_usage",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "transaction_id": f"fluxmeter-{customer_id}-{usage['event_count']}",
                "properties": {
                    "input_tokens": usage["input_tokens"],
                    "output_tokens": usage["output_tokens"],
                    "total_cost_usd": usage["cost_usd"],
                },
            }
        ],
    )
```

---

## Zuora

Zuora uses Usage Records on subscriptions. FluxMeter aggregates; Zuora invoices.

### Sync usage to Zuora

```python
import httpx

ZUORA_API = "https://rest.zuora.com"
ZUORA_TOKEN = "your-oauth-token"
FLUXMETER_API = "http://localhost:8000"


def sync_to_zuora(customer_id: str, subscription_id: str, charge_id: str):
    """Push FluxMeter usage to Zuora as a Usage Record."""
    usage = httpx.get(f"{FLUXMETER_API}/usage/customer/{customer_id}").json()

    httpx.post(
        f"{ZUORA_API}/v1/usage",
        headers={"Authorization": f"Bearer {ZUORA_TOKEN}"},
        json={
            "accountNumber": customer_id,
            "subscriptionNumber": subscription_id,
            "chargeNumber": charge_id,
            "quantity": usage["total_tokens"] / 1_000_000,  # Zuora bills per unit
            "startDateTime": "2026-06-01T00:00:00",
            "endDateTime": "2026-06-30T23:59:59",
            "UOM": "million_tokens",
            "description": f"AI token usage: {usage['input_tokens']} input + {usage['output_tokens']} output",
        },
    )
```

### Zuora-specific notes

- Zuora charges are typically per-unit (e.g., per million tokens). Divide by 1M.
- Usage records are typically synced at end of billing period (monthly).
- FluxMeter handles real-time enforcement; Zuora handles the invoice.
- For prepaid/credits: use Zuora's Prepaid Balance feature alongside FluxMeter's budget system.

---

## Generic Webhook Integration

For any billing system not listed above, use a periodic job that reads from FluxMeter API and pushes to your billing system:

```python
import httpx
import schedule
import time

FLUXMETER_API = "http://localhost:8000"
YOUR_BILLING_WEBHOOK = "https://your-billing.com/webhook/usage"


def sync_all_customers():
    """Sync all customer usage to your billing system."""
    # Get global usage to find active customers
    global_usage = httpx.get(f"{FLUXMETER_API}/usage/global").json()

    # For each customer with usage, push to your system
    # (In production, maintain a customer registry)
    for customer_id in get_active_customers():
        usage = httpx.get(f"{FLUXMETER_API}/usage/customer/{customer_id}").json()
        httpx.post(YOUR_BILLING_WEBHOOK, json={
            "customer_id": customer_id,
            "period": "2026-06",
            "usage": usage,
        })


# Run hourly
schedule.every(1).hour.do(sync_all_customers)
while True:
    schedule.run_pending()
    time.sleep(60)
```

---

## Re-rating and tiered pricing

FluxMeter v2.4 ships **flat**, **volume**, and **graduated** pricing in Lite and Flink. Retroactive re-rating (`POST /rerate/preview`, `POST /rerate/apply`) remains a **flat-model** shortcut: it applies `(new_price - old_price) × aggregate_token_counts` from Redis.

### Why tier models need replay

Volume and graduated pricing depend on **when** each event occurred relative to monthly cumulative volume. Aggregate counters alone do not store:

- Which tier applied to each event
- `monthly_tokens_before` at event time
- Split token counts across tier boundaries (graduated)

Changing tier breakpoints or rates therefore requires **reprocessing raw events**, not counter math.

### Recommended workflow

1. **Flat price change** (same model, no tiers): use `/rerate/preview` → `/rerate/apply`.
2. **Tier catalog change** (add tiers, change breakpoints, switch `pricing_mode`):
   - Update catalog via `PUT /admin/pricing` or `PRICING_FILE`
   - Replay `token-events` from Kafka (or DLQ) through the pipeline with a new consumer group / reset offsets
   - See [runbooks/dlq-replay.md](runbooks/dlq-replay.md)
3. **Hybrid with Orb / Metronome**: export final `cost_usd` from FluxMeter after replay; let the billing platform own invoice-grade re-rating if you sync totals only.

### Orb / Metronome note

If FluxMeter handles real-time tier enforcement and you sync **pre-computed `cost_usd`** hourly, the external platform does not need to re-implement tiers — but you must replay locally before syncing corrected totals.

---

## Integration Pattern Summary

| Platform | Sync frequency | FluxMeter provides | Platform provides |
|----------|---------------|-------------------|-------------------|
| Lago | Hourly | Usage events | Invoicing, payment |
| OpenMeter | Hourly | CloudEvents | Usage aggregation, billing |
| Orb | Hourly | Token counts | Pricing models, invoicing |
| Metronome | Hourly | Usage records | Stripe invoicing |
| Zuora | Monthly | Usage quantity | Subscriptions, revenue recognition |

**FluxMeter's unique value in every integration:** real-time budget enforcement, agent span attribution, and sub-second guardrails. The billing platform handles everything after the usage is recorded.
