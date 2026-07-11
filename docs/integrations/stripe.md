# Stripe Meters integration

FluxMeter owns **real-time metering and budget enforcement**. Stripe owns **invoicing and payment collection**. This recipe wires hourly (or monthly) usage deltas from FluxMeter into [Stripe Billing Meters](https://docs.stripe.com/billing/subscriptions/usage-based).

## Architecture

```
App → FluxMeter (check / ingest) → Redis counters
              ↓ hourly export loop
         Stripe MeterEvent → Invoice line items
```

## Prerequisites

- FluxMeter API running (Lite or Full)
- Stripe account with Billing Meters enabled
- `stripe` Python package in API container (included in `api/requirements.txt`)

## 1. Create Stripe meters

In Stripe Dashboard → Billing → Meters, create one or both:

| Mode | Meter event name (env) | Value |
|------|------------------------|-------|
| Event count | `STRIPE_METER_NAME` (default `token_events_processed`) | Integer event count delta |
| Cost | `STRIPE_COST_METER_NAME` (default `token_cost_usd_cents`) | USD cents delta |

Attach meters to your usage-based price on the subscription product.

## 2. Configure FluxMeter

```bash
export STRIPE_API_KEY=sk_test_...
export BILLING_EXPORT_TARGETS=stripe          # default
export STRIPE_EXPORT_MODE=events              # or cost
export BILLING_EXPORT_PERIOD=hourly           # or monthly
export BILLING_EXPORT_INTERVAL=3600
export STRIPE_METER_NAME=token_events_processed
```

Restart the API. The background loop starts automatically when `STRIPE_API_KEY` is set.

## 3. Link customers

```bash
curl -X POST "http://localhost:8000/admin/billing/cust_123/link-stripe" \
  -H "X-API-Key: $FLUXMETER_ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{"stripe_customer_id": "cus_abc123"}'
```

Or use the generic link endpoint:

```bash
curl -X POST "http://localhost:8000/admin/billing/cust_123/link" \
  -H "X-API-Key: $FLUXMETER_ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{"platform": "stripe", "external_customer_id": "cus_abc123"}'
```

## 4. Verify export

After ingest activity, check API logs for `Reported usage for N customers`. In Stripe Dashboard → Meters → Events, confirm deltas appear within one export interval.

## Reconciliation checklist

| Check | How |
|-------|-----|
| FluxMeter total vs Stripe meter | Compare `GET /usage/customer/{id}` `event_count` / `cost_usd` with Stripe meter aggregates |
| Delta tracking | Redis keys `billing:{id}:last_reported_events`, `last_reported_cost_usd` |
| Tier repricing | Replay Kafka / rerun Lite ingest before syncing corrected totals (see [integrations.md](../integrations.md#re-rating-and-tiered-pricing)) |

## Division of responsibility

| FluxMeter | Stripe |
|-----------|--------|
| Sub-second `/check` deny | Subscription contracts |
| Prepaid wallet / holds | Payment collection |
| Per-model cost from `pricing.json` | Tax, dunning, customer portal |
| Hourly usage delta export | Invoice generation |

See also: [integrations.md](../integrations.md) · [api-reference.md](../api-reference.md)
