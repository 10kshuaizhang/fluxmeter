# Tier pricing in the authoritative Flink path

FluxMeter v2.4+ applies **volume** and **graduated** tiers using a monthly token meter (`customer_model` scope, UTC calendar month).

## Where volume lives

Flink keeps keyed `ValueState` per `tenant\|customer\|model` and projects the period counter to `{customer}:model:{model}:period:{YYYY-MM}:volume_tokens`.

The Redis period counter is an observability projection, not the source of truth for tier placement. Audit tier state from Flink checkpoints or an authorized Kafka replay.

## Prepaid token packages

`POST /budget/{id}/package` sets `package:{id}:tokens_remaining`; the Flink event projection performs drawdown.

## Re-rating

Flat models: `/rerate/preview` + `/rerate/apply`. Tier models: **422** — replay Kafka. See [integrations.md](integrations.md#re-rating-and-tiered-pricing).

## Stripe export

```bash
STRIPE_EXPORT_MODE=events   # default — meter event counts
STRIPE_EXPORT_MODE=cost       # meter USD cents delta (token_cost_usd_cents)
BILLING_EXPORT_PERIOD=hourly  # default
BILLING_EXPORT_PERIOD=monthly # once per UTC calendar month
```
