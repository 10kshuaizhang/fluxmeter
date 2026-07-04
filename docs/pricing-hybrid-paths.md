# Tier pricing: Lite vs Flink volume state

FluxMeter v2.4+ applies **volume** and **graduated** tiers using a monthly token meter (`customer_model` scope, UTC calendar month).

## Where volume lives

| Path | Volume state | Redis key |
|------|--------------|-----------|
| **Lite** (`POST /ingest`) | Atomic read/increment in Lua | `{customer}:model:{model}:period:{YYYY-MM}:volume_tokens` |
| **Flink** (Full) | Keyed Flink `ValueState` per `tenant\|customer\|model` | Same key written on **window sink** (approximate) |

## Hybrid deployments

If you run **both** Lite ingest and Flink on the same Redis:

1. **Tier cost** is computed correctly on each path independently (Flink state vs Lite Lua).
2. **Redis period counter** is updated by Lite per event and by Flink per closed window (`total_tokens` sum). The counter is for **observability / cross-path dashboards**, not the source of truth for Flink tier selection.
3. **Do not** use Redis period key alone to audit Flink tier placement — use Flink checkpoints or replay from Kafka.

## Prepaid token packages

`POST /budget/{id}/package` sets `package:{id}:tokens_remaining`. Lite ingest atomically decrements before billing. When exhausted, ingest returns `package_exhausted`. Budget USD deduction still applies unless you configure package-only customers without a budget key.

## Re-rating

Flat models: `/rerate/preview` + `/rerate/apply`. Tier models: **422** — replay Kafka. See [integrations.md](integrations.md#re-rating-and-tiered-pricing).

## Stripe export

```bash
STRIPE_EXPORT_MODE=events   # default — meter event counts
STRIPE_EXPORT_MODE=cost       # meter USD cents delta (token_cost_usd_cents)
BILLING_EXPORT_PERIOD=hourly  # default
BILLING_EXPORT_PERIOD=monthly # once per UTC calendar month
```
