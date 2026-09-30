# Show HN: FluxMeter – real-time budget enforcement for AI token billing

I work on billing systems, and when I started building AI side projects I ran into a problem I think a lot of AI app teams are going to hit:

Token usage can run away much faster than traditional metering systems can react.

The integration problem is to attribute usage to each downstream customer and make an admission decision before upstream work, while retaining a durable accounting path afterwards.

So I built FluxMeter, an open-source metering engine for AI token billing with pre-request budget checks.

The core idea is simple:

```text
Before each LLM call:
  GET /budget/cust_123/check

If allowed:
  make the LLM call

After the call:
  POST /ingest {
    customerId,
    modelId,
    inputTokens,
    outputTokens
  }
```

Every public usage event enters through HTTP. FluxMeter waits for Kafka acknowledgement and retry-identity finalization, then Flink performs accounting in Redis. A `202` receipt is not completed settlement.

For streaming workloads, there is also a reserve/reconcile flow:

```text
POST /budget/cust_123/reserve
→ stream tokens
POST /ingest
POST /budget/cust_123/reconcile
```

The pre-request check uses effective balance:

```text
available = balance - held
```

The caller can honor a denial before the next LLM call. A check creates no hold; concurrent requests, cached fallback decisions, and estimated streaming usage mean this is not a zero-overspend guarantee.

```text
API → Kafka → Flink → Redis → alerts/webhooks
```

That path includes windowed aggregation, span attribution, DLQ replay, idempotent sinks, and budget kill signals.

ClickHouse is an optional audit cold store. Published throughput evidence distinguishes HTTP custody from full-pipeline settlement and internal Kafka/Flink bursts; see `docs/load-testing.md`. The sustained 10K single-event and 100K batch acceptance gates remain open.

Some implementation details:

- One deployment path: HTTP API + Kafka + Flink + Redis, `make demo`
- SaaS-style control plane scaffold: `make start-saas`
- HTTP-only Python SDK 2.0.1 in this checkout: `pip install ./sdk/python` (package-index publication not reverified)
- HTTP-only JS SDK in repo
- External pricing config via JSON + admin API
- Microdollar precision using integer arithmetic
- Multi-provider token normalization
- Agent span attribution for grouping multi-call tool/agent runs (`GET /usage/span/{id}`)
- Period/day/session billing queries for customer portals (v2.6.1, Redis rollup buckets)
- Retroactive re-rating for provider price changes
- Stripe Billing Meters export
- Helm chart, Prometheus alerts, DR runbook

Honest caveats:

- This is self-hosted, not a hosted SaaS product.
- Demo mode can run with auth disabled; production compose enforces API keys.
- Tiered pricing (flat / volume / graduated) is applied by Flink; use `contrib/pricing/tiered-example.json` as a template.
- Agent spans use session windows, so long-running agents need careful timeout handling.
- Capacity and costs must be measured for your workload; no sustained 100K capacity or monthly infrastructure price is promised.
- `make demo` verifies a unique event’s customer tokens and cost with no provider key. The wrapper warns on metering errors and can notify `on_metering_error`; callers still need a delivery-recovery path.

I would especially like feedback on:

1. Is pre-request check + reserve/reconcile the right model for streaming agent workloads?
2. For people running Flink in billing or financial systems: what operational problems should I expect?
3. Is agent span attribution useful for billing/debugging, or would you model this differently?

Website: https://fluxmeter.dev
GitHub: https://github.com/10kshuaizhang/fluxmeter
