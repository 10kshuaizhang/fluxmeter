# Show HN: FluxMeter – real-time budget enforcement for AI token billing

I work on billing systems, and when I started building AI side projects I ran into a problem I think a lot of AI app teams are going to hit:

Token usage can run away much faster than traditional metering systems can react.

In my case, an agent loop burned through about $200 of tokens in under a minute because usage was only being checked periodically. By the time the system noticed, the budget was already gone.

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

In Lite mode, this is just API + Redis Lua for atomic balance deduction and idempotency. No Kafka or Flink required.

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

So a customer can be stopped before the next LLM call instead of after a delayed batch query catches up.

There is also a Full mode for higher-volume setups:

```text
API → Kafka → Flink → Redis → alerts/webhooks
```

That path includes windowed aggregation, span attribution, DLQ replay, idempotent sinks, and budget kill signals.

I also included a ClickHouse baseline that consumes from the same Kafka topic. On my local tests, Flink gave sub-second budget enforcement, while ClickHouse materialized views lagged by several seconds to tens of seconds. If you do not need sub-second enforcement, store-then-query is probably simpler and may be the right choice.

Some implementation details:

- Lite path: API → Redis Lua, `make demo`
- Full path: Kafka + Flink + Redis, `make demo-full`
- SaaS-style control plane scaffold: `make start-saas`
- Python SDK on PyPI: `pip install fluxmeter`
- JS SDK in repo
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
- Tiered pricing (flat / volume / graduated) ships in Lite + Flink; use `contrib/pricing/tiered-example.json` as a template.
- Agent spans use session windows, so long-running agents need careful timeout handling.
- In my local setup, Redis Lua becomes the bottleneck above roughly 100K sustained events/sec.

I would especially like feedback on:

1. Is pre-request check + reserve/reconcile the right model for streaming agent workloads?
2. For people running Flink in billing or financial systems: what operational problems should I expect?
3. Is agent span attribution useful for billing/debugging, or would you model this differently?

Website: https://fluxmeter.dev
GitHub: https://github.com/10kshuaizhang/fluxmeter
