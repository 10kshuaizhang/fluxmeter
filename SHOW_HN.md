# Show HN: FluxMeter – Real-time budget enforcement for AI token billing (1M events/sec)

I work on billing systems for a living. When I started building AI side projects, I hit the same problem every AI app team hits: a runaway agent loop burned through $200 of tokens in 45 seconds because the metering system only checks usage every 30 seconds.

So I built FluxMeter — an open source metering engine that enforces token budgets in <10ms per request.

**The problem it solves:** Your customer prepays $50 for tokens. They fire up an agent that makes 20 LLM calls in a loop. Traditional metering (store events, query every 30s) doesn't notice the budget is blown until $200 later. FluxMeter checks the balance before every single LLM call and says "no" the instant it's exhausted.

**How it works:**

```
Your app → GET /budget/cust_123/check (< 10ms)
           → allowed: true → proceed with LLM call
           → allowed: false → reject, return 402

After the LLM call completes:
Your app → POST /ingest {customerId, modelId, inputTokens, outputTokens}
           → Kafka → Flink (10s window) → Redis (atomic deduction) → Kafka alert
```

The pre-request check is a Redis GET with an in-process cache fallback (3-layer: cache 0.01ms → Redis 1-5ms → configurable fail policy). The post-hoc aggregation uses Flink's windowed stream processing for exact billing math.

**What makes it different from "just use ClickHouse":**

I included a ClickHouse baseline in the repo consuming from the same Kafka topic. Same data, same machine. Flink delivers budget enforcement in <1 second. ClickHouse materialized views have 8-43 second lag. Enough time for an agent to spend $200.

That said, if you don't need sub-second enforcement, store-then-query is simpler and might be enough. The repo lets you compare both patterns.

**Technical highlights:**
- 1M events/sec sustained on a single machine (docker-compose, 4GB TaskManagers)
- Microdollar precision (long arithmetic, no float accumulation errors)
- Multi-provider normalization (OpenAI, Anthropic, Google — all token types)
- Agent span attribution (group 5 LLM calls in a tool-use loop → one cost number)
- Retroactive re-rating (provider drops price mid-month → instant credit)
- Zero data loss (WAL + acks=all + checkpoint + sink idempotency)

**Integration:** Python SDK, HTTP endpoint (`curl` is enough), or direct Kafka producer.

**What I'd love feedback on:**
- Is the pre-request check approach the right tradeoff for agent workloads?
- Anyone running Flink for billing at scale — what operational issues should I expect?
- Is the agent span attribution (grouping multi-call runs) useful for your billing?

**Honest caveats:**
- Early project (v1.0-rc1) — no production hardening beyond the docs
- No multi-tenant auth (add your own middleware)
- Pricing is hardcoded (external config planned)
- Session windows for agent spans can stay open indefinitely if the agent never stops

**Stack:** Java 17 (Flink), Python (SDK + API), Kafka, Redis, Grafana.

`make demo` runs everything.

GitHub: [link]
