# Show HN: FluxMeter – Real-time budget enforcement for AI token billing

I work on billing systems for a living. When I started building AI side projects, I hit the same problem every AI app team hits: a runaway agent loop burned through $200 of tokens in 45 seconds because the metering system only checks usage every 30 seconds.

So I built FluxMeter — an open source metering engine that enforces token budgets in <10ms per request.

**The problem it solves:** Your customer prepays $50 for tokens. They fire up an agent that makes 20 LLM calls in a loop. Traditional metering (store events, query every 30s) doesn't notice the budget is blown until $200 later. FluxMeter checks the balance before every single LLM call and says "no" the instant it's exhausted.

**How it works:**

```
Your app → GET /budget/cust_123/check (< 10ms)
           → allowed: true → proceed with LLM call
           → allowed: false → reject, return 402

Streaming (optional):
  POST /budget/cust_123/reserve  → holds estimate (balance unchanged)
  → LLM stream → POST /ingest
  → POST /budget/cust_123/reconcile → releases hold
  (Flink Sink deducts actual cost in 10s windows — single path, no double-charge)

After each LLM call:
Your app → POST /ingest {customerId, modelId, inputTokens, outputTokens}
           → Kafka → Flink (10s window) → Redis (atomic deduction) → Kafka alert / webhook
```

The pre-request check uses effective balance (`balance - held`) with a three-layer stack (in-process cache → Redis → configurable fail policy). Post-hoc aggregation is Flink windowed processing with Lua atomicity and SET NX idempotency.

**What makes it different from "just use ClickHouse":**

I included a ClickHouse baseline in the repo consuming from the same Kafka topic. Same data, same machine. Flink delivers budget enforcement in <1 second. ClickHouse materialized views have 8-43 second lag. Enough time for an agent to spend $200.

That said, if you don't need sub-second enforcement, store-then-query is simpler and might be enough. The repo lets you compare both patterns.

**Technical highlights:**
- ~50K events/sec sustained on local docker-compose (single TaskManager); 500K+ on scaled Flink clusters (see `docs/load-testing.md`)
- External pricing via `config/pricing.json` + `GET/PUT /pricing` (no hardcoded deploy)
- Microdollar precision (long arithmetic, no float accumulation errors)
- Multi-provider normalization (OpenAI, Anthropic, Google — all token types)
- Agent span attribution (group LLM calls in a tool-use loop → one cost number)
- Retroactive re-rating (provider drops price mid-month → instant credit)
- Customer-scoped API keys, budget webhooks, reconciliation job, DLQ replay
- Zero data loss (WAL + acks=all + checkpoint + sink idempotency)
- Helm chart + Prometheus alert rules for production

**Integration:** Python SDK (`pip install fluxmeter`), JS SDK, HTTP ingest (`curl`), or direct Kafka.

**What I'd love feedback on:**
- Is the pre-request check + hold/reserve model the right tradeoff for streaming agent workloads?
- Anyone running Flink for billing at scale — what operational issues should I expect?
- Is the agent span attribution (grouping multi-call runs) useful for your billing?

**Honest caveats:**
- **v2.0.1** — production-hardening landed (auth, webhooks, reconciliation, E2E tests), but not a hosted SaaS
- Demo mode allows unauthenticated API (`FLUXMETER_AUTH_OPTIONAL=true`); production overlay enforces keys — see `docker-compose.prod.yml`
- Tiered pricing schema exists; engine uses first tier until monthly volume tracking ships
- Session windows for agent spans can stay open if the agent never stops (60s gap)
- Self-hosted: you run Kafka, Flink, Redis (or use `make demo-lite` for API-only)

**Stack:** Java 17 (Flink), Python (SDK + API), Kafka, Redis, Grafana.

```bash
make demo-lite   # fastest: API → Redis
make demo        # full: Kafka + Flink + load generator
make load-test   # staged benchmark
```

GitHub: https://github.com/10kshuaizhang/fluxmeter
