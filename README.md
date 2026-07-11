# FluxMeter

**Website:** [fluxmeter.dev](https://fluxmeter.dev) · **Docs:** [fluxmeter.dev/docs](https://fluxmeter.dev/docs) · **Blog:** [Agent cost control](https://fluxmeter.dev/blog/stop-runaway-agent-costs)

Open-source, self-hostable **real-time AI token metering and budget enforcement**. Call `GET /budget/{id}/check` before every LLM request — sub-10ms latency, 1M+ events/sec in Full mode. Built for agent loops and prepaid token products where batch billing is too slow. **v3.1** adds Monetization Intelligence v1.0 — pricing optimizer, profitability dashboard, forecasts, alerts, and Finance-ready reports on top of the same metered data.

**When to use FluxMeter:** prepaid token wallets, agent loop cost control, self-hosted LLM metering, export to Stripe/Lago/Orb/Metronome.

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

**[fluxmeter.dev](https://fluxmeter.dev)** — overview, quick start, architecture · **v3.1.0** · **Open spec + SDKs** · **1M+ events/sec** · **<10ms budget check** · **Multi-provider**

**Links:** [Website](https://fluxmeter.dev) · [GitHub](https://github.com/10kshuaizhang/fluxmeter) · [PyPI](https://pypi.org/project/fluxmeter/) · [Docs](https://github.com/10kshuaizhang/fluxmeter/tree/main/docs) · [API reference](docs/api-reference.md) · [OpenAPI](spec/openapi/openapi.yaml)

![FluxMeter Demo](demo.gif)

## Who is this for

- **AI app builders** shipping LLM wrappers, agent platforms, or code assistants that bill per token
- **Platform teams** that need real-time cost visibility across OpenAI, Anthropic, and Google models
- **Anyone who's been burned** by a runaway agent loop spending $500 in 30 seconds before the billing system noticed

If your customers prepay for tokens and you need to cut them off the instant they run out — not 30 seconds later — FluxMeter does that.

## Repository layout (OpenCore)

| Layer | Path | Purpose |
|-------|------|---------|
| **Spec** | [`spec/`](spec/) | Event schema, OpenAPI, semantic conventions |
| **SDKs** | [`sdk/python/`](sdk/python/), [`sdk/js/`](sdk/js/) | Python + JS clients |
| **Community** | [`contrib/`](contrib/) | Provider mappings, pricing, connectors |
| **Engine** | [`src/`](src/) | Flink reference implementation (aggregation, budget enforcement) |
| **Demo** | `make demo` or `make demo-full` | Lite (API→Redis, default) or Full (Kafka→Flink→Redis) |

## Budget Enforcement (the core feature)

Set a prepaid balance. FluxMeter enforces it in <10ms per request:

```bash
# Set $50 budget, alert at $5 remaining, max 100 requests/minute
curl -X POST localhost:8000/budget/cust_123 \
  -H 'Content-Type: application/json' \
  -d '{"balance_usd": 50.0, "alert_threshold_usd": 5.0, "max_rpm": 100}'

# Pre-request check — call this BEFORE every LLM request
curl "localhost:8000/budget/cust_123/check?estimated_cost_usd=0.05"
# → {"allowed": true, "balance_usd": 47.23, "held_usd": 0.0, "effective_balance_usd": 47.23, ...}
# → {"allowed": false, "reason": "budget_exhausted", "source": "redis"}
# → {"allowed": false, "reason": "rate_limited", "max_rpm": 100}
```

**Two-layer enforcement:**

| Layer | Latency | What it does |
|-------|---------|--------------|
| Pre-request check | <10ms | `GET /budget/{id}/check` — blocks request before tokens are burned |
| Post-window deduction | 10-15s | Flink aggregates → atomic Lua deduction → Kafka kill signal |

The pre-request check uses a three-layer resilience stack (in-process cache → Redis → configurable fail policy) so it never blocks your agent's hot path, even during Redis outages.

## Quick Start

**Lite** (1 minute, no Flink — default):

```bash
git clone https://github.com/10kshuaizhang/fluxmeter.git
cd fluxmeter
make demo
```

**Full** (Kafka + Flink + 1M eps benchmark):

```bash
make demo-full
```

Starts Kafka, Flink, Redis, and the API. Open:

- **API docs:** http://localhost:8000/docs
- **Flink UI:** http://localhost:8081
- **Grafana:** http://localhost:3000

## Integration (3 ways)

**Python SDK** (richest — WAL, auto-extraction, streaming):
```python
from fluxmeter import FluxMeter

meter = FluxMeter(kafka_brokers="localhost:9094")
meter.track_openai("cust_123", openai_response, latency_ms=1200)
```

**Wrap (path activation — check before every call):**
```python
from openai import OpenAI
from fluxmeter import FluxMeter, wrap, BudgetExceededError

meter = FluxMeter(api_url="http://localhost:8000")  # Lite HTTP, no Kafka
client = wrap(OpenAI(), meter, customer_id="cust_123", fail_open=True)
try:
    client.chat.completions.create(model="gpt-4o-mini", messages=[...])
except BudgetExceededError:
    ...  # never hit the provider
```

**JavaScript SDK** (HTTP or Kafka):
```typescript
import { FluxMeter } from "@fluxmeter/client";
const meter = new FluxMeter({ apiUrl: "http://localhost:8000" });
await meter.trackOpenAI("cust_123", openaiResponse);
```

**HTTP API** (zero dependencies — any language, curl, serverless):
```bash
curl -X POST localhost:8000/ingest \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"cust_123","modelId":"gpt-4o","inputTokens":500,"outputTokens":150}'
```

**Direct Kafka** (highest throughput — any Kafka client library):
```
Topic: token-events
Format: JSON per spec/schema/token-event-v1.json, keyed by customerId
OpenAPI: spec/openapi/openapi.yaml
```

## Query API

| Endpoint | Description |
|----------|-------------|
| `GET /usage/global` | Total events, tokens, cost |
| `GET /usage/customer/{id}` | Per-customer breakdown |
| `GET /usage/customer/{id}/period/{YYYY-MM}` | Monthly usage (UTC calendar) |
| `GET /usage/customer/{id}/day/{YYYY-MM-DD}` | Daily usage |
| `GET /usage/session/{id}` | Session/project aggregated cost |
| `GET /usage/customer/{id}/model/{model}` | Per-model detail |
| `GET /usage/span/{id}` | Agent span cost (total cost of an agent run) |
| `GET /budget/{id}/check` | Pre-request allow/deny (<10ms, uses `balance - held`) |
| `POST /budget/{id}` | Set balance + threshold + rate limit |
| `POST /budget/{id}/topup` | Add credits |
| `POST /budget/{id}/reserve` | Hold estimate for streaming (does not deduct balance) |
| `POST /budget/{id}/reconcile` | Release hold after stream ends |
| `POST /budget/{id}/webhook` | Configure HTTPS alerts (EXHAUSTED / LOW) |
| `GET /pricing` | Current pricing catalog |
| `POST /admin/customers/{id}/api-keys` | Create customer-scoped API key |
| `GET /admin/reconciliation` | Balance drift snapshot |
| `POST /ingest` | HTTP event ingest |
| `POST /ingest/batch` | Batch ingest (up to 1000) |
| `POST /rerate/preview` | Preview price change impact |
| `POST /rerate/apply` | Apply retroactive re-rating |

Full reference: [docs/api-reference.md](docs/api-reference.md)

### Customer billing queries (v2.6.1)

Expose usage to end users without a separate warehouse:

```bash
# Token reseller: monthly / daily spend
curl localhost:8000/usage/customer/cust_123/period/2026-07
curl localhost:8000/usage/customer/cust_123/day/2026-07-05

# Agent platform: cost of one run (set parentSpanId on every child LLM call)
curl localhost:8000/usage/span/span_agent_42

# Multi-turn project (lite ingest + sessionId)
curl localhost:8000/usage/session/sess_456
```

| Use case | Field on ingest | Query |
|----------|-----------------|-------|
| Per-model lifetime | — | `GET /usage/customer/{id}/model/{model}` |
| Monthly invoice | — | `GET /usage/customer/{id}/period/{YYYY-MM}` |
| Today's spend | — | `GET /usage/customer/{id}/day/{YYYY-MM-DD}` |
| One agent task | `parentSpanId` | `GET /usage/span/{id}` |
| Conversation / project | `sessionId` (lite) | `GET /usage/session/{id}` |

## Architecture

```
[Your App] → [Kafka] → [Flink: aggregation] → [Redis] → [API]
     │              │              │                │
  SDK/HTTP     budget-alerts   keyed by         Budget check
  ingest       ← kill signals  (customer,model) (3-layer cache)
                               10s windows
```

**Key design choices:**
- Incremental aggregation — O(keys) memory, not O(events)
- Atomic budget deduction via Redis Lua script
- Microdollar precision (long) — no float accumulation errors
- Sink idempotency (SHA-256 + SET NX) — no double-billing on replay
- Three-layer budget check (cache → Redis → fail policy) — never blocks

## Event Schema

Each event = one LLM API call:

```json
{
  "customerId": "cust_123",
  "modelId": "gpt-4o",
  "provider": "openai",
  "inputTokens": 1250,
  "outputTokens": 847,
  "cacheReadTokens": 200,
  "reasoningTokens": 0,
  "parentSpanId": "span_agent_42",
  "sessionId": "sess_123",
  "timestamp": 1718534400000,
  "latencyMs": 1340
}
```

**Providers:** OpenAI (gpt-4o, gpt-4o-mini, o1, o3-mini), Anthropic (claude-opus-4, claude-sonnet-4, claude-haiku-4), Google (gemini-1.5-pro, gemini-1.5-flash)

**Token categories:** input, output, cached, reasoning, embedding — each priced independently.

## Durability

No single-component failure loses billing data:

| Failure | Protection |
|---------|-----------|
| Kafka down | SDK writes to local WAL (disk), flushes on recovery |
| Broker crash | `acks=all` — all replicas have the event |
| Flink restart | Checkpoints restore state + offsets exactly |
| Flink replay | Sink idempotency (SET NX) prevents double-counting |
| Redis restart | AOF persistence + named volume |
| Duplicate events | Sink-level dedup (SHA-256 window ID, 10-min TTL) |
| Late events | Routed to DLQ topic, not silently dropped |

## Performance

Load tested with `make load-test` (see [docs/load-testing.md](docs/load-testing.md)):

| Environment | 10K eps | 50K eps | 500K+ target |
|-------------|---------|---------|--------------|
| **Local docker-compose** (1 TM, 4 slots) | ~9K avg / ~18K peak | ~49K avg / ~92K peak | ~40–45K avg (Redis/Flink bound) |
| **Reference cluster** (2 TM, 8 slots, prior runs) | Stable | Stable | 500K indefinite; 1M bursts |

For sustained 500K+ eps, scale TaskManagers and use managed Kafka/Redis — [docs/production-deploy.md](docs/production-deploy.md).

## Integrations

Connect FluxMeter to your billing platform: [docs/integrations.md](docs/integrations.md)
- Stripe, Lago, OpenMeter, Orb, Metronome, Zuora

SDK publishing: [docs/pypi-release.md](docs/pypi-release.md)

## Production Deployment

Kubernetes + RocksDB + S3 checkpoints: [docs/production-deploy.md](docs/production-deploy.md)

Helm chart: [deploy/helm/README.md](deploy/helm/README.md)

Estimated cost: ~$1,550/month for 100K events/sec on AWS.

## Makefile

```bash
make demo        # Lite (default): Redis + API + Grafana
make demo-full   # Full: build + start-full + submit job + generate
make demo-lite   # Alias for make demo
make start       # Start lite stack (default)
make start-full  # Start full infrastructure (Kafka + Flink)
make start-lite  # Alias for make start
make submit-job  # Submit Flink job (full mode)
make generate    # Run load generator (1M target, continuous)
make load-test   # Staged load test 10K→1M
make load-test-quick  # Staged 10K→500K
make test-e2e    # Integration + v2 E2E tests
make test-lite   # Lite production pytest suite
make test-unit        # Python + Java unit tests (no Docker)
make test-unit-redis  # Lite Lua + rollup tests (needs Redis)
make test-java        # Java unit tests only
make benchmark        # Streaming vs batch comparison
make validate-spec    # Validate schema + OpenAPI artifacts
make stop        # Stop containers
make clean       # Stop + remove volumes + clean
```

## What's next

See **[ROADMAP.md](ROADMAP.md)** for the full plan. Highlights:

- [x] Tiered pricing (flat / volume / graduated) in Lite + Flink — see `contrib/pricing/tiered-example.json`
- [ ] Full multi-tenant RBAC / org model
- [x] Wrap SDK + mid-stream kill (`wrap(OpenAI())`, `StreamKilledError`) — full HTTP proxy still Phase 5
- [ ] `@fluxmeter/client` on npm
- [x] Webhook delivery for budget alerts
- [x] Customer-scoped API keys
- [x] Dual-path Lite / Full / SaaS (v2.1–2.2)
- [x] Python SDK 1.1.0 on PyPI (HTTP lite + Kafka)

## Requirements

- Docker & Docker Compose
- Java 17 (building the engine)
- Python 3.9+ (SDK and API)

## License

Apache 2.0
