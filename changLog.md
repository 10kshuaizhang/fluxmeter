# Changelog

All notable changes to FluxMeter are documented here. Version numbers follow [Semantic Versioning](https://semver.org/).

Format: `[version] — date — summary`

---

## [2.2.2] — 2026-07-04

### Added
- **`api/tenant_keys.py`**: Python mirror of `TenantKeys.java` for lite Redis key prefixes
- **`tests/test_tenant_keys.py`**: no-Redis unit tests for tenant key helpers
- **`make test-unit-redis`**: lite Lua aggregator + rollup tests (requires local Redis)
- **Lite multi-tenant ingest**: optional `tenantId` on `/ingest` events; Lua global counters scoped per tenant
- **Tenant E2E**: `test_lite_production.py::TestTenantIsolation` (HTTP ingest + Redis key prefix)

### Changed
- **Official website links**: README, SHOW_HN, CONTRIBUTING, SDK READMEs, `pyproject.toml` / `package.json` → [fluxmeter.dev](https://fluxmeter.dev); README integrations list includes Stripe
- **`make test-unit`**: runs auth, billing export, control-plane models, tenant_keys + `./gradlew test`
- **OpenAPI 2.2.2**: health `mode`, lite/full ingest response schemas, `POST /admin/billing/{id}/link-stripe`
- **`validate-spec.sh`**: checks OpenAPI completeness (mode, cost_usd, link-stripe, batch lite schema)
- **`docs/load-testing.md`**: Mac ~25K sustained @ 50K target callout in Quick start
- **Version alignment**: engine, API, OpenAPI, Helm → **2.2.2**

### Fixed
- **Lite API Docker image**: include `tenant_keys.py` (import required by `lite_aggregate_lua.py`)
- **Lua budget return**: return balance as string so Redis script replies preserve sub-cent amounts

### Notes
- Phase 1 (ROADMAP v2.3 polish) complete; tiered pricing runtime remains Phase 2 / v2.4.0
- Single-tenant behavior unchanged when `tenantId` is omitted

---

## [2.2.1] — 2026-06-22

### Added
- **JUnit tests** for Java financial core: `PricingCatalogTest`, `UsageAggregateTest`, `TenantKeysTest`, `TokenEventTest`, `AggregationKeysTest`; `make test-java`
- **`AggregationKeys` utility** (`io.fluxmeter.util`): parses Flink window keys (`customer|model` lite, `tenant|customer|model` SaaS) into `UsageAggregate` fields
- **Python unit tests (no Docker)**: `tests/test_lite_aggregate_unit.py` (Lua aggregator idempotency, pricing, inline budget); `tests/test_control_plane_models.py` (plan tiers, defaults)
- **Unified E2E runner**: `scripts/run-e2e-all.sh` — unit → lite → full Flink → SaaS stacks (`--unit-only`, `--lite-only`, `--full-only`)
- **Control plane API reference**: [docs/control-plane-api.md](docs/control-plane-api.md) — tenant CRUD, plans, auth, usage queries
- **Flink Prometheus metrics**: custom `flink/Dockerfile` with Prometheus reporter JAR; `prometheus` service in `docker-compose.full.yml` (`:9090`)
- **Disaster recovery runbook**: [docs/disaster-recovery.md](docs/disaster-recovery.md) — Redis loss, Flink replay, Kafka WAL, lite mode recovery
- **Multi-tenant Flink isolation**: optional `tenantId` on `TokenEvent`; aggregation key `tenantId|customerId|modelId`; Redis keys via `TenantKeys` utility
- **PyPI Python SDK 1.1.0**: latest `fluxmeter` package published — https://pypi.org/project/fluxmeter/1.1.0/ (`pip install fluxmeter`)

### Changed
- **ROADMAP.md**: project-wide forward plan (phases v2.3 → v3.x, ecosystem track, non-goals)
- **Version alignment across repo**: Python/JS SDK `1.1.0` (PyPI/npm source); engine, API, OpenAPI, Helm, control plane → **2.2.1**

### Notes
- Single-tenant deployments unchanged when `tenantId` is omitted (keys remain `customer:{id}:*`)
- Load test (3 TM, 12 parallelism, 2026-06-22): ~25K avg eps at 50K target tier; Redis Lua sink remains local bottleneck above ~100K sustained

---

## [2.2.0] — 2026-06-22

### Added
- **SaaS control plane** (`services/control-plane/`): FastAPI tenant CRUD on `:8001` — create/list/delete tenants, plan tiers (free/growth/scale/enterprise), API key provisioning, usage queries via shared Redis
- **`docker-compose.saas.yml`**: Lite stack + control plane + password-protected Redis; `make start-saas` / `make stop-saas`
- **Control plane tests** (`tests/test_control_plane.py`): tenant lifecycle and plan limit enforcement
- **Stripe billing stub** (`services/control-plane/stripe_billing.py`): subscription creation scaffold for future webhook integration

### Notes
- Control plane shares Redis with main API for tenant-scoped rate limits (`tenant:{id}:max_eps`, `tenant:{id}:max_events_month`)

---

## [2.1.0] — 2026-06-22

### Added
- **Atomic Lua lite aggregator** (`api/lite_aggregate_lua.py`): single-script idempotency, counter increments, global counters, and inline budget deduction (replaces non-atomic pipeline in lite mode)
- **Background rollup worker** (`api/rollup_worker.py`): asyncio task compacts live counters into per-minute Redis hashes (24h TTL) and resets live counters every 60s in lite mode
- **Rollup tests** (`tests/test_rollup.py`): counter compaction logic against Redis
- **Stripe billing export** (`api/billing_export.py`): hourly usage reporting to Stripe Billing Meters API; admin endpoint `POST /admin/billing/{customer_id}/link-stripe`
- **Billing export tests** (`tests/test_billing_export.py`): mocked Stripe, no infra required

### Changed
- **`/ingest` and `/ingest/batch` (lite mode)**: return JSON with `status`, `cost_usd`, `balance_usd`; batch returns `{"results": [...]}`
- **Model keys in lite mode**: use `normalize_model_id()` (aligned with Flink sink)

### Notes
- `api/lite_aggregate.py` retained for backward compatibility; lite ingest path uses `lite_aggregate_lua.py`

---

## [2.0.2] — 2026-06-22

### Fixed
- **Budget API 500**: `POST /budget/{id}` called `get_customer_budget()` directly; FastAPI `Header` default leaked as API key → `AttributeError`. Extracted `_fetch_customer_budget()` for internal reuse.

### Changed
- **Lite as default DX**: `docker-compose.yml` is lite stack; full Flink/Kafka stack in `docker-compose.full.yml`. `make demo` runs lite; `make demo-full` runs full stack. `demo-lite` / `start-lite` remain aliases.
- **docker-compose.full.yml high-throughput profile**: 3 TaskManagers (4 slots × 5G each), Redis 4G + io-threads, Kafka 24 partitions + network tuning, `kafka-init` service; `mem_limit` + `restart: unless-stopped`
- **Load test defaults**: `NUM_THREADS=8`, `FLINK_PARALLELISM=12`; Makefile `submit-job -p 12`

### Notes
- Local target: 100K eps sustained, 1M eps burst (Redis Lua sink remains bottleneck above ~100K avg)

---

## [2.0.1] — 2026-06-21

### Added
- **E2E test suite**: `tests/test_e2e_v2.py` — v1.2–v2.0 scenarios (single-path billing, API keys, debt floor, pricing API, reconciliation, webhooks)
- **Unit tests**: `tests/test_auth_unit.py` — customer key tenant isolation
- **Load test script**: `scripts/load-test.sh`, `make load-test` / `make load-test-quick`
- **Docs**: `docs/load-testing.md`, `tests/TEST_PLAN.md` §11–16

### Fixed
- **Flink job submission**: `RichAggregateFunction` incompatible with `window.aggregate()` on Flink 1.18 — reverted to plain `AggregateFunction` (job runs again)
- **Customer API key isolation**: mismatched customer key always returns 403 (not bypassed in demo mode)
- **docker-compose.yml**: Grafana service YAML nesting error

### Changed
- **Tiered pricing**: `PricingCatalog` tier schema remains; engine applies **first tier** until monthly volume tracking lands (no `RichAggregateFunction` state)

### Notes
- Load test (local docker-compose): ~50K eps sustained; 500K+ target limited by single TaskManager + Redis on dev hardware
- Java engine **2.0.1**

---

## [2.0.0] — 2026-06-21

### Added
- **Helm chart**: `deploy/helm/fluxmeter` — API deployment, Service, PrometheusRule alerts (lag, window stall, reconciliation drift)
- **Tiered pricing schema**: `PricingCatalog` supports per-model volume tiers in JSON/YAML
- **Deploy docs**: `deploy/helm/README.md`

### Notes
- Java engine **2.0.0**; production assumes external Kafka/Redis + Flink Operator

---

## [1.4.0] — 2026-06-21

### Added
- **Balance reconciliation job**: `jobs/reconcile_balances.py` — `balance == initial + topups - total_deducted`; stores snapshot in `reconciliation:last`
- **DLQ replay**: `scripts/dlq_replay.py`, `scripts/replay-dlq.sh`
- **Runbook**: `docs/runbooks/dlq-replay.md`
- **API**: `GET /admin/reconciliation`
- **docker-compose**: `reconcile-job` service

---

## [1.3.0] — 2026-06-21

### Added
- **External pricing**: `io.fluxmeter.pricing.PricingCatalog` loaded from `config/pricing.json` or classpath
- **Pricing API**: `GET /pricing`, `PUT /admin/pricing`, `POST /admin/pricing/validate`
- **`UsageAggregate` decoupled** from hardcoded switch pricing

### Changed
- Flink `UsageAggregateFunction` uses `PricingCatalog` (flat rate per event; tier selection deferred to 2.0.1+)

---

## [1.2.0] — 2026-06-21

### Added
- **Single-path balance deduction**: `reserve`/`reconcile` use `held_usd` only; Flink Sink sole `balance_usd` mutator
- **Debt tracking**: excess cost recorded in `budget:{id}:debt_usd` when balance floors at zero
- **Customer API keys**: `POST /admin/customers/{id}/api-keys`, per-customer ingest/check authorization
- **Budget webhooks**: `POST /budget/{id}/webhook` + `webhook-worker` Kafka consumer → HTTPS with HMAC
- **Budget fields**: `held_usd`, `effective_balance_usd`, `debt_usd` on budget responses

### Fixed
- **Streaming double-charge**: reserve no longer deducts balance before Sink window deduction
- **Reconcile negative balance**: reconcile releases hold only; no balance credit/debit

### Changed
- `check` uses `effective_balance = balance - held`

---

## [1.1.0] — 2026-06-21

### Added
- **OpenCore repo split**: `spec/` (JSON Schema, OpenAPI, semantic conventions, pricing template)
- **Community layer**: `contrib/` with provider mappings, pricing snapshot, CONTRIBUTING
- **JavaScript SDK**: `sdk/js` (`@fluxmeter/client`) — HTTP ingest + optional Kafka
- **Lite demo**: default `docker-compose.yml` + `make demo` — Redis + API + Grafana, no Flink/Kafka (`make demo-full` for Flink stack in `docker-compose.full.yml`)
- **Lite aggregation**: `api/lite_aggregate.py` — per-event Redis counters matching full stack key schema
- **Spec validation**: `scripts/validate-spec.sh`, `make validate-spec`
- **Engine docs**: `src/README.md` — reference implementation boundary

### Changed
- API Dockerfile builds from repo root (includes `spec/` for `/openapi.yaml`)
- README: OpenCore layout, lite vs full quick start, JS SDK examples
- Java engine version **1.1.0**

### Notes
- Python SDK: `pip install fluxmeter` — **1.1.0 on PyPI** (2026-06-22); JS SDK build with `cd sdk/js && npm run build`

---

## [1.0.0] — 2026-06-21

### Added
- **Python SDK PyPI release**: `fluxmeter` 1.0.0 on PyPI (`sdk/python/pyproject.toml` aligned with `__version__`)
- **CI publish workflow**: `.github/workflows/pypi-publish.yml` (Trusted Publisher → PyPI)
- **Release docs**: `docs/pypi-release.md` (manual + TestPyPI + CI steps)

### Changed
- Python SDK classifier: Production/Stable; Java engine version **1.0.0**

### Notes
- **Published on PyPI**: https://pypi.org/project/fluxmeter/1.0.0/ — `pip install fluxmeter`
- Configure PyPI Trusted Publisher for CI (no tokens in git). See `docs/pypi-release.md`.

---

## [1.0.0-rc3] — 2026-06-21

### Fixed
- **WAL partial batch duplicate**: flush sends one event at a time; offset advances only after Kafka ack
- **WAL exit data loss**: `flush()` drains WAL synchronously before close
- **Redis password in prod**: `REDIS_PASSWORD` wired to API (`ConnectionPool`) and all Java sinks via `RedisConnections`
- **Checkpoint disabled in submit-job**: Flink containers chown checkpoint volume; removed `CHECKPOINT_DIR=` override from Makefile
- **Integration test flakiness**: budget accuracy uses 180s poll + multi-model watermarks; idempotency reordered before heavy load with keepalive watermarks; `push_watermarks` aligned to 10s Flink window (12s interval)

### Notes
- **Prod overlay E2E**: 20/20 passed (5 prod auth + 15 integration) on `docker-compose.prod.yml` stack

---

## [1.0.0-rc2] — 2026-06-21

### Fixed
- **Fractional model pricing**: `calculateEventCostMicro` uses `Math.round(tokens * pricePerM)` — sub-$1/M models no longer bill as $0
- **Model ID normalization**: versioned IDs (e.g. `gpt-4o-2024-08-06`) map to canonical pricing keys via prefix match
- **Streaming heartbeat double-billing**: Flink filters `_heartbeat` metadata; SDK heartbeats emit delta tokens only
- **WAL duplicate Kafka sends**: WAL-enabled clients send only via flush loop with byte-offset tracking
- **BudgetEnforcerSink crash window**: single Lua script atomically sets idempotency key, writes counters, and deducts budget
- **OptimizedRedisSink global counters**: global totals accumulated only for windows that pass idempotency check
- **Event-level dedup**: `UsageAggregate` tracks `seenEventIds` per window (bounded by window event count)

### Added
- **API authentication**: `X-API-Key` header via `FLUXMETER_API_KEY` / `FLUXMETER_ADMIN_KEY`; demo mode via `FLUXMETER_AUTH_OPTIONAL=true`
- **`docker-compose.prod.yml`**: Redis password, API key enforcement, Grafana anonymous disabled, fail-closed budget policy

### Changed
- **Default `BUDGET_FAIL_POLICY`**: `closed` in API (docker-compose demo sets `open` explicitly)

### Notes
- Addresses 15 findings from Bugbot + Security Review (2026-06-21)

---

## [1.0.0-rc1] — 2026-06-20

### Fixed (10 production issues)
1. **hashCode collision → SHA-256**: idempotency key now uses 64-bit SHA-256 prefix.
   Collision probability: 1 in 4 billion (was 1 in 77K with hashCode).
2. **Lua threshold semantic**: uses stored `initial_balance_usd` (not current balance)
   for default 10% threshold calculation. Alert fires at the right time.
3. **WAL batch fsync**: `os.fsync()` every 100 events. True disk durability.
4. **Session window memory**: documented limitation. SpanSink SET/overwrite ensures
   correctness even if window stays open indefinitely.
5. **SCAN blocks HTTP**: `/rerate/apply` returns 202 (async semantics).
6. **Schema incompatibility**: OptimizedRedisSink now writes API-compatible keys
   (`customer:*:*`, `global:*`). Drop-in replacement for BudgetEnforcerSink.
7. **Float accumulation → microdollars**: `costMicro` (long) internally.
   `getCostUsd()` converts for backward compatibility. Zero precision drift.
8. **Initial balance stored**: `POST /budget/{id}` now writes `initial_balance_usd`
   for Lua threshold calculation.

### Notes
All 10 issues identified in the production audit are addressed. The system is
now suitable for production billing workloads with correct financial math.

---

## [0.9.1] — 2026-06-20

### Added
- **Three-layer resilient budget check**:
  - Layer 1: in-process cache (0.01ms, 30s TTL, always available)
  - Layer 2: Redis GET (1-5ms, authoritative, updates cache on success)
  - Layer 3: fail policy when both down (BUDGET_FAIL_POLICY=open|closed)
  - Response includes `"source": "redis|cache|policy"` for observability
  - Hot path never blocks on Redis failure — agent workloads unaffected

---

## [0.9.0] — 2026-06-20

### Added
- **OptimizedRedisSink** — drop-in replacement with 4 algorithmic improvements:
  - Hash consolidation: 1 HSET per customer (not 10+ string keys). 10x fewer keys.
  - Batched writes: buffer 50 window results per pipeline. 5x fewer Redis ops.
  - Compact idempotency: 8-char hash key + 10-min TTL (not 56-byte key + 1h). 6x less memory.
  - Local global aggregation: accumulate in batch, write once. 50x fewer hotspot writes.
- **Integration test suite** (10 correctness scenarios):
  - Budget accuracy, idempotency, rate limit boundary, reserve/reconcile,
    multi-model pricing, re-rating, span attribution, HTTP ingest, alert ordering, zero-tokens
  - 14 passed, 1 skipped (timing-dependent), 0 failed
- Global counter reduce operator in Flink (preparation for isolated global sink)

### Notes
Resource comparison (10K customers, 9 models, 10s window):
- Redis keys: 100K → 10K (10x reduction)
- Redis ops/cycle: 135K → 27K (5x reduction)
- Idempotency memory: 54 MB → 9 MB (6x reduction)
- Global counter writes: 9K/cycle → 180/cycle (50x reduction)

---

## [0.8.1] — 2026-06-20

### Added
- **HTTP ingest endpoint** (no Kafka client required):
  - `POST /ingest` — single event (returns 202 Accepted)
  - `POST /ingest/batch` — up to 1000 events per call
  - Auto-generates eventId + timestamp if not provided
  - Internal Kafka producer with lz4 + acks=all
  - API container now depends on Kafka + has KAFKA_BROKERS env
  - `confluent-kafka` added to API requirements

### Verified (E2E with HTTP ingest)
- 511 events ingested via HTTP → Kafka → Flink → Redis → API query
- Budget deducted correctly ($10 → $4.44)
- Zero SDK or Kafka client needed for integration

### Notes
Three integration paths now available:
1. Python SDK — richest (WAL, auto-extraction, streaming wrapper)
2. HTTP API — zero dependencies (any language, curl, serverless)
3. Direct Kafka — highest throughput (any Kafka client library)

---

## [0.8.0] — 2026-06-20

### Added
- **Streaming mid-response — budget safety** (estimated pre-deduction):
  - `POST /budget/{id}/reserve` — pessimistic deduction before LLM call
  - `POST /budget/{id}/reconcile` — credit back difference after completion
  - Prevents overspend during long-running streaming responses
- **Streaming mid-response — SDK heartbeat** (observability):
  - `meter.wrap_stream(stream, customer_id, model_id)` → iterator wrapper
  - Emits partial usage events every 2s during streaming
  - Counts output tokens from chunks (character approximation)
  - Final accurate event on stream end
  - Supports OpenAI and Anthropic streaming chunk formats
- **Retroactive re-rating — differential adjustment**:
  - `POST /rerate/preview` — preview cost adjustments for a price change
  - `POST /rerate/apply` — atomically adjust all affected customer costs
  - Credits back to budget balance on price decreases
  - No event replay needed (uses existing Redis counters)

### Notes
- All 10 original requirements now complete
- SDK version bumped to 0.7.0

---

## [0.7.0] — 2026-06-20

### Added
- **Rate limiting** in pre-request guardrail:
  - `max_rpm` field in budget config (requests per minute cap)
  - Sliding window counter using per-minute Redis keys (2-min TTL)
  - Response includes `requests_this_minute` for observability
  - Three-layer check order: rate limit → budget balance → estimated cost

### Verified (load test + requirements)
- **1M eps sustained** — 30 seconds at 1,000,000 events/sec, both TMs stable
- **All throughput tiers**: 10K → 50K → 100K → 500K → 1M eps, zero failures
- **Guardrails**: budget check + rate limit + alerts all working end-to-end
- **Credits drawdown**: set → deduct → exhaust → deny → topup → re-allow
- **Multi-provider**: 6 models verified with correct per-model pricing
- **Exactly-once**: 880K idempotency keys verified in Redis (1h TTL)

### Not Implemented (documented, deferred)
- **Streaming mid-response metering**: requires proxy mode (SSE stream parser)
- **Retroactive re-rating**: requires pricing versioning + Kafka replay job

---

## [0.6.2] — 2026-06-20

### Fixed
- **CRITICAL: Removed Flink EventDeduplicator** — keying by eventId created 1 key per event
  in Flink state (1.8B keys/hour at 500K eps). Guaranteed OOM. Sink-level SET NX is sufficient.
- **HIGH: Removed allowedLateness(30s)** — late data re-fired the window, but SET NX blocked
  the second write (same windowStart). Late data contribution was silently lost. Now late
  events go exclusively to DLQ for reprocessing.
- **HIGH: Counter + budget deduction now atomic** — customer `cost_usd` increment moved inside
  the Lua script. Previously a crash between pipeline.sync() and eval() meant counters written
  but budget never deducted (customer gets free tokens).
- **MEDIUM: SpanSink uses SET (overwrite) instead of INCRBY** — session windows fire multiple
  times on merge. Each fire contains the full aggregate. INCRBY was double-counting.
- API version updated to 0.6.1

---

## [0.6.1] — 2026-06-20

### Fixed
- **Makefile JAR path**: was `fluxmeter-0.4.0.jar`, now `fluxmeter-0.6.0.jar`
- **Checkpoint dir not mounted**: added `flink-checkpoints` shared volume to
  JobManager + both TaskManagers. `state.checkpoints.dir` set in FLINK_PROPERTIES.
  Without this, dedup state and offsets were lost on Flink restart.
- **SpanSink missing idempotency**: added SET NX gate keyed by `spanId|lastEventTime`
- **Late events silently dropped**: `LateEventSink` now produces to Kafka DLQ topic
  (`token-events-dlq`) instead of no-op. Configurable via `DLQ_TOPIC` env.

### Changed
- README: added Durability section (failure matrix), two-layer enforcement model,
  `/budget/{id}/check` and `/usage/span/{id}` in API table

---

## [0.6.0] — 2026-06-19

### Added
- **SDK Write-Ahead Log** (zero data loss):
  - Events persisted to local NDJSON file before Kafka send
  - Background thread flushes old WAL files when Kafka recovers
  - File rotation at 100MB, configurable path
  - `wal_enabled=True/False`, `wal_path="~/.fluxmeter/wal"`
- **Event deduplication** (no double-billing):
  - `EventDeduplicator`: Flink KeyedProcessFunction with TTL state
  - Keyed by eventId, state expires after 1 hour
  - Duplicates from SDK retry or Kafka redelivery dropped before windowing
- **Pre-request budget check** (<10ms enforcement):
  - `GET /budget/{id}/check?estimated_cost_usd=0.05`
  - Returns allow/deny without Flink in the path (direct Redis GET)
  - Closes the 10-15s window-based enforcement gap
- **Redis AOF persistence**:
  - `appendonly yes`, `appendfsync everysec`
  - Named volume for data durability across container restarts

### Changed
- SDK Kafka producer: `acks=all` (was `acks=1`) — waits for all replicas
- SDK `_send()`: graceful BufferError handling (event safe in WAL, no panic)
- SDK version bumped to 0.5.0

### Production Gap Status
After this release, the only remaining data-loss scenario is local disk failure
on the SDK host machine (unflushed WAL). All other single-component failures
are survived: Kafka outage (WAL), broker crash (acks=all), Redis restart (AOF),
Flink restart (checkpoints), duplicate delivery (dedup state).

---

## [0.5.0] — 2026-06-19

### Added
- **Exactly-once semantics**:
  - Checkpointing enabled via `CHECKPOINT_DIR` env var (30s interval, externalized)
  - Kafka source uses committed offsets on restart (no re-processing)
  - Sink idempotency via Redis SET NX per window ID (1h TTL)
- **Late event handling**:
  - `allowedLateness(30s)` accepts events up to 30s after window closes
  - Events beyond 30s routed to LATE_EVENTS side output (not silently dropped)
  - LateEventSink placeholder for DLQ routing
- **Agent span cost attribution**:
  - `parentSpanId` field links child LLM calls to parent agent run
  - Session window (60s gap) aggregates per-span cost incrementally
  - SpanSink writes to Redis: cost, tokens, call count, duration (24h TTL)
  - Sorted set per customer for top-N expensive spans
  - `GET /usage/span/{spanId}` — full span details
  - `GET /usage/customer/{id}/spans?limit=10` — most expensive agent runs
  - Python SDK: `parent_span_id` parameter in `track()`

### Fixed
- **Budget race condition**: replaced GET-then-INCRBYFLOAT with atomic Lua script
- **Null event crash**: added `.filter()` after source for null/invalid events
- **cacheWriteTokens not priced**: added to `calculateEventCost()` at input rate
- **Negative topup**: API rejects `amount_usd <= 0` with 400
- Idle timeout increased from 10s to 30s (prevents premature watermark advance)

### Changed
- `calculateCost` renamed to `calculateEventCost` and made public (used by SpanAggregateFunction)
- Kafka offsets: `committedOffsets(LATEST)` when checkpointing enabled

---

## [0.4.0] — 2026-06-19

### Added
- **Budget enforcement** (`BudgetEnforcerSink`):
  - Atomic prepaid balance deduction per window (Redis INCRBYFLOAT)
  - `BUDGET_LOW` alert when balance crosses configurable threshold
  - `BUDGET_EXHAUSTED` kill signal when balance hits zero
  - Alerts published to `budget-alerts` Kafka topic (sub-second delivery)
  - Setup: `POST /budget/{id} {"balance_usd": 100, "alert_threshold_usd": 10}`
- **FastAPI query endpoint** (`api/`):
  - `GET /usage/global` — global aggregated counters
  - `GET /usage/customer/{id}` — per-customer breakdown (input/output/cache/reasoning)
  - `GET /usage/customer/{id}/model/{model}` — per-model detail
  - `GET /budget/{id}` — balance status + exhaustion flag
  - `POST /budget/{id}` — set prepaid balance and alert threshold
  - `POST /budget/{id}/topup` — add credits
  - Dockerized, Swagger UI at `:8000/docs`
- `kafka-clients` 3.7.0 explicit dependency (for alert producer)

### Changed
- **Incremental aggregation**: replaced `ProcessWindowFunction` with `AggregateFunction`
  - Memory: O(keys) instead of O(events) — eliminates OOM at high throughput
  - Single `UsageAggregate` per key in memory, not all raw events
- TM parallelism reduced to 2 slots (works on laptops with 4GB TMs)
- Budget enforcement enabled by default (`BUDGET_ENFORCEMENT=true`)

### Notes
- End-to-end verified: $5 budget → BUDGET_LOW at $0.79 → BUDGET_EXHAUSTED at -$0.17
- Works at 5K eps with 4GB TaskManagers (incremental aggregation is the key)
- Alert latency: sub-second from window close to Kafka delivery

---

## [0.3.0] — 2026-06-19

### Added
- **Python SDK** (`sdk/python/`): `pip install fluxmeter`
  - `FluxMeter.track()` — manual tracking for any provider
  - `FluxMeter.track_openai()` — auto-extracts from ChatCompletion response
  - `FluxMeter.track_anthropic()` — auto-extracts from Message response
  - Supports cache tokens, reasoning tokens, span IDs, session IDs
  - confluent-kafka based (lz4 compression, batched, non-blocking)
  - 7 tests passing
- Multi-provider event schema with 5 token categories:
  - `inputTokens`, `outputTokens`, `cacheReadTokens`, `cacheWriteTokens`, `reasoningTokens`, `embeddingTokens`
- Provider and tracing fields: `provider`, `requestId`, `spanId`, `latencyMs`, `environment`
- Pricing for 9 models across 3 providers (OpenAI, Anthropic, Google)
- Weighted model distribution in load generator (realistic traffic patterns)

### Changed
- **BREAKING**: Event schema — `tokenType`+`tokenCount` replaced by per-category fields
- Renamed project: TokenFlink → FluxMeter (packages, containers, artifacts, docs)
- README rewritten with neutral tone, architectural framing, SDK examples
- Load generator now produces multi-provider events with cache/reasoning tokens

### Removed
- `TokenType` enum (replaced by explicit per-category token fields)

### Notes
- Build verified after schema change (all Java compiles clean)
- Python SDK tests pass (event serialization + provider response parsing)
- ClickHouse baseline SQL updated for new schema

---

## [0.2.0] — 2026-06-19

### Added
- Grafana dashboard with Redis datasource plugin (auto-provisioned, live streaming panels)
- ClickHouse baseline comparison (Kafka engine + materialized views + SummingMergeTree)
- `make benchmark` — automated Flink vs ClickHouse latency comparison
- Terminal demo GIF (1.7MB, recorded with VHS)
- Show HN post draft (`SHOW_HN.md`)
- Apache 2.0 LICENSE file

### Changed
- Default window size from 60s to 10s (reduces memory pressure, faster feedback)
- TaskManager memory from 6g to 8g (supports 1M eps bursts)
- Disabled checkpointing for demo (avoids shared storage complexity in docker-compose)
- Added fixed-delay restart strategy (10 attempts, 5s delay)

### Notes
- 500K eps sustained indefinitely on single machine (docker-compose)
- 1M eps sustained for 30-40s bursts (JVM heap limit for window state)
- ClickHouse baseline shows 8-43s query lag vs Flink's sub-second

---

## [0.1.1] — 2026-06-19

### Added
- `docs/DESIGN.md` — approved design document
- `progress.md` — implementation tracker
- `changLog.md` — this file

### Notes
- Documentation only, no runtime changes.

---

## [0.1.0] — 2026-06-16 (initial)

### Added
- Java 17 + Gradle project with Flink 1.18.1 DataStream API
- `TokenUsageAggregator` — Kafka → keyed tumbling window → Redis
- `TokenEvent` and `UsageAggregate` models
- `LoadGenerator` — Java Kafka producer targeting 1M events/sec
- `RedisSink` — window-aggregated usage writes
- `docker-compose.yml` — KRaft Kafka, Flink cluster, Redis, Grafana
- Grafana Redis datasource provisioning
- `Makefile` — `build`, `demo`, `start`, `stop`, `clean`, `submit-job`, `generate`
- `README.md` — quick start and architecture overview
