# FluxMeter Progress

Tracks implementation status against [docs/DESIGN.md](docs/DESIGN.md). See [changLog.md](changLog.md) for version history and [ROADMAP.md](ROADMAP.md) for forward-looking plan.

**Current version:** 2.7.0 · Python SDK **1.4.0**  
**Current phase:** Phase 3 — Path activation (v2.7) **complete** (npmjs.org push if/when `npm login`)  
**Design status:** APPROVED (2026-06-16)  
**Research:** [docs/industry-billing-research-2026.md](docs/industry-billing-research-2026.md) · plan: [ROADMAP.md](ROADMAP.md)

## Phase 3 Checklist (ROADMAP v2.7 path activation) — **done**

| Item | Status |
|------|--------|
| Mid-stream kill demo (GIF + thin proxy/SDK path) | Done — `demos/path_activation_demo.py` (+ `--live`); StreamKilledError in wrap |
| Wrap SDK `wrap(OpenAI())` fail-open (Python) | Done — SDK 1.4.0 `fluxmeter.wrap` on PyPI |
| npm publish `@fluxmeter/client` | Pack-ready **1.3.0**; registry push blocked without `npm login` (see sdk/js/README) |
| Lite budget webhook (no Kafka dependency) | Done — `webhook_deliver` on Lite `/ingest` |
| Light hierarchy caps (parent span/session at `check`) | Done — `POST /budget/{id}/cap` |
| Soft alert thresholds (70% / 90% warn) | Done — `BUDGET_WARN` with `warn_pct` 70/90 |

## Phase 4 Checklist (ROADMAP v2.8 complementary export)

| Item | Status |
|------|--------|
| Metronome / Orb / Stripe production exporters | Not started (Stripe stub exists) |
| Partner docs (`metronome.md` / `orb.md` / `stripe.md`) | Not started |
| Agent hierarchy budgets (parent→child reserve) | Not started |
| Per-key / API-key budgets | Not started |
| Feature / workflow metadata dims | Not started |

## Phase 2 Checklist (ROADMAP v2.4–2.6 billing depth) ✓

| Item | Status |
|------|--------|
| Tiered pricing in engine (Lite + Flink) | Done (2.4.0) |
| Stripe Checkout wiring (control plane) | Done (2.5.0) |
| Calendar-aligned billing windows (rollup month + export period) | Done (2.5.0) |
| Cost-based Stripe export (`STRIPE_EXPORT_MODE=cost`) | Done (2.5.0) |
| Credits / prepaid token packages | Done (2.5.0) |
| Period / day / session billing queries | Done (2.6.1) |
| Lite span aggregation (`parentSpanId`) | Done (2.6.2) |

## Phase 1 Checklist (ROADMAP v2.3 polish) ✓

| Item | Status |
|------|--------|
| README / SHOW_HN version sync | Done |
| Official website links in docs + SDK metadata | Done |
| `make test-unit` + `make test-unit-redis` | Done |
| OpenAPI 2.2.x + `validate-spec.sh` | Done |
| Lite `tenantId` key isolation + E2E | Done |
| `AggregationKeys` + `make test-java` | Done |
| Local load-test Mac ceiling docs | Done |

---

## Phase Overview

| Phase | Scope | Status |
|-------|-------|--------|
| Weekend 1 | Core pipeline, load gen, Grafana, ClickHouse baseline | Done |
| Weekend 2 | Python SDK + event schema upgrade + README polish | Done |
| Weekend 3 | Budget enforcer + kill signals + credits drawdown + API | Done |
| Week 4 | Exactly-once, span attribution, code review fixes | Done |
| Week 4b | Production hardening (WAL, persistence, dedup, pre-request check) | Done |
| Week 4c | Remaining gaps (checkpoints wiring, DLQ, span dedup, docs) | Done |
| Week 4d | Architectural review fixes (dedup OOM, late data loss, atomicity, span overwrite) | Done |
| Week 4e | Rate limiting, load test (1M eps), requirements verification | Done |
| Week 4f | Streaming metering + retroactive re-rating | Done |
| Week 4g | HTTP ingest endpoint + e2e verification | Done |
| Week 4h | Performance optimization (OptimizedRedisSink, batching, hash consolidation) | Done |
| Week 4i | Integration tests (15 scenarios, 15 passed) | Done |
| v1.2–v2.0 | Billing path, pricing catalog, reconciliation, Helm | Done |
| v2.2.x | Control plane scaffold, polish, tests | Done |
| v2.4–2.6 | Tiered pricing, billing export/packages, period/span queries, China models | Done |
| **v2.7 Phase 3** | Path activation: kill demo, wrap, webhook, hierarchy, soft warns | **Done** |
| v2.8 Phase 4 | Metronome/Orb exporters + agent hierarchy budgets | **Active / next** |
| v3.0 Phase 5 | Gateway path (meter + limit + mid-flight kill) | Planned |
| v3.1+ Phase 6 | Multi-tenant SaaS RBAC (demand-gated) | Planned |
| Distribution | Python **1.4.0 on PyPI**; JS SDK **1.3.0** pack-ready | Partial (npmjs pending auth) |

---

## Week 4 Checklist

| # | Task | Status | Notes |
|---|------|--------|-------|
| 1 | Code review (critical findings) | Done | Budget race (Lua), null filter, cacheWrite pricing, negative topup |
| 2 | Exactly-once + checkpointing | Done | CHECKPOINT_DIR env, committed offsets, externalized state |
| 3 | Sink idempotency | Done | Redis SET NX per window ID, 1h TTL |
| 4 | Late event handling | Done | allowedLateness(30s) + side output for beyond-30s |
| 5 | Agent span cost attribution | Done | parentSpanId, session windows, SpanSink, API |

---

## Weekend 1 Checklist

| # | Task | Status | Notes |
|---|------|--------|-------|
| 1 | Init repo: Java 17, Gradle, Flink 1.18 | Done | `build.gradle`, Gradle wrapper |
| 2 | `docker-compose.yml`: Kafka (KRaft), Flink JM + 2 TMs, Redis, Grafana | Done | + ClickHouse added |
| 3 | `TokenUsageAggregator` Flink job | Done | Keyed by `(customer_id, model_id)`, 10s tumbling window |
| 4 | Load generator (Java Kafka producer) | Done | 1M eps sustained, 4 threads, rate-limited |
| 5 | Redis sink (window-aggregated writes) | Done | Pipelined post-aggregation writes |
| 6 | Grafana dashboard | Done | Auto-provisioned with Redis datasource plugin |
| 7 | ClickHouse naive baseline | Done | Kafka engine + materialized views, 8-43s lag proven |
| 8 | `make demo` one-command startup | Done | Build, start infra, submit job, run generator |
| 9 | Terminal demo GIF + README polish | Done | VHS recording, HN-ready README |
| 10 | HN launch post | Done | `SHOW_HN.md` drafted |

---

## Weekend 2 Checklist

| # | Task | Status | Notes |
|---|------|--------|-------|
| 1 | Rename project TokenFlink → FluxMeter | Done | All packages, containers, docs renamed |
| 2 | Upgrade event schema to multi-provider format | Done | 9 models, 5 token categories, tracing fields |
| 3 | Python SDK (`pip install fluxmeter`) | Done | **1.1.0 on PyPI**; `track()`, `track_openai()`, `track_anthropic()`, 7 tests |
| 4 | README tone rewrite | Done | Neutral framing, architectural comparison, SDK examples |
| 5 | FastAPI query endpoint | Done | Usage + budget CRUD; v2.6.1 adds period/day/session billing queries |

---

## Weekend 3 Checklist

| # | Task | Status | Notes |
|---|------|--------|-------|
| 1 | BudgetEnforcerSink | Done | Atomic Lua script for balance deduction + threshold check |
| 2 | FastAPI query endpoint | Done | /usage/global, /usage/customer/{id}, period/day/session, span, /budget/{id} |
| 3 | Incremental aggregation (AggregateFunction) | Done | Fixed OOM — O(keys) memory instead of O(events) |
| 4 | End-to-end verification | Done | $5 budget → exhausted → kill signal in Kafka |
| 5 | Re-record demo GIF | Done | 1.3MB, shows API + budget enforcement |

---

## Success Criteria (Build)

| Criterion | Target | Status |
|-----------|--------|--------|
| Throughput | 500K+ eps sustained; 1M+ target | **Done** — 500K indefinite, 1M for 30-40s bursts |
| Aggregation latency | p99 < 500ms Kafka → Redis | **Done** — sub-second (10s window) |
| Demo GIF | Terminal recording | **Done** — 1.3MB GIF via VHS |
| Python SDK | 3-line integration | **Done** — `meter.track_openai(...)` |
| Multi-provider schema | OpenAI + Anthropic + Google | **Done** — 9 models, per-category pricing |
| Budget enforcement | Real-time balance deduction + alerts | **Done** — atomic Lua, BUDGET_LOW + EXHAUSTED |
| Query API | REST endpoints for usage + budget + spans | **Done** — period/day/session + span; FastAPI :8000/docs |
| Exactly-once | No double-counting on replay | **Done** — checkpointing + SET NX idempotency |
| Agent span attribution | Cost rollup per agent run | **Done** — session windows + SpanSink |
| Zero data loss | Events survive any single-component failure | **Done** — WAL + AOF + acks=all + dedup |
| Pre-request guardrail | <10ms budget check before LLM call | **Done** — GET /budget/{id}/check |
| Rate limiting | Per-customer requests/minute cap | **Done** — max_rpm config, sliding window |
| Load test 1M eps | Sustained throughput at scale | **Done** — 1M eps, 437 MB/s, TMs stable |
| Streaming mid-response | Budget safety + observability during stream | **Done** — reserve/reconcile + SDK heartbeat wrapper |
| Retroactive re-rating | Adjust costs after price change | **Done** — differential adjustment (preview + apply) |

---

## Open Questions (from design)

| Question | Status |
|----------|--------|
| ClickHouse vs Postgres baseline | Resolved — ClickHouse chosen and implemented |
| Real OpenAI API shapes vs synthetic events | Resolved — SDK auto-extracts from real responses |
| GitHub org vs personal account | Unresolved |
| License (Apache 2.0 vs AGPL) | Resolved — Apache 2.0 |
| Open source timing | Ready — v0.5 has all core differentiators |

---

## Recent Activity

- **2026-07-06** — **Phase 3 closed**: soft `BUDGET_WARN` 70/90 ladder; Python SDK **1.4.0** published to PyPI; npm pack ready (npmjs needs login).
- **2026-07-06** — **v2.7.0 Phase 3 path activation**: Lite webhooks (no Kafka); Python `wrap()` + HTTP meter + stream kill; hierarchy caps at `/check`; `demos/path_activation_demo.py`; JS SDK 1.3.0.
- **2026-07-06** — **优先级重排 + 行业校准**：[ROADMAP.md](ROADMAP.md) 下一主线改为 Phase 3 **Path activation**（kill demo / wrap SDK / npm / Lite webhook），exporters + hierarchy budgets 为 Phase 4，Gateway 产品化 Phase 5，Full SaaS RBAC **后移并 demand-gated**；调研报告 [docs/industry-billing-research-2026.md](docs/industry-billing-research-2026.md)（Cursor/Copilot、LiteLLM、Kong/OpenMeter、SpendGuard、Salesforce Flex、Anthropic spend limits、转售 wallet 等）。
- **2026-07-06** — **战略定位 vs Metronome/Stripe/Orb**（保留）：runtime 蓝海、complement don’t replace、不抢 invoice/contract/payment SoR；叙事杠杆提前到 v2.7。
- **2026-07-05** — **客户故事文档**：[`docs/customer-stories-lite.md`](docs/customer-stories-lite.md) — TokenBridge / ClipLive SaaS 风格 Use Case + 4 周并行实施方案。
- **2026-07-05** — **v2.6.2 Lite span**：`parentSpanId` ingest → `increment_span` + E2E tests；客户 B 剪辑任务可 `GET /usage/span/{job_id}`。
- **2026-07-05** — **客户接入文档**：[`docs/customer-integration-lite.md`](docs/customer-integration-lite.md) — Token 中转站 + 直播 AI 剪辑 Lite 实施方案；Review 缺口（webhook、metadata、双账本、Python HTTP SDK）。

- **2026-07-05** — **v2.6.1 regression**: billing query E2E tests; Flink tier-pricing fix (`MonthlyVolumeStampFunction`); Dockerfile `usage_buckets.py`; test harness IPv4 + expanded `make test-unit` / `run-e2e-all.sh`.
- **2026-07-05** — **v2.6.0 Chinese domestic models**: 20-model pricing catalog, SDK `track_*()` for 8 providers, contrib provider docs + `china-2026-07.json` reference.
- **2026-07-04** — **v2.5.0 Phase 2 complete**: Stripe export modes, prepaid packages, Checkout, rollup month buckets, hybrid docs; tag v2.5.0.
- **2026-07-04** — **v2.4.0 tiered pricing**: flat/volume/graduated in Lite + Flink; `contrib/pricing/tiered-example.json`.
- **2026-07-04** — **Phase 1 closed**: HTTP tenant E2E in `test_lite_production.py`; doc version sync (`production-deploy.md`, `load-testing.md` → 2.2.2); ROADMAP Phase 1 table marked complete. Hotfix: Dockerfile `tenant_keys.py`, Lua balance string return.
- **2026-07-04** — **v2.2.2 Phase 1 polish**: `make test-unit` expanded (billing, control-plane models, tenant_keys + Java); `make test-unit-redis` for lite Lua + rollup; OpenAPI health `mode`, lite ingest responses, `link-stripe`; `api/tenant_keys.py` + lite Lua `tenantId` isolation; `validate-spec.sh` content checks; load-test Mac ceiling note.
- **2026-06-24** — **SHOW_HN.md** synced to v2.2.1: Lite-first narrative, honest throughput numbers, SaaS/Stripe/PyPI caveats; title hook shifted from 1M eps to <10ms budget check.
- **2026-06-22** — **ROADMAP.md**: project-wide forward plan (v2.3 polish → v2.4 tiered pricing → v3.0 SaaS → streaming proxy).
- **2026-06-22** — **v2.2.1 CTO follow-up (tests + docs)**: `AggregationKeys` utility + JUnit suite; Python unit tests for lite Lua aggregator and control-plane models; `scripts/run-e2e-all.sh` (unit → lite → full → SaaS); [docs/control-plane-api.md](docs/control-plane-api.md). Prior: Prometheus reporter, DR runbook, Flink `tenantId` key isolation.
- **2026-06-22** — **Phase 5 dual-path**: SaaS control plane (`services/control-plane/`) — tenant CRUD, plan tiers, API key provisioning, usage endpoint; `docker-compose.saas.yml` + `make start-saas`. Version 2.2.0.
- **2026-06-22** — **Phase 4 dual-path**: Stripe billing export (`billing_export.py`) reports hourly event counts to Stripe Meters API when `STRIPE_API_KEY` is set; admin `POST /admin/billing/{id}/link-stripe`; unit tests with mocked Stripe.
- **2026-06-22** — **Phase 3 dual-path**: Background rollup worker (`rollup_worker.py`) compacts live counters into per-minute Redis hashes with 24h TTL; wired into API startup in lite mode; tests in `test_rollup.py`.
- **2026-06-22** — **Phase 2 dual-path**: Atomic Lua lite aggregator (`lite_aggregate_lua.py`) with inline budget deduction; production tests (`test_lite_production.py`); lite `/ingest` returns cost/balance JSON. Version 2.1.0.
- **2026-06-22** — **Phase 1 dual-path**: Lite promoted to default (`docker-compose.yml`, `make demo`); full Flink stack in `docker-compose.full.yml` (`make demo-full`, `make start-full`). Makefile aliases `demo-lite`/`start-lite`; added `test-lite`.
- **2026-06-22** — **v2.0.2**: Budget API 500 fix (`_fetch_customer_budget`); docker-compose.full.yml scaled to 3 TM / Redis 4G / Kafka 24 partitions for 100K–1M local load test profile.
- **2026-06-21** — **v2.0.1**: E2E suite (`test_e2e_v2.py`), staged `scripts/load-test.sh`, Flink `UsageAggregateFunction` fix (job submit on 1.18), customer-key 403 regression fix.
- **2026-06-21** — **v2.0.0**: Helm chart, tiered pricing schema, Prometheus alerts. v1.4 reconciliation + DLQ replay. v1.3 pricing catalog. v1.2 single-path billing, customer keys, webhooks.
- **2026-07-04** — **Open-source launch polish**: README top block from `fluxmeter-web` PyPI snippet; PyPI keywords/description aligned; `scripts/set-github-topics.sh` for repo topics (`llm-billing`, `token-metering`, `ai-agents`, …).
- **2026-06-21** — **PyPI**: `fluxmeter==1.0.0` published — https://pypi.org/project/fluxmeter/
- **2026-06-21** — Code review fixes #1–#4: WAL per-event ack + flush drain, Redis password wiring, Flink checkpoint volume permissions. Version 1.0.0-rc3.
- **2026-06-21** — Code review remediation (15 findings): pricing fix, model normalization, WAL dedup, atomic BudgetEnforcerSink, API auth, docker-compose.prod.yml. Version 1.0.0-rc2.
- **2026-06-20** — Fixed 10 production issues: SHA-256 idempotency (collision-safe), Lua threshold semantic (initial balance), WAL batch fsync, schema compatibility, float→microdollars (long), re-rate async, session window docs. Version 1.0.0-rc1.
- **2026-06-20** — Three-layer resilient budget check: in-process cache (0.01ms) → Redis (1-5ms) → fail policy (open/closed). Hot path never blocks on infra failure. Version 0.9.1.
- **2026-06-20** — Week 4i: integration test suite (10 scenarios, 14 passed). Correctness verified: budget accuracy, idempotency, rate limits, pricing, re-rating, spans, HTTP ingest, zero-token edge case.
- **2026-06-20** — Week 4h: OptimizedRedisSink — hash consolidation (10x fewer keys), batched writes (5x fewer ops), compact idempotency (6x less memory), local global aggregation (50x fewer hotspot writes). Version 0.9.0.
- **2026-06-20** — Week 4g: HTTP ingest endpoint (POST /ingest, POST /ingest/batch). E2E verified: 511 events via HTTP → Kafka → Flink → Redis → API. Zero SDK/Kafka client dependency for integrators. Version 0.8.1.
- **2026-06-20** — Week 4f: streaming mid-response metering (reserve/reconcile + SDK heartbeat wrapper) and retroactive re-rating (differential adjustment via preview/apply). All 10/10 requirements complete. Version 0.8.0.
- **2026-06-20** — Week 4e: rate limiting added to guardrail endpoint (max_rpm). Load tested: 10K→50K→100K→500K→1M eps all stable. Version 0.7.0.
- **2026-06-20** — Week 4d: architectural review fixes. Removed Flink dedup operator (OOM at production throughput), removed allowedLateness (caused silent data loss with SET NX), made counter+budget atomic (Lua script), SpanSink overwrite instead of increment (session merge double-count). Version 0.6.2.
- **2026-06-20** — Week 4c: wiring fixes. Checkpoint dir mounted (shared volume for JM+TMs), Makefile JAR path fixed, SpanSink idempotency, late event DLQ (Kafka producer), README updated with durability matrix and two-layer enforcement. Version 0.6.1.
- **2026-06-19** — Week 4b: production hardening. SDK WAL (zero data loss on Kafka outage), Redis AOF persistence, Kafka acks=all, event deduplication (Flink keyed state), pre-request budget check endpoint (<10ms). Version 0.6.0.
- **2026-06-19** — Week 4: exactly-once semantics (checkpointing + SET NX idempotency), late event handling (side output), agent span cost attribution (parentSpanId + session windows + SpanSink + API). Fixed code review P1s (budget race via Lua, null filter, cacheWrite pricing, negative topup). Version 0.5.0.
- **2026-06-19** — Weekend 3: budget enforcement (BudgetEnforcerSink with BUDGET_LOW/EXHAUSTED alerts), FastAPI query endpoint (usage + budget CRUD), incremental aggregation fix (OOM prevention). End-to-end verified: $5 budget → exhausted → kill signal. Version 0.4.0.
- **2026-06-19** — Weekend 2 work: renamed to FluxMeter, upgraded event schema (multi-provider, per-category tokens, tracing), built Python SDK with OpenAI/Anthropic auto-extraction (7 tests), rewrote README with neutral tone. Version 0.3.0.
- **2026-06-19** — Weekend 1 complete. Core pipeline (1M eps), Grafana dashboard, ClickHouse baseline, demo GIF, README polish, Show HN post drafted. Version 0.2.0.
