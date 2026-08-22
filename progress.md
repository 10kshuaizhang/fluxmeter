# FluxMeter Progress

Tracks implementation status against [docs/DESIGN.md](docs/DESIGN.md). See [changLog.md](changLog.md) for version history and [ROADMAP.md](ROADMAP.md) for forward-looking plan.

**Current version:** 4.8.2 · Python SDK **2.0.0**
**Current phase:** Metering custody/performance hardening (**active**) · Pillar B Intelligence (**complete/demand-gated**) · Phase G Gateway (**done**)
**Design status:** APPROVED (2026-06-16) · Intelligence pivot APPROVED (2026-07-11)  
**Research:** [docs/industry-billing-research-2026.md](docs/industry-billing-research-2026.md) · plan: [ROADMAP.md](ROADMAP.md) · pivot: [docs/superpowers/specs/2026-07-11-intelligence-pivot-design.md](docs/superpowers/specs/2026-07-11-intelligence-pivot-design.md)

## Phase 5 Checklist (Intelligence MVP — Pillar B) — **done**

| Item | Status |
|------|--------|
| Root Cause Analysis (model / agent / team / customer drill-down) | Done — `GET /intelligence/root-cause` + `root_cause.py` |
| Unit Economics (revenue vs cost, margin, loss alerts + recommendations) | Done — `GET /intelligence/unit-economics` + revenue store |
| Scenario Simulation (≥3 what-if scenario types) | Done — model switch, prompt reduction, token grant |
| Dual-source ingest: FluxMeter native + overlay (OpenMeter or Langfuse) | Done — native reader + OpenMeter overlay connector |
| Prescriptive Finance/CEO summary page | Done — unit economics recommendations + root-cause narrative |
| Landing page realignment (Layer 4 narrative) | Done — README + docs/intelligence-api.md |

## Phase 6 Checklist (Intelligence v1.0 — Pillar B) — **done**

| Item | Status |
|------|--------|
| Pricing Optimizer (recommendations + ROI) | Done — `GET /intelligence/pricing-recommendations` |
| Profitability Dashboard (margin + trends) | Done — `GET /intelligence/profitability` |
| Anomaly Alerts + webhook workflow | Done — `intel_alert_worker.py` + `POST /intelligence/alerts/webhook` |
| Basic Forecasting (spend vs budget) | Done — `GET /intelligence/forecast` |
| Export / Sharing (Finance report) | Done — `GET /intelligence/report?format=markdown` |

## Phase G Checklist (Gateway P1 — Pillar A) — **done**

| Item | Status |
|------|--------|
| Streaming AI proxy (OpenAI-compatible, auto-ingest) | Done — `POST /v1/chat/completions` on `:8080` |
| Pre-request budget check (deny before upstream) | Done — HTTP 402, zero upstream calls when exhausted |
| Mid-response stream kill | Done — `stream_guard.py`, <1s in unit tests |
| Docker / compose gateway service | Done — `docker-compose.yml` + full `api/` Dockerfile |
| Docs + demo | Done — `docs/gateway.md`, `demos/gateway_demo.py`, `make demo-gateway` |

## Metering maintenance (Pillar A — ongoing)

| Item | Status |
|------|--------|
| Single HTTP→Kafka→Flink correctness regression | Done — Tencent Kafka pause preserved no-false-202/idempotency; targeted identity expiry fixed Redis backlog starvation; heartbeat watermarks materialized silent windows in 17s; TaskManager restart recovered 14/14 tasks with exact Redis 2/56 and two audit rows |
| HTTP custody contract | Done — tenant-scoped compact identity, ACK+finalize `202`, uncertain late reconciliation, bounded backpressure, per-item batch outcomes |
| Four deep metering modules | Done — Custody/Pricing/Reservation/Budget interfaces; shallow helpers removed; atomic Gateway Reservation open |
| Reserve → meter → kill → audit proof | Done — `make demo-proof`; live Gateway/Kafka/Flink/Redis/ClickHouse assertions, no provider key |
| Public HTTP throughput evidence | Partial — the 35-minute independent-host Custody run accepted 18,063,140 events at 10,034.90 eps, p50 36ms / p99 173ms, with zero rejection/transport error. It passed the 50/200ms SLO but the strict verdict failed on 26,949 generator drops. Fixing benchmark Job parallelism from 2 to 12 raised a full-stack 60-second sample to 7,772 eps and left only 333 lag; a split-Redis A/B peaked at 9,197 eps but missed latency. The 100K batch stage reached 32,567 eps, so sustained full-pipeline gates remain open |
| Pricing catalog + exporter maintenance | Ongoing |
| Python SDK + JS npm publish | Partial — HTTP-only 2.0.0 packages ready for release |
| Phase G Gateway proxy (side track, non-blocking) | Done — `gateway_app.py` + `:8080` + docs/gateway.md |

## Foundation F4 Checklist (v2.8 complementary export) — **done**

| Item | Status |
|------|--------|
| Metronome / Orb / Stripe production exporters | Done — `billing_export.py` + `BILLING_EXPORT_TARGETS` |
| Partner docs (`metronome.md` / `orb.md` / `stripe.md`) | Done — `docs/integrations/` |
| Agent hierarchy budgets (parent→child reserve) | Done — `reserve?parent_span_id=` + Lua |
| Per-key / API-key budgets | Done — admin budget endpoint + `/check` |
| Feature / workflow metadata dims | Done — `billing_dims.py` + `GET /usage/dim` |
| Open token-event interop | Done — `spec/schema/external-export-mappings.md` |

## Foundation F3 Checklist (v2.7 path activation) — **done**

| Item | Status |
|------|--------|
| Mid-stream kill demo (GIF + thin proxy/SDK path) | Done — `demos/path_activation_demo.py` (+ `--live`); StreamKilledError in wrap |
| Wrap SDK `wrap(OpenAI())` fail-open (Python) | Done — SDK 1.4.0 `fluxmeter.wrap` on PyPI |
| npm publish `@fluxmeter/client` | Pack-ready **1.3.0** + `.github/workflows/npm-publish.yml`; set repo secret `NPM_TOKEN` then run workflow |
| Lite budget webhook (no Kafka dependency) | Done — `webhook_deliver` on Lite `/ingest` |
| Light hierarchy caps (parent span/session at `check`) | Done — `POST /budget/{id}/cap` |
| Soft alert thresholds (70% / 90% warn) | Done — `BUDGET_WARN` with `warn_pct` 70/90 |

## Foundation F2 Checklist (v2.4–2.6 billing depth) ✓

| Item | Status |
|------|--------|
| Tiered pricing in engine (Lite + Flink) | Done (2.4.0) |
| Stripe Checkout wiring (control plane) | Done (2.5.0) |
| Calendar-aligned billing windows (rollup month + export period) | Done (2.5.0) |
| Cost-based Stripe export (`STRIPE_EXPORT_MODE=cost`) | Done (2.5.0) |
| Credits / prepaid token packages | Done (2.5.0) |
| Period / day / session billing queries | Done (2.6.1) |
| Lite span aggregation (`parentSpanId`) | Done (2.6.2) |

## Foundation F1 Checklist (v2.3 polish) ✓

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
| **F3 v2.7** | Path activation: kill demo, wrap, webhook, hierarchy, soft warns | **Done** |
| F4 v2.8 | Metronome/Orb exporters + agent hierarchy budgets | Done |
| **Phase 5** | Intelligence MVP: root cause, unit economics, simulation, overlay | **Done** |
| Phase G | Gateway proxy meter+limit+kill (metering side track) | **Done** — 3.2.0 |
| **Phase 6** | Intelligence v1.0: pricing optimizer, profitability, alerts, forecast, export | **Done** |
| Phase 7+ | Hosted SaaS, NL agent, enterprise RBAC (demand-gated) | Planned |
| Distribution | Python **1.4.0 on PyPI**; JS SDK **1.3.0** pack-ready | Partial (npmjs pending auth) |

---

## Week 4 Checklist

| # | Task | Status | Notes |
|---|------|--------|-------|
| 1 | Code review (critical findings) | Done | Budget race (Lua), null filter, cacheWrite pricing, negative topup |
| 2 | Exactly-once + checkpointing | Done | CHECKPOINT_DIR env, committed offsets, externalized state |
| 3 | Sink idempotency | Done | Redis SET NX per window ID, 1h TTL |
| 4 | Late event handling | Done | No allowedLateness (avoids SET NX conflict); sideOutputLateData → Kafka DLQ; 5s OOO + 15s source idleness + dedicated non-billable watermark heartbeat |
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
| Throughput | HTTP 10K single / 100K batch sustained | **Partial** — 30-minute Custody throughput crossed 10K at p50 36ms / p99 173ms, but 0.149% generator offers dropped and downstream lag reached 13.66M. With the benchmark corrected to p12, full-stack 60s single reached 7,772 eps with near-zero end lag; split Redis peaked at 9,197 eps. Batch reached 32,567 eps. Neither sustained full-pipeline gate is passed |
| Event-to-billing latency | p99 < 20s accepted HTTP → Redis | **Done (steady state)** — 200/200 v4.8.2 events materialized on the retained p12 stack; p50 466ms, p99 897ms, max 910ms. Backlog recovery is reported separately and is not represented as steady-state latency |
| Demo GIF | Terminal recording | **Done** — 1.3MB GIF via VHS |
| Python SDK | 3-line integration | **Done** — `meter.track_openai(...)` |
| Multi-provider schema | OpenAI + Anthropic + Google | **Done** — 9 models, per-category pricing |
| Budget enforcement | Real-time balance deduction + alerts | **Done** — atomic Lua, BUDGET_LOW + EXHAUSTED |
| Query API | REST endpoints for usage + budget + spans | **Done** — period/day/session + span; FastAPI :8000/docs |
| Effectively-once | No double billing across retry/replay | **Done** — tenant retry identity + short Flink safety dedup + sink SET NX; Kafka pause and TaskManager restart fault probes preserved exact counts |
| Agent span attribution | Cost rollup per agent run | **Done** — session windows + SpanSink |
| Zero accepted-event loss | Events with `202` survive fault injection | **Done** — Tencent Kafka pause/recovery, Redis freeze/retry, and TaskManager restart probes retained accepted events without double billing |
| Pre-request guardrail | <10ms budget check before LLM call | **Done** — GET /budget/{id}/check |
| Rate limiting | Per-customer requests/minute cap | **Done** — max_rpm config, sliding window |
| Internal 1M benchmark | Engine-only burst, never an HTTP claim | Historical — retained as regression context, not a current release gate |
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

- **2026-08-22** — **v4.8.2 formal cloud follow-up**: revised the single-event Custody SLO to p50 ≤50ms / p99 ≤200ms (25/100ms stretch) and raised generator slots to 4,000. The 5m+30m run accepted 18,063,140 events at 10,034.90 eps with p50 36ms / p99 173ms and no rejection/transport error, but 26,949 generator offers dropped and full-pipeline lag reached 13.66M. Diagnosed Redis OOM as a second 30-day one-key-per-event projection registry; bounded it to the 600-second Flink crash-safety horizon, drained all accepted events, removed only benchmark projection markers, and verified a live 576-second TTL. Corrected the benchmark's hidden `-p 2` submission to configurable p12: the full-stack sample improved from 7,369 to 7,772 eps and ended with only 333 lag, while a temporary split-Redis A/B peaked at 9,197 eps but still missed latency and was removed. Batch reached 32,567 eps, so sustained pipeline gates remain open. Final p12 steady-state accepted-to-Redis projection latency passed at p99 897ms across 200/200 events. Known-cost billing and ClickHouse A1–A7 passed; added Redis-loaded startup gating after live AOF recovery exposed a permanent Flink failure mode.
- **2026-08-22** — **v4.8.1 ingress/checkpoint hardening**: made optional anonymous authentication an async no-Redis fast path, moved scoped identity reads off the event loop, removed FastAPI dependency-graph work from the two ingest routes, and registered the intelligence router after ingress. The final independent-host short diagnostic offered 10.05K eps and accepted all 301,502 events at 10,034.95 eps with no drops/errors; p50 29ms / p99 134ms still failed the latency gate, so no 35-minute claim was made. Removed an unused global `keyBy("global").reduce(...)` branch whose state alone reached 465 MiB and triggered checkpoint failure; the replacement job had 12 rather than 14 tasks and its initial checkpoint fell from about 503 MiB to 21.99 MiB. RocksDB and dedicated-Custody-Redis experiments were reverted after worsening latency.
- **2026-08-22** — **independent-host v4.8.0 gate**: added an 8C16G same-VPC Java load host and swept 10–14 API workers plus 1–5ms batch windows. Best safe short run: 9,814.66 accepted eps at 12 workers / 2ms, p50 49ms, p99 151ms, zero rejection/transport error. A dedicated Custody Redis was explicitly disproved (9,710.71 eps) and removed; 13 workers also regressed. Final restored profile remained healthy and measured 9,740.31 eps / p99 142ms. The 35-minute gate was correctly skipped because the deterministic short gate still misses throughput and latency requirements.
- **2026-08-22** — **v4.8.0 Custody microbatch + Java HTTP gate**: single `/ingest` requests now enter a bounded 64-item, 1ms cross-request batcher inside the Custody module; tenant contexts never mix and 202 still follows Kafka ACK plus identity finalize. Replaced the single-event Python/httpx gate with a Java 17 open-loop runner that counts offered/completed/accepted/rejected/dropped traffic and fails on throughput or latency. Tencent 16C32G safe tuning reached 7,812.72 eps on 8 API workers (p50 192ms, p99 372ms), up 3.10x from the 2,522.73 eps Java baseline; a cleanup-disabled diagnostic reached 8,711.62 eps but was reverted. The co-located generator used 3.3–5.2 CPU cores, so publication-grade 10K evidence still requires a separate same-VPC load host.
- **2026-08-22** — **v4.7.2 fault-liveness fixes**: custody claims now expire the requested identity directly even behind >205K older shard members. Flink consumes a dedicated one-partition non-billable heartbeat topic so 15-second-idle business partitions cannot pin event-time windows; a cloud single-event/no-follow-up probe materialized exactly 1 event / 26 tokens in 17 seconds. The final TaskManager restart rerun restored 14/14 tasks and materialized the exact 2-event/56-token aggregate 17 seconds after the post-recovery event, with exactly two ClickHouse audit rows. Added focused Python/Java regressions and retained standard late-event routing semantics.
- **2026-08-22** — **v4.7.1 honest Linux benchmark harness**: distributed single-event offered load across multiple generator processes, replaced unbounded latency samples with mergeable millisecond histograms, added a benchmark-vs-production custody capacity model, and bounded the benchmark identity window to 5 minutes with a 6 GiB Redis data limit. Corrected Tencent 16C32G single short gate: 1,318.85 eps, p50 402ms, p99 3018ms. Batch staircase placed the p99<500ms boundary near 30K eps (29,586 eps / p99 423ms); 35K degraded to p99 894ms, and the safe 100K smoke delivered 25,645 eps / p99 5.7s. Production retention projections remain multi-TiB; both formal release gates stay open.
- **2026-08-22** — **Tencent fault injection**: Kafka pause passed (`503 custody_uncertain` → idempotent `202`, one audit row). Redis freeze failed retry liveness because a pending event ranked behind 203,581 expired shard identities while cleanup removes only 64/request. TaskManager restart restored checkpoint 83 and preserved both audit events, but the recovered event-time window did not materialize during 150 seconds of idle traffic; a later event advanced the watermark and produced the exact 2-event/56-token aggregate.
- **2026-08-22** — **v4.7.0 executable metering proof**: added deterministic `make demo-proof` path through Gateway → Custody → Kafka → Flink → Redis plus ClickHouse cold audit; Gateway now returns Reservation headers and an SSE metering receipt. Live Mac run held $0.000078, killed at 301 output tokens / $0.000181, settled to $0 held / $0.000819 balance, and matched the raw audit row. Verification: 104 API/core + 17 SDK Python tests, JS build, 52 Java tests, spec checks, and JAR passed.
- **2026-08-22** — **v4.6.0 deep modules**: replaced shallow Custody/Pricing/Reservation/Budget call graphs with four interfaces; fixed tenant cache/RPM leakage, hierarchy held-cost omission, pricing validation drift, and Gateway orphan-hold crash window; added shared Python/Java tenant scope keys and ADR-026. Python regression 115 passed (3 host-Redis skips) plus Redis-backed 5/5; clean Java suite and shadow JAR passed.

- **2026-08-17** — **v4.5.0 metering hardening**: compact tenant-sharded 30-day custody identity; ACK+finalize `202`; pending/uncertain state machine with late reconciliation; async bounded Kafka dispatcher; per-item batch validation/backpressure; 10-minute Flink safety dedup; open-loop HTTP gate artifacts. Python regression 97 passed and Java tests passed. Mac diagnostics did not pass 10K/100K; Linux release evidence remains pending.

- **2026-08-16** — **v4.4.1**: README + OpenAPI + api-reference synced to live routes/Custody (docs-only).

- **2026-08-16** — **v4.4.0**: ADR-025 Cold Store (`raw_events` + DLQ, Trusted Envelope); closed stale PR #2; `make test-cold-store`.

- **2026-08-16** — **v4.3.0**: UsageQuery (tenant lifetime reads + rerate) + ProxiedCompletion; stream ≥400 settles Reservation.

- **2026-08-16** — **v4.2.0**: delete Lite orphans + OptimizedRedisSink; Pricing Catalog golden vectors; RollupStore period-customer index (Flink SADD + SCAN fallback).

- **2026-08-16** — **v4.1.1**: Intelligence `/forecast` passes API-key `tenant_id` into budget read (completes tenant Budget seam for forecast).

- **2026-08-16** — **v4.1.0 architecture deepen**: Token Event Custody (`accept`/`accept_many`) shared by HTTP+Gateway; Reservation contract + `expire_reservations`; tenant Budget key alignment with legacy read fallback; `CONTEXT.md` glossary.

- **2026-08-16** — **v4.0.1 HTTP batch performance**: concurrent Kafka ACK collection plus one-command Redis batch identity operations; clean Docker E2E passed 27/27. Measured 291.20 single events/s and 14,006.89 batch events/s without failures at stable concurrency; 10K/100K release gates remain open because the single Redis idempotency store saturates at batch c100.

- **2026-08-16** — Live v4 verification: all 16 integration and 11 v2 E2E cases passed after contract updates; fixed Compose Flink submission/checkpoint initialization. HTTP ingress measured 205.81 eps single and 185.91 eps at batch-size 100/concurrency 10; the 10K/100K release gates remain unmet.
- **2026-08-15** — **v4.0.0 single path**: public HTTP custody with trusted envelopes and 30-day event identity; Kafka/Flink-only billing; HTTP-only SDKs; Gateway durable outbox/reservation expiry; one base compose architecture and mode-free health/readiness.

- **2026-07-12** — **Architecture Decision Records**: [`docs/ADR.md`](docs/ADR.md) (中文) · [`docs/ADR-en.md`](docs/ADR-en.md) (English) — 23 ADRs (Flink/Lite/EO/Intelligence pivot) with git commit evidence; decision patterns + timeline.
- **2026-07-12** — **Demo v3.2.1 refresh**: `demos/full_demo.py` (metering + Intelligence + Gateway + wrap); `make demo-run` / `demo-run-live` / `demo-record`; updated `demo.tape`.
- **2026-07-12** — **v3.2.1 Lite lifetime usage fix**: rollup archives `customer:{id}:buf:*` only; `GET /usage/customer/{id}` lifetime totals match Full mode post-rollup.
- **2026-07-11** — **Intelligence scope closed (MVP)**: Pillar B complete at 3.0–3.1; no 4.0.0 Intelligence track — Phase 7+ demand-gated only.
- **2026-07-11** — **v3.2.0 Phase G Gateway P1**: OpenAI-compatible proxy (`gateway_app.py` :8080), pre-check + stream kill + proxy-only ingest; `budget_gate.py`; `docs/gateway.md`; Dockerfile full `api/` copy.
- **2026-07-11** — **v3.1.0 Phase 6 Intelligence v1.0**: pricing optimizer, profitability dashboard, forecast, anomaly alerts worker, report export; 29 intelligence tests green.
- **2026-07-11** — **v3.0.0 Intelligence MVP**: root cause, unit economics, simulation (3 scenario types), OpenMeter overlay; model-period rollup on lite ingest; Layer 4 product narrative; `docs/intelligence-api.md` + OpenAPI `/intelligence/*`.
- **2026-07-11** — **Intelligence MVP implementation plan**: [docs/superpowers/plans/2026-07-11-intelligence-mvp.md](docs/superpowers/plans/2026-07-11-intelligence-mvp.md); [docs/DESIGN.md](docs/DESIGN.md) amended for dual-pillar platform vision.
- **2026-07-11** — **v2.8.0 Phase 4 complementary export**: Metronome/Orb/Stripe multi-target export; partner recipes; hierarchy `reserve?parent_span_id=`; per-key API budgets; metadata dims + `GET /usage/dim`; interop spec; Python SDK **1.5.0**.
- **2026-07-11** — **v2.7.1 Flink EO hardening**: explicit checkpoint EXACTLY_ONCE + timeout; RedisSink atomic Lua; Java late/watermark + WindowMetadata + RedisSink idempotency tests; TEST_PLAN #2 window SET NX; `make correctness-bench`.
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
