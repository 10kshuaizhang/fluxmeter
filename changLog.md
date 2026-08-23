# Changelog

All notable changes to FluxMeter are documented here. Version numbers follow [Semantic Versioning](https://semver.org/).

Format: `[version] — date — summary`

---

## [4.8.3] — 2026-08-23

### Changed
- Aligned `AGENTS.md` and `CLAUDE.md` with the only supported HTTP→Kafka→Flink architecture, current Makefile targets, OpenCore layout, and dual-pillar roadmap (v4.8.x metering hardening).
- Synced README / ROADMAP current-version stamps to 4.8.3.

### Notes
- Docs-only agent-guide sync; no runtime behavior change. Removed stale Lite/full dual-path and Weekend 1–4 guidance.

## [4.8.2] — 2026-08-22

### Changed
- Reclassified the single-event HTTP Custody release SLO to p50 ≤50ms / p99 ≤200ms, with the former 25/100ms limits retained as a stretch target, and raised the separate-host Java runner to 4,000 request slots so its own in-flight ceiling does not create false drops.
- Bounded Flink per-event projection idempotency to a configurable crash-safety window (`EVENT_PROJECTION_IDEMPOTENCY_TTL_SECONDS`, 600 seconds by default). The client retry identity remains a separate compact 30-day Custody record.
- Made Job parallelism configurable and set the 12-slot benchmark profile to parallelism 12 instead of silently submitting every benchmark Job with the base `-p 2` default.

### Fixed
- Removed the second 30-day one-key-per-event registry from the Flink projection sink. A 35-minute 10K run created millions of these keys, exhausted Redis at 6 GiB, and caused `OOM command not allowed` restarts; projection markers now expire with the 10-minute Flink replay-safety horizon.
- Added a Redis `PONG` healthcheck and conditioned Flink submission, API, and Gateway startup on Redis being fully loaded. This prevents AOF `BUSYLOADING` from exhausting the Flink restart strategy and leaving a failed job after the database recovers.

### Notes
- The 5-minute-warmup + 30-minute single-event run accepted 18,063,140 events at 10,034.90 eps with zero rejection or transport error and p50 36ms / p99 173ms. The Custody throughput/latency SLO passed, but the strict run verdict remained false because the 2,000-slot generator dropped 26,949 offered requests (0.149%).
- That run also exposed 13.66 million business events of downstream lag and Redis OOM pressure. After safely draining Kafka to zero, 14 failed checkpoints / 7 restores were diagnosed, only `projection:*` benchmark markers were removed, and the live 4.8.2 marker TTL was verified at 576 seconds from a 600-second configuration.
- A follow-up full-stack 60-second single sample at 4,000 slots reached 7,369.36 eps (p50 99ms / p99 1,679ms), while the 100K batch sample reached 32,567.14 eps (p50 2,949ms / p99 3,959ms). Both failed, showing that the co-located sustained Metering pipeline—not isolated HTTP Custody—is now the capacity boundary.
- Correcting benchmark parallelism from 2 to 12 improved the full-stack 60-second sample to 7,772.28 eps and reduced end-of-run business lag from about 178K to 333; all six checkpoints completed, though p50 222ms / p99 1,472ms still failed. A temporary split Custody Redis improved throughput to 9,197.21 eps without warmup and 8,952.97 eps after warmup, but remained outside the latency/throughput gate and was removed.
- Deterministic billing correctness passed at 1,000,001 input tokens / $0.15, with 39/39 checkpoints; ClickHouse cold-audit A1–A7 passed 12/12 assertions. The formal 10K full-pipeline and 100K batch gates remain open.
- On the retained p12/main-Redis configuration, a 200-event steady-state accepted-to-Redis projection sample materialized 200/200 events with p50 465.85ms, p99 896.81ms, and max 909.60ms, passing the <20s event-to-billing criterion.

## [4.8.1] — 2026-08-22

### Changed
- Made optional anonymous authentication an async no-identity-store fast path and moved Redis-backed scoped-key lookup off the event loop.
- Authenticated `/ingest` and `/ingest/batch` explicitly inside their handlers, avoiding FastAPI dependency-graph and thread-pool overhead on the public metering hot path while preserving the same rejection contract.
- Registered the broad Intelligence router after the fixed ingress routes, and set the measured benchmark profile to 12 API workers with a one-millisecond Custody microbatch window.
- Gave the formal Java gate 10.05K offered-load headroom and 2,000 request slots while retaining a strict 10K accepted-throughput minimum; this prevents its drain-time denominator from making the minimum mathematically unreachable.

### Fixed
- Removed an unused global Flink `Keyed Reduce` branch that accumulated unbounded state despite having no sink. On the Tencent stack that operator held 465 MiB of a 503 MiB checkpoint and eventually failed with `ArrayIndexOutOfBoundsException`; the replacement job runs 12 instead of 14 tasks and its initial checkpoint measured 21.99 MiB.
- Increased the Java generator unit-test concurrency allowance so full-suite scheduler variance does not cause false failures.

### Notes
- The final independent-host short diagnostic offered 10.05K events/s and accepted all 301,502 events at 10,034.95 events/s with zero rejection, transport error, or generator drop. p50 29ms and p99 134ms still exceed the 25/100ms latency limits, so the 35-minute single-event gate was deliberately not started and both formal public gates remain open.
- Embedded RocksDB checkpoints and a second Redis dedicated to Custody were both measured and reverted: each reduced this co-located benchmark's latency/capacity rather than improving the release result.
- Final cloud health after the API deployment: API `ready` at 4.8.1, Flink 12/12 `RUNNING`, 32/32 successful checkpoints (latest 0.82 MiB / 22ms), and zero lag on every business-event partition.

## [4.8.0] — 2026-08-22

### Added
- A bounded cross-request Custody microbatcher for single-event HTTP ingest, with a 64-item default, one-millisecond flush window, tenant-context isolation, bounded queueing, and per-request result mapping.
- A Java 17 open-loop public HTTP gate that reports offered, completed, accepted, rejected, transport-error, concurrency-drop, p50, p99, and maximum-latency evidence and exits non-zero when any gate fails.
- Regression coverage for microbatch coalescing, tenant isolation, the HTTP batcher seam, Java gate accounting, and benchmark-safe runtime tuning.

### Changed
- `POST /ingest` delegates to the shared Custody batcher while retaining its existing ACK-plus-finalize 202 contract and retry/error mappings.
- The benchmark API defaults to the measured 12-worker / 2ms profile, disables per-request access logging, removes redundant Kafka producer linger after application-level coalescing, and exposes worker, batch-window, queue, shard, and cleanup controls for repeatable experiments.
- `make http-load-test-single` uses the Java runner instead of the CPU-limited Python/httpx single-event generator.

### Notes
- Tencent 16C32G safe tuning reached 7,812.72 accepted events/s on eight API workers (p50 192ms, p99 372ms), 3.10x the prior 2,522.73 eps Java baseline. Every completed request returned 202 with zero transport errors.
- A cleanup-disabled diagnostic reached 8,711.62 eps but was reverted because it does not safely reclaim retained identities. During live sampling the co-located Java runner consumed 3.3–5.2 CPU cores, so formal 10K evidence requires a separate same-VPC load host. The 10K/100K public gates remain open.
- A separate 8C16G same-VPC runner measured a best safe 9,814.66 eps (p50 49ms, p99 151ms) at 12 workers / 2ms, with zero rejection or transport error. 13–14 workers regressed and a dedicated Custody Redis experiment did not improve results, so it was removed. The strict short gate still fails and the 35-minute release run was not started.

## [4.7.2] — 2026-08-22

### Added
- A dedicated one-partition `metering-watermarks` Kafka topic and elected API heartbeat publisher for bounded event-time window materialization during zero business traffic.
- Regression coverage for targeted identity expiry, single-publisher heartbeat envelopes, source idleness, and existing late-event side-output behavior.

### Changed
- Flink consumes business and watermark topics with five-second bounded out-of-orderness and 15-second source idleness; internal heartbeat events are filtered before deduplication, projection, aggregation, and billing.

### Fixed
- A retry now expires its own custody identity atomically before state evaluation, so it cannot remain `pending` behind an arbitrarily large shard cleanup backlog.
- Completed event-time windows no longer require a later billable event to advance the watermark after every business partition becomes idle.

### Notes
- Tencent 16C32G verification reclaimed a target identity ranked 205,310 behind expired members and returned `202 accepted`. A single event with no later business traffic materialized as exactly 1 event / 26 tokens in 17 seconds. A final TaskManager restart restored 14/14 tasks and produced exact Redis 2-event/56-token state plus two ClickHouse audit rows, again materializing 17 seconds after the post-recovery event.
- Focused custody/gateway Python regressions and the full Java suite passed locally; cloud Java regressions and live API/Kafka/Flink/Redis probes passed.

## [4.7.1] — 2026-08-22

### Added
- Multi-process HTTP load generation with offered-rate/concurrency partitioning and merged bounded latency histograms.
- `scripts/benchmark_capacity.py` to report bounded benchmark identity memory separately from the production 30-day retention footprint.
- Regression coverage for worker planning, result aggregation, capacity projection, and benchmark Compose limits.

### Changed
- The benchmark overlay uses an explicitly labeled five-minute custody identity TTL, 6 GiB Redis `maxmemory`, and an 8 GiB container limit; production defaults remain unchanged.
- The 10K single-event Make target uses eight generator processes and 800 total connections.

### Fixed
- Prevented a single CPU-bound Python/httpx process from being mistaken for the 10K HTTP custody capacity of the deployed stack.
- Prevented the 32GB benchmark protocol from using the base 384 MiB Redis limit while comparing projections against a fictional 20 GiB safety line.

### Notes
- Initial Tencent 16 vCPU / 32 GiB diagnosis measured ~429 single eps with the old one-process harness. The corrected eight-process 30s+60s gate measured 1,318.85 eps, p50 402ms, p99 3018ms, and 79,752 accepted events with one transport failure; the 10K gate remains open.
- Measured Redis growth was ~789 bytes per accepted event: about 2.2 GiB at the five-minute benchmark TTL, but 18.6 TiB at the production 30-day retry window and 10K eps. A single Redis node is not a valid production retention design at that cardinality.
- Batch diagnostics on the same host found the p99<500ms boundary near 30K eps: 29,586 eps / p99 423ms at the 30K tier, versus 33,983 eps / p99 894ms at 35K. A safety-shortened 100K smoke reached 25,645 eps / p99 5.7s and projected 14.5 GiB even at the five-minute benchmark TTL, so the full 100K run was deliberately skipped.
- Fault diagnostics: Kafka pause/recovery preserved the no-false-202 contract, reconciled an uncertain delivery, and produced one audit row. Redis freeze preserved safety but exposed retry starvation behind 203,581 expired identities in one shard. Flink TaskManager restart restored checkpointed state and both audit rows, but idle event-time traffic required a later event to advance the watermark before the exact aggregate materialized.

## [4.7.0] — 2026-08-22

### Added
- Deterministic `make demo-proof` acceptance path for reserve → meter → kill → settle → audit, using a local OpenAI-compatible stream and the deployed Gateway, Custody, Kafka, Flink, Redis, and ClickHouse services.
- Gateway Reservation response headers and structured streaming-kill metering receipts with token counts, metered cost, and reserved cost.
- Focused tests for proof receipt parsing, polling behavior, response headers, and streaming metering evidence.

### Changed
- The proof overlay points only the provider transport at a local mock; all metering, enforcement, settlement, and audit work remains on the production path.
- Flink job submission now treats restarting/initializing jobs as active to avoid duplicate jobs during repeated local compose starts.
- Repaired `demos/full_demo.py` after the v4.6 deep-module migration so it uses `TokenEventCustody` and `PricingCatalog` directly.

### Notes
- Live Mac proof: reserved $0.000078; killed after 301 output tokens at $0.000181; Flink released the hold, deducted actual usage to a $0.000819 balance, and ClickHouse recorded matching `_stream_killed=true` audit metadata.
- Verification: 104 API/core Python tests, 17 Python SDK tests, JS TypeScript build, 52 Java tests, shadow JAR, OpenAPI/spec validation, and `git diff --check` passed.
- Public 10K/100K HTTP performance gates remain open and are not claimed by this release.

## [4.6.0] — 2026-08-22

### Added
- Four explicit deep-module interfaces: `TokenEventCustody`, `PricingCatalog`, `Reservation`, and `Budget` (ADR-026).
- Interface tests for tenant-isolated Budget cache/RPM, hierarchy holds, atomic Reservation open, replay/conflict, and idempotent settlement/expiry.
- Tenant-scoped span/session key vocabulary shared by Python and Java projections.

### Changed
- HTTP and Gateway callers now pass `CustodyConfig` + `CustodyContext` instead of assembling Custody from topic, TTL, identity, and failure parameters.
- Gateway hold creation and durable Reservation registration now occur atomically in one Redis Lua transition.
- Pricing catalog construction is the sole validation path and also owns exact usage quotes and advisory Gateway estimates.
- Budget configuration, top-up, snapshot, authorization, cache fallback, RPM accounting, hierarchy caps, and API-key caps now live behind `Budget`.

### Fixed
- Prevented same-named customers from sharing cached Budget decisions, rate-limit counters, or prepaid token packages across tenants.
- Hierarchy authorization now counts existing scope holds as well as settled spend.
- Removed the crash window that could leave a Gateway hold without a Reservation record.
- Hot pricing updates now reload the current API/Gateway process after validation.
- Isolated the Java event-projection Reservation assertion from a concurrently running local Gateway expiry worker.

### Removed
- Shallow `budget_gate.py`, `budget_ops.py`, and `gateway/pricing_estimate.py` modules.

### Notes
- Python module regression: 115 passed / 3 local-Redis cases skipped in sandbox; the Redis-backed file passed 5/5 separately with host access. Clean Java suite and v4.6.0 shadow JAR build passed.
- HTTP 10K/100K Linux performance gates remain open from v4.5; this release does not change that claim.

## [4.5.0] — 2026-08-17

### Added
- Compact tenant-sharded Redis custody identities with per-field expiry indexes, a 30-day accepted retry window, 120-second pending claims, and 600-second uncertain claims.
- Late Kafka delivery reconciliation, bounded async producer dispatch, custody-stage/outcome metrics, per-item batch validation, and explicit `429` overload behavior.
- Reproducible HTTP gate runner with open/closed load models, minimal/typical/heavy payloads, noisy-tenant profiles, warmup/measurement phases, host snapshots, latency/capacity assertions, and JSON artifacts.
- Java coverage for the bounded Flink event safety dedup.

### Changed
- `202 Accepted` now requires both Kafka broker ACK and identity finalization. ACK timeouts/finalize ambiguity return retryable `503 custody_uncertain`; definitive failure releases the claim.
- Client retry identity is `(tenant_id, eventId)`. Batch rows preserve independent rejected, conflict, pending, uncertain, unavailable, overloaded, accepted, or quarantined outcomes.
- Flink event-ID state is restricted to a 10-minute crash-window safety TTL instead of duplicating the 30-day HTTP identity registry.
- Public performance gates are 10K single-event eps (p50 <25ms, p99 <100ms) and 100K batch-event eps (1,000 items, p99 <500ms), using 5-minute warmup plus 30-minute measurement.

### Fixed
- Closed ACK-timeout callback races so late success/failure reconciles identity exactly once.
- Preserved batch producer queue saturation as `overloaded` instead of accidentally treating it as success.
- Flink container builds now copy a stable `build/docker/fluxmeter.jar`, eliminating the hard-coded 4.0.0 JAR path that could run stale engine code.

### Notes
- Python unit/contract regression: 97 passed. Java test suite: passed.
- Local Kafka-pause and Redis-pause probes returned retryable `503` without a false `202`.
- Short MacBook diagnostics passed only low-load smoke traffic and failed the 10K/100K public gates. The formal 16 vCPU / 32GB Linux report and full fault-injection audit remain pending; v4.5.0 is not production throughput-validated.

## [4.4.1] — 2026-08-16

### Changed
- README / OpenAPI / `docs/api-reference.md` synced to current FastAPI routes and Custody semantics (batch `results`, 207/409/503, `held_usd` / package / pricing / admin keys).
- Token-event `environment` is a free-form string (matches `IngestEvent`).

### Notes
- Docs-only PATCH; runtime behavior unchanged from 4.4.0.

## [4.4.0] — 2026-08-16

### Added
- **Cold Store** (ADR-025): ClickHouse `raw_events` + DLQ via Kafka group `fluxmeter-cold-store`; Trusted Envelope extract (flat JSON still accepted).
- `make apply-cold-store-init` / `make test-cold-store` (A1–A7); runbook `docs/runbooks/cold-store-dlq.md`.
- Spec/plan under `docs/superpowers/` (relanded from closed PR #2 as 4.4.0).

### Changed
- Benchmark aggregates moved to `baseline/benchmark_init.sql` (applied by `make benchmark` only).
- ADR-004 amended: aggregates remain benchmark-only; Cold Store is the audit path on the same CH deployment.

### Notes
- Non-goals unchanged: Lite-only cold path, ADR-2 query API, ADR-3 reconciliation.
- Wire-up: `docker-compose.benchmark.yml` (not deleted `docker-compose.full.yml`).

## [4.3.0] — 2026-08-16

### Added
- **UsageQuery** module for tenant-aware lifetime counter reads (`customer:` / `global:`) with legacy dual-read.
- **ProxiedCompletion** Gateway orchestration (`run`) consolidating estimate → gate → reserve → upstream → Custody.

### Fixed
- Usage / spans / budget `total_spent` / billing export / rerate now resolve tenant-prefixed Flink keys (no bare-key read leak).
- Gateway stream upstream HTTP ≥400 settles Reservation (aligned with non-stream); skips Custody ingest after settle.

### Changed
- Rerate preview/apply dual-scans tenant + bare model keys; cost writes follow the scanned key family; budget adjust uses tenant-aware prefix.

## [4.2.0] — 2026-08-16

### Removed
- Deleted Lite orphan writers (`increment_session` / `increment_span`) and unwired `OptimizedRedisSink`.
- Removed Pricing Catalog `to_lua_spec` / Lite pricing_spec surface.

### Added
- Shared Pricing Catalog golden vectors (`docs/contracts/pricing-vectors.json`) run by Python and Java.
- `RollupStore` with `idx:period:{YYYY-MM}:customers` written by BudgetEnforcerSink; SCAN fallback when index missing.

### Changed
- Gateway stream estimate uses catalog `cost_micro` (no `tier_at_token(0)` shortcut).
- `demos/full_demo.py` self-check uses Custody + seeded rollups (no Lite imports).

## [4.1.1] — 2026-08-16

### Fixed
- `GET /intelligence/forecast` now resolves `tenant_id` from the API key so customer-scope budgets read the tenant-prefixed Redis key.

## [4.1.0] — 2026-08-16

### Added
- Deep Token Event Custody module (`accept` / `accept_many`) shared by HTTP ingest and Gateway.
- Reservation lifecycle contract (`docs/contracts/reservation.md`) with shared test vectors; sole `expire_reservations` entry.
- Tenant-aligned Budget Redis keys (`budget_prefix`) with legacy read fallback; Gateway holds store `tenant_id`.

### Changed
- Gateway metering goes through Custody (stable `res:{reservationId}` event identity, outbox buffer on Kafka down).
- EventProjectionSink / BudgetEnforcerSink share `TenantKeys.windowReservationsKey`.

### Notes
- Architecture deepening from the 2026-08-16 review (Custody → Reservation → tenant Budget keys).

## [4.0.1] — 2026-08-16

### Changed
- Batch HTTP ingestion now claims event identities in one Redis command, enqueues the full batch before polling, and awaits Kafka broker acknowledgements concurrently.
- The benchmark overlay runs four API workers with larger bounded Kafka producer queues; the HTTP load tool now records request latency, status counts, and JSON artifacts.

### Fixed
- Prevented the Compose one-shot submitter from creating duplicate Flink jobs when the API service is rebuilt or restarted.

### Notes
- Clean Docker E2E passed 16 integration and 11 v2 scenarios.
- Stable local HTTP measurements: 291.20 events/s single (c100) and 14,006.89 events/s batch (size 1,000, c10), both with zero failures. The 10K/100K release gates remain unmet; batch c100 saturated the single Redis idempotency store.

## [4.0.0] — 2026-08-15

### Changed
- Fixed automatic Flink submission in Compose by targeting the JobManager REST endpoint explicitly and initializing shared checkpoint-volume ownership.
- Hardened the HTTP benchmark against request timeouts and aligned live E2E assertions with the v4 batch/idempotency contract.
- Consolidated public event ingestion on HTTP with Kafka acknowledgement before `202 Accepted`; Flink is now the only billing and aggregation engine.
- Added versioned trusted envelopes, authentication-derived tenant context, 30-day event identity, batch per-event outcomes, timestamp quarantine, and dependency readiness.
- Made Python and JavaScript SDKs HTTP-only with bounded retries, stable event IDs, and typed delivery failures.
- Made Gateway metering durable through a Redis outbox and expiry-safe asynchronous reservation reconciliation.
- Replaced Lite/Full compose families with one base stack plus SaaS, production, and benchmark overlays; Flink jobs submit automatically.

### Removed
- Removed direct Redis ingest/rollup modules, direct customer Kafka SDK modes and Python WAL, `FLUXMETER_LITE_MODE`, `docker-compose.full.yml`, and Lite/Full Make targets.

### Notes
- This is a breaking release. See [`docs/migration-4.0.md`](docs/migration-4.0.md) and ADR-024.

## [3.2.1] — 2026-07-12

### Fixed
- **Lite rollup lifetime usage**: `customer:{id}:*` counters are no longer reset by the rollup worker; ingest mirrors increments to `customer:{id}:buf:*` for period/day/minute compaction only
- **`GET /usage/customer/{id}`** in Lite mode now returns lifetime cumulative totals (aligned with Full mode and API docs)
- **`GET /budget/{id}` `total_spent_usd`** follows corrected lifetime `customer:{id}:cost_usd`

### Notes
- Upgrade from pre-3.2.1 Lite: lifetime history lost by old rollup resets is not recoverable; one-time legacy pending drain rolls the last open buffer window into period buckets
- **Demo refresh**: `demos/full_demo.py`, updated `demo.tape` (v3.2.1), `make demo-run` / `make demo-run-live` / `make demo-record`

## [3.2.0] — 2026-07-11

### Added
- **Gateway MVP** (`api/gateway/`, `gateway_app.py`): OpenAI-compatible proxy on `:8080` with pre-check, stream reserve, mid-flight kill, proxy-only ingest
- **Shared budget gate** (`api/budget_gate.py`) — `/check` and Gateway reuse same logic
- **Docs**: [`docs/gateway.md`](docs/gateway.md), [`demos/gateway_demo.py`](demos/gateway_demo.py)
- **Docker**: `gateway` service in lite compose; Dockerfile copies full `api/` tree (fixes Intelligence module gap)

### Changed
- Engine / API / Gateway version **3.2.0**; Phase G P1 complete (P2 LiteLLM/TPM/predictive deferred)
- `make demo` prints Gateway URL; `make demo-gateway` for mock self-check

### Notes
- Stream kill uses heuristic token estimate when provider omits usage chunks (same as SDK `wrap()`)
- Helm gateway deployment deferred to Phase G.1 — same image, `uvicorn gateway_app:app`

---

## [3.1.0] — 2026-07-11

### Added
- **Pricing Optimizer**: `GET /intelligence/pricing-recommendations` — rule-based price increase / model switch with annual ROI
- **Profitability Dashboard**: `GET /intelligence/profitability` — cross-customer margin, product breakdown, multi-month trend
- **Spend Forecast**: `GET /intelligence/forecast` — linear EOM projection vs budget
- **Anomaly Alerts**: background worker + `POST /intelligence/alerts/webhook` — `INTEL_COST_SPIKE`, `INTEL_MARGIN_LOSS`, `INTEL_FORECAST_RISK`
- **Report Export**: `GET /intelligence/report?format=markdown|json` — Finance/CEO one-pager
- **Native reader extensions**: daily costs, global period trends, dim margin series

### Changed
- Engine / API version **3.1.0**; Phase 6 Intelligence v1.0 complete
- [`docs/intelligence-api.md`](docs/intelligence-api.md) — Phase 6 endpoints

### Notes
- Alert webhook: `FLUXMETER_INTEL_WEBHOOK_URL` env or admin POST; debounce 24h per alert type
- Product revenue allocation uses cost-share heuristic (ponytail); per-SKU revenue deferred to Phase 7+

---

## [3.0.0] — 2026-07-11

### Added
- **Intelligence MVP**: root cause analysis, unit economics, scenario simulation, OpenMeter overlay ingest (`api/intelligence/`)
- **Model-period rollup** on lite ingest (`usage_buckets.py` + `lite_aggregate_lua.py`)
- **Docs**: [docs/intelligence-api.md](docs/intelligence-api.md), landing copy, OpenAPI `/intelligence/*` paths

### Changed
- **Product narrative** shifts to Layer 4 (Monetization Intelligence); metering pillar retained and maintained
- Engine / API version **3.0.0**

### Notes
- Major bump = product narrative shift, not breaking API for existing metering endpoints

---

## [2.8.0] — 2026-07-11

### Added
- **Multi-platform billing export**: Metronome + Orb + hardened Stripe in `api/billing_export.py` (`BILLING_EXPORT_TARGETS`, idempotent deltas, Stripe retry)
- **Generic customer link**: `POST /admin/billing/{id}/link` (`platform`: stripe | metronome | orb)
- **Partner recipes**: [docs/integrations/stripe.md](docs/integrations/stripe.md), [metronome.md](docs/integrations/metronome.md), [orb.md](docs/integrations/orb.md)
- **Hierarchy reserve-confirm**: `POST /budget/{id}/reserve?parent_span_id=` atomically holds customer + span cap pool
- **Per-key API budgets**: `POST /admin/customers/{id}/apikeys/{key_id}/budget`; enforced on `/check` with customer API keys
- **Metadata dims**: ingest `metadata` (whitelist `FLUXMETER_USAGE_DIMS`); `GET /usage/dim/{key}/{value}?period=YYYY-MM`
- **Interop spec**: [spec/schema/external-export-mappings.md](spec/schema/external-export-mappings.md)
- **Python SDK 1.5.0**: `reserve(parent_span_id=)` / `reconcile(parent_span_id=)`

### Changed
- [docs/integrations.md](docs/integrations.md) trimmed; first-class export docs linked
- Engine / API version **2.8.0**; Phase 4 complementary export complete

### Notes
- Export tests: `tests/test_billing_export_partners.py`, `tests/test_hierarchy_reserve.py` (requires `lupa` for fakeredis Lua)

---

## [2.7.1] — 2026-07-11

### Fixed
- **RedisSink crash window**: SET NX + pipeline replaced with a single Lua EVAL (idempotency + counters + rollup) so a mid-flight crash cannot leave `applied:*` set without writes

### Added
- **Flink harness tests**: `LateDataSideOutputTest` (watermark → late side output, no allowedLateness), `WindowMetadataFunctionTest`, `RedisSinkIdempotencyTest` (Redis-gated)
- **TEST_PLAN #2**: `TestIdempotency.test_window_replay_set_nx_not_double_counted` (window SET NX replay)
- **`make correctness-bench`**: known-event cost/counter assertion + Flink checkpoint health summary

### Changed
- Checkpoint config: explicit `EXACTLY_ONCE`, 10m timeout, `tolerableCheckpointFailureNumber(3)`
- Docs: EO semantics in [load-testing.md](docs/load-testing.md); `progress.md` late-data note aligned with code (no allowedLateness → DLQ)

### Notes
- Engine version **2.7.1**; Java tests need `--add-opens` for Flink serializers on Java 17+

---

## [2.7.0] — 2026-07-06

### Added
- **Lite budget webhooks**: `BUDGET_LOW` / `BUDGET_EXHAUSTED` / **`BUDGET_WARN`** fire on `/ingest` without Kafka (`api/webhook_deliver.py`)
- **Soft alert ladder**: `BUDGET_WARN` at 70% and 90% of `(initial_balance + topups)` spent (`warn_pct`, `spent_pct` in payload; `BUDGET_WARN_PCTS` env)
- **Python `wrap(OpenAI())`**: pre-call `check`, post-call `track`, fail-open; mid-stream `StreamKilledError` when est cost exceeds reserve (SDK **1.4.0**)
- **HTTP-mode Python SDK**: `FluxMeter(api_url=...)` for Lite ingest / check / reserve (no Kafka dependency)
- **Hierarchy caps**: `POST /budget/{id}/cap` + `check?parent_span_id=` / `session_id=` enforce span/session hard max spend
- **Path activation demo**: `demos/path_activation_demo.py` (self-check + optional `--live`)
- **JS SDK 1.3.0**: built for npm (`sdk/js`)

### Changed
- Engine / API version **2.7.0**; Phase 3 path activation complete (npm registry push optional if unauthenticated)

### Notes
- Full-mode webhooks still use Kafka `webhook-worker`; Lite shares HMAC payload shape
- Python SDK **1.4.0** on PyPI

---

## [2.6.2] — 2026-07-05

### Added
- **Lite span aggregation**: `parentSpanId` on `/ingest` increments `span:{id}:*` counters (mirrors Flink `SpanSink` schema); `GET /usage/span/{id}` and `/usage/customer/{id}/spans` work on Lite path
- **`usage_buckets.increment_span`**: 24h TTL, customer ZSET by cost, duration from first/last event timestamps

### Changed
- **Docs**: [customer-integration-lite.md](docs/customer-integration-lite.md) §6.1 marked shipped; api-reference billing guide notes Lite span support

### Notes
- Span keys use INCRBY (per-event delta); Full Flink path still SETs window totals — query API shape is identical

---

## [2.6.1] — 2026-07-05

### Added
- **Period/day usage queries**: `GET /usage/customer/{id}/period/{YYYY-MM}`, `GET /usage/customer/{id}/day/{YYYY-MM-DD}` — reads Redis rollup buckets (lite rollup worker + Flink `RedisSink`)
- **Session usage query**: `GET /usage/session/{id}` — lite ingest aggregates by `sessionId` (90d TTL, configurable)
- **`api/usage_buckets.py`**: shared rollup key helpers and session counters

### Changed
- **Docs / OpenAPI**: period, day, session query endpoints; billing query guide; semantic conventions; integrations table

### Fixed
- **Dockerfile**: include `usage_buckets.py` (lite API crash on v2.6.1 deploy)
- **Flink job submit**: tier volume state moved to `MonthlyVolumeStampFunction` (Flink rejects RichFunction in aggregate+ProcessWindowFunction)
- **SaaS compose**: wire `FLUXMETER_API_KEY` / `FLUXMETER_ADMIN_KEY` for auth-enforced API tests
- **Tests**: E2E harness uses `127.0.0.1` (macOS `localhost` → IPv6 503); pricing admin test restores catalog; rerate apply timeout 120s

### Notes
- Full-mode session aggregation still lite-only on ingest; Kafka/Flink path unchanged (use `parentSpanId` for tasks)
- Day/month buckets accumulate from deploy forward; no historical backfill

---

## [2.6.0] — 2026-07-05

### Added
- **Chinese domestic model pricing**: 20 models across DeepSeek, Qwen, GLM, Moonshot, Doubao, Baichuan, MiniMax, Hunyuan in `config/pricing.json`
- **SDK track helpers**: `track_deepseek`, `track_qwen`, `track_glm`, `track_moonshot`, `track_doubao`, `track_baichuan`, `track_minimax`, `track_hunyuan` (Python + JS)
- **Provider docs**: 8 mapping guides in `contrib/providers/` + `contrib/pricing/china-2026-07.json`
- **LoadGenerator**: DeepSeek, Qwen, GLM, Moonshot in demo traffic mix

### Changed
- **Python SDK** → **1.3.0**; **JS SDK** → **1.2.0**
- **Pricing catalog** version 2, effective 2026-07-01

### Notes
- Qwen Plus/Max use flat base-tier pricing; >256K input tier surcharges documented in contrib
- `hunyuan-lite` priced at $0 (free tier)

---

## [2.5.0] — 2026-07-04

### Added
- **Stripe export modes**: `STRIPE_EXPORT_MODE=events|cost`, `BILLING_EXPORT_PERIOD=hourly|monthly`
- **Rollup month buckets**: `rollup:{customer}:period:{YYYY-MM}` aligned to billing calendar
- **Prepaid token packages**: `POST/GET /budget/{id}/package` + lite ingest drawdown
- **Stripe Checkout**: `POST /tenants/{id}/checkout` on control plane
- **Flink → Redis period sync**: `RedisSink` increments `period:*:volume_tokens`
- **Docs**: [docs/pricing-hybrid-paths.md](docs/pricing-hybrid-paths.md)
- **Tests**: `test_tier_e2e.py`, `test_phase2_billing.py`

### Changed
- **Python SDK** → **1.2.0** (engine 2.5.0 compatible; no breaking API changes)
- **Version alignment**: engine, API, OpenAPI, Helm, control plane → **2.5.0**
- **Phase 2 ROADMAP** items marked complete (tier pricing shipped in 2.4.0)
- **Link audit**: README/SDK metadata — `fluxmeter.dev` for marketing site only; technical docs → GitHub `docs/`; PyPI `Documentation` URL fixed (was duplicate homepage)
- **README / PyPI positioning**: marketing snippet at README top (website + blog links, use-case line, license badge); PyPI `description` + `keywords` synced from marketing snippet

---

## [2.4.0] — 2026-07-04

### Added
- **Tiered pricing engine**: `pricing_mode` `flat` | `volume` | `graduated` in `PricingCatalog` (Java) and `api/pricing_loader.py` (Lite)
- **Lite volume tracking**: Redis `period:{YYYY-MM}:volume_tokens` + atomic tier cost in Lua (`lite_aggregate_lua.py`)
- **Flink volume state**: `UsageAggregateFunction` with keyed `ValueState` + `MonthlyVolumeMeter`
- **Example catalog**: `contrib/pricing/tiered-example.json`
- **Tests**: `PricingCatalogTest`, `UsageAggregateTierTest`, `MonthlyVolumeMeterTest`, `test_pricing_loader.py`, Lite Redis tier tests
- **Re-rating guard**: `/rerate/*` returns `422` for non-flat models

### Changed
- **Pricing API validation**: monotonic tiers, open-ended last tier, `volume_scope` / `billing_period` checks
- **Version alignment**: engine, API, OpenAPI, Helm → **2.4.0**
- **Docs**: tier pricing in `docs/api-reference.md`; re-rating + tier replay in `docs/integrations.md`

### Notes
- Default `config/pricing.json` stays flat — no behavior change until you opt into a tier catalog
- Span attribution (`parentSpanId` windows) still uses flat tier selection (volume meter is `customer|model` scoped)
- Phase 2 Stripe Checkout wiring remains on ROADMAP; tier pricing P0 is complete

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
