# Load Testing

FluxMeter has separate benchmarks for the public HTTP entrance and the internal Kafka/Flink engine. Engine results must not be presented as HTTP ingress throughput.

**Performance overview:** [fluxmeter.dev](https://fluxmeter.dev) · methodology in this doc

## Quick start

```bash
make build
make start-benchmark

# Mac docker-compose honest ceiling: ~25K avg sustained at 50K target tier
# (Redis Lua sink bound — not Flink). See Reference results below.

# Staged tiers: 10K → 50K → 100K → 500K → 1M eps (15s each)
make load-test

# Skip 1M tier
make load-test-quick

# Known-event correctness (cost/counters) + Flink checkpoint health
make correctness-bench

# Release gate: 10K single-event HTTP eps and 100K batch-event HTTP eps
make http-load-test

# Manual infinite run at 1M target
make generate
```

## Effectively-once semantics

FluxMeter’s financial EO is **application-level effectively-once**:

1. HTTP custody claims `(tenant_id, eventId)` in compact tenant-sharded Redis hashes. `202` is returned only after Kafka ACK and the identity is finalized for the 30-day client retry window.
2. ACK timeouts enter a 10-minute `uncertain` state. A late Kafka callback reconciles the identity to accepted or releases it after a definitive failure; requests never receive a false `202`.
3. Flink checkpoints (30s, `CheckpointingMode.EXACTLY_ONCE`) restore operator state + Kafka offsets when `CHECKPOINT_DIR` is set. Its event-ID state is a bounded 10-minute crash-window safety net, not a second 30-day identity store.
4. Redis sinks use window-level `SET NX` inside a single Lua EVAL so a crash cannot mark a window applied without writing counters. Late events go to the Kafka DLQ; there is no `allowedLateness` re-fire.

Throughput load tests do **not** assert EO; use `make correctness-bench` and `TestIdempotency` in `tests/test_integration.py` for correctness.

## Staged script

`scripts/load-test.sh` submits the Flink job if needed, then runs each tier and writes:

- Summary: `load-test-results/run-<timestamp>.txt`
- Per-tier logs: `load-test-results/tier-<eps>-<timestamp>.log`

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKERS` | `localhost:9094` | Host-facing Kafka port |
| `NUM_CUSTOMERS` | `10000` | Synthetic customer pool |
| `NUM_THREADS` | `8` | Producer threads |
| `FLINK_PARALLELISM` | `12` | Flink job parallelism |
| `DURATION_SEC` | `20` | Seconds per tier |
| `QUICK` | `0` | Set `1` to skip 1M tier |

## HTTP entrance release gate

The single-event gate uses the Java 17 `io.fluxmeter.generator.HttpLoadGenerator`; the batch-event gate continues to use `scripts/http-load-test.py`. Both measure the complete public custody contract through `/ingest` or `/ingest/batch`: identity claim, Kafka broker ACK, and identity finalize. The Java runner counts offered, completed, accepted, rejected, transport-error, and concurrency-dropped requests separately and exits non-zero unless throughput and latency gates all pass.

Formal runs use open-loop offered load, a 5-minute warmup, and a 30-minute measurement on a 16 vCPU / 32GB Linux host:

- Single: 10K events/s, typical payload, p50 ≤50ms, p99 ≤200ms, zero rejection, transport error, generator drop, accepted-event loss, or duplication. The former 25/100ms limits remain the stretch target.
- Batch: 100K events/s, 1,000 items/request, typical payload, p99 <500ms, zero accepted-event loss or duplication.
- Fault suite: Kafka and Redis interruption plus Flink restart; noisy-tenant and minimal/typical/heavy payload profiles are recorded separately.

The single-event gate offers 10.05K events/s with 4,000 Java async request slots and requires at least 10K accepted events/s. The small offered-load headroom is intentional: because throughput is calculated through drain completion, offering exactly the minimum makes the minimum mathematically unreachable after any non-zero drain. The larger slot count was measured after the former 2,000-slot setting dropped 0.149% of offers during a formal run; it is generator headroom, not server concurrency guidance. A Python/httpx process is not valid evidence for a 10K request/s gate because it saturates the generator before the service. For publication-grade results, run the Java gate on a separate same-VPC host so its CPU does not compete with the API, Kafka, Flink, Redis, and ClickHouse.

The benchmark overlay intentionally sets the HTTP retry-identity TTL to five minutes and gives Redis a 6 GiB `maxmemory` inside an 8 GiB container. This bounds the 32GB-host throughput experiment; it does **not** weaken the production Custody default, which remains 30 days. Flink's separate per-event projection marker has a 10-minute default because it protects only checkpoint/replay safety; it must never duplicate the 30-day client-retry registry. Use `scripts/benchmark_capacity.py` with before/after Redis memory samples to report both:

- benchmark steady-state memory at the bounded TTL; and
- production retention capacity at the full 30-day TTL.

`make -k http-load-test` runs both gates even when the first fails. Use `--traffic-model closed` only to find saturation; closed-loop output is not release evidence. The 100K batch number is 30-minute processing capacity, not a claim that a single Redis node can retain 30 days of 100K new identities/s.

### Local MacBook development results (2026-08-17, v4.5.0)

Short smoke runs on the only available MacBook Docker stack:

- Single at a 20 eps target: 20.19 eps, p50 14.92ms, p99 37.74ms, zero failures.
- Single at a 200 eps target: 58.91 eps, p50 463.51ms, p99 3273.01ms, zero failures; capacity/latency gate failed.
- Batch at a 10K eps target: best observed short run 5,206.69 eps with p99 2303ms and zero failures; other runs degraded under full-stack contention and one produced a retryable 503.

These are diagnostic runs, not the 35-minute formal protocol. Both public HTTP release gates remain open, and v4.5.0 must not be described as production throughput-validated until the Linux report passes. JSON artifacts are under `load-test-results/http-*.json`.

### Tencent CVM diagnostic results (2026-08-22, v4.7.1)

On one 16 vCPU / 32 GiB Tencent CVM, the corrected eight-process generator completed a 30-second warmup plus 60-second single-event measurement:

- 1,318.85 accepted events/s against the 10K target;
- p50 402ms, p99 3018ms;
- 79,752 accepted events and one `RemoteProtocolError`;
- API near its four-worker CPU ceiling while all eight co-located generator processes also saturated one CPU each.

The run failed the release gate and Stage 8 was deliberately not started. Redis grew by 60.0 MiB, or about 789 bytes per accepted event. That projects to roughly 2.2 GiB at the benchmark's five-minute TTL but 18.6 TiB at the production 30-day retry window and 10K eps. The next valid experiment needs a separate load-generator host and a horizontally scalable identity backend/design; increasing the single Redis limit is not a production solution.

Batch diagnostics on the same host used 1,000 events/request:

- 10K target: 10,001 eps, p99 130ms, passed.
- 20K target: 19,799 eps, p99 192ms; latency passed, strict throughput missed by ~1%.
- 30K target: 29,586 eps, p99 423ms; practical p99<500ms boundary.
- 35K target: 33,983 eps, p99 894ms; latency knee crossed.
- Safety-shortened 100K target: 25,645 eps, p99 5.7s, zero event failures; full run skipped.

At 35K offered load the API used ~3.47 CPU cores and Redis ~1 core, while the Flink TaskManager used ~0.2 core. This locates the current batch boundary in synchronous HTTP custody/identity work rather than the streaming aggregation engine. The 100K smoke measured about 520 Redis bytes per accepted event, projecting to 14.5 GiB at the five-minute benchmark TTL and 122.6 TiB at 30 days/100K eps.

### Tencent CVM fault injection (2026-08-22, v4.7.1)

- **Kafka pause — pass:** the in-flight request returned `503 custody_uncertain`, never a false `202`. After broker recovery, the same payload returned `202 idempotent=true`; a new event returned `202 accepted`. Each event appeared exactly once in ClickHouse and Flink stayed 14/14 `RUNNING`.
- **Redis freeze — fail (liveness):** the API returned `503 identity_store_unavailable` in about five seconds and emitted no audit event. A queued pending claim remained blocked beyond its 120-second score because its shard had 203,581 older expired identities and each claim cleans only 64. Safety held, but advertised retry timing did not.
- **TaskManager restart — partial:** checkpoint 83 restored and the job returned to 14/14 `RUNNING` in about 34 seconds. Both events appeared once in ClickHouse. During 150 seconds with no later traffic, Redis exposed only one event; sending a later zero-token event advanced the watermark and produced the exact two-event, 56-token aggregate within five seconds.

At v4.7.1 the fault matrix therefore remained open: it required expiry cleanup that could not starve a specific pending identity, plus bounded materialization for completed event-time windows when every business source partition was idle. Full command outcomes are recorded in `load-test-results/tencent-20260822-v471/fault-injection-summary.md`.

### Tencent CVM fault remediation (2026-08-22, v4.7.2)

- **Redis backlog — fixed:** the claim Lua script checks and removes the requested identity by its own expiry score before evaluating state. A live retry whose target ranked 205,310 in the shard returned `202 accepted` immediately.
- **Idle event-time window — fixed:** the API elects one publisher across its workers and emits non-billable trusted heartbeats to the dedicated one-partition `metering-watermarks` topic. Flink consumes it alongside business events, marks silent business partitions idle after 15 seconds, and filters heartbeats before billing. A unique 26-token event with no later business traffic materialized exactly once in Redis after 17 seconds.
- **TaskManager restart — pass:** the final combined rerun materialized the pre-restart event, restarted the TaskManager, returned the original job to 14/14 `RUNNING`, accepted a post-recovery event, and materialized the exact combined `event_count=2` / `total_tokens=56` after 17 seconds with no manual business-event watermark. ClickHouse contained exactly the two test events.
- Standard five-second bounded out-of-orderness, no `allowedLateness`, late-event DLQ routing, and sink window idempotency remain unchanged.

### Tencent CVM single-event optimization (2026-08-22, v4.8.0–v4.8.1)

Single-event HTTP ingest coalesces concurrent calls behind the Custody interface into bounded 64-item batches with a one-millisecond flush window. Tenant/auth contexts never mix, and each HTTP response still waits for its own Kafka ACK and identity finalize result. The measured benchmark profile uses 12 API workers, disables per-request Uvicorn access logging, and sets Kafka producer linger to zero because the Custody layer already presents a group of messages.

The Java gate measured the following short diagnostics on the same 16C32G host:

- prior Java baseline, four API workers: 2,522.73 eps, p50 59ms, p99 192ms;
- microbatch plus logging/linger tuning, four workers: 4,815.32 eps, p50 162ms, p99 239ms;
- safe eight-worker profile: 7,812.72 eps, p50 192ms, p99 372ms, all 235,181 completed requests returned 202;
- cleanup-disabled diagnostic: 8,711.62 eps, p50 160ms, p99 362ms; reverted because it is not retention-safe.

An independent 8C16G Java runner in the same VPC removed generator contention. Profiling then led to an async anonymous-auth fast path, explicit in-handler authentication for the two ingress routes, and earlier fixed-route matching. A 10K offered run completed all 300,000 requests with p50 30ms / p99 74ms before route ordering; a later exact-10K run measured p50 25ms / p99 90ms but dropped 24 offers at the generator. The final strict short diagnostic offered 10.05K events/s with 2,000 slots and accepted all 301,502 events at 10,034.95 eps with zero rejection, transport error, or generator drop. Its p50 29ms / p99 134ms still failed the latency limits, so the 35-minute run was not started.

Repeated traffic also exposed an unrelated Flink stability defect: an unused `aggregates.keyBy(agg -> "global").reduce(...)` stream had no sink but still retained an ever-growing global value. It accounted for 465 MiB of a roughly 503 MiB checkpoint before failing with `ArrayIndexOutOfBoundsException`. Removing it reduced the execution graph from 14 to 12 tasks and the clean replacement job's initial checkpoint to 21.99 MiB; later checkpoint size still reflects the bounded event-dedup state. An Embedded RocksDB experiment produced much smaller checkpoints but worsened the co-located HTTP latency, so HashMap remains the benchmark backend. A current-version dedicated Custody Redis re-test was also reverted after no latency gain.

The throughput threshold is therefore demonstrated only as a short diagnostic; the latency and duration requirements remain open, as does the 100K batch gate. Full evidence and profiling artifacts are in [`load-test-results/tencent-20260822-v480/performance-optimization-summary.md`](../load-test-results/tencent-20260822-v480/performance-optimization-summary.md).

### Tencent CVM formal follow-up (2026-08-22, v4.8.2)

The single-event latency requirement was separated into an attainable production SLO (p50 ≤50ms / p99 ≤200ms) and the former 25/100ms stretch target. A 5-minute warmup plus 30-minute run from the independent 8C16G host then produced:

- 18,063,140 accepted events at 10,034.90 accepted events/s;
- zero rejection and zero transport error;
- p50 36ms, p99 173ms, max 1,309ms;
- 26,949 generator drops out of 18,090,089 offers (0.149%), so the strict gate still failed.

During that run, Kafka business lag reached about 13.66 million events. Redis eventually reached its 6 GiB `maxmemory`; Flink recorded `OOM command not allowed`, 14 failed checkpoints, and seven restores. Root cause was a second one-key-per-event `projection:*` registry retained for 30 days in addition to the compact Custody registry. v4.8.2 bounds this projection marker to 600 seconds by default. The accepted backlog was drained to zero before the old job was canceled; only benchmark `projection:*` markers were then unlinked. The replacement job ran 12/12 tasks, and a live marker measured 576 seconds remaining from the configured 600-second TTL.

A five-minute full-stack stage remained stable—no Flink restart and all checkpoints completed—but lag grew while ingress and Flink competed for the same Redis and drained only after traffic stopped. The benchmark was then found to provide 12 TaskManager slots while hardcoding Job submission to `-p 2`. Making parallelism configurable and submitting at p12 improved a 60-second full-stack sample from 7,369.36 to 7,772.28 eps and reduced end-of-run lag from about 178K to 333. All six checkpoints succeeded, although p50 222ms / p99 1,472ms still failed the gate. A temporary second Redis for Custody improved the no-warmup sample to 9,197.21 eps and the warmed sample to 8,952.97 eps; both missed throughput/latency, so the experiment was removed rather than promoted. The 100K batch stage accepted every completed batch item but reached only 32,567.14 events/s with p50 2,949ms / p99 3,959ms. These results establish two distinct claims:

- isolated HTTP Custody can cross 10K eps at the 50/200ms SLO;
- the corrected p12 co-located 16C32G full Metering pipeline nearly keeps pace at 7.77K eps but cannot yet sustain 10K single-event eps at the latency SLO, and the 100K batch gate remains open.

Correctness remained intact after recovery: the known-cost bench reported 1,000,001 input tokens and $0.15 with 39/39 successful checkpoints, and the ClickHouse cold-audit suite passed all 12 A1–A7 assertions. On the final p12/main-Redis configuration with business lag at zero, 200/200 accepted events reached the Redis cost/budget projection transaction at p50 465.85ms, p99 896.81ms, and max 909.60ms, passing the <20s steady-state event-to-billing criterion.

Redis AOF loading also exposed a startup-order defect: API workers initially exited on `BUSYLOADING`, then recovered through their restart policy, but the Flink job exhausted its restart attempts and remained failed. The base Compose stack now healthchecks for Redis `PONG`; Job submission, API, and Gateway wait for Redis to become fully available, and API/Gateway wait for successful Job submission.

## Internal engine reference results (2026-06-22)

MacBook docker-compose, **3 TaskManagers × 4 slots**, parallelism 12, `fluxmeter-2.6.1`:

| Target EPS | Avg EPS | Peak EPS | Notes |
|------------|---------|----------|-------|
| 10K | ~98% | ~23K | Stable |
| 50K | ~96% | ~130K | Stable |
| 100K | target | varies | Requires the benchmark overlay |
| 500K–1M | burst | ~145K+ | Local Redis Lua sink bound; peak bursts OK |

Prior run (2026-06-21, 1 TM): 50K stable; 100K+ Redis-bound.

For 500K+ sustained throughput, use multiple TaskManagers, more slots, and production Kafka/Redis (see [production-deploy.md](production-deploy.md)).

### High-throughput local profile

The benchmark overlay keeps the same ingestion semantics while scaling resources and exposing Kafka only for trusted operator load generation:

```bash
make start-benchmark
NUM_THREADS=8 make load-test
```

## Generator internals

`io.fluxmeter.generator.LoadGenerator` — weighted multi-model traffic, rate-limited per thread:

```bash
KAFKA_BROKERS=localhost:9094 TARGET_EPS=100000 NUM_THREADS=4 \
  java -cp build/libs/fluxmeter-*.jar io.fluxmeter.generator.LoadGenerator
```
