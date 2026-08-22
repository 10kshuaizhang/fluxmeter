# Tencent CVM single-event performance optimization

Date: 2026-08-22  
Target: 16 vCPU / 32 GiB Tencent CVM  
Load generator: separate 8 vCPU / 16 GiB Tencent CVM in the same VPC  
Boundary: public `POST /ingest`, Redis claim → Kafka `acks=all` → Redis finalize → HTTP 202

## Retained changes

- Bounded cross-request Custody microbatch: 64 items, 1ms flush window, 20,000 queued requests per API worker.
- Tenant/auth contexts remain isolated; per-request status and ordering are preserved.
- Java 17 open-loop HTTP gate with offered, completed, accepted, rejected, transport-error, concurrency-drop, p50, p99, and maximum-latency counters.
- Async optional-auth fast path: anonymous benchmark traffic avoids identity Redis reads, while scoped-key lookup runs off the event loop.
- `/ingest` and `/ingest/batch` authenticate inside the route rather than through FastAPI's dependency graph; fixed ingress routes are registered before the broad Intelligence router.
- Benchmark defaults: 12 API workers, 1ms microbatch wait, no Uvicorn access log, Kafka producer linger 0ms, and safe request-time expiry cleanup of 64.
- The formal single-event command offers 10.05K events/s with 4,000 slots, retains a strict 10K accepted-throughput minimum, and uses p50 ≤50ms / p99 ≤200ms as the production SLO (25/100ms remains the stretch target).
- Removed an unused Flink global reduce stream that accumulated unbounded checkpoint state despite having no sink.
- Bounded the distinct Flink `projection:*` replay marker to 600 seconds instead of duplicating the compact 30-day Custody identity with one long-lived Redis key per event.

## Results

| Configuration | Accepted eps | p50 | p99 | Notes |
|---|---:|---:|---:|---|
| Pre-change Java baseline, 4 API workers | 2,522.73 | 59ms | 192ms | No cross-request microbatch |
| Microbatch, 4 workers, no access log / 0ms linger | 4,815.32 | 162ms | 239ms | 144,923 accepted |
| Safe optimized profile, 8 workers, cleanup 64 / 1ms | 7,812.72 | 192ms | 372ms | 235,181 accepted; all completed returned 202 |
| Separate load host, 12 workers / 2ms | 9,814.66 | 49ms | 151ms | Pre-profile best; no rejection or transport error |
| Async auth deployed, exact 10K | 9,922.35 | 41ms | 114ms | Repeated result after real image rebuild |
| Manual hot-route auth, exact 10K | 9,994.14 | 30ms | 74ms | All 300,000 completed and accepted |
| Ingress route ordering, exact 10K | 9,994.13 | 25ms | 90ms | 299,976 accepted; 24 generator drops |
| 10.1K offer, 800 slots | 10,069.68 | 24ms | 100ms | 491 generator drops; not a pass |
| 10.1K offer, 1,200 slots | 10,090.25 | 25ms | 93ms | 38 generator drops; not a pass |
| 10.1K offer, 2,000 slots | 10,093.33 | 27ms | 107ms | Zero drops; latency failed |
| Final strict candidate, 10.05K / 2,000 slots | **10,034.95** | **29ms** | **134ms** | 301,502/301,502 accepted; zero drops/errors |
| Formal 5m + 30m, 10.05K / 2,000 slots | **10,034.90** | **36ms** | **173ms** | 18,063,140 accepted; zero reject/transport error; 26,949 generator drops |
| v4.8.2 full-stack, 60s / 4,000 slots | 7,369.36 | 99ms | 1,679ms | 442,682 accepted; 160,321 generator drops; downstream Redis contention |
| v4.8.2 p12 full-stack, 60s / 4,000 slots | 7,772.28 | 222ms | 1,472ms | 468,783 accepted; end lag 333; 6/6 checkpoints |
| Temporary split Custody Redis, p12 / 60s | 9,197.21 | 395ms | 660ms | Throughput improved 18%; still failed and was removed |

The final short candidate proves that the optimized stack can cross 10K accepted events/s without loss at this scale. The later 35-minute run passed the revised 50/200ms Custody throughput/latency SLO, but did **not** pass the strict gate because the 2,000-slot generator dropped 0.149% of offers. Raising generator slots to 4,000 removed that artificial ceiling in a 10.5K diagnostic, but full-stack Redis contention then exposed the separate sustained Metering capacity limit.

## Profiling evidence

`py-spy` isolated FastAPI dependency solving/thread-pool dispatch, route matching, and custody/identity work on the GIL path. The retained auth and route changes removed the avoidable framework overhead; Pydantic and JSON were individually small contributors.

- [`profiles/fluxmeter-api-cpu.svg`](profiles/fluxmeter-api-cpu.svg)
- [`profiles/fluxmeter-api-wall.svg`](profiles/fluxmeter-api-wall.svg)
- [`profiles/fluxmeter-api-gil.svg`](profiles/fluxmeter-api-gil.svg)
- [`profiles/fluxmeter-api-gil-stable.svg`](profiles/fluxmeter-api-gil-stable.svg)

## Flink checkpoint finding

Repeated runs grew Redis into the millions of identities and exposed a separate streaming-state defect. The unused `aggregates.keyBy(agg -> "global").reduce(...)` branch had no sink but remained part of the execution graph. Its single `Keyed Reduce` operator held 465,158,470 bytes of a roughly 503 MiB checkpoint and eventually failed with `ArrayIndexOutOfBoundsException`.

After removal, the replacement job ran 12/12 tasks instead of 14/14 with no `Keyed Reduce`; its initial checkpoint fell to 21.99 MiB and completed in 243ms. Under later traffic the bounded event-dedup state still grew to about 129 MiB, but the runaway single global value and its observed checkpoint crash are gone. After the 4.8.1 API deployment, the job remained 12/12 `RUNNING` with 32/32 successful checkpoints; the latest checkpoint was 0.82 MiB in 22ms. All 12 business-topic partitions had zero lag. The continuously produced heartbeat partition stayed bounded three records ahead of its last committed checkpoint offset.

## v4.8.2 full-pipeline finding

The formal ingress run accumulated about 13.66 million business events behind Flink even though the public endpoint continued acknowledging Kafka custody. Redis reached 6 GiB and the TaskManager logged `OOM command not allowed when used memory > 'maxmemory'`; the old job recorded 14 failed checkpoints and seven restores. The main capacity defect was `EventProjectionSink` retaining one `projection:<sha256(eventId)>` key for 30 days—an unbounded second idempotency registry that contradicted the compact Custody design.

After draining all accepted events, only the benchmark projection markers were unlinked. v4.8.2 sets their default TTL to 600 seconds, configurable through `EVENT_PROJECTION_IDEMPOTENCY_TTL_SECONDS`; a live event measured 576 seconds remaining. The first replacement Job still inherited the base command's hidden `-p 2` despite the benchmark TaskManager exposing 12 slots. Job parallelism is now configurable and the benchmark defaults to p12. The corrected Job improved the 60-second full-stack sample from 7,369.36 to 7,772.28 eps and reduced end lag from about 178K to 333, with 6/6 checkpoints successful. A temporary split Custody Redis raised throughput to 9,197.21 eps without warmup (8,952.97 warmed), but p50/p99 remained 395/660ms or worse; it was removed. The 100K batch sample reached 32,567.14 eps (p50 2,949ms / p99 3,959ms). The synchronous Custody/Redis path remains the sustained pipeline bottleneck.

Post-recovery correctness stayed green: known-cost billing returned 1,000,001 input tokens and $0.15 with 39/39 checkpoints, and ClickHouse A1–A7 passed 12/12 assertions. On the retained p12/main-Redis configuration, a 200-event zero-backlog accepted-to-Redis projection sample materialized every event at p50 465.85ms / p99 896.81ms / max 909.60ms, passing the <20s event-to-billing target.

The Redis restart also showed that short-form Compose dependencies were insufficient: API workers initially saw `BUSYLOADING` and self-recovered, while the Flink job exhausted its restart strategy and remained failed. The retained base Compose now uses a Redis `PONG` healthcheck and gates Job submission, API, and Gateway startup on a fully loaded Redis.

## Rejected experiments

- **More API workers / shorter wait:** 13 workers and a 0.5ms microbatch wait both regressed throughput or tail latency; 12 workers / 1ms was retained.
- **Dedicated Custody Redis:** re-tested after the HTTP bottleneck shifted; 10,036 eps with p50 39ms / p99 168ms did not improve the gate, so the extra service was removed.
- **Embedded RocksDB state backend:** reduced checkpoint size dramatically, but the co-located strict test fell to 9,526 eps with p50 41ms / p99 590ms. HashMap was restored for this benchmark profile.

## Conclusion

The original 2,522.73 eps baseline improved to a lossless 10,034.95 eps short run—3.98×—and two serious state-retention defects were removed. HTTP Custody can cross 10K eps at the 50/200ms SLO, but this single-host stack cannot sustain the same rate through Flink billing while sharing Redis with Custody. The strict 10K full-pipeline and 100K batch gates remain open.
