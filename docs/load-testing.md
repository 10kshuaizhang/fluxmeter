# Load Testing

FluxMeter includes a Java Kafka load generator and a staged benchmark script.

**Performance overview:** [fluxmeter.dev](https://fluxmeter.dev) · methodology in this doc

## Quick start

```bash
make build
make start-full && sleep 15 && make submit-job

# Mac docker-compose honest ceiling: ~25K avg sustained at 50K target tier
# (Redis Lua sink bound — not Flink). See Reference results below.

# Staged tiers: 10K → 50K → 100K → 500K → 1M eps (15s each)
make load-test

# Skip 1M tier
make load-test-quick

# Known-event correctness (cost/counters) + Flink checkpoint health
make correctness-bench

# Manual infinite run at 1M target
make generate
```

## Exactly-once semantics (Full / Flink)

FluxMeter’s financial EO is **application-level effectively-once**:

1. Flink checkpoints (30s, `CheckpointingMode.EXACTLY_ONCE`) restore operator state + Kafka offsets when `CHECKPOINT_DIR` is set.
2. Redis sinks use window-level `SET NX` (`applied:{customer}|{model}|{windowStart}`) inside a single Lua EVAL so a crash cannot mark a window applied without writing counters (`BudgetEnforcerSink` / `RedisSink`).
3. Watermarks: 5s bounded out-of-orderness + 30s idleness; **no** `allowedLateness` (late events → Kafka DLQ) so window re-fires cannot fight SET NX.

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

## Reference results (2026-06-22)

MacBook docker-compose, **3 TaskManagers × 4 slots**, parallelism 12, `fluxmeter-2.6.1`:

| Target EPS | Avg EPS | Peak EPS | Notes |
|------------|---------|----------|-------|
| 10K | ~98% | ~23K | Stable |
| 50K | ~96% | ~130K | Stable |
| 100K | target | varies | Requires scaled compose (see docker-compose.full.yml) |
| 500K–1M | burst | ~145K+ | Local Redis Lua sink bound; peak bursts OK |

Prior run (2026-06-21, 1 TM): 50K stable; 100K+ Redis-bound.

For 500K+ sustained throughput, use multiple TaskManagers, more slots, and production Kafka/Redis (see [production-deploy.md](production-deploy.md)).

### High-throughput local profile

`docker-compose.full.yml` defaults to 3 TaskManagers (12 slots), Redis 4G + io-threads, Kafka 24 partitions:

```bash
make start-full     # ~12 GB RAM recommended
make submit-job     # parallelism 12
NUM_THREADS=8 make load-test
```

## Generator internals

`io.fluxmeter.generator.LoadGenerator` — weighted multi-model traffic, rate-limited per thread:

```bash
KAFKA_BROKERS=localhost:9094 TARGET_EPS=100000 NUM_THREADS=4 \
  java -cp build/libs/fluxmeter-*.jar io.fluxmeter.generator.LoadGenerator
```
