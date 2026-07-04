# Load Testing

FluxMeter includes a Java Kafka load generator and a staged benchmark script.

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

# Manual infinite run at 1M target
make generate
```

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

MacBook docker-compose, **3 TaskManagers × 4 slots**, parallelism 12, `fluxmeter-2.2.2`:

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
