# Load Testing

FluxMeter includes a Java Kafka load generator and a staged benchmark script.

## Quick start

```bash
make build
make start && sleep 15 && make submit-job

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
| `NUM_THREADS` | `4` | Producer threads |
| `DURATION_SEC` | `20` | Seconds per tier |
| `QUICK` | `0` | Set `1` to skip 1M tier |

## Reference results (2026-06-21)

MacBook docker-compose, single TaskManager (4 slots), `fluxmeter-2.0.1`:

| Target EPS | Avg EPS | Peak EPS | Notes |
|------------|---------|----------|-------|
| 10K | ~9.3K | ~18K | Stable |
| 50K | ~49K | ~92K | Stable |
| 100K | varies | varies | Warm Kafka between tiers |
| 500K+ | ~40–45K | ~67–88K | Local Redis/Flink bound |

Pipeline verified: `global:total_events` and `last_window_end` advance during load.

For 500K+ sustained throughput, use multiple TaskManagers, more slots, and production Kafka/Redis (see [production-deploy.md](production-deploy.md)).

## Generator internals

`io.fluxmeter.generator.LoadGenerator` — weighted multi-model traffic, rate-limited per thread:

```bash
KAFKA_BROKERS=localhost:9094 TARGET_EPS=100000 NUM_THREADS=4 \
  java -cp build/libs/fluxmeter-*.jar io.fluxmeter.generator.LoadGenerator
```
