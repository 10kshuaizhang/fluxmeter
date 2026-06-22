# FluxMeter Reference Engine

Apache Flink streaming aggregation, budget enforcement, and Redis sinks.
This is the **reference implementation** of the open spec in [`spec/`](../spec/).

## What lives here

| Package | Role |
|---------|------|
| `io.fluxmeter.model` | TokenEvent, UsageAggregate, SpanAggregate |
| `io.fluxmeter.job` | TokenUsageAggregator Flink job |
| `io.fluxmeter.sink` | Redis sinks, BudgetEnforcerSink, SpanSink |
| `io.fluxmeter.generator` | Load generator for benchmarks |

## Build

```bash
./gradlew shadowJar
# → build/libs/fluxmeter-1.x.x.jar
```

## Not required for adoption

Integrators can consume `token-events` JSON with any stream processor, or use **lite mode** (`make demo`, alias `make demo-lite`) which aggregates in the API layer without Flink.

## Performance-critical internals

Window tuning, incremental aggregation (OOM-safe), OptimizedRedisSink batching, and budget Lua scripts are maintained here — not duplicated in `spec/` or `contrib/`.
