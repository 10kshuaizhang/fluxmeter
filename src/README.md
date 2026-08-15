# FluxMeter Reference Engine

Apache Flink streaming aggregation, budget enforcement, and Redis sinks.

**Website:** [fluxmeter.dev](https://fluxmeter.dev) · This is the **reference implementation** of the open spec in [`spec/`](../spec/).

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

## Customer boundary

Applications integrate through the HTTP API or Gateway. Kafka and this engine are mandatory internal components; only trusted operator replay and benchmark tools publish directly.

## Performance-critical internals

Window tuning, incremental aggregation (OOM-safe), OptimizedRedisSink batching, and budget Lua scripts are maintained here — not duplicated in `spec/` or `contrib/`.
