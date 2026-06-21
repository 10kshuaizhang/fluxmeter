# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run

```bash
./gradlew shadowJar          # Build fat JAR (output: build/libs/fluxmeter-<version>.jar)
make demo                    # One-command: build + start infra + submit Flink job + run load generator
make start                   # Start infrastructure only (Kafka, Flink, Redis, Grafana)
make submit-job              # Submit the Flink job to the running cluster
make generate                # Run the load generator locally (needs Java 17)
make load-test               # Staged load test (10K→1M eps)
make test-e2e                # Integration + v2 E2E tests
make stop                    # Stop all containers
make clean                   # Stop containers + clean build artifacts
```

## Architecture

FluxMeter is a streaming metering engine for AI token billing, built on Apache Flink.

**Data flow:**
```
LoadGenerator -> Kafka (token-events topic, 12 partitions)
    -> Flink TokenUsageAggregator (keyed by customer_id|model_id, 1-min tumbling window)
    -> RedisSink (pipelined writes of aggregated counters)
    -> Grafana (polls Redis for dashboard)
```

**Key design decisions:**
- Java 17 core engine for maximum Flink performance (no PyFlink serialization overhead)
- Window aggregation before Redis sink (~167 writes/sec for 10K customers, not 1M raw events/sec)
- Flat per-token pricing hardcoded in `UsageAggregate.calculateCost()` (v0.1 simplification)
- Events keyed by composite `customer_id|model_id` string for per-customer-per-model aggregation
- Checkpointing every 30s with hashmap state backend

**Infrastructure (docker-compose):**
- Kafka: KRaft mode (no ZooKeeper), single broker, 12 partitions
- Flink: 1 JobManager + 2 TaskManagers (4 slots each = 8 total parallelism)
- Redis: aggregated counters store (global + per-customer + per-model)
- Grafana: dashboard on port 3000 (admin/fluxmeter)

## Project Layout

- `src/main/java/io/fluxmeter/model/` - Data models (TokenEvent schema, UsageAggregate with cost calc)
- `src/main/java/io/fluxmeter/job/` - Flink job entry point and window function
- `src/main/java/io/fluxmeter/sink/` - Redis sink with connection pooling and pipelining
- `src/main/java/io/fluxmeter/generator/` - Multi-threaded Kafka load generator with rate limiting
- `docker-compose.yml` - Full infrastructure stack
- `grafana/provisioning/` - Auto-configured Grafana datasources

## Roadmap Context

This is Weekend 1 (performance demo). Planned additions:
- Weekend 2: Python SDK (`fluxmeter-client` on PyPI) + FastAPI query endpoint
- Weekend 3-4: Real-time budget enforcement (kill signals via Kafka control topic)
- Later: Multi-provider normalization, tiered pricing, credits/prepaid drawdown

Design doc: [docs/DESIGN.md](docs/DESIGN.md)

## Project Tracking (required)

Every meaningful change must update the root tracking files. Do this in the same session as the code change — not as a follow-up.

### Files

| File | Purpose |
|------|---------|
| [docs/DESIGN.md](docs/DESIGN.md) | Approved product/architecture spec — update only when scope or direction changes |
| [progress.md](progress.md) | Live implementation status vs design milestones |
| [changLog.md](changLog.md) | Versioned release history (Semantic Versioning) |

### When to update

**Always update `progress.md` and `changLog.md` when you:**
- Ship a feature, fix, or refactor that changes runtime behavior
- Complete or partially complete a checklist item in `progress.md`
- Bump the version in `build.gradle`
- Add or remove infrastructure, APIs, or user-facing docs

**Skip tracking updates for:** typo fixes, comment-only edits, or formatting with no behavioral impact.

### Version bumps (`changLog.md` + `build.gradle`)

- **PATCH** (0.1.x): bug fixes, small improvements, docs-only releases
- **MINOR** (0.x.0): new features within the current phase (e.g. ClickHouse baseline, Grafana dashboard)
- **MAJOR** (x.0.0): breaking API/schema changes or phase transitions (e.g. Python SDK launch)

After bumping, sync `progress.md` header (`Current version:`) and `build.gradle` `version`.

### `changLog.md` format

Add a new section at the top (below the header), newest first:

```markdown
## [0.1.2] — YYYY-MM-DD

### Added / Changed / Fixed / Removed
- Concise bullet per change

### Notes
- Optional context (benchmark results, known gaps, migration notes)
```

### `progress.md` format

1. Update **Current version** and **Current phase** if either changed.
2. Move checklist rows from Not started → Partial → Done as work lands; add notes.
3. Update **Success Criteria** status when measured or delivered.
4. Append a dated line under **Recent Activity** summarizing the session.

### Example workflow

After implementing the ClickHouse baseline:
1. Add `baseline/` code and docker-compose service
2. Bump `build.gradle` to `0.2.0`, add `[0.2.0]` entry to `changLog.md`
3. Mark checklist row #7 Done in `progress.md`, update Recent Activity
4. If a success criterion was measured, update that table too
