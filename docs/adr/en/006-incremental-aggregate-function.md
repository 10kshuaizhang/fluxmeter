# ADR-006: Incremental AggregateFunction

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-19 |
| **Version** | 0.4.0 |
| **Lang** | English |

## Context

Initial `ProcessWindowFunction` buffered all raw events per window. 500K eps × 10s window = 5M events/key peak → **OOM**.

## Decision

Switch to **`AggregateFunction`**: maintain a single `UsageAggregate` accumulator per window. Memory **O(keys)** not O(events).

## Consequences

✅ Stable at 5K eps on 4GB TMs; ✅ 1M eps bursts for 30–40s. ❌ Cannot operate on full event lists inside windows (late data handled via side-output DLQ).

## Evidence

changLog 0.4.0; `src/main/java/io/fluxmeter/job/UsageAggregateFunction.java`.

---

**中文:** [ADR-006](../zh/006-incremental-aggregate-function.md)
