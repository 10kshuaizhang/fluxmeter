# ADR-001: Streaming-First, Reject Store-then-Query

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-16 |
| **Version** | 0.1.0 |
| **Lang** | English |

## Context

AI token billing needs real-time aggregation, exactly-once semantics, and backpressure. Mainstream tools (OpenMeter, Orb, Metronome) use store-then-query (persist events → batch aggregate → query). DESIGN.md evaluated three paths: A Weekend Rocket (performance demo), B Domain-first (billing semantics), C Budget Enforcer (kill signal).

## Decision

Choose **Approach A**: Apache Flink 1.18 DataStream API (Java 17) as the metering core. Kafka ingest → keyed tumbling windows → Redis sink. ClickHouse baseline for comparison only, not the product path.

## Consequences

✅ 500K eps sustained, 1M eps bursts proven; p99 aggregation latency sub-second (10s windows). ❌ Higher ops complexity than Redis/API-only; requires Flink + Kafka skills.

## Evidence

DESIGN.md Approach A; commit `81968fd` init; changLog 0.2.0: ClickHouse 8–43s lag vs Flink sub-second.

## Architect's note

AI token metering is **continuous aggregation + exactly-once**, not an OLAP query problem. Store-then-query is always one beat late in agent-loop workloads.

---

**中文:** [ADR-001](../zh/001-streaming-first-no-store-then-query.md)
