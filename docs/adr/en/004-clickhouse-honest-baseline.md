# ADR-004: ClickHouse as Honest Baseline

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-19 |
| **Version** | 0.2.0 |
| **Lang** | English |

## Context

DESIGN Open Question #1: ClickHouse vs Postgres baseline? Postgres would show a more dramatic gap but is an unfair comparison.

## Decision

Keep `baseline/`: ClickHouse Kafka engine for comparison latency numbers. **Derived aggregates** (`usage_per_minute`) remain **benchmark-only** via `baseline/benchmark_init.sql` — not the billing path.

**Amended 2026-08-16 (ADR-025):** the same ClickHouse deployment also hosts the auditable **Cold Store** (`raw_events`). That is an audit copy of Kafka Token Events / Trusted Envelopes, still not billing truth. See [ADR-025](025-auditable-clickhouse-cold-store.md).

## Consequences

✅ Reproducible numbers for HN/README; ✅ honest comparison builds credibility. ❌ Baseline maintenance cost; not on the production path.

## Evidence

changLog 0.2.0; `make benchmark`; DESIGN Open Questions resolved.

---

**中文:** [ADR-004](../zh/004-clickhouse-honest-baseline.md)
