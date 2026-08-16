# ADR-004: ClickHouse 作为诚实对照组

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-19 |
| **Version** | 0.2.0 |
| **Lang** | 中文 |

## Context

DESIGN Open Question #1：ClickHouse vs Postgres baseline？Postgres 差距更 dramatic 但不公平。

## Decision

保留 `baseline/`：ClickHouse Kafka engine 用于对照延迟数字。**派生聚合**（`usage_per_minute`）仍仅通过 `baseline/benchmark_init.sql` / `make benchmark` — 非计费路径。

**2026-08-16 修订（ADR-025）：** 同一 ClickHouse 部署也承载可审计 **Cold Store**（`raw_events`）。那是 Kafka Token Event / Trusted Envelope 的审计副本，仍非计费真相。见 [ADR-025](025-auditable-clickhouse-cold-store.md)。

## Consequences

✅ HN/README 有可复现数字；✅ 诚实对比建立 credibility。❌ baseline 维护成本；不参与生产路径。

## Evidence

changLog 0.2.0；`make benchmark`；DESIGN Open Questions resolved。

---

**English:** [ADR-004](../en/004-clickhouse-honest-baseline.md)
