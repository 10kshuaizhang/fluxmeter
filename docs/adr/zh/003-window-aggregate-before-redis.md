# ADR-003: 窗口聚合后再写 Redis

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-16 |
| **Version** | 0.1.0 |
| **Lang** | 中文 |

## Context

原始事件 1M eps 若 per-event 写 Redis，单实例 Redis 无法承受。需在流处理层降维。

## Decision

Keyed by `(customer_id, model_id)` → tumbling window（初版 60s，后改 **10s** 降内存）→ **聚合后** pipeline 写 Redis。10K customers × 9 models × 10s window ≈ **167 writes/sec**。

## Consequences

✅ Redis 成为查询层而非事件日志；Grafana 可直接 poll。❌ 查询粒度受窗口限制（后由 pre-request check 补实时性）。

## Evidence

DESIGN.md Next Steps #5；changLog 0.2.0 窗口 60s→10s；`src/main/java/io/fluxmeter/sink/RedisSink.java`。

---

**English:** [ADR-003](../en/003-window-aggregate-before-redis.md)
