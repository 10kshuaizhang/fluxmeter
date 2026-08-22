# ADR-006: Incremental AggregateFunction

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-19 |
| **Version** | 0.4.0 |
| **Lang** | 中文 |

## Context

初版 `ProcessWindowFunction` 在窗口内攒全量 raw events。500K eps × 10s window = 500 万 events/key 峰值 → **OOM**。

## Decision

改用 **`AggregateFunction`**：窗口内只维护单个 `UsageAggregate` 累加器。内存 **O(keys)** 而非 O(events)。

## Consequences

✅ 5K eps 在 4GB TM 上稳定；✅ 1M eps burst 可跑 30–40s。❌ 窗口内无法做需要全量 event list 的操作（后由 side output DLQ 补 late data）。

## Evidence

changLog 0.4.0；`src/main/java/io/fluxmeter/job/UsageAggregateFunction.java`。

---

**English:** [ADR-006](../en/006-incremental-aggregate-function.md)
