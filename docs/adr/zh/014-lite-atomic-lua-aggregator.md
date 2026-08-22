# ADR-014: Lite 原子 Lua 聚合器

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-22 |
| **Version** | 2.1.0 |
| **Lang** | 中文 |

## Context

初版 `lite_aggregate.py` 用 Redis pipeline — idempotency、counters、budget deduction 非原子，crash window 可导致 double-count 或漏扣。Rollup worker 需 compaction live counters 但不破坏 lifetime totals。

## Decision

• **`lite_aggregate_lua.py`**：单 Lua EVAL 完成 idempotency + counters + inline budget + global counters。
• **`rollup_worker.py`**：asyncio 60s 周期，compact 到 `buf:*` / period / day buckets。
• **3.2.1 教训**：lifetime counters（`customer:{id}:*`）与 buffer counters（`customer:{id}:buf:*`）**分离** — rollup 只 archive buf，不 reset lifetime。

## Consequences

✅ Lite path 金融语义对齐 Full Flink sink。❌ Lua 脚本调试成本；rollup 逻辑错误可导致 lifetime 数据丢失（3.2.1 已修复，旧数据不可恢复）。

## Evidence

changLog 2.1.0, 3.2.1；commit `0be0827`；`api/rollup_worker.py` ponytail comment。

---

**English:** [ADR-014](../en/014-lite-atomic-lua-aggregator.md)
