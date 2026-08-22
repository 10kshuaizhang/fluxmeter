# ADR-014: Lite Atomic Lua Aggregator

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-22 |
| **Version** | 2.1.0 |
| **Lang** | English |

## Context

Initial `lite_aggregate.py` used Redis pipelines — idempotency, counters, and budget deduction were not atomic; crash windows could double-count or miss deductions. Rollup worker must compact live counters without destroying lifetime totals.

## Decision

• **`lite_aggregate_lua.py`**: single Lua EVAL for idempotency + counters + inline budget + global counters.
• **`rollup_worker.py`**: asyncio 60s cycle, compact into `buf:*` / period / day buckets.
• **3.2.1 lesson**: separate lifetime counters (`customer:{id}:*`) from buffer counters (`customer:{id}:buf:*`) — rollup archives buf only, never resets lifetime.

## Consequences

✅ Lite path financial semantics align with Full Flink sink. ❌ Lua debugging cost; rollup bugs can lose lifetime data (fixed in 3.2.1; legacy data not recoverable).

## Evidence

changLog 2.1.0, 3.2.1; commit `0be0827`; `api/rollup_worker.py` ponytail comment.

---

**中文:** [ADR-014](../zh/014-lite-atomic-lua-aggregator.md)
