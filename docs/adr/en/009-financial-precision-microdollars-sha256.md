# ADR-009: Financial Precision: Microdollars + SHA-256

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-20 |
| **Version** | 1.0.0-rc1 |
| **Lang** | English |

## Context

Production audit found 10 financial/durability issues: float drift, hashCode collisions (1/77K), wrong Lua threshold semantics, partial WAL ack data loss, etc.

## Decision

• Internal **`costMicro` (long)**, external `getCostUsd()` conversion — zero precision drift.
• Idempotency keys use **SHA-256 64-bit prefix**, collision probability 1/4B.
• Lua threshold based on `initial_balance_usd`, not current balance.
• WAL per-event ack + drain on exit.

## Consequences

✅ 1.0.0-rc1 declared suitable for production billing workloads. ❌ Migration cost (internal long, API still float USD).

## Evidence

changLog 1.0.0-rc1 (10 issues); commit `e9642a5` rc3 WAL fix.

---

**中文:** [ADR-009](../zh/009-financial-precision-microdollars-sha256.md)
