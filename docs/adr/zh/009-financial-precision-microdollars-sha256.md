# ADR-009: 金融精度：microdollars + SHA-256

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-20 |
| **Version** | 1.0.0-rc1 |
| **Lang** | 中文 |

## Context

Production audit 发现 10 项金融/durability 问题：float 累加漂移、hashCode 碰撞（1/77K）、Lua threshold 语义错误、WAL 部分 ack 丢数据等。

## Decision

• 内部用 **`costMicro` (long)**，对外 `getCostUsd()` 转换 — 零精度漂移。
• Idempotency key 用 **SHA-256 64-bit prefix**，碰撞概率 1/4B。
• Lua threshold 基于 `initial_balance_usd` 而非 current balance。
• WAL 逐 event ack + exit 前 drain。

## Consequences

✅ 1.0.0-rc1 声明「适合 production billing workloads」。❌ 迁移成本（内部 long，API 仍 float USD）。

## Evidence

changLog 1.0.0-rc1（10 issues）；commit `e9642a5` rc3 WAL fix。

---

**English:** [ADR-009](../en/009-financial-precision-microdollars-sha256.md)
