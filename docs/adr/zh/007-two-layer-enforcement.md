# ADR-007: 双层 Enforcement

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-19 |
| **Version** | 0.6.0 |
| **Lang** | 中文 |

## Context

Flink 窗口关窗后才扣费（10–15s 延迟）。Agent 循环可在 15 秒内烧光预算而系统无感知。行业共识：Pre-call > post-call（见 industry-billing-research-2026.md）。

## Decision

**双层模型**：
• **Layer 1 — Pre-request check**：`GET /budget/{id}/check`，<10ms，请求 provider 前硬闸。
• **Layer 2 — Post-window deduction**：Flink 聚合 → 原子 Lua 扣费 → Kafka kill signal。

## Consequences

✅ 关闭 10–15s enforcement gap；✅ SHOW_HN 标题从 1M eps 转向 <10ms check（v2.2.1）。❌ 集成者必须在 hot path 调 check（后由 wrap/proxy 强制，ADR-017）。

## Evidence

changLog 0.6.0；README two-layer table；progress Success Criteria #11。

---

**English:** [ADR-007](../en/007-two-layer-enforcement.md)
