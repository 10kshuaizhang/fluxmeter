# ADR-019: Intelligence Pivot（Layer 4）

| Field | Value |
|-------|-------|
| **Status** | Accepted（决策反转） |
| **Date** | 2026-07-11 |
| **Version** | 3.0.0 |
| **Lang** | 中文 |

## Context

L3 Metering（OpenMeter, Metronome, Lago）crowded；L4 Intelligence（root cause, unit economics, simulation）blue ocean。FinOps Foundation 2026：98% teams managing AI spend。Metering 已是 credibility 资产，非唯一卖点。

## Decision

• **双柱平台**：Pillar A Metering **保留维护**，非 deprecated；Pillar B Intelligence 为 **产品叙事主战场**。
• Tagline：「OpenMeter tells you what happened; FluxMeter tells you what to do next.」
• Phase 5 MVP（3.0.0）：root cause + unit economics + simulation + OpenMeter overlay。
• Phase 6 v1.0（3.1.0）：pricing optimizer + profitability + forecast + alerts + report。

## Consequences

✅ L4 差异化；✅ 同一 rollups feed Intelligence。❌ 双柱维护负担；README/HN 叙事需同步（commit `36eef03` strategic positioning）。

## Evidence

commit `36eef03`, `83d23ca`；`docs/superpowers/specs/2026-07-11-intelligence-pivot-design.md`；changLog 3.0.0, 3.1.0。

---

**English:** [ADR-019](../en/019-intelligence-pivot-layer-4.md)
