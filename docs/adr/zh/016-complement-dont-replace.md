# ADR-016: Complement, Don't Replace

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-07-04 |
| **Version** | 2.8.0 |
| **Lang** | 中文 |

## Context

2026-07 行业调研确认：Metronome/Orb/Stripe/Lago 在 Invoice SoR（合同、rating、invoice、payment）已成熟。FluxMeter 若自建 invoice 平台将进入 red ocean 且分散 engineering focus。

## Decision

• **定位 Runtime SoR**：meter + check + reserve + kill + export。
• **Export 而非 Replace**：Stripe / Metronome / Orb multi-target（`BILLING_EXPORT_TARGETS`）；partner recipes in `docs/integrations/`。
• **显式 non-goals**：ASC 606、MoR、multi-year commits、true-ups、replacing Langfuse as trace SoR。

## Consequences

✅ 与 invoice 平台共生；✅ 「FluxMeter + Metronome」recipe 可卖。❌ 不做 standalone billing product；客户须自备 invoice SoR 或使用 Stripe export。

## Evidence

changLog 2.5.0, 2.8.0；`docs/industry-billing-research-2026.md`；ROADMAP explicit non-goals。

---

**English:** [ADR-016](../en/016-complement-dont-replace.md)
