# ADR-015: 外部 Pricing Catalog

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-21 |
| **Version** | 2.4.0 |
| **Lang** | 中文 |

## Context

初版 `UsageAggregate.calculateCost()` 硬编码 flat rate — 无法应对 9+ models、cache/reasoning 差价、volume/graduated tiers。

## Decision

• **`config/pricing.json`** 外部 catalog；Java `PricingCatalog` + Python `pricing_loader.py` 双实现。
• 支持 `flat` / `volume` / `graduated` + `volume_scope` / `billing_period`（2.4.0）。
• **Re-rate**：differential adjustment（preview + apply），**非 event replay**（0.8.0）。
• Admin API：`GET /pricing`, `PUT /admin/pricing`, `POST /admin/pricing/validate`。

## Consequences

✅ 20+ models（含中国 domestic 2.6.0）无需改代码；✅ contrib 社区可 PR pricing snapshot。❌ Java/Python 双实现须同步；tier re-rate 对 non-flat 返回 422（已知限制）。

## Evidence

changLog 1.3.0, 2.4.0, 2.6.0；`src/main/java/io/fluxmeter/pricing/PricingCatalog.java`。

---

**English:** [ADR-015](../en/015-external-pricing-catalog.md)
