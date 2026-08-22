# ADR-015: External Pricing Catalog

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-21 |
| **Version** | 2.4.0 |
| **Lang** | English |

## Context

Initial `UsageAggregate.calculateCost()` hardcoded flat rates — cannot handle 9+ models, cache/reasoning price deltas, volume/graduated tiers.

## Decision

• External **`config/pricing.json`**; dual implementation in Java `PricingCatalog` + Python `pricing_loader.py`.
• Support `flat` / `volume` / `graduated` + `volume_scope` / `billing_period` (2.4.0).
• **Re-rate**: differential adjustment (preview + apply), **not event replay** (0.8.0).
• Admin API: `GET /pricing`, `PUT /admin/pricing`, `POST /admin/pricing/validate`.

## Consequences

✅ 20+ models (incl. China domestic 2.6.0) without code changes; ✅ contrib community can PR pricing snapshots. ❌ Java/Python must stay in sync; tier re-rate returns 422 for non-flat (known limit).

## Evidence

changLog 1.3.0, 2.4.0, 2.6.0; `src/main/java/io/fluxmeter/pricing/PricingCatalog.java`.

---

**中文:** [ADR-015](../zh/015-external-pricing-catalog.md)
