# ADR-012: Apache 2.0 + OpenCore Layout

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-21 |
| **Version** | 1.1.0 |
| **Lang** | English |

## Context

DESIGN Open Question #4: Apache 2.0 vs AGPL? Cloud hosts without contributing vs maximum adoption.

## Decision

• **Apache 2.0** license.
• **OpenCore repo layout**: `spec/` (schema + OpenAPI) + `sdk/` (Python + JS) + `contrib/` (provider mappings) + `src/` (Java reference engine).
• **Commercial model**: all features open source; revenue from Hosted SaaS + onboarding + enterprise support (demand-gated).

## Consequences

✅ Maximum adoption; ✅ spec is the product surface, engine is reference implementation. ❌ Cloud vendors may fork without contributing (accepted trade-off).

## Evidence

changLog 1.1.0; DESIGN Open Questions resolved; Intelligence pivot spec monetization table.

---

**中文:** [ADR-012](../zh/012-apache-2-opencore-layout.md)
