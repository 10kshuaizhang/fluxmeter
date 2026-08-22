# ADR-016: Complement, Don't Replace

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-07-04 |
| **Version** | 2.8.0 |
| **Lang** | English |

## Context

July 2026 industry research: Metronome/Orb/Stripe/Lago own Invoice SoR (contracts, rating, invoices, payments). Building invoice platform in-house enters red ocean and splits engineering focus.

## Decision

• **Position as Runtime SoR**: meter + check + reserve + kill + export.
• **Export, don't replace**: Stripe / Metronome / Orb multi-target (`BILLING_EXPORT_TARGETS`); partner recipes in `docs/integrations/`.
• **Explicit non-goals**: ASC 606, MoR, multi-year commits, true-ups, replacing Langfuse as trace SoR.

## Consequences

✅ Coexist with invoice platforms; ✅ "FluxMeter + Metronome" recipes as GTM asset. ❌ Not a standalone billing product; customers need their own invoice SoR or Stripe export.

## Evidence

changLog 2.5.0, 2.8.0; `docs/industry-billing-research-2026.md`; ROADMAP explicit non-goals.

---

**中文:** [ADR-016](../zh/016-complement-dont-replace.md)
