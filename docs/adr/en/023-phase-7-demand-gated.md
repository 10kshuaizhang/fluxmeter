# ADR-023: Phase 7+ Demand-Gated

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-07-11 |
| **Version** | 3.1.0 |
| **Lang** | English |

## Context

SaaS control plane scaffold shipped (2.2.0 `:8001` tenant CRUD), but Full RBAC / SSO / NL agent / Hosted SaaS need engineering investment without validated demand.

## Decision

• **Intelligence complete at 3.1.0** — no separate 4.0.0 Intelligence track.
• **Phase 7+ demand-gated**: Hosted SaaS, NL agent, enterprise RBAC, A/B pricing experiments — **start only with traction**.
• **Ongoing metering maintenance** not optional — every release requires `make test-java` + `make test-lite` green.

## Consequences

✅ Avoids premature SaaS build; ✅ clear open-source → paid conversion path. ❌ npm registry push still pending auth; Hosted SaaS not launched.

## Evidence

ROADMAP Phase 7+ table; progress.md Phase 7+ Planned; Intelligence pivot spec monetization.

---

**中文:** [ADR-023](../zh/023-phase-7-demand-gated.md)
