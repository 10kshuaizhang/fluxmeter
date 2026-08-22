# ADR-021: Gateway as Side Track

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-07-11 |
| **Version** | 3.2.0 |
| **Lang** | English |

## Context

Original ROADMAP Gateway competed with Intelligence MVP for engineering bandwidth. Intelligence pivot spec: Gateway **must not block** 3.0.0 Intelligence tag.

## Decision

• **3.0.0** = Intelligence MVP major.
• **3.2.0** = Gateway P1 (proxy + pre-check + stream kill).
• Share `api/budget_gate.py` — `/check` and Gateway use same logic, no duplicate implementation.
• P2 (LiteLLM hooks, TPM limits, predictive cost) explicitly backlog, not active.

## Consequences

✅ Intelligence ships first to validate PMF; ✅ Gateway reuses budget gate. ❌ Helm gateway deployment deferred to Phase G.1.

## Evidence

changLog 3.2.0; `docs/gateway.md`; Intelligence pivot spec Phase G section.

---

**中文:** [ADR-021](../zh/021-gateway-as-side-track.md)
