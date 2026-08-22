# ADR-011: Reserve/Reconcile Single-Path Billing

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-20 |
| **Version** | 1.2.0 |
| **Lang** | English |

## Context

Streaming LLM responses can last 30–120s. Waiting for window close allows concurrent requests to overdraw the same wallet. Industry Hold step: SpendGuard reserve/commit, Stripe auth/capture.

## Decision

• `POST /budget/{id}/reserve` — pessimistic hold (`held_usd`), does not deduct balance directly.
• `POST /budget/{id}/reconcile` — release hold, settle difference.
• **Flink Sink is the sole `balance_usd` mutator** (1.2.0 fixes streaming double-charge).
• `check` uses `effective_balance = balance - held`.

## Consequences

✅ Budget safety in streaming; ✅ semantic foundation for SDK `wrap_stream` + Gateway stream kill. ❌ Higher integration complexity (three APIs: check / reserve / reconcile).

## Evidence

changLog 0.8.0, 1.2.0; industry-billing-research §3 Hold step.

---

**中文:** [ADR-011](../zh/011-reserve-reconcile-single-path-billing.md)
