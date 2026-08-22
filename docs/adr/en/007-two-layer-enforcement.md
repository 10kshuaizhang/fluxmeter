# ADR-007: Two-Layer Enforcement

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-19 |
| **Version** | 0.6.0 |
| **Lang** | English |

## Context

Flink deducts budget only after window close (10–15s delay). Agent loops can exhaust budget in 15s with no system awareness. Industry consensus: pre-call > post-call (industry-billing-research-2026.md).

## Decision

**Two-layer model**:
• **Layer 1 — Pre-request check**: `GET /budget/{id}/check`, <10ms, hard gate before provider call.
• **Layer 2 — Post-window deduction**: Flink aggregate → atomic Lua deduction → Kafka kill signal.

## Consequences

✅ Closes 10–15s enforcement gap; ✅ SHOW_HN hook shifts from 1M eps to <10ms check (v2.2.1). ❌ Integrators must call check on hot path (later enforced via wrap/proxy, ADR-017).

## Evidence

changLog 0.6.0; README two-layer table; progress Success Criteria #11.

---

**中文:** [ADR-007](../zh/007-two-layer-enforcement.md)
