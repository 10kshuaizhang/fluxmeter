# ADR-008: Three-Layer Resilient Budget Check

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-20 |
| **Version** | 0.9.1 |
| **Lang** | English |

## Context

If pre-request check hard-depends on Redis, Redis outages block the agent hot path — unacceptable for agent platforms.

## Decision

Three-layer resilience stack:
1. **In-process cache** (0.01ms, 30s TTL)
2. **Redis GET** (1–5ms, authoritative, updates cache on success)
3. **Fail policy** (`BUDGET_FAIL_POLICY=open|closed` when Redis is down)
Response includes `"source": "redis|cache|policy"` for observability. Gateway reuses the same logic (`api/budget_gate.py`).

## Consequences

✅ Agent workloads don't stall on infra failure. ❌ Cache may be slightly stale for 30s (acceptable — post-window remains authoritative settlement).

## Evidence

changLog 0.9.1; `api/budget_gate.py` docstring.

---

**中文:** [ADR-008](../zh/008-three-layer-resilient-budget-check.md)
