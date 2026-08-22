# ADR-008: 三层弹性 Budget Check

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-20 |
| **Version** | 0.9.1 |
| **Lang** | 中文 |

## Context

Pre-request check 若强依赖 Redis，Redis 故障会阻塞 agent hot path — 对 agent 平台不可接受。

## Decision

三层 resilience stack：
1. **In-process cache**（0.01ms，30s TTL）
2. **Redis GET**（1–5ms，权威源，成功后更新 cache）
3. **Fail policy**（`BUDGET_FAIL_POLICY=open|closed`，Redis 不可用时）
Response 含 `"source": "redis|cache|policy"` 可观测。Gateway 复用同一逻辑（`api/budget_gate.py`）。

## Consequences

✅ Agent 工作负载不因 infra 故障停摆。❌ Cache 30s 内可能略 stale（可接受 — post-window 仍是权威结算）。

## Evidence

changLog 0.9.1；`api/budget_gate.py` docstring。

---

**English:** [ADR-008](../en/008-three-layer-resilient-budget-check.md)
