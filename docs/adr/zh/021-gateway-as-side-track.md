# ADR-021: Gateway 作为 Side Track

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-07-11 |
| **Version** | 3.2.0 |
| **Lang** | 中文 |

## Context

原 ROADMAP Gateway 与 Intelligence MVP 竞争 engineering bandwidth。Intelligence pivot spec 明确：Gateway **不阻塞** 3.0.0 Intelligence tag。

## Decision

• **3.0.0** = Intelligence MVP major。
• **3.2.0** = Gateway P1（proxy + pre-check + stream kill）。
• 共享 `api/budget_gate.py` — `/check` 与 Gateway 同一逻辑，避免双实现。
• P2（LiteLLM hooks, TPM limits, predictive cost）明确 backlog，非 active。

## Consequences

✅ Intelligence 先 ship 验证 PMF；✅ Gateway 复用 budget gate。❌ Helm gateway deployment deferred to Phase G.1。

## Evidence

changLog 3.2.0；`docs/gateway.md`；Intelligence pivot spec Phase G section。

---

**English:** [ADR-021](../en/021-gateway-as-side-track.md)
