# ADR-023: Phase 7+ Demand-Gated

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-07-11 |
| **Version** | 3.1.0 |
| **Lang** | 中文 |

## Context

SaaS control plane scaffold 已 ship（2.2.0 `:8001` tenant CRUD），但 Full RBAC / SSO / NL agent / Hosted SaaS 需要 engineering 投入且无 validated demand。

## Decision

• **Intelligence complete at 3.1.0** — 无 separate 4.0.0 Intelligence track。
• **Phase 7+ demand-gated**：Hosted SaaS, NL agent, enterprise RBAC, A/B pricing experiments — **仅在有 traction 时启动**。
• **Ongoing metering maintenance** 非 optional — 每个 release 须 `make test-java` + `make test-lite` green。

## Consequences

✅ 避免 premature SaaS build；✅ 开源获客 → 付费转化路径清晰。❌ npm registry push 仍 pending auth；Hosted SaaS 未 launch。

## Evidence

ROADMAP Phase 7+ table；progress.md Phase 7+ Planned；Intelligence pivot spec monetization。

---

**English:** [ADR-023](../en/023-phase-7-demand-gated.md)
