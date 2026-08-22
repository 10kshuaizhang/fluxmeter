# ADR-012: Apache 2.0 + OpenCore 分层

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-21 |
| **Version** | 1.1.0 |
| **Lang** | 中文 |

## Context

DESIGN Open Question #4：Apache 2.0 vs AGPL？云厂商 host-without-contribute 风险 vs 最大 adoption。

## Decision

• **Apache 2.0** license。
• **OpenCore 仓库分层**：`spec/`（schema + OpenAPI）+ `sdk/`（Python + JS）+ `contrib/`（provider mappings）+ `src/`（Java reference engine）。
• **商业模型**：全功能开源；revenue 来自 Hosted SaaS + onboarding + enterprise support（demand-gated）。

## Consequences

✅ 最大 adoption；✅ spec 是产品 surface，engine 是 reference implementation。❌ 云厂商可 fork 不提供回馈（accepted trade-off）。

## Evidence

changLog 1.1.0；DESIGN Open Questions resolved；Intelligence pivot spec monetization table。

---

**English:** [ADR-012](../en/012-apache-2-opencore-layout.md)
