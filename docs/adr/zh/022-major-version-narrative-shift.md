# ADR-022: Major Version = 叙事变更

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-07-11 |
| **Version** | 3.0.0 |
| **Lang** | 中文 |

## Context

Semver 惯例：major = breaking API。3.0.0 是产品叙事 shift（Layer 3 → Layer 4），metering endpoints 无 breaking change。

## Decision

**3.0.0 major bump 因 narrative shift**，非 API break。changLog 明确注明：「Major bump = product narrative shift, not breaking API for existing metering endpoints。」

## Consequences

✅ 版本号传达战略转向；✅ 现有集成者无 forced migration。❌ Semver purists 可能困惑 — 文档须 explicit。

## Evidence

changLog 3.0.0 Notes。

---

**English:** [ADR-022](../en/022-major-version-narrative-shift.md)
