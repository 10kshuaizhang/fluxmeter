# ADR-022: Major Version = Narrative Shift

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-07-11 |
| **Version** | 3.0.0 |
| **Lang** | English |

## Context

Semver convention: major = breaking API. 3.0.0 is a product narrative shift (Layer 3 → Layer 4) with no breaking metering endpoint changes.

## Decision

**3.0.0 major bump for narrative shift**, not API break. changLog explicitly notes: "Major bump = product narrative shift, not breaking API for existing metering endpoints."

## Consequences

✅ Version communicates strategic pivot; ✅ no forced migration for existing integrators. ❌ Semver purists may be confused — document explicitly.

## Evidence

changLog 3.0.0 Notes.

---

**中文:** [ADR-022](../zh/022-major-version-narrative-shift.md)
