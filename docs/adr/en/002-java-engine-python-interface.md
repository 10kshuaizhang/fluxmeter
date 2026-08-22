# ADR-002: Java Engine + Python Interface (Hybrid Stack)

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-16 |
| **Version** | 0.1.0 |
| **Lang** | English |

## Context

Split audience: billing engineers want throughput and EO semantics; AI developers want three-line `pip install` integration. PyFlink unifies language but adds serialization overhead.

## Decision

**Java 17 Flink for the engine, Python for SDK + FastAPI API layer**. Explicitly reject PyFlink rewrite (ROADMAP non-goals). Engine and SDK use **independent semver** (engine 3.x / SDK 1.5.x).

## Consequences

✅ 1M+ eps without PyFlink overhead; AI community adopts via Python. ❌ Dual-language maintenance; pricing logic must exist in Java + Python (later unified via `config/pricing.json`).

## Evidence

DESIGN.md "Architecture: Java Core + Python SDK"; commit `81968fd`.

---

**中文:** [ADR-002](../zh/002-java-engine-python-interface.md)
