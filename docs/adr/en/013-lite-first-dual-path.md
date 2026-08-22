# ADR-013: Lite-First Dual Path

| Field | Value |
|-------|-------|
| **Status** | Accepted (reversal) |
| **Date** | 2026-06-22 |
| **Version** | 2.0.2 |
| **Lang** | English |

## Context

v0.x–v1.x default `make demo` started Kafka + Flink + Redis — too heavy for side projects / <100K eps integrators. 1M eps is a credibility asset, not default DX.

## Decision

**Pivot in five commits on 2026-06-22**:
• `docker-compose.yml` = **Lite** (API → Redis Lua, no Kafka/Flink)
• `docker-compose.full.yml` = Full stack
• `make demo` = Lite; `make demo-full` = Full
• Same Redis key schema + OpenAPI contract
• Later layers: Lua aggregator (2.1.0) → rollup worker → Stripe export → SaaS control plane

## Consequences

✅ `docker-compose up` in ~30s; ✅ 90% of integrators zero Flink ops. ❌ Dual-path correctness regression tax (`make test-lite` + `make test-java`); Lite/Full semantics must stay aligned.

## Evidence

commits `66a1c70`, `abd76ae`, `75a3bf6`, `ac3f956`, `682ae1a`; `docs/superpowers/plans/2026-06-22-dual-path-lite-saas.md`.

## Architect's note

Demo throughput ≠ default DX. Disprove "you need Flink to meter" first; let 1M eps users take the Full path.

---

**中文:** [ADR-013](../zh/013-lite-first-dual-path.md)
