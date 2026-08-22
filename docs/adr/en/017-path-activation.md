# ADR-017: Path Activation

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-07-06 |
| **Version** | 3.2.0 |
| **Lang** | English |

## Context

Industry decision waterfall Step 0: **must traffic pass through a control point?** Pure SDK libraries (call `check`) leak — developers forget pre-call check → overdraft. LiteLLM/Portkey/reseller gateways enforce on the proxy path. 2026-07-06 ROADMAP reprioritized path activation before Full SaaS RBAC.

## Decision

Three-layer progression:
1. **`wrap(OpenAI())`** — SDK 1.4.0, fail-open, pre-call check + post-call track + mid-stream kill (2.7.0)
2. **Lite webhooks** — `BUDGET_LOW` / `EXHAUSTED` / `WARN` 70/90 without Kafka (2.7.0)
3. **Gateway proxy** — OpenAI-compatible `:8080`, pre-check + stream reserve + mid-flight kill + proxy-only ingest (3.2.0)

## Consequences

✅ Integrators can't "forget check"; ✅ TokenBridge/ClipLive customer stories land. ❌ Proxy adds latency hop; stream kill uses char/4 heuristic when provider omits usage (ponytail).

## Evidence

commits `7d8ad82`, `83d23ca`; changLog 2.7.0, 3.2.0; `api/gateway/stream_guard.py`.

---

**中文:** [ADR-017](../zh/017-path-activation.md)
