# Design: FluxMeter Intelligence Pivot (Layer 4)

**Date:** 2026-07-11  
**Status:** APPROVED (user review)  
**Scope:** Roadmap restructure — product narrative shift while retaining metering pillar

## Summary

FluxMeter repositions from "runtime guardrail only" to **AI Monetization Platform**: **Layer 3 metering + guardrail** (maintained) plus **Layer 4 monetization intelligence** (primary delivery track).

## Decisions (locked)

| Decision | Choice |
|----------|--------|
| Metering fate | **Retain and maintain** — dual pillar, not archived |
| Product narrative | Layer 4 Intelligence primary; Layer 3 metering ongoing |
| Phase 5 Gateway | **Side track (Phase G)** — parallel, does not block Intelligence MVP |
| MVP data sources | **Dual-source:** FluxMeter native + ≥1 overlay (OpenMeter or Langfuse) |
| Commercial model | **All open source**; Hosted SaaS + onboarding + enterprise support |
| Roadmap structure | **方案 A:** renumber — Foundation F0–F4 done, Phase 5 Intelligence MVP active |

## Dual-pillar architecture

```text
[Pillar A: Metering & Guardrail]
  Lite/Full ingest → pricing → check/reserve/kill → export to invoice SoR
  Ongoing maintenance + Phase G Gateway evolution

[Pillar B: Monetization Intelligence]
  Inputs: native FluxMeter usage/cost + overlay (Langfuse/OpenMeter/Helicone)
  Outputs: root cause, unit economics, simulation, (later) pricing optimizer
  Optional feedback: budget alerts, recommendations → Pillar A enforcement
```

## Phase 5 Intelligence MVP (2–3 months)

1. **Root Cause Analysis** — automatic spend delta decomposition
2. **Unit Economics** — revenue vs cost, margin, loss alerts, simple recommendations
3. **Scenario Simulation** — model switch / prompt / promo what-if
4. **Dual-source ingest** — native + one overlay connector
5. **Prescriptive summary** — Finance/CEO one-pager (P1)
6. **Landing** — "OpenMeter tells you what happened; FluxMeter tells you what to do next"

## Phase G (metering side track)

Original Gateway plan: proxy meter + limit + mid-flight kill. Ships as 3.0.x without blocking 3.0.0 Intelligence MVP tag.

## Phase 6 Intelligence v1.0

Pricing Optimizer, Profitability Dashboard, Anomaly Alerts, Forecasting, Export/Sharing.

## Non-goals (Phase 5)

- Replacing observability or invoice platforms as SoR
- Pricing Optimizer, NL agent, SSO/RBAC in MVP
- Freezing or deprecating metering engine

## Monetization

| Surface | Model |
|---------|-------|
| Engine + Intelligence features | Open source |
| Hosted SaaS | Paid — connectors, uptime, managed ingest |
| Onboarding | Paid — 30-min margin opportunity session |
| Enterprise | Paid — SSO/RBAC/compliance implementation |

## Version plan

- **3.0.0** — Intelligence MVP (narrative major; document in changLog)
- **3.0.x** — Gateway + metering maintenance
- **3.1+/4.0** — Intelligence v1.0

## References

- Market map and MVP priority list: user strategy brief 2026-07-11
- Prior runtime positioning: [ROADMAP.md](../../../ROADMAP.md) pre-pivot, [docs/industry-billing-research-2026.md](../../industry-billing-research-2026.md)
