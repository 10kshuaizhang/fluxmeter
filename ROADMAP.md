# FluxMeter Roadmap

Forward-looking plan for the FluxMeter project. **Website:** [fluxmeter.dev](https://fluxmeter.dev). For **what shipped**, see [changLog.md](changLog.md). For **milestone checklists**, see [progress.md](progress.md). For **architecture intent**, see [docs/DESIGN.md](docs/DESIGN.md).

**Current version:** 2.5.0 (engine) · 1.2.0 (Python SDK on PyPI)  
**Last updated:** 2026-07-04

---

## Vision

Become the **open source standard** for real-time AI token metering and budget enforcement — streaming-first, self-hostable, and provider-agnostic. Compete on sub-second guardrails and exactly-once financial correctness, not on being another analytics dashboard.

**North star:** A developer runs `make demo`, calls `GET /budget/{id}/check` before every LLM request, and never wakes up to a runaway agent bill.

---

## Where we are today

| Layer | Status | Notes |
|-------|--------|-------|
| **Lite path** (default) | Shipped | API → Redis Lua; rollup worker; Stripe Meters export |
| **Full path** (Flink) | Shipped | 1M eps bursts; span attribution; DLQ; budget kill signals |
| **SaaS scaffold** | Shipped | Control plane `:8001`; tenant CRUD + plan limits; not a hosted product |
| **Open spec** | Shipped | `spec/schema`, OpenAPI, semantic conventions |
| **Python SDK** | Shipped | PyPI `fluxmeter` 1.2.0 (HTTP lite + Kafka full) |
| **JS SDK** | In repo | `@fluxmeter/client` — not on npm yet |
| **Production ops** | Partial | Helm, DR runbook, Prometheus profile, reconciliation job |
| **Tiered pricing engine** | Done | flat / volume / graduated; Lite + Flink; see `contrib/pricing/tiered-example.json` |

### Deployment paths

```
Lite (make demo)     →  side projects, <100K eps, zero Flink ops
Full (make demo-full) →  100K–1M eps, spans, DLQ, Kafka alerts
SaaS (make start-saas) → multi-tenant product builders
```

---

## Roadmap overview

```mermaid
gantt
    title FluxMeter roadmap (indicative)
    dateFormat YYYY-MM
    section v2.x Hardening
    Tiered pricing in Flink/Lite     :done, 2026-07, 6w
    JS SDK npm publish               :2026-07, 2w
    Docs + README version sync       :done, 2026-06, 1w
    section v3.0 SaaS
    Tenant-scoped lite ingest        :done, 2026-07, 4w
    Control plane billing UI stub    :2026-09, 6w
    RBAC + org model                 :2026-10, 8w
    section v3.x Platform
    Streaming proxy (mid-response kill) :2026-11, 8w
    Hosted SaaS (optional)           :2027-01, 12w
    section Ecosystem
    contrib provider packs           :2026-07, ongoing
    Orb/Lago/Stripe deep integrations :2026-08, ongoing
```

Timelines are **indicative**, not commitments.

---

## Phase 1 — v2.3: Polish & correctness ✓

**Target release:** 2.2.2 ✓ (2026-07-04)

---

## Phase 2 — v2.4–2.5: Billing depth ✓

**Goal:** Production-grade pricing and export without a hosted SaaS.  
**Target release:** 2.5.0 ✓ (2026-07-04) — tier pricing in 2.4.0, billing export/packages in 2.5.0

| Item | Priority | Success criteria | Status |
|------|----------|------------------|--------|
| **Tiered pricing in engine** | P0 | Integration test per tier boundary | ✓ 2.4.0 |
| Stripe Checkout wiring | P1 | Test mode E2E | ✓ 2.5.0 (mock + endpoint) |
| Calendar-aligned billing windows | P2 | Hourly + monthly export modes | ✓ 2.5.0 |
| Cost-based Stripe export | P2 | Config flag | ✓ `STRIPE_EXPORT_MODE=cost` |
| Credits / prepaid packages | P2 | API + docs | ✓ `/budget/{id}/package` |

**Follow-ups (Phase 2.1):** Kafka replay job for tier re-rating; Stripe Checkout live E2E with test keys; span-level tier pricing.

---

## Phase 3 — v3.0: Multi-tenant SaaS (medium-term)

**Goal:** Turn the control plane scaffold into a credible self-hosted SaaS backend.

| Item | Priority | Description | Success criteria |
|------|----------|-------------|------------------|
| **Full multi-tenant RBAC** | P0 | Org → tenant → customer hierarchy; role-based admin | API + control plane tests |
| Per-tenant API routing | P1 | Tenant API keys enforce scope on ingest/check/usage | 403 on cross-tenant access |
| Plan enforcement | P1 | Hard-stop ingest when `max_eps` / monthly cap exceeded | `test_control_plane.py` extended |
| Tenant usage dashboard | P2 | Grafana dashboard template per tenant | Provisioning doc |
| Postgres metadata store | P2 | Move `cp:tenant:*` from Redis to durable store (optional) | Migration guide |
| Stripe multi-tenant billing | P2 | Per-tenant Stripe customer + meter mapping | Admin API |

**Non-goal for v3.0:** Fully managed hosted FluxMeter cloud (see Phase 5).

---

## Phase 4 — v3.x: Real-time kill & proxy (medium-term)

**Goal:** Architecturally impossible-without-streaming demo — cut LLM streams mid-flight.

| Item | Priority | Description | Success criteria |
|------|----------|-------------|------------------|
| **Streaming proxy** | P0 | HTTP proxy between app and provider; respects budget-alerts | Demo GIF |
| Mid-response budget kill | P0 | Terminate stream when window cost exceeds hold | Latency < 1s from alert |
| Inference gateway adapters | P1 | LiteLLM / custom gateway hooks | Example in `contrib/` |
| Predictive cost estimation | P2 | Sliding-window spend rate → early warn | Optional Flink side job |

Reference: original DESIGN “Approach C” deferred item #18.

---

## Phase 5 — Platform & distribution (long-term)

| Item | Priority | Description |
|------|----------|-------------|
| **npm publish** `@fluxmeter/client` | P1 | Parity with Python SDK 1.2.0 HTTP transport |
| GHCR images | P2 | Pre-built API + Flink job images on release tags |
| Hosted SaaS (optional) | P3 | Managed Lite/Full tiers — only if community demand |
| Flink SQL / Table API port | P3 | Alternative job authoring for ops teams |
| Multi-region active-active | P3 | Kafka + Redis global; documented trade-offs |

---

## Ecosystem track (ongoing)

Parallel to version phases — grows the OpenCore surface without coupling to engine releases.

| Track | Items |
|-------|-------|
| **Spec** | `token-event-v2` only when breaking; keep v1 stable |
| **contrib/** | Provider adapters (Bedrock, Azure, Vertex), community pricing tables |
| **Integrations** | Deep guides for Lago, OpenMeter, Orb, Metronome, Zuora ([docs/integrations.md](docs/integrations.md)) |
| **ClickHouse baseline** | Keep benchmark honest vs store-then-query |
| **Community** | SHOW HN / launch, example apps, “FluxMeter + LangChain” cookbook |

---

## Explicit non-goals (for now)

- Replacing Stripe/Lago/Orb as **system of record** for invoicing — FluxMeter meters; platforms invoice
- Supporting non-token billing (API calls, storage GB) in core engine — use contrib connectors
- PyFlink rewrite of Java engine
- Guaranteed 1M eps on laptop docker-compose sustained (local Redis is the bottleneck)

---

## How to use this doc

| Audience | Start here |
|----------|------------|
| New contributor | [README.md](README.md) → `make demo` → this roadmap **Phase 1** |
| Billing engineer | [docs/pricing-hybrid-paths.md](docs/pricing-hybrid-paths.md) → **Phase 2** |
| SaaS builder | [docs/control-plane-api.md](docs/control-plane-api.md) → **Phase 3** |
| Ops / SRE | [docs/disaster-recovery.md](docs/disaster-recovery.md) → **Phase 5** GHCR |

**Propose changes:** Open an issue with `roadmap` label or PR that updates this file + `progress.md` checklist row.

---

## Version mapping (planned)

| Release | Theme | Engine | Python SDK |
|---------|-------|--------|------------|
| **2.5.0** ✓ | Phase 2 billing depth (export, packages, checkout) | 2.5.0 | 1.2.0 |
| **2.4.0** ✓ | Tiered pricing (flat/volume/graduated) | 2.4.0 | 1.1.x |
| **2.2.2** ✓ | Phase 1 polish | 2.2.2 | 1.1.0 |
| **3.0.0** | Multi-tenant SaaS backend | 3.0.0 | 2.0.0 |
| **3.1.0** | Streaming proxy + mid-flight kill | 3.1.0 | 2.1.0 |

SDK and engine versions are **independent semver**; table shows intended alignment milestones only.
