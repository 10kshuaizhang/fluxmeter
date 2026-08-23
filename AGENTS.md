# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Build & Run

```bash
./gradlew shadowJar          # Build fat JAR (output: build/libs/fluxmeter-<version>.jar)
make demo                    # Only architecture: HTTP + Kafka + Flink + Redis + Gateway + Grafana
make start                   # Build and start the base stack; Flink job submits automatically
make start-benchmark         # Scaled overlay + ClickHouse cold store + trusted Kafka port
make demo-proof              # Deterministic reserve → meter → kill → audit (no provider key)
make demo-gateway            # Gateway mock self-check (no live OpenAI)
make generate                # Trusted internal load generator (benchmark profile)
make load-test               # Staged internal engine load test (10K→1M eps)
make http-load-test          # Formal public HTTP gates (single + batch)
make test-e2e                # Integration + v2 E2E tests
make test-unit               # Python, SDK, JavaScript, and Java unit tests
make test-java               # Java unit tests only
make test-cold-store         # ADR-025 ClickHouse cold-store acceptance (benchmark up)
make validate-spec           # OpenAPI / schema validation
make stop                    # Stop all containers
make clean                   # Stop containers + clean build artifacts
```

There is **no Lite path**. HTTP → Kafka → Flink → Redis is the only supported architecture (ADR-024).

## Architecture

FluxMeter is a streaming metering and budget-enforcement engine for AI token billing, built on Apache Flink.

**Data flow:**
```
Application / SDK / Gateway
  → HTTP API (Custody: identity claim → Kafka ACK + finalize → 202)
  → Kafka (token-events, 12 partitions; private in base profile)
  → Flink TokenUsageAggregator (keyed by customer_id|model_id, 10-sec tumbling window)
  → RedisSink / BudgetEnforcerSink (pipelined aggregates + atomic budget deduction)
  → Query API + Grafana
ClickHouse cold store (benchmark overlay only; audit copy, not billing truth)
```

**Key design decisions:**
- HTTP is the only customer event boundary; Kafka is internal transport (ADR-024)
- Four deep modules: Custody / Pricing / Reservation / Budget (ADR-026)
- Java 17 Flink engine for aggregation and billing truth; FastAPI for ingest, query, Gateway, Intelligence
- Shared JSON pricing catalog (tiered / hybrid); Flink `PricingCatalog` is billing truth
- Events keyed by composite `customer_id|model_id` (tenant-scoped where present)
- Checkpointing every 30s; Flink per-event projection idempotency TTL defaults to 10 minutes (not a second 30-day registry)
- Pre-request budget check `<10ms` (cache → Redis → fail policy); post-window deduction ~10–15s

**Infrastructure (docker-compose):**
- Kafka: KRaft mode (no ZooKeeper), single broker, 12 partitions, **private in base**; operator port only via `start-benchmark`
- Flink: 1 JobManager + 1 TaskManager — base parallelism/slots **2**; benchmark overlay **12**
- Redis: custody identity, budgets, projections, query rollups, Gateway outbox
- API `:8000` + Gateway `:8080` + webhook-worker
- Grafana `:3000` (admin/fluxmeter)
- ClickHouse: cold-store audit on benchmark overlay (ADR-025)

## Project Layout

| Path | Purpose |
|------|---------|
| `api/` | FastAPI: Custody ingest, budget/reserve, usage query, Intelligence, Gateway |
| `src/main/java/io/fluxmeter/` | Flink engine: `job/`, `model/`, `pricing/`, `sink/`, `generator/` |
| `sdk/python/`, `sdk/js/` | HTTP SDKs (`fluxmeter` / `@fluxmeter/client`) |
| `spec/` | Event schema, OpenAPI, semantic conventions |
| `contrib/` | Provider mappings, pricing, connectors |
| `demos/` | Gateway / path-activation / proof demos |
| `baseline/` | ClickHouse baseline comparison |
| `services/control-plane/` | Control-plane / SaaS helpers |
| `docs/` | Design, ADRs, API, gateway, intelligence, runbooks |
| `tests/` | Python integration / unit / E2E |
| `docker-compose.yml` | Base stack |
| `docker-compose.benchmark.yml` | Scaled + ClickHouse overlay |
| `grafana/provisioning/` | Auto-configured Grafana datasources |

## Roadmap Context

**Current version:** engine/API **4.8.3** · Python SDK **2.0.0**  
**Active phase:** Metering custody / performance hardening (HTTP throughput gates still open)  
**Done:** Pillar B Intelligence · Phase G Gateway P1 · four deep modules · cold store

Dual-pillar product:
- **Pillar A — Metering & Guardrail:** HTTP custody, Flink billing, check/reserve/kill, export
- **Pillar B — Monetization Intelligence:** root cause, unit economics, simulation (demand-gated maintenance)

See [ROADMAP.md](ROADMAP.md), [progress.md](progress.md), [docs/DESIGN.md](docs/DESIGN.md).

## Project Tracking (required)

Every meaningful change must update the root tracking files. Do this in the same session as the code change — not as a follow-up.

### Files

| File | Purpose |
|------|---------|
| [docs/DESIGN.md](docs/DESIGN.md) | Approved product/architecture spec — update only when scope or direction changes |
| [ROADMAP.md](ROADMAP.md) | Forward-looking phases and priorities |
| [progress.md](progress.md) | Live implementation status vs design milestones |
| [changLog.md](changLog.md) | Versioned release history (Semantic Versioning) |

### When to update

**Always update `progress.md` and `changLog.md` when you:**
- Ship a feature, fix, or refactor that changes runtime behavior
- Complete or partially complete a checklist item in `progress.md`
- Bump the version in `build.gradle`
- Add or remove infrastructure, APIs, or user-facing docs

**Skip tracking updates for:** typo fixes, comment-only edits, or formatting with no behavioral impact.

### Version bumps (`changLog.md` + `build.gradle`)

- **PATCH** (4.8.x): bug fixes, small improvements, docs-only releases
- **MINOR** (4.x.0): new features within the current phase
- **MAJOR** (x.0.0): breaking API/schema changes or phase transitions

After bumping, sync `progress.md` header (`Current version:`) and `build.gradle` `version`.

### `changLog.md` format

Add a new section at the top (below the header), newest first:

```markdown
## [4.8.3] — YYYY-MM-DD

### Added / Changed / Fixed / Removed
- Concise bullet per change

### Notes
- Optional context (benchmark results, known gaps, migration notes)
```

### `progress.md` format

1. Update **Current version** and **Current phase** if either changed.
2. Move checklist rows from Not started → Partial → Done as work lands; add notes.
3. Update **Success Criteria** status when measured or delivered.
4. Append a dated line under **Recent Activity** summarizing the session.

### Example workflow

After a docs-only agent-guide sync:
1. Align `AGENTS.md` / `CLAUDE.md` with `Makefile` + README architecture
2. Bump `build.gradle` PATCH, add `[4.8.x]` entry to `changLog.md`
3. Append a Recent Activity line in `progress.md`

## Agent skills

### Issue tracker

Issues live in GitHub Issues for `10kshuaizhang/fluxmeter` (via `gh`). See `docs/agents/issue-tracker.md`.

### Triage labels

Canonical roles map 1:1 to tracker labels (`needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, `wontfix`). See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: root [`CONTEXT.md`](CONTEXT.md) + [`docs/adr/`](docs/adr/). See `docs/agents/domain.md`.

Key ADRs: **024** (single HTTP→Kafka→Flink path), **025** (ClickHouse cold store), **026** (four deep metering modules).
