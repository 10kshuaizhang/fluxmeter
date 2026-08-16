# ADR-025: Auditable ClickHouse cold store

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-08-16 |
| **Version** | 4.4.0 |
| **Lang** | English |
| **Related** | ADR-004 (benchmark aggregates), ADR-024 (Trusted Envelope on Kafka) |

## Context

ADR-004 kept ClickHouse as an honest **benchmark** baseline (store-then-query lag vs Flink). Operators also need an immutable audit copy of raw Token Events for dispute/replay without treating ClickHouse as the billing engine.

PR #2 attempted this as 3.3.0 against an older main; it was closed as conflicting. Relanded on 4.x.

## Decision

1. **Cold Store** = ClickHouse `fluxmeter.raw_events` (+ `raw_events_dlq`) fed by Kafka engine group `fluxmeter-cold-store` from topic `token-events`.
2. Messages are **Trusted Envelopes**; the MV extracts `payload.*` (flat Token Event JSON still accepted for ops).
3. Dedup identity = `event_id` via `ReplacingMergeTree(ingested_at)` + read-time `FINAL`.
4. Billing truth remains Flink → Redis. Cold store does **not** hold balance / held / invoice / derived cost columns.
5. `usage_per_minute` and other derived aggregates stay in `baseline/benchmark_init.sql` and are applied only by `make benchmark` (ADR-004 role unchanged).
6. Wired under `docker-compose.benchmark.yml` + `make apply-cold-store-init` / `make test-cold-store`. No Lite cold path (defer).

## Consequences

✅ Audit queries without blocking Flink; ✅ independent Kafka consumer group; ✅ DLQ for bad messages.  
❌ Extra ClickHouse ops cost; ❌ envelope schema must stay extractable; ❌ not a substitute for Redis UsageQuery.

## Evidence

Spec: `docs/superpowers/specs/2026-08-11-cold-store-audit-design.md` · Acceptance: `make test-cold-store` · Runbook: `docs/runbooks/cold-store-dlq.md`

---

**中文:** [ADR-025](../zh/025-auditable-clickhouse-cold-store.md)
