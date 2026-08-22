# ADR-020: Intelligence Reads Redis Rollups

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-07-11 |
| **Version** | 3.0.0 |
| **Lang** | English |

## Context

Intelligence MVP must ship in 2–3 days. Building a separate warehouse (ClickHouse/BigQuery) + ETL blocks MVP. Flink/Java engine already produces high-quality rollups.

## Decision

• **`api/intelligence/`** Python module reads `native_reader` (Redis rollups) + OpenMeter overlay connector.
• **Do not replace Flink engine**; Intelligence is a read-mostly analytics layer.
• MVP uses **ponytail heuristics**: simulation assumes input ~50% cost; profitability allocates revenue by cost-share; forecast linear EOM projection (`api/intelligence/forecast.py`).
• Each ponytail comment states ceiling + upgrade path (→ Phase 6 optimizer / per-SKU revenue).

## Consequences

✅ 3.0.0 + 3.1.0 shipped within a week; ✅ 29 intelligence tests green. ❌ Heuristic accuracy limited; no ML forecasting; overlay OpenMeter only (Langfuse backlog).

## Evidence

changLog 3.0.0, 3.1.0; `docs/intelligence-api.md`.

---

**中文:** [ADR-020](../zh/020-intelligence-reads-redis-rollups.md)
