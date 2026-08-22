# ADR-020: Intelligence 读 Redis Rollups

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-07-11 |
| **Version** | 3.0.0 |
| **Lang** | 中文 |

## Context

Intelligence MVP 需在 2–3 天 ship。建独立 warehouse（ClickHouse/BigQuery）+ ETL 会阻塞 MVP。Flink/Java 引擎已产出高质量 rollups。

## Decision

• **`api/intelligence/`** Python 模块读 `native_reader`（Redis rollups）+ OpenMeter overlay connector。
• **不替换 Flink 引擎**；Intelligence 是 read-mostly analytics layer。
• MVP 用 **ponytail 启发式**：simulation 假设 input ~50% cost；profitability 按 cost-share 分配 revenue；forecast 线性 EOM 投影（`api/intelligence/forecast.py`）。
• 每条 ponytail 注释标明 ceiling + upgrade path（→ Phase 6 optimizer / per-SKU revenue）。

## Consequences

✅ 3.0.0 + 3.1.0 一周内 ship；✅ 29 intelligence tests green。❌ 启发式精度有限；无 ML forecasting；overlay 仅 OpenMeter（Langfuse backlog）。

## Evidence

changLog 3.0.0, 3.1.0；`docs/intelligence-api.md`。

---

**English:** [ADR-020](../en/020-intelligence-reads-redis-rollups.md)
