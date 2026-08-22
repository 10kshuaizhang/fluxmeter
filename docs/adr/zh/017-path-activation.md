# ADR-017: Path Activation

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-07-06 |
| **Version** | 3.2.0 |
| **Lang** | 中文 |

## Context

行业决策瀑布 Step 0：**流量是否必经管控点？** 纯 SDK 库（调 `check`）易漏 — 应用开发者忘记 pre-call check → 透支。LiteLLM/Portkey/转售网关解法：proxy 路上强制。2026-07-06 ROADMAP 重排：Path activation 先于 Full SaaS RBAC。

## Decision

三件套递进：
1. **`wrap(OpenAI())`** — SDK 1.4.0，fail-open，pre-call check + post-call track + mid-stream kill（2.7.0）
2. **Lite webhooks** — `BUDGET_LOW` / `EXHAUSTED` / `WARN` 70/90 无 Kafka 依赖（2.7.0）
3. **Gateway proxy** — OpenAI-compatible `:8080`，pre-check + stream reserve + mid-flight kill + proxy-only ingest（3.2.0）

## Consequences

✅ 集成者无法「忘记 check」；✅ TokenBridge/ClipLive 客户故事可落地。❌ Proxy 增加 latency hop；stream kill 用 char/4 heuristic when provider omits usage（ponytail）。

## Evidence

commits `7d8ad82`, `83d23ca`；changLog 2.7.0, 3.2.0；`api/gateway/stream_guard.py`。

---

**English:** [ADR-017](../en/017-path-activation.md)
