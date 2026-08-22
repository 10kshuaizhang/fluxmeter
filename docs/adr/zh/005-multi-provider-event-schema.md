# ADR-005: 多 Provider 事件 Schema 一次到位

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-19 |
| **Version** | 0.3.0 |
| **Lang** | 中文 |

## Context

初版 schema 用 `tokenType` enum + 单一 `tokenCount`，无法表达 OpenAI cache tokens、Anthropic reasoning tokens 等 2026 年多 category 定价。

## Decision

**Breaking change 在早期完成**：`inputTokens` / `outputTokens` / `cacheReadTokens` / `cacheWriteTokens` / `reasoningTokens` / `embeddingTokens` 分字段；加 `provider`、`spanId`、`sessionId` 等 tracing 字段。项目更名 TokenFlink → **FluxMeter**。

## Consequences

✅ 后续 exporter、interop spec、Intelligence dims 有稳定契约。❌ 0.3.0 前集成者需迁移（当时无外部用户，成本可控）。

## Evidence

changLog 0.3.0 BREAKING；Weekend 2 checklist progress.md。

---

**English:** [ADR-005](../en/005-multi-provider-event-schema.md)
