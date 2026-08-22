# ADR-005: Multi-Provider Event Schema Up Front

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-19 |
| **Version** | 0.3.0 |
| **Lang** | English |

## Context

Initial schema used `tokenType` enum + single `tokenCount`, unable to express OpenAI cache tokens, Anthropic reasoning tokens, and 2026 multi-category pricing.

## Decision

**Breaking change early**: separate fields for `inputTokens`, `outputTokens`, `cacheReadTokens`, `cacheWriteTokens`, `reasoningTokens`, `embeddingTokens`; add `provider`, `spanId`, `sessionId` tracing fields. Rename TokenFlink → **FluxMeter**.

## Consequences

✅ Stable contract for exporters, interop spec, Intelligence dims. ❌ Pre-0.3.0 integrators must migrate (no external users yet — acceptable cost).

## Evidence

changLog 0.3.0 BREAKING; Weekend 2 checklist progress.md.

---

**中文:** [ADR-005](../zh/005-multi-provider-event-schema.md)
