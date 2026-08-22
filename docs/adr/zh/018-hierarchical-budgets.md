# ADR-018: 层级预算

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-07-11 |
| **Version** | 2.8.0 |
| **Lang** | 中文 |

## Context

Enterprise 场景需 Org → Team → User → Key / Agent session 层级配额防 noisy neighbor。Claude Enterprise inheritance、Cursor spend limits、LiteLLM session caps 均已产品化。

## Decision

• **`POST /budget/{id}/cap`** + `check?parent_span_id=` / `session_id=` — span/session hard max。
• **`POST /budget/{id}/reserve?parent_span_id=`** — 原子 hold customer + span cap pool。
• **Per-key API budgets** — `POST /admin/customers/{id}/apikeys/{key_id}/budget`；`/check` enforcement。
• **Metadata dims** — ingest `metadata` whitelist → `GET /usage/dim/{key}/{value}` for Intelligence attribution。

## Consequences

✅ Agent 平台可 per-run cap；✅ 转售网关可 key 级 budget。❌ 层级 reserve Lua 复杂度；Full Flink path span tier 仍 flat（ponytail in TokenUsageAggregator）。

## Evidence

changLog 2.8.0；commit `3142d0c`；SDK 1.5.0 `reserve(parent_span_id=)`。

---

**English:** [ADR-018](../en/018-hierarchical-budgets.md)
