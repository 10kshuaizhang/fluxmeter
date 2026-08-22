# ADR-018: Hierarchical Budgets

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-07-11 |
| **Version** | 2.8.0 |
| **Lang** | English |

## Context

Enterprise needs Org → Team → User → Key / Agent session quotas to prevent noisy neighbors. Claude Enterprise inheritance, Cursor spend limits, LiteLLM session caps are all productized.

## Decision

• **`POST /budget/{id}/cap`** + `check?parent_span_id=` / `session_id=` — span/session hard max.
• **`POST /budget/{id}/reserve?parent_span_id=`** — atomic hold on customer + span cap pool.
• **Per-key API budgets** — `POST /admin/customers/{id}/apikeys/{key_id}/budget`; enforced on `/check`.
• **Metadata dims** — ingest `metadata` whitelist → `GET /usage/dim/{key}/{value}` for Intelligence attribution.

## Consequences

✅ Agent platforms get per-run caps; ✅ reseller gateways get key-level budgets. ❌ Hierarchical reserve Lua complexity; Full Flink span tier still flat (ponytail in TokenUsageAggregator).

## Evidence

changLog 2.8.0; commit `3142d0c`; SDK 1.5.0 `reserve(parent_span_id=)`.

---

**中文:** [ADR-018](../zh/018-hierarchical-budgets.md)
