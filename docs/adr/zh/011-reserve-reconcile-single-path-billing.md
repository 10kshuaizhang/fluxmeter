# ADR-011: Reserve/Reconcile 单路径扣费

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-20 |
| **Version** | 1.2.0 |
| **Lang** | 中文 |

## Context

流式 LLM 响应可能持续 30–120s。若等窗口关窗才扣费，并发请求可透支同一钱包。行业 Hold 步：SpendGuard reserve/commit、Stripe auth/capture。

## Decision

• `POST /budget/{id}/reserve` — 悲观 hold（`held_usd`），不直接扣 balance。
• `POST /budget/{id}/reconcile` — 释放 hold，差额结算。
• **Flink Sink 为 `balance_usd` 唯一 mutator**（1.2.0 修复 streaming double-charge）。
• `check` 用 `effective_balance = balance - held`。

## Consequences

✅ 流式场景 budget safety；✅ SDK `wrap_stream` + Gateway stream kill 有语义基础。❌ 集成复杂度上升（三 API：check / reserve / reconcile）。

## Evidence

changLog 0.8.0, 1.2.0；industry-billing-research §3 Hold 步。

---

**English:** [ADR-011](../en/011-reserve-reconcile-single-path-billing.md)
