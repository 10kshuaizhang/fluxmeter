# ADR-010: Exactly-Once 组合与 Dedup 删除

| Field | Value |
|-------|-------|
| **Status** | Accepted（决策反转） |
| **Date** | 2026-06-20 |
| **Version** | 0.6.2 / 2.7.1 |
| **Lang** | 中文 |

## Context

Week 4 架构 review 发现三个 CRITICAL/HIGH 问题：
1. Flink `EventDeduplicator` keyed by eventId → 1 key/event → 500K eps 下 **1.8B keys/hour → 保证 OOM**。
2. `allowedLateness(30s)` + Sink SET NX → late data 重触发窗口但 NX 阻止写入 → **静默丢数据**。
3. Counter increment 与 budget deduction 非原子 → crash window 内 customer 白嫖 tokens。

## Decision

**删除 > 添加**：
• **移除** Flink EventDeduplicator — Sink-level SET NX 足够。
• **移除** allowedLateness — late events exclusively → sideOutput → Kafka DLQ。
• Counter + budget + idempotency 合并为 **单 Lua EVAL**（2.7.1 进一步消除 pipeline crash window）。
• Checkpoint：EXACTLY_ONCE + 10m timeout + `tolerableCheckpointFailureNumber(3)`。

## Consequences

✅ 生产吞吐下不会 OOM；✅ late data 不静默丢失；✅ 金融原子性。❌ DLQ 需运维 replay（`scripts/dlq_replay.py`）；late data 不进主窗口（by design）。

## Evidence

changLog 0.6.2 CRITICAL fixes；changLog 2.7.1；`src/test/java/io/fluxmeter/job/LateDataSideOutputTest.java`。

## 架构师备注

本项目最重要的架构反转之一：初版「加 dedup operator 保 EO」在 production scale 下比没有 dedup 更危险。

## 2026-08-17 修订（v4.5.0）

仍然禁止在 Flink 中保存 30 天的逐事件 registry。v4.5.0 只为 HTTP Kafka ACK 的不确定/crash 窗口恢复一个**严格限制为 10 分钟的 safety dedup**。客户端 30 天 retry identity 位于紧凑、tenant-sharded 的 Redis custody bucket；Flink state 既不是 HTTP retry 的权威来源，也不是长期 identity store。这不推翻原 ADR 的容量结论：安全 TTL 及其 state-size gate 是合约的一部分。

---

**English:** [ADR-010](../en/010-exactly-once-remove-dedup-operator.md)
