# ADR-001: 流式优先，拒绝 Store-then-Query

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-16 |
| **Version** | 0.1.0 |
| **Lang** | 中文 |

## Context

AI token 计费需要实时聚合、exactly-once、背压处理。OpenMeter/Orb/Metronome 等主流方案走 store-then-query（事件入库 → 定时聚合 → 查询）。DESIGN.md 评估了三种路径：A Weekend Rocket（性能 Demo）、B Domain-first（计费语义）、C Budget Enforcer（kill signal）。

## Decision

选 **Approach A**：Apache Flink 1.18 DataStream API（Java 17）作为计量核心。Kafka 摄入 → keyed tumbling window → Redis sink。ClickHouse baseline 仅作对照，不做主路径。

## Consequences

✅ 500K eps 可持续、1M eps burst 可证；p99 聚合延迟 sub-second（10s 窗口）。❌ 运维复杂度高于纯 Redis/API；需要 Flink + Kafka 技能栈。

## Evidence

DESIGN.md Approach A；commit `81968fd` init；changLog 0.2.0：ClickHouse 8–43s lag vs Flink sub-second。

## 架构师备注

AI token 计量本质是 **continuous aggregation + exactly-once**，不是 OLAP 查询问题。Store-then-query 在 agent 循环场景下永远慢半拍。

---

**English:** [ADR-001](../en/001-streaming-first-no-store-then-query.md)
