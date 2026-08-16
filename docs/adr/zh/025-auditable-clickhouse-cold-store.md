# ADR-025: 可审计 ClickHouse 冷存储

| 字段 | 值 |
|------|-----|
| **Status** | Accepted |
| **Date** | 2026-08-16 |
| **Version** | 4.4.0 |
| **Lang** | 中文 |
| **Related** | ADR-004（benchmark 聚合）、ADR-024（Kafka Trusted Envelope） |

## Context

ADR-004 把 ClickHouse 当作诚实 **benchmark** 对照。运营仍需要一份不可变的原始 Token Event 审计副本，且不能把 ClickHouse 当成计费引擎。

## Decision

1. **Cold Store** = `fluxmeter.raw_events`（+ DLQ），Kafka 消费组 `fluxmeter-cold-store`。
2. 消息为 **Trusted Envelope**；MV 抽取 `payload.*`（仍兼容扁平 Token Event JSON）。
3. 幂等身份 = `event_id`（`ReplacingMergeTree` + 读时 `FINAL`）。
4. 计费真相仍是 Flink → Redis；冷存不含 balance / held / invoice / 派生 cost。
5. 派生聚合仍只在 `benchmark_init.sql` / `make benchmark`（ADR-004 角色不变）。
6. 挂在 `docker-compose.benchmark.yml`；无 Lite 冷路径。

## Consequences

✅ 审计与 Flink 解耦；✅ 独立消费组；✅ DLQ。  
❌ 额外运维；❌ Envelope 抽取必须保持可用。

**English:** [ADR-025](../en/025-auditable-clickhouse-cold-store.md)
