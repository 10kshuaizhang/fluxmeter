# ADR-013: Lite-First 双路径

| Field | Value |
|-------|-------|
| **Status** | Accepted（决策反转） |
| **Date** | 2026-06-22 |
| **Version** | 2.0.2 |
| **Lang** | 中文 |

## Context

v0.x–v1.x 默认 `make demo` 启动 Kafka + Flink + Redis — 对 side project / <100K eps 集成者过重。1M eps 是 credibility 资产，不是 default DX。

## Decision

**2026-06-22 一天 5 commits 完成 pivot**：
• `docker-compose.yml` = **Lite**（API → Redis Lua，无 Kafka/Flink）
• `docker-compose.full.yml` = Full stack
• `make demo` = Lite；`make demo-full` = Full
• 同一 Redis key schema + OpenAPI 契约
• 后续叠加：Lua aggregator（2.1.0）→ rollup worker → Stripe export → SaaS control plane

## Consequences

✅ `docker-compose up` 30 秒可跑；✅ 90% 集成者零 Flink ops。❌ 双路径 correctness 回归负担（`make test-lite` + `make test-java`）；Lite/Full 语义须持续对齐。

## Evidence

commits `66a1c70`, `abd76ae`, `75a3bf6`, `ac3f956`, `682ae1a`；`docs/superpowers/plans/2026-06-22-dual-path-lite-saas.md`。

## 架构师备注

Demo 吞吐 ≠ 默认 DX。先证伪「需要 Flink 才能 meter」，再让需要 1M eps 的人走 Full path。

---

**English:** [ADR-013](../en/013-lite-first-dual-path.md)
