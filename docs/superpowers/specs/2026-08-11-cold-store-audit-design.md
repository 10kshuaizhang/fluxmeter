# Design: FluxMeter 可审计冷存储 (ClickHouse Append-Only Event Log)

**Date:** 2026-08-11  
**Status:** APPROVED — implemented in **4.4.0** (plan: [`../plans/2026-08-11-cold-store-audit.md`](../plans/2026-08-11-cold-store-audit.md))  
**Scope:** ADR-1 / ADR-025 — Kafka `token-events` → ClickHouse `raw_events` 作为可审计事实之源（计费真相仍是 Flink→Redis）  
**Kafka shape:** Trusted Envelope (`envelopeVersion` + `payload`); MV 抽取 `payload.*`，并兼容扁平 Token Event JSON。
**Supersedes draft:** uploaded `SPEC_cold_store`（窄表 / 外部 `cold_sink/` / Lite 补投 Kafka 等路线已否决）

## Non-goals

- 不实现 base_store 路由 (ADR-2)
- 不实现对账 (ADR-3)
- 不改 Lite/Full 统一收口 (ADR-4)
- **本阶段不接 Lite 同步路径**（Lite 等 ADR-4）
- 不新增外部 `cold_sink/` 消费进程
- 审计主表不存余额 / held / invoice / 聚合派生值 (ADR-0)

---

## 1. Definition of Done

一句话：**每一条经 Full/Kafka 摄取且格式合法、带 `eventId` 的原始事件，都有一份不可变、可按客户+时间范围回溯的宽表副本落在 ClickHouse；与 Flink 热路径互不阻塞。**

验收（均可演示）：

1. 向 Full 摄取入口（或直接打 `token-events`）打 N 条带唯一 `eventId` 的合法事件 → 查询 `count() FROM raw_events FINAL`（或等价读时去重）= N。
2. 同一 `eventId` 重放 3 次 → 读时去重后仍为 1 条。
3. Flink 热路径 sink 挂掉/变慢时，ClickHouse Kafka 消费组仍可继续写入（反之亦然）——独立 `kafka_group_name`。
4. 一条 SQL：按 `customer_id + [t0, t1]` 拉出该客户区间全部原始事件，字段足够按 `token-event-v1` 重算账单。
5. 主表只有事实字段 + 摄取元数据，无派生态。
6. 无 `eventId` 或无法解析的消息 **不进主表**，进入 `raw_events_dlq`，可查询、可追溯。

---

## 2. Locked decisions (grill-me)

| # | Topic | Choice |
|---|--------|--------|
| 1 | 幂等身份 | 单一 `eventId`（对齐现有 Lite Redis / Flink `EventDeduplicator` / schema） |
| 2 | 行模型 | 一行 = 一次 LLM 调用（宽表，对齐 `token-event-v1`） |
| 3 | Lite | 本阶段不进冷存；等 ADR-4 |
| 4 | 去重验收 | 读时去重（`FINAL` / `LIMIT 1 BY event_id`）；允许 merge 前物理行 > 逻辑行 |
| 5 | 与 baseline | **升级替换**现有 CH baseline 原始表为审计冷存，不并行第二套 Kafka→CH 写入 |
| 6 | 写入路径 | 继续 ClickHouse **Kafka engine + MV**（不新建外部 sink） |
| 7 | 排序 / 查询 | 主表 `ORDER BY (event_id)` + **projection** `(customer_id, event_time, event_id)` |
| 8 | 聚合 demo | 原始表守 ADR-0；`usage_per_minute` 等聚合 MV **仅** `make benchmark` profile |
| 9 | 坏消息 | 进 CH 表 `raw_events_dlq`（原始 payload + 原因 + 时间） |
| 10 | 无 eventId | 视为坏消息 → DLQ，不进主表 |
| 11 | 交付物 | DDL + DLQ 通路 + compose/init + 验收测试 + 运维 runbook/脚本；无 `cold_sink/` 模块 |

### Explicitly cut from prior draft

- `cold_sink/`（consumer/batcher/writer）
- 窄表 `meter_code` + `quantity`
- 存储层 `idempotency_key = SHA-256(payload)` 去重
- Lite 同步后补投 Kafka
- `source_mode` 双路径枚举（本阶段仅 Full；列可省略）

---

## 3. Data contract

源：`spec/schema/token-event-v1.json` + Kafka topic `token-events`（JSONEachRow，camelCase）。

主表 `fluxmeter.raw_events`（snake_case；MV 负责映射）：

| 列 | 类型 | 来源 | 说明 |
|---|---|---|---|
| `event_id` | String | `eventId` | 幂等主键；空则不进此表 |
| `customer_id` | String | `customerId` | 查询维度 |
| `request_id` | Nullable(String) | `requestId` | |
| `span_id` | Nullable(String) | `spanId` | |
| `parent_span_id` | Nullable(String) | `parentSpanId` | |
| `provider` | String | `provider` | 默认 `''` |
| `model_id` | String | `modelId` | |
| `input_tokens` | UInt32 | `inputTokens` | 整数；禁用 Float |
| `output_tokens` | UInt32 | `outputTokens` | |
| `cache_read_tokens` | UInt32 | `cacheReadTokens` | |
| `cache_write_tokens` | UInt32 | `cacheWriteTokens` | |
| `reasoning_tokens` | UInt32 | `reasoningTokens` | |
| `embedding_tokens` | UInt32 | `embeddingTokens` | |
| `event_time` | DateTime64(3) | `fromUnixTimestamp64Milli(timestamp)` | 事实时间；缺省/0 → 用 `ingested_at` |
| `latency_ms` | UInt32 | `latencyMs` | |
| `session_id` | Nullable(String) | `sessionId` | |
| `environment` | Nullable(String) | `environment` | |
| `metadata` | String | `metadata` JSON 序列化 | 原样保留；空则 `'{}'` |
| `ingested_at` | DateTime64(3) | `now64(3)` | 摄取元数据 |
| `partition_date` | Date | `MATERIALIZED toDate(event_time)` | 分区键 |

边界：无 `balance` / `held` / `invoice_id` / `aggregated_*` / `cost_usd` 派生列。

---

## 4. Table DDL (intent)

```sql
CREATE TABLE fluxmeter.raw_events
(
    event_id            String,
    customer_id         String,
    request_id          Nullable(String),
    span_id             Nullable(String),
    parent_span_id      Nullable(String),
    provider            String,
    model_id            String,
    input_tokens        UInt32,
    output_tokens       UInt32,
    cache_read_tokens   UInt32,
    cache_write_tokens  UInt32,
    reasoning_tokens    UInt32,
    embedding_tokens    UInt32,
    event_time          DateTime64(3),
    latency_ms          UInt32,
    session_id          Nullable(String),
    environment         Nullable(String),
    metadata            String,
    ingested_at         DateTime64(3) DEFAULT now64(3),
    partition_date      Date MATERIALIZED toDate(event_time),
    PROJECTION proj_customer_time
    (
        SELECT *
        ORDER BY (customer_id, event_time, event_id)
    )
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY partition_date
ORDER BY (event_id)
SETTINGS index_granularity = 8192;
```

说明：

- **ReplacingMergeTree(ingested_at)**：同 `event_id` 保留 `ingested_at` 最大的一行；幂等身份仅为 `event_id`。
- **只 INSERT**：不 `ALTER UPDATE` 改历史；迟到/纠错走后续 ADR 的补偿追加（本 ADR 不定义补偿 schema）。
- **Projection**：服务验收 #4 的客户+时间范围查询。

DLQ：

```sql
CREATE TABLE fluxmeter.raw_events_dlq
(
    raw_payload   String,
    error_reason  String,
    error_detail  String,
    ingested_at   DateTime64(3) DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (ingested_at)
TTL toDateTime(ingested_at) + INTERVAL 30 DAY;
```

读时去重模板：

```sql
SELECT count() FROM fluxmeter.raw_events FINAL
WHERE customer_id = {cid:String}
  AND event_time BETWEEN {t0:DateTime64} AND {t1:DateTime64};

-- 拉取查询（走 projection）
SELECT *
FROM fluxmeter.raw_events FINAL
WHERE customer_id = {cid:String}
  AND event_time BETWEEN {t0:DateTime64} AND {t1:DateTime64}
ORDER BY event_time, event_id;
```

---

## 5. Write topology

```text
                         ┌──> [Flink group: hot] ──> Redis          ← 已有
Kafka topic:token-events ┤
                         └──> [CH Kafka engine group: fluxmeter-cold-store]
                                   │
                                   ▼
                            Null fanout (见下)
                             ├─ event_id != ''  → raw_events
                             └─ event_id == ''  → raw_events_dlq (missing_event_id)
                             └─ parse/_error    → raw_events_dlq (parse_error)
```

### 5.1 Kafka engine settings

- `kafka_group_name = 'fluxmeter-cold-store'`（新 group；与 Flink、旧 `clickhouse-baseline` 隔离）
- `kafka_format = 'JSONEachRow'`
- **`kafka_handle_error_mode = 'stream'`**（或当前 CH 版本等价能力）：解析失败暴露 `_error` / `_raw_message`，**禁止**静默 `kafka_skip_broken_messages > 0` 作为审计默认
- 消费者并行度可保留与现网相近（如 4），以 compose 资源为准

### 5.2 Fanout pattern（单 Kafka 表 → 多去向）

ClickHouse 对同一 Kafka engine 表不宜挂多个独立消费 MV。采用经典 **Null 引擎扇出**：

1. `token_events_queue` — Kafka engine（宽字段 + 错误虚列）
2. `raw_events_ingress` — `ENGINE = Null`，单 MV 从 queue 写入（规范化列、映射 snake_case、转换 `event_time`）
3. `raw_events_mv` — 从 Null 表 `WHERE event_id != '' AND error_reason = ''` → `raw_events`
4. `raw_events_dlq_mv` — 从 Null 表其余行 → `raw_events_dlq`

实现阶段若目标 CH 版本对 error stream / Null 扇出有差异，允许等价改写，但**不得**退回「skip 坏消息且无 DLQ」。

### 5.3 Isolation (验收 #3)

冷存与热路径是不同 Kafka consumer group，不是串联。任一侧 lag/宕机不阻塞另一侧消费进度。

---

## 6. Relationship to existing baseline

| 现有对象 | 本 ADR 处置 |
|----------|-------------|
| `fluxmeter.token_events` + `token_events_mv` | **替换**为 `raw_events` 管道（init.sql 默认路径） |
| `kafka_group_name=clickhouse-baseline` | 改为 `fluxmeter-cold-store` |
| `usage_per_minute` + MV（含 `cost_usd` 派生） | **移出默认 init**；放入 benchmark-only SQL（如 `baseline/benchmark_init.sql`），仅 `make benchmark` / compose profile 加载 |
| `baseline/query.sql` / `benchmark.sh` | 更新为指向 benchmark profile 表；文档标明「对比用派生表 ≠ 审计真源」 |

---

## 7. Deliverables (implementation scope)

1. **DDL / init**
   - 重写 `baseline/init.sql`（或拆 `cold_store/init.sql` 并由 compose 挂载）：queue、Null ingress、`raw_events`、projection、`raw_events_dlq`、MVs
   - `baseline/benchmark_init.sql`：聚合对比表 MV（可选 profile）
2. **Compose**
   - 默认 ClickHouse 只启审计冷存
   - benchmark profile 额外挂载聚合 init
3. **验收**
   - 自动化测试或 `make` 目标：N 条写入、重放去重、客户时间范围查询、缺 `eventId` → DLQ、非法 JSON → DLQ
4. **运维**
   - Runbook：`docs/runbooks/cold-store-dlq.md` — 如何查 DLQ、常见 `error_reason`、是否允许人工修正后重投（重投必须带稳定 `eventId`）
   - 查询模板（FINAL / projection）写入 runbook 或 `baseline/query_audit.sql`
5. **文档**
   - README / disaster-recovery 中「cold storage」指向本表；明确 Lite 本阶段无冷副本

**不做：** Java/Python `cold_sink` 包；查询聚合 API（ADR-2）；对账 job（ADR-3）。

---

## 8. Acceptance SQL / scenarios

| ID | 场景 | 期望 |
|----|------|------|
| A1 | Full `/ingest` N 条唯一 `eventId` | `SELECT count() FROM raw_events FINAL` = N |
| A2 | 同 `eventId` 重放 ×3 | FINAL count 仍 +0（总计不增） |
| A3 | 停 Flink TaskManagers，继续 produce Kafka | `raw_events` 行数继续增加 |
| A4 | `WHERE customer_id=? AND event_time BETWEEN` | 返回宽表全字段；可重算 token 合计 |
| A5 | payload 无 `eventId` | 主表无行；`raw_events_dlq` 有 `missing_event_id` |
| A6 | 非法 JSON | 主表无行；DLQ 有 `parse_error` + `raw_payload` |
| A7 | 表扫描列名 | 无 balance/held/cost_usd/aggregated_* |

---

## 9. Boundary self-check (PR 前)

- [ ] 主表无余额/持有/发票/聚合列 (ADR-0)
- [ ] 只有 INSERT，无 UPDATE 改历史行
- [ ] 冷存 group 与 Flink group 独立；可做故障隔离演示
- [ ] 同 `event_id` 重放读时为 1 条
- [ ] 无 `eventId` / 坏 JSON 进 DLQ 而非主表
- [ ] 默认 compose 不加载派生聚合 MV；benchmark 可选
- [ ] 无外部 `cold_sink/` 模块；无 Lite 冷存写入
- [ ] 无 sum/aggregate 查询 API（留给 ADR-2）

---

## 10. Open items deferred (not blockers)

| Item | 去向 |
|------|------|
| Lite → 冷存 | ADR-4 统一收口后接入同一 topic / 同一表 |
| 补偿事件 schema（负数/修正行） | 未决；本表仅保证追加，不定义补偿方言 |
| base_store 查询服务 | ADR-2 |
| 与热路径 Redis 对账 | ADR-3 |
| 旧 `clickhouse-baseline` group 残留 offset | 部署注释：新 group 名即新消费位点；需历史回填时另做 replay |

---

## 11. Implementation note (next)

实现计划：[`../plans/2026-08-11-cold-store-audit.md`](../plans/2026-08-11-cold-store-audit.md)。按该计划 task-by-task 落地，勿另开外部 `cold_sink/`。
