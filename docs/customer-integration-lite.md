# Lite 模式客户接入实施方案

**版本：** 基于 FluxMeter **v2.6.2**  
**适用路径：** Lite（`FLUXMETER_LITE_MODE=true`，API → Redis Lua，无 Kafka/Flink）  
**客户：**  
- **客户 A** — AI Token 中转站（OpenAI 兼容 API 代理 / 聚合网关）  
- **客户 B** — 直播视频 + AI 短视频剪辑（多步骤 Agent 工作流）

本文档覆盖：业务映射、部署、接入代码、计费模型、运维验收，以及 Review 后发现的 **FluxMeter 能力缺口与补充设计**。

**客户故事（SaaS 官网风格）：** [customer-stories-lite.md](customer-stories-lite.md) — TokenBridge / ClipLive 人物、Use Case、4 周排期。

---

## 1. 为什么两者都走 Lite

| 维度 | 客户 A（中转站） | 客户 B（直播剪辑） | Lite 是否够用 |
|------|------------------|-------------------|---------------|
| 典型 QPS | 高（100–10K rpm） | 中低（按场次 burst） | ✓ Lite 单 Redis 约 **<100K eps**；A 需压测后扩容 |
| 实时预算 | 必须（预付费 token） | 需要（按创作者套餐） | ✓ `/budget/check` + Lite 内联扣款 |
| 工作流归因 | 弱（按用户/模型即可） | 强（场次 / 剪辑任务） | ✓ sessionId + **parentSpanId**（v2.6.2+） |
| 运维复杂度 | 希望极简 | 希望极简 | ✓ 三容器：Redis + API + Grafana |

**Lite 数据流：**

```
┌─────────────────┐     check (<10ms)      ┌──────────────────┐
│  客户业务服务    │ ─────────────────────► │  FluxMeter API   │
│  (Proxy / 剪辑)  │                        │  :8000           │
└────────┬────────┘                        └────────┬─────────┘
         │                                            │
         │  LLM 调用                                     │ POST /ingest
         ▼                                            ▼
┌─────────────────┐                        ┌──────────────────┐
│  上游模型商      │                        │  LiteAggregator  │
│  OpenAI/DeepSeek│                        │  (Redis Lua)     │
└─────────────────┘                        └────────┬─────────┘
                                                  │
         ┌────────────────────────────────────────┘
         ▼
┌──────────────────┐     rollup worker      ┌──────────────────┐
│  Redis 7         │ ◄───────────────────── │  日/月 bucket     │
│  计数 + 预算      │                        │  session 计数     │
└──────────────────┘                        └──────────────────┘
         │
         ▼
  Grafana / GET /usage/* / 外部账单系统
```

---

## 2. 共享基础设施部署（生产 Lite）

### 2.1 最小拓扑

```bash
# 开发验证
make demo   # Redis + API + Grafana

# 生产：在 docker-compose.yml 基础上叠加环境变量（无需 Flink）
```

**生产必改项（参考 `docker-compose.yml` + 运维惯例）：**

| 变量 | 开发默认 | 生产建议 |
|------|----------|----------|
| `FLUXMETER_LITE_MODE` | `true` | `true` |
| `FLUXMETER_AUTH_OPTIONAL` | `true` | **`false`** |
| `FLUXMETER_API_KEY` | 未设置 | 全局读写密钥 |
| `FLUXMETER_ADMIN_KEY` | 未设置 | 预算/定价管理 |
| `REDIS_PASSWORD` | 无 | **必设** |
| `BUDGET_FAIL_POLICY` | `open` | **`closed`**（Redis 故障时拒绝新请求） |
| `PRICING_FILE` | `config/pricing.json` | 客户自定义定价表 |

**Redis：** AOF + `noeviction`（见 [production-deploy.md](production-deploy.md) §3）。  
**API：** 至少 2 副本 + LB；`/health` 做 readiness。  
**备份：** 每日 RDB/AOF 快照；预算与计数不可丢。

### 2.2 密钥模型

```
平台运维     → FLUXMETER_ADMIN_KEY  （设预算、定价、开户）
客户后端     → FLUXMETER_API_KEY 或 per-customer key
终端用户     → 不直连 FluxMeter；由客户服务代调 check/ingest
```

为客户 A/B 的**每个下游计费主体**创建 scoped key：

```bash
curl -X POST http://fluxmeter:8000/admin/customers/cust_xxx/api-keys \
  -H "X-API-Key: $FLUXMETER_ADMIN_KEY"
# → {"api_key":"fm_live_...","customer_id":"cust_xxx"}
```

### 2.3 监控与告警

- Grafana：`http://<host>:3000`（已 provisioning Redis 数据源）
- 业务告警：轮询 `GET /budget/{id}` 或 ingest 响应里的 `budget_alert`
- **注意：** Lite 栈**不含** Kafka `webhook-worker`；`POST /budget/{id}/webhook` 配置的 URL **不会自动推送**（见 §6.2）

---

## 3. 客户 A — Token 中转站

### 3.1 业务特征

- 对外提供 OpenAI 兼容 `/v1/chat/completions` 等接口
- **多租户：** 每个 API Key 对应一个下游用户（`customerId`）
- **计费：** 预付费 token 包 或 USD 余额；按模型差异化单价
- **风控：** 单用户 RPM 限制、余额耗尽即时 402
- **高峰：** 同步 proxy 路径上 check + 异步 ingest 可分离

### 3.2 身份与事件映射

| 业务概念 | FluxMeter 字段 | 说明 |
|----------|----------------|------|
| 下游用户 | `customerId` | 与你们用户表主键 1:1，如 `user_8f3a` |
| 模型 | `modelId` | 上游返回的 `model` 字段，自动 prefix 归一化 |
| 幂等 | `eventId` | **强烈建议** = 上游 `requestId` 或自生成 UUID |
| 渠道/路由 | `provider` | `openai` / `deepseek` / `qwen` 等 |
| 套餐类型 | Redis `package:{id}:tokens_remaining` | Token 包计费（非 USD） |

**不要**把「你们平台」设为 `customerId`；平台自身用量若需要，单独建 `customerId=platform_internal`。

### 3.3 接入时序（推荐）

```
Client → 你们的 Gateway
           │
           ├─1─► GET /budget/{userId}/check?estimated_cost_usd=0.02
           │         allowed=false → 402 Payment Required
           │
           ├─2─► 转发上游 LLM
           │
           └─3─► POST /ingest（或 batch 异步）
                     ← {status, cost_usd, balance_usd, budget_alert?}
```

### 3.4 开户与套餐

**USD 预付费（推荐起步）：**

```bash
curl -X POST http://fluxmeter:8000/budget/user_8f3a \
  -H "X-API-Key: $FLUXMETER_ADMIN_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "balance_usd": 100.0,
    "alert_threshold_usd": 10.0,
    "max_rpm": 120
  }'
```

**Token 包（按 token 数卖，不是按 USD）：**

```bash
# 充值 1000 万 token
curl -X POST "http://fluxmeter:8000/budget/user_8f3a/package" \
  -H "X-API-Key: $FLUXMETER_ADMIN_KEY" \
  -d '{"tokens": 10000000}'

# 查询剩余
curl http://fluxmeter:8000/budget/user_8f3a/package -H "X-API-Key: $FLUXMETER_API_KEY"
```

Lite ingest 会先扣 token 包，再算 USD 成本；包耗尽返回 `{"status":"rejected","reason":"package_exhausted"}`。

### 3.5 网关嵌入示例（Node.js / 与 JS SDK）

```typescript
import { FluxMeter } from "@fluxmeter/client";

const meter = new FluxMeter({
  apiUrl: process.env.FLUXMETER_URL,
  apiKey: process.env.FLUXMETER_API_KEY,
});

async function proxyChat(userId: string, body: unknown) {
  const check = await fetch(
    `${process.env.FLUXMETER_URL}/budget/${userId}/check?estimated_cost_usd=0.05`,
    { headers: { "X-API-Key": process.env.FLUXMETER_API_KEY! } },
  );
  const gate = await check.json();
  if (!gate.allowed) {
    return { status: 402, body: { error: gate.reason } };
  }

  const t0 = Date.now();
  const upstream = await callUpstreamLLM(body);

  // ponytail: ingest 失败不阻塞用户响应，写本地 WAL 重试
  meter.trackOpenAI(userId, upstream, {
    latencyMs: Date.now() - t0,
  }).catch(err => wal.enqueue(userId, upstream, err));

  return { status: 200, body: upstream };
}
```

**Python 网关：** 当前 PyPI `fluxmeter` SDK **仅 Kafka 传输**；Lite 请用 `httpx` 直调 `/ingest`（见 §3.6）或引入 JS SDK 侧car。

### 3.6 HTTP Ingest 示例（任意语言）

```bash
curl -X POST http://fluxmeter:8000/ingest \
  -H "X-API-Key: fm_live_xxx" \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "user_8f3a",
    "modelId": "deepseek-chat",
    "provider": "deepseek",
    "inputTokens": 1250,
    "outputTokens": 430,
    "cacheReadTokens": 0,
    "eventId": "chatcmpl-abc123",
    "requestId": "chatcmpl-abc123",
    "latencyMs": 890,
    "timestamp": 1718534400000
  }'
```

响应（Lite 专有）：

```json
{
  "status": "ok",
  "cost_usd": 0.000812,
  "balance_usd": 99.12
}
```

**批量对账**（例如每分钟 flush）：

```bash
POST /ingest/batch   # 最多 1000 条/请求
```

### 3.7 定价与转售加价

默认 [`config/pricing.json`](../config/pricing.json) 已含国内外主流模型。转售加价两种方式：

1. **直接改价（简单）** — 在 `pricing.json` 里把 `input_per_m` / `output_per_m` 设为对客单价（已含毛利）
2. **阶梯价（量大客户）** — 使用 `volume` / `graduated` 模式，见 [`contrib/pricing/tiered-example.json`](../contrib/pricing/tiered-example.json)

```bash
# 热更新到 Redis（无需重启 API）
curl -X PUT http://fluxmeter:8000/admin/pricing \
  -H "X-API-Key: $FLUXMETER_ADMIN_KEY" \
  -H "Content-Type: application/json" \
  --data-binary @pricing-reseller.json
```

### 3.8 对账与外部账单

| 场景 | API |
|------|-----|
| 用户中心「本月用量」 | `GET /usage/customer/{id}/period/2026-07` |
| 日峰值监控 | `GET /usage/customer/{id}/day/2026-07-05` |
| 分模型报表 | `GET /usage/customer/{id}/model/{model}` |
| 导出到 Stripe | 配置 `STRIPE_API_KEY` + `link-stripe`（见 [integrations.md](integrations.md)） |

### 3.9 客户 A 验收清单

- [ ] 402 路径：`check` 拒绝 + ingest 后 `budget_alert=BUDGET_EXHAUSTED`
- [ ] 同一 `eventId` 重复 ingest 不双计（幂等）
- [ ] `max_rpm` 触发 `rate_limited`
- [ ] Token 包耗尽返回 `package_exhausted`
- [ ] 压测：目标峰值 eps 下 `/check` p99 < 20ms
- [ ] Redis AOF 开启，演练恢复后计数一致

---

## 4. 客户 B — 直播 + AI 短视频剪辑

### 4.1 业务特征

- **长流程：** 一场直播 → 切片 → 转写 → 高光识别 → 文案/标题 → 可能多模型
- **计费主体：** B 端客户（MCN / 主播），或每场直播单独核算
- **归因需求：** 按「直播场次」「剪辑任务」汇总成本，给创作者看账单
- **流量形态：** 场次内 burst（几分钟内数十次 LLM），整体低于中转站

### 4.2 身份与事件映射

| 业务概念 | FluxMeter 字段 | 示例 |
|----------|----------------|------|
| 付费客户（MCN/主播） | `customerId` | `creator_liwei` |
| 直播场次 | `sessionId` | `live_20260705_room8821` |
| 单次剪辑流水线 | `sessionId` 或 **`parentSpanId`** | 见 §4.4 |
| 单步 LLM 调用 | 每次 ingest 一条 | 转写、摘要、标题各一条 |
| 业务标签 | 暂存于 `requestId` 前缀 | `clip_003|step=title`（metadata 未索引） |

**模型举例：**

| 步骤 | 典型 modelId | Token 特点 |
|------|--------------|------------|
| 语音转写 | `whisper-1` 或 ASR 自建 | 高 input |
| 高光分析 | `gpt-4o` / `qwen-max` | 高 input（字幕+打点） |
| 标题文案 | `gpt-4o-mini` | 低 input，中 output |
| 缩略图 prompt | `claude-haiku-4` | 低总量 |

非 token 成本（GPU 渲染、存储）**不在 FluxMeter 范围**；可并行记录在业务 DB，账单 UI 合并展示。

### 4.3 接入时序（异步友好）

剪辑任务多为**异步 Job**，不必每次 LLM 前都 check（可在任务启动时 check 一次预估上限）：

```
1. 创建任务 job_id
2. GET /budget/creator_liwei/check?estimated_cost_usd=0.50
3. 流水线各步骤调用 LLM
4. 每步完成后 POST /ingest（带 sessionId）
5. 任务结束 GET /usage/session/{sessionId} → 写入任务成本字段
```

### 4.4 归因策略（Lite 现状下的最佳实践）

| 层级 | 推荐字段 | 查询 API | Lite 支持 |
|------|----------|----------|-----------|
| 直播场次 | `sessionId = live_{date}_{roomId}` | `GET /usage/session/{id}` | ✓ |
| 剪辑任务 | `sessionId = job_{uuid}` | 同上 | ✓ |
| Agent 多步任务 | `parentSpanId = job_{uuid}` | `GET /usage/span/{id}` | ✓ v2.6.2+ |

**推荐（v2.6.2+）：** 剪辑任务统一用 `parentSpanId=job_id`，各 LLM 步可设 `spanId`；任务结束 `GET /usage/span/{job_id}` 得一键汇总。`sessionId` 仍可用于直播场次级归因。

### 4.5 代码示例（Python 异步 Job）

```python
import httpx
import uuid

FM = "http://fluxmeter:8000"
HEADERS = {"X-API-Key": "..."}

def ingest_step(*, customer_id: str, session_id: str, model: str,
                input_t: int, output_t: int, step: str):
    payload = {
        "customerId": customer_id,
        "modelId": model,
        "sessionId": session_id,
        "inputTokens": input_t,
        "outputTokens": output_t,
        "requestId": f"{session_id}|{step}",
        "eventId": str(uuid.uuid4()),
    }
    r = httpx.post(f"{FM}/ingest", json=payload, headers=HEADERS, timeout=5)
    r.raise_for_status()
    return r.json()  # {"status":"ok","cost_usd":...}


def run_clip_pipeline(creator_id: str, live_id: str):
    job_session = f"job_{uuid.uuid4().hex[:12]}"
    live_session = f"live_{live_id}"

    gate = httpx.get(
        f"{FM}/budget/{creator_id}/check",
        params={"estimated_cost_usd": 0.5},
        headers=HEADERS,
    ).json()
    if not gate["allowed"]:
        raise RuntimeError(gate["reason"])

    # 步骤 1：转写
    ingest_step(customer_id=creator_id, session_id=job_session,
                model="whisper-1", input_t=120_000, output_t=0, step="asr")
    # 步骤 2：高光
    ingest_step(customer_id=creator_id, session_id=job_session,
                model="gpt-4o", input_t=8000, output_t=1200, step="highlight")
    # 步骤 3：标题
    ingest_step(customer_id=creator_id, session_id=job_session,
                model="gpt-4o-mini", input_t=500, output_t=80, step="title")

    job_usage = httpx.get(
        f"{FM}/usage/session/{job_session}", headers=HEADERS
    ).json()
    # 业务库：关联 live_session ↔ job_session，累加 live 总成本
    return job_usage
```

### 4.6 套餐设计建议

| 套餐 | FluxMeter 配置 | 说明 |
|------|--------------|------|
| 按场付费 | 每场前 `topup` 或固定 `balance_usd` | 小客户 |
| 月卡 | 月初 `POST /budget/{id}` 设余额 | 配合 `/period/{YYYY-MM}` 出账 |
| 按条剪辑 | 每任务 `check` 预估 $0.05–0.30 | 用 session 查询单条成本 |

### 4.7 客户 B 验收清单

- [ ] 单任务 `sessionId` 下多步 ingest，`GET /usage/session` 汇总正确
- [ ] 创作者余额不足时任务启动被拒绝
- [ ] 日/月报表可对应 MCN 结算
- [ ] 明确非 token 成本（渲染/GPU）在业务侧单独展示
- [ ] （可选）Grafana 按 customer 看模型分布

---

## 5. 两客户对比速查

| 项目 | 客户 A 中转站 | 客户 B 直播剪辑 |
|------|--------------|----------------|
| 核心路径 | 同步 check → LLM → ingest | 异步 Job，批量 ingest |
| 主键 | `customerId` = 下游用户 | `customerId` = 创作者/MCN |
| 归因 | 模型维度即可 | `sessionId` = 场次/任务 |
| 计费 | Token 包 或 USD + RPM | USD 套餐 + 按任务/session 展示 |
| SDK | **JS SDK** 或 HTTP | **httpx** 直调（Python） |
| 高峰优化 | `/ingest/batch`、ingest 异步化 | 任务级 check，步级 ingest |
| 定价 | 多模型转售价目表 | 同上，步骤间模型差异大 |

---

## 6. 能力缺口 Review 与补充设计

以下为对接 Review 时发现、**当前 Lite 路径尚未覆盖**的能力，按优先级排列。

### 6.1 ~~[P0] Lite 路径缺少 `parentSpanId` / Span 聚合~~ ✓ Shipped v2.6.2

**已交付：** Lite ingest 在 Lua 聚合成功后调用 `usage_buckets.increment_span()`，Redis key 与 Flink `SpanSink` 一致（24h TTL）。`GET /usage/span/{id}` / `/usage/customer/{id}/spans` 在 Lite 路径可用。

**原设计记录：**

```
ingest 带 parentSpanId
  → Lite Lua 或 Python 侧调用 increment_span()  # 镜像 increment_session
  → Redis keys 与 Full 模式一致：
      span:{id}:cost_usd, :total_tokens, :call_count, :duration_ms, :customer_id
      customer:{cid}:spans  (ZSET by cost)
  → GET /usage/span/{id} 无需改 API
```

**工作量：** ~80 LOC（复用 `usage_buckets.py` 模式）+ 测试对齐 `test_lite_production.py`。

---

### 6.2 [P1] Lite 模式 Budget Webhook 不触发

**现状：** `POST /budget/{id}/webhook` 写入 Redis，但 `webhook-worker` 消费 **Kafka `budget-alerts`**，Lite compose 无 Kafka。Lite ingest 仅在响应 JSON 里带 `budget_alert: BUDGET_EXHAUSTED`。

**影响：** 两客户若依赖「余额告警推送到飞书/Slack」，需自建轮询或改 FluxMeter。

**设计选项：**

| 方案 | 描述 |
|------|------|
| A. Lite 内联 webhook | Lua 或 `aggregate()` 返回 `-1` 时，API 层异步 `httpx.post(webhook_url)` |
| B. Redis 队列 worker | Lite 栈增加轻量 `lite-webhook-worker`（BLPOP，无 Kafka） |
| C. 客户侧 | 轮询 `GET /budget/{id}` 或解析 ingest 响应 |

**推荐：** 方案 A（最少组件）+ 可选 B（可靠投递重试）。

---

### 6.3 [P1] `metadata` 字段未持久化 / 不可查询

**现状：** OpenAPI / schema 定义了 `metadata`，但 `IngestEvent` Pydantic 模型**未包含**该字段，Lite 不存储。

**影响：** 无法按 `room_id`、`clip_id`、`upstream_channel` 做用量切片。

**设计（v2.8 或 token-event-v2）：**

1. Ingest 接受 `metadata`（≤8 个 key，string 值）
2. 可选「索引维度」：`metadata.room_id` → Redis `dim:room:{id}:cost_usd`（Lua INCRBYFLOAT）
3. 查询：`GET /usage/dim/{dim_key}/{dim_value}?period=2026-07`

**ponytail 起步：** 仅支持 1–2 个白名单 dim（`room_id`, `channel`），避免任意高基数。

---

### 6.4 [P1] 转售「成本 vs 售价」双账本

**现状：** 单一 `pricing.json` 计算 `cost_usd`；无 upstream cost 与 downstream price 分离。

**影响：** 客户 A 难以在 FluxMeter 内直接看毛利；需在 ERP 二次计算。

**设计：**

```json
{
  "models": {
    "gpt-4o": {
      "input_per_m": 2.50,
      "output_per_m": 10.00,
      "reseller": { "input_per_m": 3.00, "output_per_m": 12.00 }
    }
  }
}
```

- Redis 同时维护 `cost_usd`（上游）与 `bill_usd`（对客）
- `GET /usage/customer/{id}` 增加 `bill_usd` 字段
- `/budget/check` 按 `bill_usd` 扣减

**替代（零改造）：** 定价表只用对客价；上游成本在 BI 离线算。

---

### 6.5 [P2] Python SDK 无 HTTP Lite 传输

**现状：** ROADMAP 写 Python 1.3.0 支持 HTTP Lite，但 `sdk/python/fluxmeter/client.py` 仍 **仅 Kafka**。JS SDK 已支持 `apiUrl`。

**影响：** 客户 A/B 的 Python 服务需手写 httpx 或换 JS。

**Plan：** 对齐 JS SDK：`FluxMeter(api_url=..., api_key=...)` → `POST /ingest`；保留 Kafka 为可选。版本 **Python SDK 1.4.0**。

---

### 6.6 [P2] Lite 高 QPS 客户端 WAL

**现状：** Kafka SDK 有 WAL；HTTP ingest 无官方重试队列。

**影响：** 客户 A 在 ingest 失败时可能丢计量。

**Plan：** 文档已建议业务侧 WAL；contrib 提供 `fluxmeter-http-wal` 参考实现（SQLite + 后台 flush），或 Python SDK 1.4 内置。

---

### 6.7 [P3] 非 Token 计量（视频分钟 / GPU）

**Explicit non-goal**（见 ROADMAP）。客户 B 的渲染、存储、CDN 应：

- 业务 DB 记录 → 出账时与 `GET /usage/session/{id}` 的 `cost_usd` 合并；或
- 未来 `contrib/meter-units` 连接器写入同一 Redis / 外部 OpenMeter

---

## 7. 推荐实施排期

| 周 | 客户 A | 客户 B | 平台 |
|----|--------|--------|------|
| W1 | Lite 生产部署 + 定价表 + 网关 check/ingest | 任务 session 归因规范 + Job 嵌入 | 压测 `/check` |
| W2 | Token 包 / RPM 策略 + 对账脚本 | 创作者预算 + 场次报表 UI | Stripe 或人工月结 |
| W3 | 高峰 batch + 业务 WAL | 联调 `usage/session` | 评估 §6.1 span Lite |
| W4 | 上线 + 验收 §3.9 | 上线 + 验收 §4.7 | 缺口项进 ROADMAP |

---

## 8. 参考文档

| 文档 | 用途 |
|------|------|
| [api-reference.md](api-reference.md) | 全量 API |
| [integrations.md](integrations.md) | Lago / Stripe / Orb 对接 |
| [pricing-hybrid-paths.md](pricing-hybrid-paths.md) | 阶梯价 Lite vs Flink |
| [production-deploy.md](production-deploy.md) | Redis/API 生产规格 |
| [control-plane-api.md](control-plane-api.md) | 多租户 SaaS（若 A 要做二级代理） |
| [spec/schema/token-event-v1.json](../spec/schema/token-event-v1.json) | 事件 schema |

---

## 9. 变更记录

| 日期 | 说明 |
|------|------|
| 2026-07-05 | v2.6.2：Lite span 聚合落地；§6.1 关闭 |
