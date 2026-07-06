# LLM 应用计费与运行时管控 — 行业调研报告

**日期：** 2026-07-06  
**对象：** FluxMeter 能力校准与路线图排序  
**配套：** [ROADMAP.md](../ROADMAP.md) · [progress.md](../progress.md) · [customer-stories-lite.md](customer-stories-lite.md)

---

## 1. 调研动机与结论摘要

LLM 应用的成本问题已从「月末看账单」变为「agent 循环数分钟内烧光预算」。2025–2026 公开产品与研究材料显示，行业在**商业层**与**执行层**上分别收敛：

| 层 | 主流形态 | 谁在做 |
|----|----------|--------|
| **商业（Invoice SoR）** | 席位底金 + credits / prepaid wallet + overage invoice；企业 Flex Credit / Work Unit | Metronome、Orb、Stripe、Lago、Kong/OpenMeter、Salesforce/HubSpot/Workday |
| **执行（Runtime SoR）** | Pre-call hard deny → reserve → call → reconcile；必要时 mid-stream stop | LiteLLM、SpendGuard、厂商 access engine、Cursor/Claude spend limits、API 转售网关 |

**FluxMeter 定位校准：** 执行层 SoR（计量 + 硬闸 + 流式持有），**不**做 Invoice/合同/收款。财务核心（`check` / `reserve` / prepaid / tiers / exactly-once）已齐；与行业差在 **path activation**（默认走 wrap/proxy）与 **complementary export**（把规范化事件交给出账平台）。

```text
[App / Agent / Gateway]
        │
        ▼
[FluxMeter: check → hold → meter → kill]     ← runtime SoR（开放、可自建）
        │
        ├── events ──► Metronome / Orb / Stripe / Lago / Kong Billing
        └── deny/kill ──► 停止烧 token
```

**路线含义（已写入 ROADMAP）：** Phase 3 Path activation → Phase 4 exporters + 层级预算 → Phase 5 Gateway 产品化 → Phase 6 SaaS RBAC（有需求再建）。

---

## 2. 方法与资料范围

| 类型 | 材料（抽样） |
|------|----------------|
| **终端产品计费** | Cursor docs（Models & Pricing、Spend limits）；GitHub Copilot / Claude Code / Windsurf 公开对比（2026 seat + credits）；Anthropic Claude spend limits / Enterprise Admin API |
| **厂商 access** | OpenAI「Beyond rate limits」（Codex/Sora credits；同步决策 + 异步 debit） |
| **Agent / entitlement 研究** | Zylos *AI Agent Billing and Entitlement Systems*（2026-03）；Paid.ai *10 features AI Agents need*；UsageBox agent payment / metering gap（2026） |
| **协议型 runtime** | Agentic SpendGuard pre-call budget caps；Agent Spend Protocol（reserve / commit / release） |
| **Gateway** | LiteLLM multi-tenant budgets / agent session caps；Portkey / Helicone cost management；Kong Konnect prepaid credits（OpenMeter）；MLflow / AWS generative-ai-atlas LLM Gateway patterns |
| **计量与出账 OSS/商业** | OpenMeter（后并入 Kong）；OpenMonetize；Lago；Metronome / Orb / Stripe 公开定位 |
| **转售 / 中转** | 国内 OpenAI-compatible reseller 模式（prepaid wallet、key 级日/月 budget）；FluxMeter 自有客户故事 TokenBridge / ClipLive |
| **企业 Flex Credits** | Salesforce Agentforce Flex Credits rate card；HubSpot Credits；Workday Flex Credits |

公开材料，侧重 **架构模式** 与 **强制力**，非财务尽调。

---

## 3. 行业决策瀑布（共识模型）

能活过 agent 峰值的栈，普遍按四步裁决：

| 步 | 问题 | 失败形态 | 案例 |
|----|------|----------|------|
| **0. Path** | 流量是否必经管控点？ | 应用忘调 `check` → 透支 | LiteLLM / Portkey / 转售网关：代理路上强制；纯 SDK 库易漏 |
| **1. Admit** | 是否允许启动 provider 时钟？ | 事后 dashboard | OpenAI access engine；SpendGuard `request_decision`；Cursor/Claude spend limit；Anthropic org monthly cap |
| **2. Hold** | 流式最坏成本如何占坑？ | 并发请求透支同一钱包 | Stripe 式 auth/capture；SpendGuard reserve/commit；FluxMeter `held_usd` |
| **3. Settle** | 如何入账 / 出票？ | runtime 做合同与收款 | Metronome / Orb / Stripe / Kong Metering |

行业额外共识：

- **Pre-call > post-call。** 事后告警只能解释损失，不能阻止损失（Zylos；SpendGuard）。
- **Credits 是过渡本体。** CFO 懂「5 万 credits / action」，不懂「$0.000015/token」（Salesforce / HubSpot / Metronome field notes）。
- **层级配额防 noisy neighbor。** Org → Team → User → Key / Agent session（LiteLLM；Claude Enterprise inheritance；Cursor Enterprise member/team）。
- **硬闸之外要有软告警窗口。** Claude：75%/90% admin、75%/95% user；否则直接 hard stop 体验差。
- **Invoice 平台不等于 path enforcement。** Kong 文档明确：entitlement 实时跟踪，但 **Gateway 级自动 enforce 尚未内置**，需 webhook + 自建拒绝——与 FluxMeter「自托管硬闸」形成结构性空位。

---

## 4. 八类场景档案

### 4.1 API 转售 / Token 中转站

**画像：** OpenAI 兼容 `/v1/chat/completions`；预付费；多模型加价；下游 key。

**模式：** Prepaid USD（或 token 包）→ 请求前余额/预算检查 → pass-through 或拒绝 → 异步或同步扣费；key 级 daily/monthly budget。

**代表：** 国内中转站类产品（如 prepaid wallet + scoped keys）；FluxMeter 客户故事 TokenBridge。

**风险：** 余额在业务库异步对账 → 上游已出账平台赔付（透支数千美元是常见事故叙述）。

**FluxMeter：** `check` + package + pricing catalog（含国内模型）匹配核心路径；缺口：virtual key = budget 主体、成本/售价双账本、Lite webhook。

### 4.2 Agent 编排 / 多步 Runtime

**画像：** 循环、tool call、并发子 agent；单次 span 费用波动极大。

**模式：** `reserve(estimate) → LLM → commit(actual) / release`；幂等重试不重复占额；parent budget 约束 children。

**代表：** SpendGuard + ASP draft；Paid.ai 生产对话提炼；OpenAI access engine（同步 admit + 异步 debit，短暂超支可 refund）。

**FluxMeter：** API 形状已对齐（`reserve` / `reconcile`、span/session 归因）；缺口：span **作为强制钱包**（非仅查询）、并发池协调、mid-stream 杀流默认路径。

### 4.3 IDE / Coding Assistant

**画像：** 席位订阅 + included credit 池；重 agent 用户显著超标称。

**模式：**

| 产品 | 计费粗线条 |
|------|------------|
| **Cursor** | 付费档含 API credit 池；Auto/Composer 与第三方 API **双池**；Teams/Enterprise **member/team spend limit**，到线停 AI |
| **GitHub Copilot** | AI Credits（1 credit = $0.01 量级公开叙述）；席位 + allotment + overage |
| **Claude Code** | 档位封顶、无 overage（可预期，靠限流） |
| **Windsurf** | 配额/等待型，无 wallet overage |

**启示：** FluxMeter 不应做「席位商店」；应对齐其 **spend limit 引擎**（周期 cap + 硬停 + 团队/成员层）。双池可后做（便宜模型池 vs 全价池），不是 Phase 3 阻塞项。

### 4.4 企业 Flex Credits / Work Units

**画像：** Salesforce Agentforce（约 $500 / 100k credits；standard action = 20 credits；token 阈值可乘倍）；HubSpot Credits（月重置、不滚动）；Workday Flex（年可共用池）。

**模式：** Token 成本被抽象成 **业务动作**；采购语言是 credits，底层仍是计量。

**FluxMeter：** 不建 Flex SKU UI（anti-goal）。可通过外层 `cost_usd` 倍率或 metadata 对账；SoR 仍是 token 事件。

### 4.5 LLM Gateway 治理

**画像：** 企业统一入口；virtual key；路由/fallback；spend tracking。

**LiteLLM（能力最接近企业对标清单）：**

- Org → Team → User → Key 预算继承（下层不可超上层）
- Soft / hard budget；`budget_duration` 周期重置
- TPM + RPM；agent `max_budget_per_session`、`max_iterations`
- 命中即实时阻断

**Portkey / Helicone：** 网关 + 观测 + 预算策略；偏 control plane / analytics；成本路由与 fallback。

**Kong + OpenMeter：** 路径计量 → prepaid wallet → invoice；**自动 gateway enforce 仍声明为未来项**（靠通知自建拒绝）。

**FluxMeter：** 财务正确性（幂等、持有、吞吐）可强于多数网关；短板是 **默认不坐在 path 上**，以及 TPM / 完整继承树。

### 4.6 计量 → 出账平台

**OpenMeter / Kong Metering：** CloudEvents ingest、entitlement、prepaid、Stripe 联动；AI token meter。  
**Metronome / Orb：** 企业合同、rate card、重评级、invoice。  
**Lago：** 开源计费优先。  
**Stripe Meters：** 支付/税最全，AI 计费深度依赖生态。

**分工已被市场承认：** meter 可共，**guardrail 常留给应用**。FluxMeter 用 export 当下游事件源，而不是第二套合同系统。

### 4.7 观测优先栈

Helicone / Langfuse 等：trace、session、prompt、成本可视化。与 FluxMeter **正交**——可共用 session/span ID，不抢 SoR of spend。

### 4.8 厂商原生 spend limits

**Anthropic：**

- API org：tier monthly spend cap；可自设低于 cap；到线暂停至下月
- Enterprise：Admin API 成员有效限额（继承 seat tier / group / org）、period-to-date spend、上涨申请审批
- Alerts：75% / 90%（admin）、75% / 95%（user）

**OpenAI：** prepaid credits；access 路径融合 rate limit 与 credit；强调 **可证明正确性** 可容忍短暂 overshoot + refund。

**启示：** soft warn + period reset + hierarchy inheritance 是「企业好用硬闸」的标配，不仅是 balance=0 deny。

---

## 5. 模式目录（可复用检查表）

| ID | 模式 | 强制力 | 2026 普及度 |
|----|------|--------|-------------|
| M1 | Pre-call hard deny | 拒绝上路 | 必装 |
| M2 | Prepaid wallet (fiat) | 余额水位 | 必装（转售/agent） |
| M3 | Token / credit packages | 包内扣减 | 高（转售、门户） |
| M4 | Reserve / reconcile (auth/capture) | 占坑后再结算 | 高（stream/agent） |
| M5 | Soft → hard alert ladder | 可运营 | 高（Claude/Cursor） |
| M6 | Hierarchical budgets | 防 noisy neighbor | 高（LiteLLM/企） |
| M7 | Period reset caps | 订阅周期 | 高（HubSpot/Claude） |
| M8 | Dual pool pricing | 控制 vs 全价 | 中（Cursor） |
| M9 | Mid-stream kill | 掐在途流 | 低~中（叙事强） |
| M10 | Path-native proxy/wrap | 默认生效 | 高（网关/SDK wrap） |
| M11 | Event export to invoice SoR | 互补 | 高（Kong/OpenMeter） |
| M12 | Flex / outcome SKU | 商业语言 | 中（企业销售） |
| M13 | Observability / cost routing | 运维优化 | 高（但非计费 SoR） |

FluxMeter：**M1–M4 成熟**；M5–M7 / M9–M11 **路线图正中**；M8/M12/M13 **有意不做或不优先**。

---

## 6. FluxMeter 能力评分矩阵

| 模式 | 需求 | FluxMeter | 证据 / 缺口 |
|------|------|-----------|-------------|
| M1 Pre-call deny | P0 | **✓** | `GET /budget/{id}/check`，&lt;10ms，三层容灾 |
| M2 Prepaid USD | P0 | **✓** | balance、阈值、exhaust |
| M3 Token packages | P0 | **✓** | `/budget/{id}/package`，Lite drawdown |
| M4 Reserve/reconcile | P0 | **✓** | `held_usd` / effective balance |
| Multi-model pricing | P0 | **✓** | catalog；中国厂商模型 2.6.0 |
| Tiered rating | P1 | **✓** | flat / volume / graduated |
| Exactly-once | P0 | **✓** | eventId、WAL、SET NX |
| RPM | P1 | **✓** | `max_rpm` |
| TPM | P1 | **✗** | Phase 5 |
| Span/session 查询 | P0 | **✓** | 2.6.1 / 2.6.2 |
| Span/session **硬预算** | P0 | **△** | 归因有，`check` 未挂层级钱包 |
| Org→Team→Key | P0 企业 | **△** | tenant 脚手架；非完整继承 enforce |
| Soft alert ladder | P1 | **△** | `BUDGET_LOW`；无 70/90 多档；**Lite webhook 不投递** |
| Credit grants / expiry / priority burn | P1 | **✗** | 单钱包 |
| Dual pool | P1 | **✗** | 非目标短期 |
| Mid-stream kill path | P0 叙事 | **△** | Full Kafka kill；无完整 proxy demo |
| Wrap / proxy default | P0 接入 | **✗/△** | `track_*` 有；无 `wrap()` |
| Metadata / feature dims | P1 | **✗** | schema 有，不落盘 |
| COGS vs sell ledger | P1 转售 | **✗** | 文档 backlog |
| Export Metronome/Orb/Stripe | P1 | **△** | Stripe stub only |
| Invoice / Flex SKU / seats | — | **✗（正确）** | anti-goal |
| Full observability | — | **✗（正确）** | 交给 Helicone 等 |

**总评：** Runtime **财务语义**对齐 2026 潮流；**交付形态**仍接近「被调用的正确库」，而非「默认装上的控制面」。这解释了为何下一阶段是 Path activation，而不是 Full SaaS RBAC。

---

## 7. 竞争与定位简图

| 玩家类型 | 例子 | FluxMeter 关系 |
|----------|------|----------------|
| Invoice / contract | Metronome, Orb, Stripe, Lago | **互补**；export 事件 |
| Gateway + metering | Kong+OpenMeter, LiteLLM, Portkey | **分工**：它们有 path，我们有更硬的金融原语；可挂 adapter |
| Observability | Helicone, Langfuse | **正交** |
| Reseller appliances | 国内中转 | **可替换关键钱包/闸门** |
| Agent entitlement libs | SpendGuard | **同类**；迁移成本低（动词接近）|

蓝海句式（对外）：

> Use FluxMeter for **runtime** (check / hold / kill), Metronome/Orb/Stripe for **invoice**.

---

## 8. 对路线图的直接建议（已落地）

| 排序 | Phase | 交付重心 | 行业证据 |
|------|-------|----------|----------|
| **1** | **3 / v2.7** | Kill demo、wrap SDK、npm、Lite webhook、轻量 parent cap | Path + 默认可演示硬闸；Claude/Cursor 软告警可后置一档 |
| **2** | **4 / v2.8** | Metronome/Orb export、partner recipes、agent hierarchy、key budgets | Complement 策略；LiteLLM/Claude Enterprise 层级；转售 key 额度 |
| **3** | **5 / v3.0** | Deployable proxy + TPM + LiteLLM contrib | Gateway 玩家默认能力集 |
| **4** | **6 demand** | Full RBAC / Postgres metadata / Hosted | 仅当自建 SaaS 运营商明确需要 |

**明确不做（调研复核）：** ASC 606、多年度 commit UI、Flex Credit 产品面、All-in-one 观测平台、Outcome billing 引擎。

---

## 9. 风险与假设

| 假设 | 风险若错 | 缓解 |
|------|----------|------|
| Adopter 要的是「默认路径」不是「更多价目表」 | Path 投入 ROI 低 | Phase 3 先做可度量的 GIF / one-liner 接入 |
| Invoice 巨头愿意推荐 runtime 互补件 | Export 无人用 | Partner docs 冷启动成本低；乙太仍能服务自建钱包 |
| Flex Credit 可外置 | 企业只买「动作计价」黑盒 | contrib 倍率文档；不进核心 |
| Lite 仍是默认入口 | Webhook 修复优先级过高 | ROADMAP 已把 Lite webhook 标 P0 |

---

## 10. 参考链接（精选）

- Cursor: [Models & Pricing](https://cursor.com/docs/models-and-pricing), [Spend limits](https://cursor.com/help/account-and-billing/spend-limits)
- Anthropic: [Rate limits / spend limits](https://platform.claude.com/docs/en/api/rate-limits), [Spend Limits API](https://platform.claude.com/docs/en/manage-claude/spend-limits-api)
- OpenAI: [Beyond rate limits](https://openai.com/index/beyond-rate-limits/)
- LiteLLM: [Multi-tenant architecture](https://docs.litellm.ai/docs/proxy/multi_tenant_architecture), [Budgets](https://docs.litellm.ai/docs/proxy/users)
- Kong: [Prepaid credits](https://konghq.com/blog/product-releases/metering-billing-prepaid-credits), [Entitlements](https://developer.konghq.com/metering-and-billing/entitlements/)
- OpenMeter: [GitHub](https://github.com/openmeterio/openmeter)
- SpendGuard: [Pre-call budget caps](https://agenticspendguard.dev/docs/use-cases/pre-call-budget-cap/)
- Zylos: [AI Agent Billing and Entitlement Systems](https://zylos.ai/research/2026-03-31-ai-agent-billing-entitlement-systems)
- Paid.ai: [10 features AI Agents need in a billing system](https://paid.ai/blog/billing/10-features-ai-agents-need-in-a-billing-system)
- Salesforce: [Agentforce Flex Credits](https://www.salesforce.com/news/press-releases/2025/05/15/agentforce-flexible-pricing-news/)
- UsageBox: [AI agent payment stack & metering gap](https://usagebox.com/articles/ai-agent-payment-stack-2026-x402-ap2-agent-pay-metering-gap)

---

## 11. 变更记录

| 日期 | 变更 |
|------|------|
| 2026-07-06 | 初版；校准写入 ROADMAP Phase 3–6 与 progress Phase 3/4 checklist |
