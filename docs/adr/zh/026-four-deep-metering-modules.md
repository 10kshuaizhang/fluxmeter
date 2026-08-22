# ADR-026：四个深层 Metering 模块

**状态：** Accepted  
**日期：** 2026-08-22  
**版本：** 4.6.0

## 背景

Custody、Pricing、Reservation、Budget 已经存在，但调用方仍需用浅 helper 自己拼状态机。HTTP/Gateway 知道 topic 与 TTL；Gateway 通过 pass-through 模块估价；Budget cache 和 rate-limit identity 未包含 tenant；Gateway 先创建 hold、再登记 Reservation，进程可在两步之间崩溃并留下 orphan hold。

## 决策

Metering runtime 固定为四个 deep modules：

1. `TokenEventCustody` 在 `accept` / `accept_many` 后隐藏 identity、envelope、Kafka delivery、quarantine、backpressure 与 reconciliation。
2. `PricingCatalog` 统一 catalog validation、model normalization、精确 quote、tier traversal 和 Gateway advisory estimate。
3. `Reservation` 拥有所有 hold transition；Gateway `open` 用一次 Redis Lua 原子完成 reserve + register，settle/expire 保持幂等。
4. `Budget` 统一 configure、top-up、snapshot、authorization、cache fallback、rate limit、hierarchy/API-key cap；cache 与 RPM identity 必须 tenant-scoped。

HTTP route 和 Gateway orchestration 只是 adapter。删除的 `budget_gate`、`budget_ops` 和 Gateway pricing-estimate 模块不得以兼容层形式重新引入。

## 后果

- 状态机变更集中在一个 locality，并通过同一个 interface 测试。
- 相同 customer/span/session ID 不再跨 tenant 共享 Budget cache、RPM 或新 scope key。
- Gateway hold 创建与 Reservation 登记之间不再有 crash window。
- Python runtime 的 pricing validation 与 quote 不再漂移。
- Redis Lua 仍是单节点 implementation constraint；未来 distributed adapter 必须保留这些 interface 和原子 transition。

