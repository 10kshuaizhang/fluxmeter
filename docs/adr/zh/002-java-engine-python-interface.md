# ADR-002: Java 引擎 + Python 界面（混合栈）

| Field | Value |
|-------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-16 |
| **Version** | 0.1.0 |
| **Lang** | 中文 |

## Context

目标受众分裂：billing 工程师要吞吐和 EO 语义；AI 开发者要 `pip install` 三行集成。PyFlink 可统一语言，但有 serialization overhead。

## Decision

**Java 17 Flink 做引擎，Python 做 SDK + FastAPI API 层**。明确拒绝 PyFlink rewrite（ROADMAP non-goals）。Engine 与 SDK **独立 semver**（engine 3.x / SDK 1.5.x）。

## Consequences

✅ 1M+ eps 无 PyFlink 开销；AI 社区通过 Python 接入。❌ 双语言维护；pricing 逻辑需在 Java + Python 双实现（后由 `config/pricing.json` 统一）。

## Evidence

DESIGN.md「Architecture: Java Core + Python SDK」；commit `81968fd`。

---

**English:** [ADR-002](../en/002-java-engine-python-interface.md)
