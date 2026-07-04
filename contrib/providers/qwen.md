# Qwen (DashScope) → FluxMeter TokenEvent

Maps Alibaba Cloud DashScope compatible-mode Chat Completions to [`token-event-v1`](../../spec/schema/token-event-v1.json).

Base URL: `https://dashscope-intl.aliyuncs.com/compatible-mode/v1` (international) or regional endpoint.

## Response fields

| DashScope (`usage`) | FluxMeter field |
|---------------------|-----------------|
| `prompt_tokens` | `inputTokens` |
| `completion_tokens` | `outputTokens` |
| `prompt_tokens_details.cached_tokens` | `cacheReadTokens` |
| `model` | `modelId` |
| `id` | `requestId` |

Thinking mode (`enable_thinking: true`) may expose reasoning tokens in stream deltas; for billing, use `completion_tokens` from the final chunk or non-stream response.

## Common model IDs

| modelId | Notes |
|---------|-------|
| `qwen-max` | Flagship |
| `qwen-plus` | Balanced; >256K input costs more on DashScope (flat base-tier price in default catalog) |
| `qwen-turbo` | Fast/cheap |
| `qwen-long` | Long context |
| `qwen-plus-2025-12-01` | Version snapshot → prefix-matches `qwen-plus` |

## SDK

```python
meter.track_qwen("cust_123", response, latency_ms=1200)
```

```typescript
client.trackQwen("cust_123", response, { latencyMs: 1200 });
```

## Example event

```json
{
  "customerId": "cust_123",
  "provider": "qwen",
  "modelId": "qwen-plus",
  "inputTokens": 3200,
  "outputTokens": 900,
  "requestId": "chatcmpl-qw-xyz",
  "timestamp": 1718534400000,
  "latencyMs": 1100
}
```
