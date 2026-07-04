# Zhipu GLM → FluxMeter TokenEvent

Maps Zhipu AI OpenAI-compatible Chat Completions to [`token-event-v1`](../../spec/schema/token-event-v1.json).

Base URL: `https://open.bigmodel.cn/api/paas/v4/` (OpenAI-compatible path).

## Response fields

| GLM (`usage`) | FluxMeter field |
|---------------|-----------------|
| `prompt_tokens` | `inputTokens` |
| `completion_tokens` | `outputTokens` |
| `model` | `modelId` |
| `id` | `requestId` |

## Common model IDs

| modelId | Notes |
|---------|-------|
| `glm-4` | Flagship |
| `glm-4-flash` | Low latency |
| `glm-4-air` | Cost-optimized |

## SDK

```python
meter.track_glm("cust_123", response, latency_ms=800)
```

```typescript
client.trackGLM("cust_123", response, { latencyMs: 800 });
```

## Example event

```json
{
  "customerId": "cust_123",
  "provider": "zhipu",
  "modelId": "glm-4-flash",
  "inputTokens": 1500,
  "outputTokens": 400,
  "requestId": "glm-req-001",
  "timestamp": 1718534400000,
  "latencyMs": 650
}
```
