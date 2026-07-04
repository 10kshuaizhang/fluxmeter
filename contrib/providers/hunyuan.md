# Tencent Hunyuan → FluxMeter TokenEvent

Maps Tencent Cloud Hunyuan OpenAI-compatible Chat Completions to [`token-event-v1`](../../spec/schema/token-event-v1.json).

Base URL: Tencent Cloud API gateway (OpenAI-compatible mode).

## Response fields

| Hunyuan (`usage`) | FluxMeter field |
|-------------------|-----------------|
| `prompt_tokens` | `inputTokens` |
| `completion_tokens` | `outputTokens` |
| `model` | `modelId` |
| `id` | `requestId` |

## Common model IDs

| modelId | Notes |
|---------|-------|
| `hunyuan-lite` | Free tier ($0 in default catalog) |
| `hunyuan-pro` | Pro tier |

## SDK

```python
meter.track_hunyuan("cust_123", response, latency_ms=800)
```

```typescript
client.trackHunyuan("cust_123", response, { latencyMs: 800 });
```

## Example event

```json
{
  "customerId": "cust_123",
  "provider": "hunyuan",
  "modelId": "hunyuan-pro",
  "inputTokens": 2500,
  "outputTokens": 700,
  "requestId": "hy-req-001",
  "timestamp": 1718534400000,
  "latencyMs": 760
}
```
