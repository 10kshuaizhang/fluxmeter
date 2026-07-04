# Baichuan → FluxMeter TokenEvent

Maps Baichuan AI OpenAI-compatible Chat Completions to [`token-event-v1`](../../spec/schema/token-event-v1.json).

Base URL: `https://api.baichuan-ai.com/v1`

## Response fields

| Baichuan (`usage`) | FluxMeter field |
|--------------------|-----------------|
| `prompt_tokens` | `inputTokens` |
| `completion_tokens` | `outputTokens` |
| `model` | `modelId` |
| `id` | `requestId` |

## Common model IDs

| modelId | Notes |
|---------|-------|
| `baichuan4-turbo` | Baichuan 4 Turbo |

## SDK

```python
meter.track_baichuan("cust_123", response, latency_ms=700)
```

```typescript
client.trackBaichuan("cust_123", response, { latencyMs: 700 });
```

## Example event

```json
{
  "customerId": "cust_123",
  "provider": "baichuan",
  "modelId": "baichuan4-turbo",
  "inputTokens": 1800,
  "outputTokens": 500,
  "requestId": "bc-req-001",
  "timestamp": 1718534400000,
  "latencyMs": 720
}
```
