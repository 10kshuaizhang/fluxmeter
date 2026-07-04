# Moonshot / Kimi → FluxMeter TokenEvent

Maps Moonshot AI Chat Completions (OpenAI-compatible) to [`token-event-v1`](../../spec/schema/token-event-v1.json).

Base URL: `https://api.moonshot.cn/v1`

## Response fields

| Moonshot (`usage`) | FluxMeter field |
|--------------------|-----------------|
| `prompt_tokens` | `inputTokens` |
| `completion_tokens` | `outputTokens` |
| `model` | `modelId` |
| `id` | `requestId` |

## Model selection by context

| modelId | Context window |
|---------|----------------|
| `moonshot-v1-8k` | 8K |
| `moonshot-v1-32k` | 32K |
| `moonshot-v1-128k` | 128K |

Pick the model matching your prompt size; pricing differs per tier.

## SDK

```python
meter.track_moonshot("cust_123", response, latency_ms=1500)
```

```typescript
client.trackMoonshot("cust_123", response, { latencyMs: 1500 });
```

## Example event

```json
{
  "customerId": "cust_123",
  "provider": "moonshot",
  "modelId": "moonshot-v1-32k",
  "inputTokens": 8000,
  "outputTokens": 2000,
  "requestId": "cmpl-ms-abc",
  "timestamp": 1718534400000,
  "latencyMs": 1400
}
```
