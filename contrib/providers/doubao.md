# Doubao (Volcengine Ark) → FluxMeter TokenEvent

Maps ByteDance Volcengine Ark OpenAI-compatible Chat Completions to [`token-event-v1`](../../spec/schema/token-event-v1.json).

Base URL: Ark endpoint (region-specific), OpenAI-compatible `/chat/completions` path.

## Response fields

| Ark (`usage`) | FluxMeter field |
|---------------|-----------------|
| `prompt_tokens` | `inputTokens` |
| `completion_tokens` | `outputTokens` |
| `model` | `modelId` |
| `id` | `requestId` |

## Common model IDs

| modelId | Notes |
|---------|-------|
| `doubao-pro-32k` | Pro tier |
| `doubao-lite-32k` | Lite tier |

Endpoint IDs in Ark console may differ; pass the `model` string returned in the API response as `modelId`.

## SDK

```python
meter.track_doubao("cust_123", response, latency_ms=900)
```

```typescript
client.trackDoubao("cust_123", response, { latencyMs: 900 });
```

## Example event

```json
{
  "customerId": "cust_123",
  "provider": "doubao",
  "modelId": "doubao-pro-32k",
  "inputTokens": 2000,
  "outputTokens": 600,
  "requestId": "ark-req-001",
  "timestamp": 1718534400000,
  "latencyMs": 850
}
```
