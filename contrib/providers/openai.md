# OpenAI → FluxMeter TokenEvent

Maps OpenAI Chat Completions / Responses API usage to [`token-event-v1`](../../spec/schema/token-event-v1.json).

## Response fields

| OpenAI (`usage`) | FluxMeter field |
|------------------|-----------------|
| `prompt_tokens` | `inputTokens` |
| `completion_tokens` | `outputTokens` |
| `prompt_tokens_details.cached_tokens` | `cacheReadTokens` |
| `completion_tokens_details.reasoning_tokens` | `reasoningTokens` |
| `model` | `modelId` |
| `id` | `requestId` |

## SDK

```python
meter.track_openai("cust_123", response, latency_ms=1200)
```

```typescript
client.trackOpenAI("cust_123", response, { latencyMs: 1200 });
```

## Example event

```json
{
  "eventId": "550e8400-e29b-41d4-a716-446655440000",
  "customerId": "cust_123",
  "provider": "openai",
  "modelId": "gpt-4o",
  "inputTokens": 1250,
  "outputTokens": 847,
  "cacheReadTokens": 200,
  "reasoningTokens": 0,
  "requestId": "chatcmpl-abc",
  "timestamp": 1718534400000,
  "latencyMs": 1340
}
```

## Streaming

Use SDK `wrap_stream()` for heartbeat events during long streams, or HTTP `reserve` + `reconcile` for budget pessimism.
