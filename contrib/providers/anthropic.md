# Anthropic → FluxMeter TokenEvent

Maps Anthropic Messages API usage to [`token-event-v1`](../../spec/schema/token-event-v1.json).

## Response fields

| Anthropic (`usage`) | FluxMeter field |
|---------------------|-----------------|
| `input_tokens` | `inputTokens` |
| `output_tokens` | `outputTokens` |
| `cache_read_input_tokens` | `cacheReadTokens` |
| `cache_creation_input_tokens` | `cacheWriteTokens` |
| `model` | `modelId` |
| `id` | `requestId` |

## SDK

```python
meter.track_anthropic("cust_123", response, latency_ms=900)
```

```typescript
client.trackAnthropic("cust_123", response, { latencyMs: 900 });
```

## Example event

```json
{
  "customerId": "cust_123",
  "provider": "anthropic",
  "modelId": "claude-sonnet-4",
  "inputTokens": 800,
  "outputTokens": 420,
  "cacheReadTokens": 150,
  "cacheWriteTokens": 50,
  "requestId": "msg_01abc",
  "timestamp": 1718534400000
}
```

## Agent spans

Set `parentSpanId` on every tool/LLM child call to aggregate agent run cost via `GET /usage/span/{id}`.
