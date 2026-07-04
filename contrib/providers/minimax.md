# MiniMax → FluxMeter TokenEvent

Maps MiniMax OpenAI-compatible Chat Completions to [`token-event-v1`](../../spec/schema/token-event-v1.json).

Base URL: `https://api.minimax.chat/v1` (OpenAI-compatible group chat endpoint).

## Response fields

| MiniMax (`usage`) | FluxMeter field |
|-------------------|-----------------|
| `prompt_tokens` | `inputTokens` |
| `completion_tokens` | `outputTokens` |
| `model` | `modelId` |
| `id` | `requestId` |

Some MiniMax responses nest usage under `base_resp` + `usage`; normalize to top-level `usage` before calling SDK helpers, or use manual `track()`.

## Common model IDs

| modelId | Notes |
|---------|-------|
| `abab6.5-chat` | MiniMax abab6.5 chat |

## SDK

```python
meter.track_minimax("cust_123", response, latency_ms=600)
```

```typescript
client.trackMiniMax("cust_123", response, { latencyMs: 600 });
```

## Example event

```json
{
  "customerId": "cust_123",
  "provider": "minimax",
  "modelId": "abab6.5-chat",
  "inputTokens": 1200,
  "outputTokens": 350,
  "requestId": "mm-req-001",
  "timestamp": 1718534400000,
  "latencyMs": 580
}
```
