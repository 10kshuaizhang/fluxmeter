# DeepSeek → FluxMeter TokenEvent

Maps DeepSeek Chat Completions (OpenAI-compatible) usage to [`token-event-v1`](../../spec/schema/token-event-v1.json).

Base URL: `https://api.deepseek.com` (OpenAI format) or `https://api.deepseek.com/anthropic` (Anthropic format — use manual `track()` for Anthropic-format responses).

## Response fields

| DeepSeek (`usage`) | FluxMeter field |
|--------------------|-----------------|
| `prompt_tokens` | `inputTokens` |
| `completion_tokens` | `outputTokens` |
| `prompt_tokens_details.cached_tokens` | `cacheReadTokens` |
| `completion_tokens_details.reasoning_tokens` | `reasoningTokens` |
| `model` | `modelId` |
| `id` | `requestId` |

## Model aliases

| API model | Canonical pricing id |
|-----------|---------------------|
| `deepseek-v4-flash` | `deepseek-v4-flash` |
| `deepseek-v4-pro` | `deepseek-v4-pro` |
| `deepseek-chat` | `deepseek-chat` (legacy alias, deprecated 2026-07-24) |
| `deepseek-reasoner` | `deepseek-reasoner` (legacy alias, deprecated 2026-07-24) |

Version suffixes (e.g. dated snapshots) prefix-match via `prefix_models` in `config/pricing.json`.

## SDK

```python
meter.track_deepseek("cust_123", response, latency_ms=1200)
```

```typescript
client.trackDeepSeek("cust_123", response, { latencyMs: 1200 });
```

## Example event

```json
{
  "customerId": "cust_123",
  "provider": "deepseek",
  "modelId": "deepseek-v4-flash",
  "inputTokens": 5000,
  "outputTokens": 1200,
  "cacheReadTokens": 800,
  "reasoningTokens": 0,
  "requestId": "chatcmpl-ds-abc",
  "timestamp": 1718534400000,
  "latencyMs": 980
}
```
