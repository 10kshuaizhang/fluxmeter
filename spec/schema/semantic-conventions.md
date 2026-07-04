# FluxMeter Semantic Conventions v1

Field semantics for token usage events. Inspired by [OpenTelemetry semantic conventions](https://opentelemetry.io/docs/specs/semconv/).

## Identity

| Field | Required | Description |
|-------|----------|-------------|
| `eventId` | Recommended | UUID. Deduplication key across replay/checkpoint recovery. |
| `customerId` | **Yes** | Billing tenant. Kafka message key. Flink aggregation partition key (with modelId). |
| `requestId` | Optional | Provider's request identifier. |
| `spanId` | Optional | This LLM call's trace span. |
| `parentSpanId` | Optional | Agent run root span. All child LLM calls share one `parentSpanId` for cost attribution. Query via `GET /usage/span/{parentSpanId}`. |

## Provider & model

| Field | Required | Description |
|-------|----------|-------------|
| `provider` | Default `openai` | Normalized provider slug (`openai`, `anthropic`, `google`, `deepseek`, `qwen`, `zhipu`, `moonshot`, `doubao`, `baichuan`, `minimax`, `hunyuan`, …). See `contrib/providers/`. |
| `modelId` | **Yes** | Canonical or versioned model string from provider response. |

## Token categories

Each category is priced independently. Set only fields present in the provider response.

| Field | OpenAI | Anthropic | Chinese (OpenAI-compat) | Notes |
|-------|--------|-----------|-------------------------|-------|
| `inputTokens` | `usage.prompt_tokens` | `usage.input_tokens` | `usage.prompt_tokens` | Base prompt/input |
| `outputTokens` | `usage.completion_tokens` | `usage.output_tokens` | `usage.completion_tokens` | Completion/output |
| `cacheReadTokens` | `prompt_tokens_details.cached_tokens` | `cache_read_input_tokens` | `prompt_tokens_details.cached_tokens` (DeepSeek) | Discounted cached reads |
| `cacheWriteTokens` | — | `cache_creation_input_tokens` | Cache creation cost |
| `reasoningTokens` | `completion_tokens_details.reasoning_tokens` | — | o1/o3 internal reasoning |
| `embeddingTokens` | embedding APIs | — | Separate embedding billing |

**Total billable tokens** = sum of all six categories (not `input + output` alone).

## Timing

| Field | Required | Description |
|-------|----------|-------------|
| `timestamp` | **Yes** | Event time (epoch ms). Used for windowing, not ingestion time. |
| `latencyMs` | Optional | End-to-end provider latency for SLO dashboards. |

## Context

| Field | Description |
|-------|-------------|
| `sessionId` | Conversation/project ID for multi-turn attribution. Query via `GET /usage/session/{sessionId}` (lite ingest; 90d TTL). |
| `environment` | `production`, `staging`, or `development`. |
| `metadata` | Free-form string map (feature flags, plan tier, etc.). |

## Kafka transport

- **Topic**: `token-events`
- **Key**: `customerId` (bytes)
- **Value**: JSON matching `token-event-v1.json`
- **Serde**: camelCase keys (Java Jackson + Python SDK)

## Aggregation keys

Flink keyed stream: `customerId|modelId` composite string.
Span attribution keyed by `parentSpanId` in session windows → `GET /usage/span/{id}` (24h TTL).
Session counters keyed by `sessionId` on lite ingest → `GET /usage/session/{id}` (90d TTL).
Calendar billing buckets: `rollup:{customerId}:period:{YYYY-MM}`, `rollup:{customerId}:d:{YYYY-MM-DD}`.
