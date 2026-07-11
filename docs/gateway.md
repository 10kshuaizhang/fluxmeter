# FluxMeter Gateway

OpenAI-compatible HTTP proxy that **meters, limits, and kills** LLM traffic without app-side `track_*` calls.

**Stack:** Lite mode (Redis ingest) or Full mode (Kafka → Flink). Gateway shares the API Docker image; runs on port **8080**.

## Quick start (Lite demo)

```bash
make demo
# Gateway: http://localhost:8080
# API:      http://localhost:8000
```

Set a customer budget (admin key optional in demo):

```bash
curl -X POST "http://localhost:8000/budget/cust_1?balance_usd=10"
```

Call OpenAI through the gateway:

```bash
export OPENAI_API_KEY=sk-...

curl -X POST http://localhost:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "X-FluxMeter-Customer-Id: cust_1" \
  -H "Authorization: Bearer $OPENAI_API_KEY" \
  -d '{
    "model": "gpt-4o-mini",
    "messages": [{"role": "user", "content": "Hello"}]
  }'
```

Verify usage (no SDK required):

```bash
curl http://localhost:8000/usage/cust_1
```

Mock self-check (no OpenAI):

```bash
make demo-gateway
# or: PYTHONPATH=api python demos/gateway_demo.py
```

## OpenAI Python SDK

Point `base_url` at the gateway and pass FluxMeter headers:

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://localhost:8080/v1",
    api_key=os.environ["OPENAI_API_KEY"],
    default_headers={
        "X-FluxMeter-Customer-Id": "cust_1",
    },
)
resp = client.chat.completions.create(
    model="gpt-4o-mini",
    messages=[{"role": "user", "content": "Hi"}],
)
```

## Request flow

1. **Pre-check** — budget / RPM / hierarchy caps (`budget_gate.run_budget_check`)
2. **Reserve** (streaming only) — hold estimated cost in Redis
3. **Forward** — passthrough to provider (`GATEWAY_UPSTREAM_BASE`)
4. **Stream guard** — kill SSE when estimated spend exceeds hold (<1s)
5. **Ingest** — write usage to Redis (Lite) or Kafka (Full)
6. **Reconcile** — release hold after stream completes

## Headers

| Header | Required | Description |
|--------|----------|-------------|
| `X-FluxMeter-Customer-Id` | Yes | Customer to meter and enforce budget for |
| `Authorization` | Yes* | Provider API key (`Bearer sk-...`) |
| `X-API-Key` | If auth enabled | FluxMeter API key |
| `X-FluxMeter-Span-Id` | No | Parent span cap scope |
| `X-FluxMeter-Session-Id` | No | Session cap scope |

\* Or set `GATEWAY_UPSTREAM_API_KEY` / `OPENAI_API_KEY` on the gateway container.

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `GATEWAY_UPSTREAM_BASE` | `https://api.openai.com/v1` | Provider base URL |
| `GATEWAY_UPSTREAM_API_KEY` | — | Fallback provider key |
| `GATEWAY_DEFAULT_ESTIMATE_USD` | `0.05` | Pre-check / reserve estimate when `max_tokens` absent |
| `FLUXMETER_LITE_MODE` | `false` | `true` = Redis ingest (no Kafka) |
| `BUDGET_FAIL_POLICY` | `closed` | `open` / `closed` when Redis unavailable |
| `REDIS_HOST` | `localhost` | Redis for budget + ingest |

## Errors

| HTTP | Meaning |
|------|---------|
| 402 | Budget denied before upstream (`budget_exhausted`, `rate_limited`, etc.) |
| 401 | Missing provider or FluxMeter API key |

Streaming kill returns an SSE error chunk with `"code": "stream_killed"` then `[DONE]`.

## Gateway vs SDK `wrap()`

| Approach | Ingest | Integration |
|----------|--------|-------------|
| **Gateway** | Automatic at proxy | Change `base_url` only |
| **SDK `wrap()`** | Post-call `track` | Python client patch |

Use Gateway when you cannot modify app code or need a central enforcement point.

## Production deploy

Same image as API, different command:

```yaml
command: uvicorn gateway_app:app --host 0.0.0.0 --port 8080
```

Place Gateway behind ingress; keep API internal for admin/billing queries. See [production-deploy.md](production-deploy.md).

## Limitations (3.2.0 MVP)

- OpenAI-compatible `/v1/chat/completions` only (Anthropic native API: Phase G.1)
- Stream kill uses heuristic token estimate when provider omits usage chunks (`ponytail:` char/4 fallback)
- TPM limits, LiteLLM adapter, predictive cost: P2 backlog
