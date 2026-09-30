# A complete local customer-usage trial

Engine/API 4.8.4. No model-provider key is needed. Use a local development machine
with Git, Java 17, Docker + Docker Compose, and Python 3.10+ (`python3`). The demo
runs Kafka, Flink, Redis, API, Gateway, and Grafana; allow memory for the full stack.

```bash
git clone https://github.com/10kshuaizhang/fluxmeter.git
cd fluxmeter
make demo
```

`make demo` builds the engine, starts Compose, then runs `demos/quickstart.py`:

1. Wait for `GET /ready` to confirm Redis, Kafka, and Flink probe consumption.
2. Generate a unique customer and event ID. Send `/ingest` 1,200 input and 400
   output tokens for `gpt-4o-mini`; omit `timestamp` so the server assigns current time.
3. Require an `accepted` receipt. A `202` receipt is custody, **not settlement**.
4. Poll `/usage/customer/{id}` until it reports exactly one event, 1,600 total
   tokens, and numeric `cost_usd: 0.00042`, using default catalog input/output
   prices of $0.15/$0.60 per million tokens.
5. Print the response and `PASS`. Any timeout, quarantine, or mismatch exits nonzero.

The script makes no LLM call and creates synthetic test usage only. Every run
uses new IDs, so it does not depend on clearing Redis or disabling deduplication.
The normal projection delay is roughly 10–15 seconds; startup and lag can take
longer. Readiness, custody, and projection each have a default 180-second bound.

```bash
# Repeat against the running stack without rebuilding:
make demo-verify

# Optional authenticated endpoint (use an ingest/read key that can access the
# generated customers; an existing customer-scoped key will not match):
export FLUXMETER_API_KEY='your-ingest-read-key'
python3 demos/quickstart.py --url http://localhost:8000 --timeout 300

# Only when intentionally using a different catalog, supply its fixture cost:
python3 demos/quickstart.py --expected-cost 0.00042
```

Do not paste this example API key literally. Demo Compose allows unauthenticated
access when keys are unset; use [production deployment](production-deploy.md)
configuration before sharing an endpoint. Stop local services with `make stop`.

## Troubleshooting

| Failure | Check |
| --- | --- |
| `/ready` times out | `docker compose ps`; inspect `docker compose logs job-submitter jobmanager taskmanager api` and Flink UI `http://localhost:8081` |
| Custody retries `429`/`503` | Check API capacity, Redis identity storage, Kafka ACKs. The script reuses the same event ID/payload for retries |
| `401`/`403` | Match the API key and customer scope to the endpoint |
| Accepted but no projection | Check Flink job, consumer lag, and `WATERMARK_HEARTBEAT_ENABLED=true` on API for idle windows |
| Token/cost mismatch | Inspect printed last response, API/Flink catalog consistency, and custom pricing; `202` alone is not success |
| Quarantine receipt | Investigate timestamp skew; quarantined events are not normally billed |

For optional reserve → stream kill → settlement → ClickHouse audit assertions,
use a Python 3 environment with `httpx`:

```bash
python3 -m venv .venv-proof
.venv-proof/bin/python -m pip install httpx
make demo-proof PYTHON=.venv-proof/bin/python
```

It starts the audit overlay at local proof sizing (one API worker and two Flink
slots) and a local mock provider. It is a deeper control test, not a throughput benchmark. The main
terminal recording (`make demo-record`) uses the simple trial and has no manually
seeded Intelligence data. Intelligence examples elsewhere are synthetic analytics
fixtures and are not evidence of this metering path.
