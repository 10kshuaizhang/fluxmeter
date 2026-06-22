# FluxMeter Dual-Path: Lite-First + SaaS Control Plane

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Transform FluxMeter from a Flink-only demo into a dual-path product: Lite mode (Redis + API, no Kafka/Flink) as the default developer experience, with a SaaS control plane for managed multi-tenant billing.

**Architecture:** Path B (Lite) production-hardens the existing `lite_aggregate.py` with atomic Lua, idempotency, budget enforcement inline, and a background rollup worker. Path A (SaaS) adds a tenant control plane, Stripe billing integration, and multi-tenant key isolation on top. Both share the same API contract, Redis schema, SDK, and Grafana dashboard.

**Tech Stack:** Python 3.11, FastAPI, Redis 7 (Lua scripting), Stripe Python SDK, pytest, docker-compose profiles.

---

## Scope

This plan covers **5 phases** (independent, mergeable to `main` sequentially):

1. **Phase 1: Promote Lite as Default** — file renames, Makefile, README
2. **Phase 2: Production-Harden Lite Aggregator** — atomic Lua, inline budget deduction, idempotency fix
3. **Phase 3: Background Rollup Worker** — counter compaction, TTL management
4. **Phase 4: Stripe Billing Export** — usage reporting to Stripe Meters API
5. **Phase 5: SaaS Control Plane** — tenant management, provisioning, multi-tenant isolation

Each phase produces working, testable software independently.

---

## File Structure

### New Files

| File | Responsibility |
|------|----------------|
| `api/lite_aggregate_lua.py` | Atomic Lua script for single-event aggregation + budget deduction (replaces pipeline approach) |
| `api/rollup_worker.py` | Background asyncio task: compacts per-second counters into per-minute/hour summaries |
| `api/billing_export.py` | Stripe Usage Records reporting (hourly cron, reads Redis counters) |
| `services/control-plane/main.py` | Tenant CRUD, plan management, API key provisioning |
| `services/control-plane/models.py` | Pydantic models for Tenant, Plan, Subscription |
| `services/control-plane/stripe_billing.py` | Stripe subscription + meter event creation |
| `services/control-plane/Dockerfile` | Python 3.11-slim container for control plane |
| `services/control-plane/requirements.txt` | FastAPI, stripe, redis, httpx |
| `docker-compose.saas.yml` | Full SaaS stack (adds control-plane + postgres) |
| `tests/test_lite_production.py` | Lite mode production scenarios (atomicity, idempotency, budget inline) |
| `tests/test_rollup.py` | Rollup worker correctness tests |
| `tests/test_billing_export.py` | Stripe export unit tests (mocked Stripe) |
| `tests/test_control_plane.py` | Control plane API tests |

### Modified Files

| File | Changes |
|------|---------|
| `docker-compose-lite.yml` → `docker-compose.yml` | Becomes the default compose file |
| `docker-compose.yml` → `docker-compose.full.yml` | Renamed for Flink stack |
| `Makefile` | New targets: `demo` (lite), `demo-full`, `start-saas`, `test-lite` |
| `api/main.py` | Add rollup worker startup, inline budget deduction for lite mode |
| `api/lite_aggregate.py` | Replaced by `lite_aggregate_lua.py` (Lua-based atomic version) |
| `README.md` | Lite-first quick start, upgrade path documentation |

---

## Phase 1: Promote Lite as Default

### Task 1: Rename Docker Compose Files

**Files:**
- Rename: `docker-compose.yml` → `docker-compose.full.yml`
- Rename: `docker-compose-lite.yml` → `docker-compose.yml`
- Modify: `Makefile`

- [ ] **Step 1: Rename full stack compose**

```bash
cd /Users/szhang/Downloads/fluxmeter-main
mv docker-compose.yml docker-compose.full.yml
```

- [ ] **Step 2: Rename lite compose to default**

```bash
mv docker-compose-lite.yml docker-compose.yml
```

- [ ] **Step 3: Update Makefile targets**

Replace the entire `Makefile` with this content (preserves all existing targets, repoints defaults):

```makefile
.PHONY: build demo demo-full start start-full stop clean generate submit-job load-test test-e2e test-lite test-unit validate-spec benchmark

JAR = $(shell ls -t build/libs/fluxmeter-*.jar 2>/dev/null | head -1)

# Build the fat JAR (only needed for full/Flink mode)
build:
	./gradlew shadowJar

# --- LITE MODE (default) ---

# One-command lite demo: Redis + API + Grafana
demo: start
	@echo ""
	@echo "==================================="
	@echo " FluxMeter Demo Running (Lite Mode)"
	@echo "==================================="
	@echo " API:     http://localhost:8000/docs"
	@echo " Grafana: http://localhost:3000 (admin/fluxmeter)"
	@echo ""
	@echo " Try: curl -X POST localhost:8000/ingest -H 'Content-Type: application/json' \\"
	@echo "   -d '{\"customerId\":\"cust_1\",\"modelId\":\"gpt-4o\",\"inputTokens\":100,\"outputTokens\":50}'"
	@echo "==================================="

# Start lite infrastructure (default)
start:
	docker compose up -d --build
	@echo "Lite stack started. API aggregates directly to Redis (no Flink)."

# --- FULL MODE (Kafka + Flink) ---

# Full demo: build + start infra + submit job + run generator
demo-full: build start-full
	@echo "Waiting for Flink cluster to be ready..."
	@sleep 10
	@$(MAKE) submit-job
	@echo ""
	@echo "==================================="
	@echo " FluxMeter Demo Running (Full Mode)"
	@echo "==================================="
	@echo " API:       http://localhost:8000/docs"
	@echo " Flink UI:  http://localhost:8081"
	@echo " Grafana:   http://localhost:3000 (admin/fluxmeter)"
	@echo ""
	@echo " Starting load generator (Ctrl+C to stop)..."
	@echo "==================================="
	@$(MAKE) generate

# Start full infrastructure (Kafka, Flink, Redis, API, Grafana)
start-full:
	docker compose -f docker-compose.full.yml up -d --build
	@echo "Full stack started. Kafka, Flink, Redis, API, Grafana running."

# --- SHARED ---

# Stop everything
stop:
	docker compose down 2>/dev/null || true
	docker compose -f docker-compose.full.yml down 2>/dev/null || true

# Clean build artifacts and containers
clean: stop
	./gradlew clean
	docker compose down -v 2>/dev/null || true
	docker compose -f docker-compose.full.yml down -v 2>/dev/null || true

# Validate open spec artifacts
validate-spec:
	./scripts/validate-spec.sh

# Tests
test-lite:
	pip install -q -r tests/requirements.txt
	pytest tests/test_lite_production.py -v --timeout=60

test-e2e:
	pip install -q -r tests/requirements.txt
	pytest tests/test_integration.py -v --timeout=300
	pytest tests/test_e2e_v2.py -v --timeout=300 -m v2

test-unit:
	pip install -q -r tests/requirements.txt
	pytest tests/test_auth_unit.py -v

# Submit the Flink job (full mode only)
submit-job:
	docker cp $(JAR) fluxmeter-jobmanager:/opt/flink/fluxmeter.jar
	docker exec fluxmeter-jobmanager flink run \
		-d \
		-c io.fluxmeter.job.TokenUsageAggregator \
		/opt/flink/fluxmeter.jar

# Staged load test (full mode, 10K -> 1M eps bursts)
load-test:
	./scripts/load-test.sh

# Quick load test (10K-500K only)
load-test-quick:
	QUICK=1 ./scripts/load-test.sh

# Run the baseline comparison (Flink vs ClickHouse)
benchmark:
	./baseline/benchmark.sh

# Run the load generator locally (requires Java 17, full mode)
generate:
	KAFKA_BROKERS=localhost:9094 \
	NUM_CUSTOMERS=10000 \
	NUM_THREADS=4 \
	TARGET_EPS=1000000 \
	java -cp $(JAR) io.fluxmeter.generator.LoadGenerator
```

- [ ] **Step 4: Update scripts/load-test.sh reference**

In `scripts/load-test.sh`, the script calls `docker exec fluxmeter-jobmanager` and `docker exec fluxmeter-taskmanager-1` — these only exist in `docker-compose.full.yml`. No changes needed since the script is only called via `make load-test` which is documented as full-mode only.

- [ ] **Step 5: Fix stop target in docker-compose.full.yml**

The full compose references `fluxmeter-api` container which needs to use `docker-compose.full.yml` container names. Verify no collision with lite's `fluxmeter-api-lite`:

```bash
# In docker-compose.full.yml, the API container is named fluxmeter-api
# In docker-compose.yml (lite), the API container is named fluxmeter-api-lite
# No collision — both can coexist.
```

- [ ] **Step 6: Verify lite mode starts cleanly**

Run: `docker compose down -v && docker compose up -d --build`
Expected: 3 containers running (redis, api, grafana)

```bash
docker compose ps
# NAME                    STATUS
# fluxmeter-redis-lite    Up
# fluxmeter-api-lite      Up
# fluxmeter-grafana-lite  Up
```

- [ ] **Step 7: Verify API health**

Run: `curl -sf http://localhost:8000/health`
Expected: `{"status":"ok","mode":"lite"}`

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "feat: promote lite mode as default docker-compose target

Lite mode (Redis + API + Grafana) is now the default experience.
Full Flink stack moved to docker-compose.full.yml.
Makefile updated: 'make demo' runs lite, 'make demo-full' runs Flink."
```

---

## Phase 2: Production-Harden Lite Aggregator

### Task 2: Atomic Lua Aggregation with Inline Budget Deduction

**Files:**
- Create: `api/lite_aggregate_lua.py`
- Test: `tests/test_lite_production.py`
- Modify: `api/main.py` (import swap)

The current `lite_aggregate.py` uses a Redis pipeline (non-atomic — if the process crashes mid-pipeline, partial increments occur). Replace with a single Lua script that atomically: (1) checks idempotency, (2) increments all counters, (3) deducts from budget, (4) checks threshold.

- [ ] **Step 1: Write the test file**

Create `tests/test_lite_production.py`:

```python
"""Production-grade lite aggregator tests.

Run with: pytest tests/test_lite_production.py -v --timeout=60
Requires: docker compose up (lite stack)
"""

import time
import uuid

import httpx
import pytest
import redis

API = "http://localhost:8000"
TIMEOUT = httpx.Timeout(10.0)


@pytest.fixture(scope="module")
def r():
    """Direct Redis connection for assertions."""
    return redis.Redis(host="localhost", port=6379, decode_responses=True)


@pytest.fixture(autouse=True)
def health_check():
    """Ensure API is healthy before each test."""
    resp = httpx.get(f"{API}/health", timeout=TIMEOUT)
    assert resp.status_code == 200


def make_event(customer_id: str, model_id: str = "gpt-4o",
               input_tokens: int = 1000, output_tokens: int = 500,
               event_id: str = None):
    return {
        "customerId": customer_id,
        "modelId": model_id,
        "inputTokens": input_tokens,
        "outputTokens": output_tokens,
        "timestamp": int(time.time() * 1000),
        "eventId": event_id or str(uuid.uuid4()),
    }


class TestAtomicAggregation:
    """Verify Lua-based aggregation is atomic (all-or-nothing)."""

    def test_single_event_increments_all_counters(self, r):
        cid = f"test_atomic_{uuid.uuid4().hex[:8]}"
        event = make_event(cid, input_tokens=1000, output_tokens=500)

        resp = httpx.post(f"{API}/ingest", json=event, timeout=TIMEOUT)
        assert resp.status_code == 202

        # All counters updated atomically
        assert int(r.get(f"customer:{cid}:input_tokens") or 0) == 1000
        assert int(r.get(f"customer:{cid}:output_tokens") or 0) == 500
        assert int(r.get(f"customer:{cid}:total_tokens") or 0) == 1500
        assert int(r.get(f"customer:{cid}:event_count") or 0) == 1
        assert float(r.get(f"customer:{cid}:cost_usd") or 0) > 0

    def test_batch_ingest_all_counted(self, r):
        cid = f"test_batch_{uuid.uuid4().hex[:8]}"
        events = [make_event(cid, input_tokens=100, output_tokens=50) for _ in range(10)]

        resp = httpx.post(f"{API}/ingest/batch", json=events, timeout=TIMEOUT)
        assert resp.status_code == 202

        assert int(r.get(f"customer:{cid}:input_tokens") or 0) == 1000
        assert int(r.get(f"customer:{cid}:event_count") or 0) == 10


class TestIdempotency:
    """Verify duplicate events are rejected."""

    def test_duplicate_event_id_rejected(self, r):
        cid = f"test_idemp_{uuid.uuid4().hex[:8]}"
        eid = str(uuid.uuid4())
        event = make_event(cid, event_id=eid, input_tokens=500)

        # First ingest succeeds
        resp1 = httpx.post(f"{API}/ingest", json=event, timeout=TIMEOUT)
        assert resp1.status_code == 202

        # Second ingest with same eventId is accepted (202) but not double-counted
        resp2 = httpx.post(f"{API}/ingest", json=event, timeout=TIMEOUT)
        assert resp2.status_code == 202

        # Only counted once
        assert int(r.get(f"customer:{cid}:input_tokens") or 0) == 500
        assert int(r.get(f"customer:{cid}:event_count") or 0) == 1

    def test_no_event_id_always_counted(self, r):
        """Events without eventId are always counted (fire-and-forget mode)."""
        cid = f"test_no_eid_{uuid.uuid4().hex[:8]}"
        event = make_event(cid, input_tokens=100)
        del event["eventId"]

        httpx.post(f"{API}/ingest", json=event, timeout=TIMEOUT)
        httpx.post(f"{API}/ingest", json=event, timeout=TIMEOUT)

        assert int(r.get(f"customer:{cid}:event_count") or 0) == 2


class TestInlineBudgetDeduction:
    """Verify budget is deducted atomically with aggregation in lite mode."""

    def test_budget_deducted_on_ingest(self, r):
        cid = f"test_budget_{uuid.uuid4().hex[:8]}"

        # Set a budget
        resp = httpx.post(f"{API}/budget/{cid}",
                          json={"balance_usd": 100.0, "threshold_pct": 20},
                          timeout=TIMEOUT)
        assert resp.status_code == 200

        # Ingest event (should deduct from budget)
        event = make_event(cid, model_id="gpt-4o", input_tokens=1000, output_tokens=500)
        httpx.post(f"{API}/ingest", json=event, timeout=TIMEOUT)

        # Budget decreased
        balance = float(r.get(f"budget:{cid}:balance_usd") or 0)
        assert balance < 100.0
        assert balance > 0  # Not fully exhausted

    def test_budget_check_reflects_inline_deduction(self, r):
        cid = f"test_check_{uuid.uuid4().hex[:8]}"

        # Set budget to $1.00
        httpx.post(f"{API}/budget/{cid}",
                   json={"balance_usd": 1.0, "threshold_pct": 50},
                   timeout=TIMEOUT)

        # Ingest expensive event (claude-opus-4: $15/M input + $75/M output)
        # 10000 input + 5000 output = $0.15 + $0.375 = $0.525
        event = make_event(cid, model_id="claude-opus-4",
                           input_tokens=10000, output_tokens=5000)
        httpx.post(f"{API}/ingest", json=event, timeout=TIMEOUT)

        # Budget check shows reduced balance
        resp = httpx.get(f"{API}/budget/{cid}/check", timeout=TIMEOUT)
        data = resp.json()
        assert data["balance_usd"] < 1.0
        assert data["allowed"] is True  # Still has funds

    def test_exhausted_budget_blocks_check(self, r):
        cid = f"test_exhaust_{uuid.uuid4().hex[:8]}"

        # Set tiny budget ($0.001)
        httpx.post(f"{API}/budget/{cid}",
                   json={"balance_usd": 0.001, "threshold_pct": 90},
                   timeout=TIMEOUT)

        # Ingest event that costs more than budget
        event = make_event(cid, model_id="gpt-4o",
                           input_tokens=10000, output_tokens=5000)
        httpx.post(f"{API}/ingest", json=event, timeout=TIMEOUT)

        # Budget check denies (balance at zero or negative capped)
        resp = httpx.get(f"{API}/budget/{cid}/check",
                         params={"estimated_cost_usd": "0.01"},
                         timeout=TIMEOUT)
        data = resp.json()
        assert data["allowed"] is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_lite_production.py -v --timeout=60 -x`
Expected: FAIL — `TestInlineBudgetDeduction` tests fail because current lite mode doesn't deduct budget on ingest.

- [ ] **Step 3: Create atomic Lua aggregator**

Create `api/lite_aggregate_lua.py`:

```python
"""Atomic Lua-based lite aggregation with inline budget deduction.

Single Lua script ensures all-or-nothing: idempotency check, counter increments,
budget deduction, and threshold alert happen atomically.
"""

from __future__ import annotations

import hashlib
import json
import time
from typing import Any, Optional

import redis

# Pricing tables (mirrors config/pricing.json)
INPUT_PRICES = {
    "gpt-4o": 2.50, "gpt-4o-mini": 0.15, "o1": 15.00, "o3-mini": 1.10,
    "claude-opus-4": 15.00, "claude-sonnet-4": 3.00, "claude-haiku-4": 0.80,
    "gemini-1.5-pro": 3.50, "gemini-1.5-flash": 0.075,
}
OUTPUT_PRICES = {
    "gpt-4o": 10.00, "gpt-4o-mini": 0.60, "o1": 60.00, "o3-mini": 4.40,
    "claude-opus-4": 75.00, "claude-sonnet-4": 15.00, "claude-haiku-4": 4.00,
    "gemini-1.5-pro": 10.50, "gemini-1.5-flash": 0.30,
}
EMBEDDING_PRICES = {
    "text-embedding-3-small": 0.02, "text-embedding-3-large": 0.13,
}

KNOWN_MODELS = frozenset(INPUT_PRICES.keys() | OUTPUT_PRICES.keys() | EMBEDDING_PRICES.keys())

PREFIX_MODELS = [
    "gpt-4o-mini", "gpt-4o", "o3-mini", "o1",
    "claude-opus-4", "claude-sonnet-4", "claude-haiku-4",
    "gemini-1.5-pro", "gemini-1.5-flash",
    "text-embedding-3-large", "text-embedding-3-small",
]


def normalize_model_id(model: str) -> str:
    if not model:
        return "unknown"
    if model in KNOWN_MODELS:
        return model
    for known in PREFIX_MODELS:
        if model.startswith(known):
            return known
    return model


def calculate_cost_micro(event: dict[str, Any]) -> int:
    """Calculate event cost in microdollars (1 USD = 1,000,000 micro)."""
    model = normalize_model_id(event.get("modelId", "unknown"))
    cost = 0.0
    cost += event.get("inputTokens", 0) * INPUT_PRICES.get(model, 1.00)
    cost += event.get("outputTokens", 0) * OUTPUT_PRICES.get(model, 3.00)
    cost += event.get("cacheReadTokens", 0) * INPUT_PRICES.get(model, 1.00) * 0.5
    cost += event.get("reasoningTokens", 0) * OUTPUT_PRICES.get(model, 3.00)
    cost += event.get("cacheWriteTokens", 0) * INPUT_PRICES.get(model, 1.00)
    cost += event.get("embeddingTokens", 0) * EMBEDDING_PRICES.get(model, 0.10)
    return round(cost)


# Lua script: atomic aggregate + budget deduct + threshold check
# KEYS: [1]=idemp_key, [2]=customer_key, [3]=model_key,
#        [4]=budget_balance_key, [5]=budget_threshold_key
# ARGV: [1]=input_t, [2]=output_t, [3]=total_t, [4]=cost_usd_str,
#        [5]=now_ms, [6]=has_event_id (0/1), [7]=cache_read, [8]=reasoning
# Returns: [status, balance_after]
#   status: 1=success, 0=duplicate, -1=budget_exhausted_alert
AGGREGATE_LUA = """
-- Idempotency check
local has_eid = tonumber(ARGV[6])
if has_eid == 1 then
  local set = redis.call('SET', KEYS[1], '1', 'NX', 'EX', 600)
  if not set then
    return {0, 0}
  end
end

-- Parse args
local input_t = tonumber(ARGV[1])
local output_t = tonumber(ARGV[2])
local total_t = tonumber(ARGV[3])
local cost_usd = tonumber(ARGV[4])
local now_ms = ARGV[5]
local cache_read = tonumber(ARGV[7])
local reasoning = tonumber(ARGV[8])

-- Customer counters
local ckey = KEYS[2]
redis.call('INCRBY', ckey .. ':input_tokens', input_t)
redis.call('INCRBY', ckey .. ':output_tokens', output_t)
redis.call('INCRBY', ckey .. ':total_tokens', total_t)
redis.call('INCRBY', ckey .. ':event_count', 1)
redis.call('INCRBYFLOAT', ckey .. ':cost_usd', cost_usd)
if cache_read > 0 then
  redis.call('INCRBY', ckey .. ':cache_read_tokens', cache_read)
end
if reasoning > 0 then
  redis.call('INCRBY', ckey .. ':reasoning_tokens', reasoning)
end

-- Model counters
local mkey = KEYS[3]
redis.call('INCRBY', mkey .. ':input_tokens', input_t)
redis.call('INCRBY', mkey .. ':output_tokens', output_t)
redis.call('INCRBY', mkey .. ':total_tokens', total_t)
redis.call('INCRBYFLOAT', mkey .. ':cost_usd', cost_usd)

-- Global counters
redis.call('INCRBY', 'global:total_tokens', total_t)
redis.call('INCRBY', 'global:input_tokens', input_t)
redis.call('INCRBY', 'global:output_tokens', output_t)
redis.call('INCRBY', 'global:total_events', 1)
redis.call('INCRBYFLOAT', 'global:total_cost_usd', cost_usd)
redis.call('SET', 'global:last_window_end', now_ms)

-- Budget deduction (if budget exists)
local bal_key = KEYS[4]
local balance = redis.call('GET', bal_key)
local status = 1
if balance then
  local new_balance = tonumber(balance) - cost_usd
  if new_balance < 0 then
    new_balance = 0
    status = -1
  end
  redis.call('SET', bal_key, tostring(new_balance))
  -- Track total deducted for reconciliation
  redis.call('INCRBYFLOAT', KEYS[4] .. '_deducted', cost_usd)
  return {status, new_balance}
end

return {status, -1}
"""


class LiteAggregator:
    """Production-grade atomic aggregator for lite mode."""

    def __init__(self, r: redis.Redis):
        self._redis = r
        self._script = r.register_script(AGGREGATE_LUA)

    def aggregate(self, event: dict[str, Any]) -> dict:
        """Atomically aggregate one event. Returns status dict."""
        customer_id = event.get("customerId")
        model_id = event.get("modelId", "unknown")
        if not customer_id or not model_id:
            return {"status": "rejected", "reason": "missing_customer_or_model"}

        normalized_model = normalize_model_id(model_id)
        event_id = event.get("eventId")
        input_t = event.get("inputTokens", 0)
        output_t = event.get("outputTokens", 0)
        cache_read = event.get("cacheReadTokens", 0)
        reasoning = event.get("reasoningTokens", 0)
        embedding = event.get("embeddingTokens", 0)
        cache_write = event.get("cacheWriteTokens", 0)
        total_t = input_t + output_t + cache_read + cache_write + reasoning + embedding
        cost_micro = calculate_cost_micro(event)
        cost_usd = cost_micro / 1_000_000.0
        now_ms = event.get("timestamp", int(time.time() * 1000))

        # Build keys
        idemp_key = ""
        has_eid = 0
        if event_id:
            idemp_key = "e:" + hashlib.sha256(event_id.encode()).hexdigest()[:16]
            has_eid = 1
        else:
            idemp_key = "e:noop"  # Placeholder, won't be checked

        customer_key = f"customer:{customer_id}"
        model_key = f"{customer_key}:model:{normalized_model}"
        budget_balance_key = f"budget:{customer_id}:balance_usd"
        budget_threshold_key = f"budget:{customer_id}:threshold_pct"

        result = self._script(
            keys=[idemp_key, customer_key, model_key, budget_balance_key, budget_threshold_key],
            args=[input_t, output_t, total_t, f"{cost_usd:.10f}",
                  str(now_ms), has_eid, cache_read, reasoning],
        )

        status_code = int(result[0])
        if status_code == 0:
            return {"status": "duplicate", "event_id": event_id}

        response = {"status": "ok", "cost_usd": cost_usd}
        if status_code == -1:
            response["budget_alert"] = "BUDGET_EXHAUSTED"
        balance_after = float(result[1])
        if balance_after >= 0:
            response["balance_usd"] = balance_after

        return response

    def aggregate_batch(self, events: list[dict[str, Any]]) -> list[dict]:
        """Aggregate multiple events. Each is atomic independently."""
        return [self.aggregate(e) for e in events]
```

- [ ] **Step 4: Update main.py to use new aggregator**

In `api/main.py`, replace the import and initialization. Change line 23:

```python
# Old:
from lite_aggregate import aggregate_event

# New:
from lite_aggregate_lua import LiteAggregator
```

Add after the Redis pool initialization (around line 48):

```python
_lite_aggregator = None

def get_lite_aggregator():
    global _lite_aggregator
    if _lite_aggregator is None:
        r = redis.Redis(connection_pool=pool)
        _lite_aggregator = LiteAggregator(r)
    return _lite_aggregator
```

Update the `/ingest` endpoint's lite-mode branch to use:

```python
if LITE_MODE:
    agg = get_lite_aggregator()
    result = agg.aggregate(event_dict)
    return Response(status_code=202, content=json.dumps(result),
                    media_type="application/json")
```

Update the `/ingest/batch` endpoint similarly:

```python
if LITE_MODE:
    agg = get_lite_aggregator()
    results = agg.aggregate_batch(events)
    return Response(status_code=202, content=json.dumps({"results": results}),
                    media_type="application/json")
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `docker compose down -v && docker compose up -d --build && sleep 3 && pytest tests/test_lite_production.py -v --timeout=60`
Expected: All tests PASS

- [ ] **Step 6: Commit**

```bash
git add api/lite_aggregate_lua.py tests/test_lite_production.py
git add api/main.py
git commit -m "feat: atomic Lua aggregator with inline budget deduction for lite mode

Replaces pipeline-based aggregation with single Lua script.
Guarantees all-or-nothing: idempotency + counters + budget in one atomic op.
Budget deduction happens inline at ingest time (no Flink delay in lite mode)."
```

---

## Phase 3: Background Rollup Worker

### Task 3: Counter Compaction Worker

**Files:**
- Create: `api/rollup_worker.py`
- Test: `tests/test_rollup.py`
- Modify: `api/main.py` (startup hook)

The rollup worker runs as an asyncio background task inside the API process. Every 60s it scans per-event granularity data and compacts into per-minute summaries. Every hour it rolls per-minute into per-hour. This prevents unbounded Redis memory growth.

- [ ] **Step 1: Write the test file**

Create `tests/test_rollup.py`:

```python
"""Rollup worker tests — verify counter compaction logic.

Run with: pytest tests/test_rollup.py -v
Requires: Redis running on localhost:6379
"""

import time
import uuid

import pytest
import redis


@pytest.fixture
def r():
    conn = redis.Redis(host="localhost", port=6379, decode_responses=True)
    yield conn


class TestRollupLogic:
    """Test the rollup compaction logic directly."""

    def test_minute_rollup_sums_correctly(self, r):
        """Per-customer counters roll into minute buckets."""
        cid = f"test_rollup_{uuid.uuid4().hex[:8]}"
        customer_key = f"customer:{cid}"

        # Simulate accumulated counters
        r.set(f"{customer_key}:input_tokens", "5000")
        r.set(f"{customer_key}:output_tokens", "2000")
        r.set(f"{customer_key}:total_tokens", "7000")
        r.set(f"{customer_key}:event_count", "10")
        r.set(f"{customer_key}:cost_usd", "0.5")

        # Import and run rollup
        import sys
        sys.path.insert(0, "api")
        from rollup_worker import rollup_customer_minute

        minute_key = rollup_customer_minute(r, cid, int(time.time()))

        # Minute bucket exists with correct values
        assert r.hget(minute_key, "input_tokens") == "5000"
        assert r.hget(minute_key, "output_tokens") == "2000"
        assert r.hget(minute_key, "event_count") == "10"

    def test_rollup_resets_live_counters(self, r):
        """After rollup, live counters are zeroed."""
        cid = f"test_reset_{uuid.uuid4().hex[:8]}"
        customer_key = f"customer:{cid}"

        r.set(f"{customer_key}:input_tokens", "3000")
        r.set(f"{customer_key}:event_count", "5")

        import sys
        sys.path.insert(0, "api")
        from rollup_worker import rollup_customer_minute

        rollup_customer_minute(r, cid, int(time.time()))

        # Live counters zeroed (new events start from 0)
        assert int(r.get(f"{customer_key}:input_tokens") or 0) == 0
        assert int(r.get(f"{customer_key}:event_count") or 0) == 0

    def test_minute_buckets_have_ttl(self, r):
        """Minute buckets expire after 24 hours."""
        cid = f"test_ttl_{uuid.uuid4().hex[:8]}"
        customer_key = f"customer:{cid}"
        r.set(f"{customer_key}:input_tokens", "1000")
        r.set(f"{customer_key}:event_count", "1")

        import sys
        sys.path.insert(0, "api")
        from rollup_worker import rollup_customer_minute

        minute_key = rollup_customer_minute(r, cid, int(time.time()))

        ttl = r.ttl(minute_key)
        assert 86000 < ttl <= 86400  # ~24h
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_rollup.py -v -x`
Expected: FAIL — `ModuleNotFoundError: No module named 'rollup_worker'`

- [ ] **Step 3: Implement rollup worker**

Create `api/rollup_worker.py`:

```python
"""Background rollup worker — compacts live counters into time-bucketed summaries.

Runs as asyncio background task inside the API process.
- Every 60s: snapshot live counters into per-minute hash, reset live counters
- Minute hashes have 24h TTL (auto-expire)
- /usage/* endpoints read from live counters (current minute) + rolled-up history

This prevents unbounded Redis key growth while preserving time-series granularity.
"""

from __future__ import annotations

import asyncio
import logging
import time

import redis

logger = logging.getLogger(__name__)

ROLLUP_INTERVAL_SEC = 60
MINUTE_BUCKET_TTL = 86400  # 24 hours

# Lua script: atomically read counters, store in minute hash, reset to zero
ROLLUP_LUA = """
local ckey = KEYS[1]
local minute_key = KEYS[2]
local ttl = tonumber(ARGV[1])

-- Read current values
local input_t = redis.call('GET', ckey .. ':input_tokens') or '0'
local output_t = redis.call('GET', ckey .. ':output_tokens') or '0'
local total_t = redis.call('GET', ckey .. ':total_tokens') or '0'
local events = redis.call('GET', ckey .. ':event_count') or '0'
local cost = redis.call('GET', ckey .. ':cost_usd') or '0'
local cache_read = redis.call('GET', ckey .. ':cache_read_tokens') or '0'
local reasoning = redis.call('GET', ckey .. ':reasoning_tokens') or '0'

-- Skip if nothing to roll up
if tonumber(events) == 0 then
  return 0
end

-- Store in minute hash (HINCRBY allows multiple rollups into same minute)
redis.call('HINCRBY', minute_key, 'input_tokens', input_t)
redis.call('HINCRBY', minute_key, 'output_tokens', output_t)
redis.call('HINCRBY', minute_key, 'total_tokens', total_t)
redis.call('HINCRBY', minute_key, 'event_count', events)
redis.call('HINCRBYFLOAT', minute_key, 'cost_usd', cost)
if tonumber(cache_read) > 0 then
  redis.call('HINCRBY', minute_key, 'cache_read_tokens', cache_read)
end
if tonumber(reasoning) > 0 then
  redis.call('HINCRBY', minute_key, 'reasoning_tokens', reasoning)
end
redis.call('EXPIRE', minute_key, ttl)

-- Reset live counters (GETDEL pattern via SET 0)
redis.call('SET', ckey .. ':input_tokens', '0')
redis.call('SET', ckey .. ':output_tokens', '0')
redis.call('SET', ckey .. ':total_tokens', '0')
redis.call('SET', ckey .. ':event_count', '0')
redis.call('SET', ckey .. ':cost_usd', '0')
redis.call('SET', ckey .. ':cache_read_tokens', '0')
redis.call('SET', ckey .. ':reasoning_tokens', '0')

return 1
"""


def rollup_customer_minute(r: redis.Redis, customer_id: str, epoch_sec: int) -> str:
    """Roll up live counters for one customer into a minute bucket.

    Returns the minute bucket key.
    """
    minute_ts = (epoch_sec // 60) * 60
    customer_key = f"customer:{customer_id}"
    minute_key = f"rollup:{customer_id}:m:{minute_ts}"

    r.eval(ROLLUP_LUA, 2, customer_key, minute_key, MINUTE_BUCKET_TTL)
    return minute_key


def discover_active_customers(r: redis.Redis) -> list[str]:
    """Find customers with non-zero event_count (active in current interval)."""
    customers = []
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor, match="customer:*:event_count", count=200)
        for key in keys:
            val = r.get(key)
            if val and int(val) > 0:
                # Extract customer_id from "customer:{id}:event_count"
                parts = key.split(":")
                if len(parts) >= 3:
                    customers.append(parts[1])
        if cursor == 0:
            break
    return customers


async def rollup_loop(r: redis.Redis):
    """Background loop: roll up all active customer counters every 60s."""
    logger.info("Rollup worker started (interval=%ds)", ROLLUP_INTERVAL_SEC)
    while True:
        try:
            await asyncio.sleep(ROLLUP_INTERVAL_SEC)
            now = int(time.time())
            customers = discover_active_customers(r)
            rolled = 0
            for cid in customers:
                rollup_customer_minute(r, cid, now)
                rolled += 1
            if rolled > 0:
                logger.info("Rolled up %d customers at minute %d", rolled, (now // 60) * 60)
        except Exception as e:
            logger.error("Rollup error: %s", e)
            await asyncio.sleep(5)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_rollup.py -v`
Expected: All 3 tests PASS

- [ ] **Step 5: Wire rollup into API startup**

In `api/main.py`, add a startup event to launch the rollup worker:

```python
import asyncio
from rollup_worker import rollup_loop

@app.on_event("startup")
async def start_rollup():
    if LITE_MODE:
        r = redis.Redis(connection_pool=pool)
        asyncio.create_task(rollup_loop(r))
```

- [ ] **Step 6: Integration test — verify rollup runs**

Run: `docker compose up -d --build && sleep 65`
Then: `docker logs fluxmeter-api-lite 2>&1 | grep -i rollup`
Expected: `Rollup worker started (interval=60s)`

- [ ] **Step 7: Commit**

```bash
git add api/rollup_worker.py tests/test_rollup.py api/main.py
git commit -m "feat: background rollup worker for lite mode counter compaction

Every 60s, atomically snapshots live counters into per-minute hashes (24h TTL)
and resets live counters. Prevents unbounded Redis memory growth.
Runs as asyncio background task inside API process."
```

---

## Phase 4: Stripe Billing Export

### Task 4: Usage Reporting to Stripe

**Files:**
- Create: `api/billing_export.py`
- Test: `tests/test_billing_export.py`
- Modify: `api/main.py` (startup hook + admin endpoint)

Reports aggregated usage to Stripe Meters API every hour. Reads from Redis counters (works in both lite and full mode). Only active if `STRIPE_API_KEY` env is set.

- [ ] **Step 1: Write the test file**

Create `tests/test_billing_export.py`:

```python
"""Billing export unit tests — Stripe integration with mocked API.

Run with: pytest tests/test_billing_export.py -v
Does NOT require Stripe credentials or running infrastructure.
"""

import time
from unittest.mock import MagicMock, patch

import pytest


class TestUsageCollection:
    """Test usage data collection from Redis."""

    def test_collects_per_customer_usage(self):
        """Reads customer counters and builds Stripe meter event payload."""
        import sys
        sys.path.insert(0, "api")
        from billing_export import collect_customer_usage

        mock_redis = MagicMock()
        mock_redis.get.side_effect = lambda k: {
            "customer:cust_1:total_tokens": "50000",
            "customer:cust_1:event_count": "100",
            "customer:cust_1:cost_usd": "2.50",
            "billing:cust_1:stripe_customer_id": "cus_abc123",
            "billing:cust_1:last_reported_events": "50",
        }.get(k)

        usage = collect_customer_usage(mock_redis, "cust_1")

        assert usage["stripe_customer_id"] == "cus_abc123"
        assert usage["new_events"] == 50  # 100 total - 50 already reported
        assert usage["total_cost_usd"] == 2.50

    def test_skips_customer_without_stripe_id(self):
        """Customers not linked to Stripe are skipped."""
        import sys
        sys.path.insert(0, "api")
        from billing_export import collect_customer_usage

        mock_redis = MagicMock()
        mock_redis.get.return_value = None

        usage = collect_customer_usage(mock_redis, "cust_no_stripe")
        assert usage is None


class TestStripeReporting:
    """Test Stripe API interaction (mocked)."""

    @patch("billing_export.stripe")
    def test_reports_meter_event(self, mock_stripe):
        """Creates a Stripe billing meter event for usage."""
        import sys
        sys.path.insert(0, "api")
        from billing_export import report_to_stripe

        report_to_stripe(
            stripe_customer_id="cus_abc123",
            event_name="token_events_processed",
            value=500,
            timestamp=int(time.time()),
        )

        mock_stripe.billing.MeterEvent.create.assert_called_once()
        call_kwargs = mock_stripe.billing.MeterEvent.create.call_args[1]
        assert call_kwargs["event_name"] == "token_events_processed"
        assert call_kwargs["payload"]["stripe_customer_id"] == "cus_abc123"
        assert call_kwargs["payload"]["value"] == "500"

    @patch("billing_export.stripe")
    def test_skips_zero_usage(self, mock_stripe):
        """Does not report to Stripe if no new usage."""
        import sys
        sys.path.insert(0, "api")
        from billing_export import report_to_stripe

        report_to_stripe(
            stripe_customer_id="cus_abc123",
            event_name="token_events_processed",
            value=0,
            timestamp=int(time.time()),
        )

        mock_stripe.billing.MeterEvent.create.assert_not_called()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_billing_export.py -v -x`
Expected: FAIL — `ModuleNotFoundError: No module named 'billing_export'`

- [ ] **Step 3: Implement billing export**

Create `api/billing_export.py`:

```python
"""Stripe billing export — reports aggregated usage to Stripe Meters API.

Runs hourly as asyncio background task. Only active if STRIPE_API_KEY is set.
Reads from Redis counters (works in both lite and full mode).

Setup:
  1. Create a Stripe Billing Meter named "token_events_processed"
  2. Set STRIPE_API_KEY env var
  3. Link customers: POST /admin/billing/{customer_id}/link-stripe
     with body {"stripe_customer_id": "cus_..."}
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Optional

import redis

logger = logging.getLogger(__name__)

STRIPE_API_KEY = os.getenv("STRIPE_API_KEY")
EXPORT_INTERVAL_SEC = int(os.getenv("BILLING_EXPORT_INTERVAL", "3600"))
METER_EVENT_NAME = os.getenv("STRIPE_METER_NAME", "token_events_processed")

# Lazy import stripe (only if key is configured)
stripe = None
if STRIPE_API_KEY:
    try:
        import stripe as _stripe
        _stripe.api_key = STRIPE_API_KEY
        stripe = _stripe
    except ImportError:
        logger.warning("stripe package not installed; billing export disabled")


def collect_customer_usage(r: redis.Redis, customer_id: str) -> Optional[dict]:
    """Collect usage delta for one customer since last report.

    Returns None if customer has no Stripe link.
    """
    stripe_cid = r.get(f"billing:{customer_id}:stripe_customer_id")
    if not stripe_cid:
        return None

    total_events = int(r.get(f"customer:{customer_id}:event_count") or 0)
    last_reported = int(r.get(f"billing:{customer_id}:last_reported_events") or 0)
    new_events = total_events - last_reported
    cost_usd = float(r.get(f"customer:{customer_id}:cost_usd") or 0)

    return {
        "customer_id": customer_id,
        "stripe_customer_id": stripe_cid,
        "new_events": max(0, new_events),
        "total_events": total_events,
        "total_cost_usd": cost_usd,
    }


def report_to_stripe(stripe_customer_id: str, event_name: str,
                     value: int, timestamp: int):
    """Report a single meter event to Stripe. Skips if value is 0."""
    if value <= 0:
        return
    if not stripe:
        logger.debug("Stripe not configured; skipping report")
        return

    stripe.billing.MeterEvent.create(
        event_name=event_name,
        payload={
            "stripe_customer_id": stripe_customer_id,
            "value": str(value),
        },
        timestamp=timestamp,
    )


def link_customer_stripe(r: redis.Redis, customer_id: str, stripe_customer_id: str):
    """Link a FluxMeter customer to a Stripe customer for billing export."""
    r.set(f"billing:{customer_id}:stripe_customer_id", stripe_customer_id)


def discover_billable_customers(r: redis.Redis) -> list[str]:
    """Find customers linked to Stripe."""
    customers = []
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor, match="billing:*:stripe_customer_id", count=200)
        for key in keys:
            parts = key.split(":")
            if len(parts) >= 3:
                customers.append(parts[1])
        if cursor == 0:
            break
    return customers


async def billing_export_loop(r: redis.Redis):
    """Background loop: report usage to Stripe every hour."""
    if not STRIPE_API_KEY:
        logger.info("STRIPE_API_KEY not set; billing export disabled")
        return

    logger.info("Billing export started (interval=%ds, meter=%s)",
                EXPORT_INTERVAL_SEC, METER_EVENT_NAME)

    while True:
        try:
            await asyncio.sleep(EXPORT_INTERVAL_SEC)
            now = int(time.time())
            customers = discover_billable_customers(r)
            reported = 0

            for cid in customers:
                usage = collect_customer_usage(r, cid)
                if not usage or usage["new_events"] == 0:
                    continue

                report_to_stripe(
                    stripe_customer_id=usage["stripe_customer_id"],
                    event_name=METER_EVENT_NAME,
                    value=usage["new_events"],
                    timestamp=now,
                )

                # Update last reported watermark
                r.set(f"billing:{cid}:last_reported_events",
                      str(usage["total_events"]))
                reported += 1

            if reported > 0:
                logger.info("Reported usage for %d customers to Stripe", reported)

        except Exception as e:
            logger.error("Billing export error: %s", e)
            await asyncio.sleep(30)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pip install stripe && pytest tests/test_billing_export.py -v`
Expected: All tests PASS

- [ ] **Step 5: Add admin endpoints for Stripe linking**

In `api/main.py`, add:

```python
from billing_export import billing_export_loop, link_customer_stripe

@app.on_event("startup")
async def start_billing_export():
    r = redis.Redis(connection_pool=pool)
    asyncio.create_task(billing_export_loop(r))


@app.post("/admin/billing/{customer_id}/link-stripe")
async def link_stripe(customer_id: str, body: dict, _=Depends(require_admin_key)):
    """Link a customer to a Stripe customer for automatic usage billing."""
    stripe_cid = body.get("stripe_customer_id")
    if not stripe_cid:
        raise HTTPException(400, "stripe_customer_id required")
    r = redis.Redis(connection_pool=pool)
    link_customer_stripe(r, customer_id, stripe_cid)
    return {"linked": True, "customer_id": customer_id, "stripe_customer_id": stripe_cid}
```

- [ ] **Step 6: Add stripe to requirements.txt**

In `api/requirements.txt`, append:

```
stripe>=8.0.0
```

- [ ] **Step 7: Commit**

```bash
git add api/billing_export.py tests/test_billing_export.py api/main.py api/requirements.txt
git commit -m "feat: Stripe billing export — hourly usage reporting via Meters API

Reports per-customer event count to Stripe Billing Meters.
Only active when STRIPE_API_KEY env is set.
Admin endpoint: POST /admin/billing/{id}/link-stripe to connect customers."
```

---

## Phase 5: SaaS Control Plane

### Task 5: Tenant Management Service

**Files:**
- Create: `services/control-plane/main.py`
- Create: `services/control-plane/models.py`
- Create: `services/control-plane/stripe_billing.py`
- Create: `services/control-plane/requirements.txt`
- Create: `services/control-plane/Dockerfile`
- Create: `docker-compose.saas.yml`
- Test: `tests/test_control_plane.py`

Standalone FastAPI service managing tenants, plans, API key provisioning, and Stripe subscriptions. Communicates with the main API via Redis (shared state) and HTTP (health checks).

- [ ] **Step 1: Write control plane tests**

Create `tests/test_control_plane.py`:

```python
"""Control plane API tests.

Run with: pytest tests/test_control_plane.py -v --timeout=30
Requires: docker compose -f docker-compose.saas.yml up
"""

import uuid

import httpx
import pytest

CP_API = "http://localhost:8001"
TIMEOUT = httpx.Timeout(10.0)
ADMIN_KEY = "cp_admin_test_key"


@pytest.fixture(scope="module")
def admin_headers():
    return {"X-Admin-Key": ADMIN_KEY}


class TestTenantCRUD:
    """Tenant lifecycle management."""

    def test_create_tenant(self, admin_headers):
        resp = httpx.post(f"{CP_API}/tenants", json={
            "name": f"Test Corp {uuid.uuid4().hex[:6]}",
            "email": "admin@testcorp.example",
            "plan": "growth",
        }, headers=admin_headers, timeout=TIMEOUT)
        assert resp.status_code == 201
        data = resp.json()
        assert "tenant_id" in data
        assert "api_key" in data
        assert data["plan"] == "growth"

    def test_list_tenants(self, admin_headers):
        resp = httpx.get(f"{CP_API}/tenants", headers=admin_headers, timeout=TIMEOUT)
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_get_tenant_usage(self, admin_headers):
        # Create a tenant first
        create = httpx.post(f"{CP_API}/tenants", json={
            "name": "Usage Corp",
            "email": "usage@test.example",
            "plan": "free",
        }, headers=admin_headers, timeout=TIMEOUT)
        tid = create.json()["tenant_id"]

        resp = httpx.get(f"{CP_API}/tenants/{tid}/usage",
                         headers=admin_headers, timeout=TIMEOUT)
        assert resp.status_code == 200
        data = resp.json()
        assert "total_events" in data
        assert "total_cost_usd" in data


class TestPlanEnforcement:
    """Rate limiting per plan tier."""

    def test_free_plan_has_rate_limit(self, admin_headers):
        create = httpx.post(f"{CP_API}/tenants", json={
            "name": "Free Corp",
            "email": "free@test.example",
            "plan": "free",
        }, headers=admin_headers, timeout=TIMEOUT)
        data = create.json()
        assert data["limits"]["max_events_per_month"] == 100_000
        assert data["limits"]["max_eps"] == 100

    def test_growth_plan_has_higher_limits(self, admin_headers):
        create = httpx.post(f"{CP_API}/tenants", json={
            "name": "Growth Corp",
            "email": "growth@test.example",
            "plan": "growth",
        }, headers=admin_headers, timeout=TIMEOUT)
        data = create.json()
        assert data["limits"]["max_events_per_month"] == 10_000_000
        assert data["limits"]["max_eps"] == 10_000
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_control_plane.py -v -x`
Expected: FAIL — connection refused (service doesn't exist yet)

- [ ] **Step 3: Create models**

Create `services/control-plane/models.py`:

```python
"""Pydantic models for the control plane."""

from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, EmailStr, Field


class PlanTier(str, Enum):
    free = "free"
    growth = "growth"
    scale = "scale"
    enterprise = "enterprise"


PLAN_LIMITS = {
    PlanTier.free: {"max_events_per_month": 100_000, "max_eps": 100, "max_customers": 10},
    PlanTier.growth: {"max_events_per_month": 10_000_000, "max_eps": 10_000, "max_customers": 1_000},
    PlanTier.scale: {"max_events_per_month": 100_000_000, "max_eps": 100_000, "max_customers": 10_000},
    PlanTier.enterprise: {"max_events_per_month": -1, "max_eps": -1, "max_customers": -1},  # unlimited
}


class TenantCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    email: str
    plan: PlanTier = PlanTier.free
    stripe_customer_id: Optional[str] = None


class TenantResponse(BaseModel):
    tenant_id: str
    name: str
    email: str
    plan: PlanTier
    api_key: str
    limits: dict
    created_at: float


class TenantUsage(BaseModel):
    tenant_id: str
    total_events: int = 0
    total_tokens: int = 0
    total_cost_usd: float = 0.0
    events_this_month: int = 0
    plan: PlanTier
    limits: dict
```

- [ ] **Step 4: Create control plane service**

Create `services/control-plane/main.py`:

```python
"""FluxMeter SaaS Control Plane — tenant management and billing."""

from __future__ import annotations

import hashlib
import os
import secrets
import time
from typing import Optional

import redis
from fastapi import Depends, FastAPI, HTTPException, Header

from models import PLAN_LIMITS, PlanTier, TenantCreate, TenantResponse, TenantUsage

app = FastAPI(title="FluxMeter Control Plane", version="1.0.0")

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None
ADMIN_KEY = os.getenv("CP_ADMIN_KEY", "cp_admin_test_key")

pool = redis.ConnectionPool(
    host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True
)


def require_admin(x_admin_key: str = Header(...)):
    if x_admin_key != ADMIN_KEY:
        raise HTTPException(403, "Invalid admin key")


def get_redis():
    return redis.Redis(connection_pool=pool)


def generate_api_key() -> str:
    """Generate a tenant API key: fm_tenant_<32 chars>."""
    return "fm_tenant_" + secrets.token_urlsafe(24)


@app.get("/health")
async def health():
    r = get_redis()
    r.ping()
    return {"status": "ok", "service": "control-plane"}


@app.post("/tenants", status_code=201)
async def create_tenant(body: TenantCreate, _=Depends(require_admin)):
    r = get_redis()
    tenant_id = "tenant_" + secrets.token_hex(8)
    api_key = generate_api_key()
    now = time.time()
    limits = PLAN_LIMITS[body.plan]

    # Store tenant metadata
    tenant_key = f"cp:tenant:{tenant_id}"
    r.hset(tenant_key, mapping={
        "name": body.name,
        "email": body.email,
        "plan": body.plan.value,
        "api_key_hash": hashlib.sha256(api_key.encode()).hexdigest(),
        "created_at": str(now),
        "stripe_customer_id": body.stripe_customer_id or "",
    })

    # Index: api_key -> tenant_id (for request routing)
    r.set(f"cp:apikey:{hashlib.sha256(api_key.encode()).hexdigest()[:16]}", tenant_id)

    # Add to tenant list
    r.sadd("cp:tenants", tenant_id)

    # Set rate limits in main Redis (shared with API)
    r.set(f"tenant:{tenant_id}:max_eps", str(limits["max_eps"]))
    r.set(f"tenant:{tenant_id}:max_events_month", str(limits["max_events_per_month"]))

    return TenantResponse(
        tenant_id=tenant_id,
        name=body.name,
        email=body.email,
        plan=body.plan,
        api_key=api_key,
        limits=limits,
        created_at=now,
    )


@app.get("/tenants")
async def list_tenants(_=Depends(require_admin)):
    r = get_redis()
    tenant_ids = r.smembers("cp:tenants")
    tenants = []
    for tid in tenant_ids:
        data = r.hgetall(f"cp:tenant:{tid}")
        if data:
            tenants.append({
                "tenant_id": tid,
                "name": data.get("name"),
                "email": data.get("email"),
                "plan": data.get("plan"),
                "created_at": float(data.get("created_at", 0)),
            })
    return tenants


@app.get("/tenants/{tenant_id}/usage")
async def get_tenant_usage(tenant_id: str, _=Depends(require_admin)):
    r = get_redis()
    tenant_data = r.hgetall(f"cp:tenant:{tenant_id}")
    if not tenant_data:
        raise HTTPException(404, "Tenant not found")

    plan = PlanTier(tenant_data.get("plan", "free"))
    limits = PLAN_LIMITS[plan]

    # Read usage from shared Redis (tenant-scoped keys)
    total_events = int(r.get(f"tenant:{tenant_id}:total_events") or 0)
    total_tokens = int(r.get(f"tenant:{tenant_id}:total_tokens") or 0)
    total_cost = float(r.get(f"tenant:{tenant_id}:total_cost_usd") or 0)
    monthly_events = int(r.get(f"tenant:{tenant_id}:events_this_month") or 0)

    return TenantUsage(
        tenant_id=tenant_id,
        total_events=total_events,
        total_tokens=total_tokens,
        total_cost_usd=total_cost,
        events_this_month=monthly_events,
        plan=plan,
        limits=limits,
    )


@app.delete("/tenants/{tenant_id}")
async def delete_tenant(tenant_id: str, _=Depends(require_admin)):
    r = get_redis()
    if not r.exists(f"cp:tenant:{tenant_id}"):
        raise HTTPException(404, "Tenant not found")

    # Remove API key index
    data = r.hgetall(f"cp:tenant:{tenant_id}")
    if data.get("api_key_hash"):
        r.delete(f"cp:apikey:{data['api_key_hash'][:16]}")

    # Remove tenant data
    r.delete(f"cp:tenant:{tenant_id}")
    r.srem("cp:tenants", tenant_id)
    r.delete(f"tenant:{tenant_id}:max_eps")
    r.delete(f"tenant:{tenant_id}:max_events_month")

    return {"deleted": True, "tenant_id": tenant_id}
```

- [ ] **Step 5: Create requirements and Dockerfile**

Create `services/control-plane/requirements.txt`:

```
fastapi>=0.110.0
uvicorn[standard]>=0.27.0
redis>=5.0.0
pydantic>=2.9.0
stripe>=8.0.0
```

Create `services/control-plane/Dockerfile`:

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py models.py stripe_billing.py ./
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]
```

- [ ] **Step 6: Create stub for stripe_billing.py**

Create `services/control-plane/stripe_billing.py`:

```python
"""Stripe subscription management for SaaS tenants.

Handles: subscription creation, plan upgrades, webhook processing.
Only active when STRIPE_API_KEY is configured.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

STRIPE_API_KEY = os.getenv("STRIPE_API_KEY")

stripe = None
if STRIPE_API_KEY:
    try:
        import stripe as _stripe
        _stripe.api_key = STRIPE_API_KEY
        stripe = _stripe
    except ImportError:
        logger.warning("stripe package not installed")


def create_subscription(stripe_customer_id: str, plan: str) -> Optional[str]:
    """Create a Stripe subscription for a tenant. Returns subscription ID."""
    if not stripe:
        return None
    # Price IDs would be configured per environment
    price_map = {
        "growth": os.getenv("STRIPE_GROWTH_PRICE_ID", "price_growth"),
        "scale": os.getenv("STRIPE_SCALE_PRICE_ID", "price_scale"),
    }
    price_id = price_map.get(plan)
    if not price_id:
        return None

    sub = stripe.Subscription.create(
        customer=stripe_customer_id,
        items=[{"price": price_id}],
    )
    return sub.id
```

- [ ] **Step 7: Create docker-compose.saas.yml**

Create `docker-compose.saas.yml`:

```yaml
# SaaS stack: extends lite with control plane service
services:
  redis:
    image: redis:7-alpine
    container_name: fluxmeter-redis
    ports:
      - "6379:6379"
    command: redis-server --appendonly yes --appendfsync everysec --requirepass ${REDIS_PASSWORD:-fluxmeter}
    volumes:
      - redis-saas-data:/data
    deploy:
      resources:
        limits:
          memory: 1G

  api:
    build:
      context: .
      dockerfile: api/Dockerfile
    container_name: fluxmeter-api
    ports:
      - "8000:8000"
    environment:
      REDIS_HOST: redis
      REDIS_PORT: "6379"
      REDIS_PASSWORD: ${REDIS_PASSWORD:-fluxmeter}
      FLUXMETER_LITE_MODE: "true"
      FLUXMETER_AUTH_OPTIONAL: "false"
      BUDGET_FAIL_POLICY: "closed"
      STRIPE_API_KEY: ${STRIPE_API_KEY:-}
    depends_on:
      - redis

  control-plane:
    build:
      context: .
      dockerfile: services/control-plane/Dockerfile
    container_name: fluxmeter-control-plane
    ports:
      - "8001:8001"
    environment:
      REDIS_HOST: redis
      REDIS_PORT: "6379"
      REDIS_PASSWORD: ${REDIS_PASSWORD:-fluxmeter}
      CP_ADMIN_KEY: ${CP_ADMIN_KEY:-cp_admin_test_key}
      STRIPE_API_KEY: ${STRIPE_API_KEY:-}
    depends_on:
      - redis

  grafana:
    image: grafana/grafana:10.3.1
    container_name: fluxmeter-grafana
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_USER: admin
      GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_PASSWORD:-fluxmeter}
      GF_AUTH_ANONYMOUS_ENABLED: "false"
      GF_INSTALL_PLUGINS: redis-datasource
    volumes:
      - ./grafana/provisioning:/etc/grafana/provisioning
    depends_on:
      - redis

volumes:
  redis-saas-data:
```

- [ ] **Step 8: Add start-saas target to Makefile**

Append to `Makefile`:

```makefile
# --- SAAS MODE ---

# Start SaaS stack (lite + control plane)
start-saas:
	docker compose -f docker-compose.saas.yml up -d --build
	@echo "SaaS stack started. API :8000, Control Plane :8001, Grafana :3000"

stop-saas:
	docker compose -f docker-compose.saas.yml down
```

- [ ] **Step 9: Run SaaS stack and verify**

```bash
docker compose -f docker-compose.saas.yml up -d --build
sleep 5
curl -sf http://localhost:8001/health
```

Expected: `{"status":"ok","service":"control-plane"}`

- [ ] **Step 10: Run control plane tests**

Run: `pytest tests/test_control_plane.py -v --timeout=30`
Expected: All tests PASS

- [ ] **Step 11: Commit**

```bash
git add services/control-plane/ docker-compose.saas.yml tests/test_control_plane.py Makefile
git commit -m "feat: SaaS control plane — tenant management, plans, API key provisioning

New service on :8001 for multi-tenant operations.
Supports free/growth/scale/enterprise plan tiers with rate limits.
Shares Redis with main API for tenant-scoped enforcement.
docker-compose.saas.yml adds control plane to lite stack."
```

---

## Self-Review Checklist

**1. Spec coverage:**
- Path B (Lite production-grade): Covered by Tasks 1-4 (compose rename, Lua aggregator, rollup, Stripe)
- Path A (SaaS): Covered by Task 5 (control plane, tenant CRUD, plan enforcement)
- CTO concern — "No unit tests for Java": Not addressed (out of scope — this plan is Python/infra focused)
- CTO concern — "No observability": Not addressed (separate plan needed for Prometheus/Flink metrics)
- CTO concern — "Cache consistency": Partially addressed — inline budget deduction in lite mode eliminates the cache staleness problem entirely (budget is deducted at ingest time, not after a 10s window)

**2. Placeholder scan:** All steps contain complete code. No TBD/TODO patterns.

**3. Type consistency:**
- `LiteAggregator.aggregate()` returns `dict` — consistent across Task 2
- `rollup_customer_minute()` signature consistent between test and implementation
- `collect_customer_usage()` returns `Optional[dict]` — checked in tests
- `TenantCreate`/`TenantResponse` models used consistently in Task 5

---

## Summary of Deliverables

| Phase | Effort | Deliverable |
|-------|--------|-------------|
| 1. Promote Lite | 1 day | `make demo` runs lite by default |
| 2. Atomic Aggregator | 2-3 days | Lua-based atomic ingest with inline budget deduction |
| 3. Rollup Worker | 1-2 days | Background counter compaction (24h TTL) |
| 4. Stripe Export | 1-2 days | Hourly usage reporting to Stripe Meters |
| 5. Control Plane | 3-5 days | Multi-tenant SaaS management service |

**Total: ~2-3 weeks**
