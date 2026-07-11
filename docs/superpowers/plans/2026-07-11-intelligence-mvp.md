# Phase 5 Intelligence MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship Intelligence MVP (3.0.0): Root Cause Analysis, Unit Economics, Scenario Simulation, dual-source ingest (native FluxMeter + OpenMeter overlay) — while metering pillar stays maintained.

**Architecture:** New `api/intelligence/` Python package reads native Redis rollups (period buckets, dims, model-period keys), optional overlay events in Redis, and exposes prescriptive REST endpoints mounted on existing FastAPI. Pure analysis functions are testable without Redis; API layer uses `fakeredis` in unit tests.

**Tech Stack:** Python 3.11+, FastAPI, Redis, existing `pricing_loader` catalog, pytest + fakeredis

**Spec:** [docs/superpowers/specs/2026-07-11-intelligence-pivot-design.md](../specs/2026-07-11-intelligence-pivot-design.md)

---

## File map

| File | Responsibility |
|------|----------------|
| `api/intelligence/__init__.py` | Package marker |
| `api/intelligence/models.py` | Pydantic request/response models |
| `api/intelligence/revenue_store.py` | Redis revenue per customer/period |
| `api/intelligence/native_reader.py` | Scan rollups, dims, model-period costs |
| `api/intelligence/root_cause.py` | Period delta decomposition |
| `api/intelligence/unit_economics.py` | Margin + rule-based recommendations |
| `api/intelligence/simulation.py` | What-if scenarios (pure functions) |
| `api/intelligence/connectors/openmeter.py` | Overlay import + merge |
| `api/intelligence/routes.py` | FastAPI router |
| `api/main.py` | Mount router, bump version |
| `api/lite_aggregate_lua.py` | Increment model-period cost on ingest |
| `tests/test_intelligence_*.py` | Unit + API tests |
| `docs/intelligence-api.md` | User-facing API guide |
| `docs/landing-intelligence-copy.md` | Copy for fluxmeter.dev (external site) |

---

### Task 1: Model-period rollup on Lite ingest

**Files:**
- Modify: `api/lite_aggregate_lua.py` (after successful ingest, ~line 327)
- Modify: `api/usage_buckets.py` (add helper key builder)
- Test: `tests/test_intelligence_model_period.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_intelligence_model_period.py
import sys
sys.path.insert(0, "api")

import fakeredis
from usage_buckets import model_period_key, read_usage_bucket


def test_model_period_key_format():
    assert model_period_key("cust-a", "gpt-4o", "2026-07") == "rollup:cust-a:model:gpt-4o:period:2026-07"


def test_read_model_period_bucket(r=None):
    r = r or fakeredis.FakeRedis(decode_responses=True)
    key = model_period_key("cust-a", "gpt-4o", "2026-07")
    r.hset(key, mapping={"cost_usd": "12.5", "event_count": "3", "total_tokens": "100",
                         "input_tokens": "60", "output_tokens": "40"})
    data = read_usage_bucket(r, key)
    assert data["cost_usd"] == 12.5
    assert data["event_count"] == 3
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_intelligence_model_period.py -v`  
Expected: FAIL — `model_period_key` not defined

- [ ] **Step 3: Add helper + lite ingest increment**

```python
# api/usage_buckets.py
def model_period_key(customer_id: str, model_id: str, period: str) -> str:
    return f"rollup:{customer_id}:model:{model_id}:period:{period}"
```

```python
# api/lite_aggregate_lua.py — after cost_usd computed, before return
from usage_buckets import model_period_key
from pricing_loader import billing_period_month

period = billing_period_month(now_ms)
mp_key = model_period_key(customer_id, normalized_model, period)
pipe = self._redis.pipeline()
pipe.hincrbyfloat(mp_key, "cost_usd", cost_usd)
pipe.hincrby(mp_key, "event_count", 1)
pipe.hincrby(mp_key, "total_tokens", total_t)
pipe.hincrby(mp_key, "input_tokens", input_t)
pipe.hincrby(mp_key, "output_tokens", output_t)
pipe.expire(mp_key, DAY_BUCKET_TTL)
pipe.execute()
```

- [ ] **Step 4: Run test — PASS**

Run: `pytest tests/test_intelligence_model_period.py -v`

- [ ] **Step 5: Commit**

```bash
git add api/usage_buckets.py api/lite_aggregate_lua.py tests/test_intelligence_model_period.py
git commit -m "feat(intelligence): model-period rollup keys on lite ingest"
```

---

### Task 2: Intelligence models + revenue store

**Files:**
- Create: `api/intelligence/__init__.py`
- Create: `api/intelligence/models.py`
- Create: `api/intelligence/revenue_store.py`
- Test: `tests/test_intelligence_revenue_store.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_intelligence_revenue_store.py
import sys
sys.path.insert(0, "api")

import fakeredis
from intelligence.revenue_store import get_revenue, set_revenue


def test_set_and_get_revenue():
    r = fakeredis.FakeRedis(decode_responses=True)
    set_revenue(r, "cust-a", "2026-07", revenue_usd=500.0, source="manual")
    assert get_revenue(r, "cust-a", "2026-07") == {"revenue_usd": 500.0, "source": "manual"}
    assert get_revenue(r, "cust-a", "2026-06") is None
```

- [ ] **Step 2: Run — FAIL**

Run: `pytest tests/test_intelligence_revenue_store.py -v`

- [ ] **Step 3: Implement**

```python
# api/intelligence/revenue_store.py
from __future__ import annotations
import json
import redis

def _key(customer_id: str, period: str) -> str:
    return f"intel:revenue:{customer_id}:{period}"

def set_revenue(r: redis.Redis, customer_id: str, period: str, *, revenue_usd: float, source: str = "manual") -> None:
    r.set(_key(customer_id, period), json.dumps({"revenue_usd": revenue_usd, "source": source}))

def get_revenue(r: redis.Redis, customer_id: str, period: str) -> dict | None:
    raw = r.get(_key(customer_id, period))
    return json.loads(raw) if raw else None
```

```python
# api/intelligence/models.py — core response shapes
from pydantic import BaseModel, Field

class Contributor(BaseModel):
    dimension: str
    key: str
    current_cost_usd: float
    baseline_cost_usd: float
    delta_usd: float
    delta_pct: float
    share_of_total_delta_pct: float

class RootCauseReport(BaseModel):
    period: str
    baseline_period: str
    total_cost_usd: float
    baseline_cost_usd: float
    delta_usd: float
    delta_pct: float
    summary: str
    contributors: list[Contributor]

class CustomerEconomics(BaseModel):
    customer_id: str
    period: str
    revenue_usd: float | None
    cost_usd: float
    margin_usd: float | None
    margin_pct: float | None
    status: str  # profitable | loss | unknown_revenue
    recommendation: str | None

class SimulationResult(BaseModel):
    scenario: str
    annual_savings_usd: float | None
    annual_profit_delta_usd: float | None
    notes: str
```

- [ ] **Step 4: Run — PASS**

- [ ] **Step 5: Commit**

---

### Task 3: Native reader

**Files:**
- Create: `api/intelligence/native_reader.py`
- Test: `tests/test_intelligence_native_reader.py`

- [ ] **Step 1: Write failing test with seeded fakeredis**

```python
# tests/test_intelligence_native_reader.py
import sys
sys.path.insert(0, "api")

import fakeredis
from intelligence.native_reader import list_customer_period_costs, list_model_period_costs, list_dim_period_costs
from usage_buckets import rollup_month_key, model_period_key


def test_list_customer_period_costs():
    r = fakeredis.FakeRedis(decode_responses=True)
    r.hset(rollup_month_key("a", "2026-07"), mapping={"cost_usd": "10", "event_count": "1",
           "total_tokens": "100", "input_tokens": "50", "output_tokens": "50"})
    r.hset(rollup_month_key("b", "2026-07"), mapping={"cost_usd": "20", "event_count": "1",
           "total_tokens": "100", "input_tokens": "50", "output_tokens": "50"})
    costs = list_customer_period_costs(r, "2026-07")
    assert costs == {"a": 10.0, "b": 20.0}


def test_list_model_period_costs():
    r = fakeredis.FakeRedis(decode_responses=True)
    key = model_period_key("a", "gpt-4o", "2026-07")
    r.hset(key, mapping={"cost_usd": "7", "event_count": "1", "total_tokens": "10",
                         "input_tokens": "5", "output_tokens": "5"})
    models = list_model_period_costs(r, "2026-07", customer_id="a")
    assert models == {"gpt-4o": 7.0}
```

- [ ] **Step 2: Run — FAIL**

- [ ] **Step 3: Implement native_reader**

```python
# api/intelligence/native_reader.py
from __future__ import annotations
import redis
from usage_buckets import read_usage_bucket, rollup_month_key, model_period_key
from billing_dims import ALLOWED_DIMS, read_dim_usage
from pricing_loader import billing_period_month

def list_customer_period_costs(r: redis.Redis, period: str) -> dict[str, float]:
    out: dict[str, float] = {}
    cursor = 0
    pattern = f"rollup:*:period:{period}"
    while True:
        cursor, keys = r.scan(cursor, match=pattern, count=200)
        for key in keys:
            parts = key.split(":")
            if len(parts) == 4 and parts[0] == "rollup" and parts[2] == "period":
                cid = parts[1]
                data = read_usage_bucket(r, key)
                if data:
                    out[cid] = data["cost_usd"]
        if cursor == 0:
            break
    return out

def list_model_period_costs(r: redis.Redis, period: str, *, customer_id: str | None = None) -> dict[str, float]:
    out: dict[str, float] = {}
    pattern = f"rollup:{customer_id}:model:*:period:{period}" if customer_id else f"rollup:*:model:*:period:{period}"
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor, match=pattern, count=200)
        for key in keys:
            parts = key.split(":")
            # rollup:{cid}:model:{model}:period:{period}
            if len(parts) == 6 and parts[2] == "model" and parts[4] == "period":
                model_id = parts[3]
                data = read_usage_bucket(r, key)
                if data:
                    out[model_id] = out.get(model_id, 0.0) + data["cost_usd"]
        if cursor == 0:
            break
    return out

def list_dim_period_costs(r: redis.Redis, period: str) -> dict[str, dict[str, float]]:
    """{dim_key: {dim_value: cost}} for whitelisted dims."""
    result: dict[str, dict[str, float]] = {d: {} for d in ALLOWED_DIMS}
    for dim_key in ALLOWED_DIMS:
        cursor = 0
        while True:
            cursor, keys = r.scan(cursor, match=f"dim:{dim_key}:*:period:{period}:cost_usd", count=200)
            for key in keys:
                parts = key.split(":")
                dim_value = parts[2]
                cost = float(r.get(key) or 0)
                if cost > 0:
                    result[dim_key][dim_value] = cost
            if cursor == 0:
                break
    return result
```

- [ ] **Step 4: Run — PASS**

- [ ] **Step 5: Commit**

---

### Task 4: Root Cause Analysis

**Files:**
- Create: `api/intelligence/root_cause.py`
- Test: `tests/test_intelligence_root_cause.py`

- [ ] **Step 1: Write failing test**

```python
# tests/test_intelligence_root_cause.py
import sys
sys.path.insert(0, "api")

import fakeredis
from intelligence.root_cause import analyze_root_cause
from usage_buckets import rollup_month_key, model_period_key


def test_root_cause_model_dominates():
    r = fakeredis.FakeRedis(decode_responses=True)
    for period, cost in [("2026-06", "100"), ("2026-07", "140")]:
        r.hset(rollup_month_key("c1", period), mapping={
            "cost_usd": cost, "event_count": "1", "total_tokens": "10",
            "input_tokens": "5", "output_tokens": "5"})
    r.hset(model_period_key("c1", "gpt-4o", "2026-06"), mapping={
        "cost_usd": "60", "event_count": "1", "total_tokens": "10", "input_tokens": "5", "output_tokens": "5"})
    r.hset(model_period_key("c1", "gpt-4o", "2026-07"), mapping={
        "cost_usd": "100", "event_count": "1", "total_tokens": "10", "input_tokens": "5", "output_tokens": "5"})
    report = analyze_root_cause(r, period="2026-07", baseline_period="2026-06", scope="global")
    assert report.delta_usd == 40.0
    assert report.delta_pct == 40.0
    top = report.contributors[0]
    assert top.dimension == "model"
    assert "gpt-4o" in top.key
    assert "40%" in report.summary or "40.0%" in report.summary
```

- [ ] **Step 2: Run — FAIL**

- [ ] **Step 3: Implement analyze_root_cause**

Logic:
1. Sum customer period costs → total current / baseline
2. Build contributor list from: models (global), customers, dims (feature/room_id)
3. Sort by `abs(delta_usd)` desc, take top 10
4. `share_of_total_delta_pct = delta_usd / total_delta * 100` (guard div-by-zero)
5. `summary` template: `"Cost {delta_pct:+.1f}% vs {baseline_period}. Top driver: {dimension} {key} ({share:.0f}% of increase)."`

For `scope=customer:{id}` filter lists to that customer only.

- [ ] **Step 4: Run — PASS**

- [ ] **Step 5: Commit**

---

### Task 5: Unit Economics

**Files:**
- Create: `api/intelligence/unit_economics.py`
- Test: `tests/test_intelligence_unit_economics.py`

- [ ] **Step 1: Write failing test**

```python
def test_unit_economics_loss_recommendation():
    r = fakeredis.FakeRedis(decode_responses=True)
    r.hset(rollup_month_key("cust-a", "2026-07"), mapping={
        "cost_usd": "620", "event_count": "1", "total_tokens": "10", "input_tokens": "5", "output_tokens": "5"})
    set_revenue(r, "cust-a", "2026-07", revenue_usd=500.0)
    rows = compute_unit_economics(r, period="2026-07")
    row = next(x for x in rows if x.customer_id == "cust-a")
    assert row.status == "loss"
    assert row.margin_usd == -120.0
    assert row.recommendation is not None
    assert "upgrade" in row.recommendation.lower()
```

- [ ] **Step 2–4: Implement rule-based recommendations**

Rules (ponytail: simple heuristics, upgrade path = Phase 6 pricing optimizer):
- `margin_usd < 0` → `"Customer losing money — suggest plan upgrade or usage cap"`
- `margin_pct < 10` and revenue known → `"Low margin — review model mix or pricing"`
- no revenue → `status=unknown_revenue`, `recommendation="Connect revenue (OpenMeter overlay or POST /intelligence/revenue)"`

- [ ] **Step 5: Commit**

---

### Task 6: Scenario Simulation

**Files:**
- Create: `api/intelligence/simulation.py`
- Test: `tests/test_intelligence_simulation.py`

- [ ] **Step 1: Write failing tests for three scenario types**

```python
from intelligence.simulation import simulate_model_switch, simulate_prompt_reduction, simulate_token_grant
from intelligence.models import SimulationRequest  # or plain kwargs

def test_simulate_model_switch():
    # 1000 USD/mo on gpt-4o → same tokens on claude-sonnet — use pricing_loader rates
    result = simulate_model_switch(
        input_tokens=1_000_000, output_tokens=500_000,
        from_model="gpt-4o", to_model="claude-sonnet-4-6",
        monthly_occurrences=1,
    )
    assert result.scenario == "model_switch"
    assert result.annual_savings_usd is not None

def test_simulate_prompt_reduction():
    result = simulate_prompt_reduction(cost_usd=1000.0, input_reduction_pct=20.0)
    assert result.annual_savings_usd == 2400.0  # 200/mo * 12

def test_simulate_token_grant():
    result = simulate_token_grant(
        cost_usd=1000.0, grant_tokens=1_000_000,
        signup_lift_pct=30.0, avg_revenue_per_customer_usd=50.0, customer_count=100,
    )
    assert result.scenario == "token_grant"
    assert result.annual_profit_delta_usd is not None
```

- [ ] **Step 2–4: Implement using `pricing_loader.get_catalog()` for model_switch cost math**

- [ ] **Step 5: Commit**

---

### Task 7: OpenMeter overlay connector

**Files:**
- Create: `api/intelligence/connectors/__init__.py`
- Create: `api/intelligence/connectors/openmeter.py`
- Test: `tests/test_intelligence_openmeter.py`

- [ ] **Step 1: Write failing test**

Accept OpenMeter-style JSON batch (meter events with `customerId`, `value`, `subject`, `time`):

```python
SAMPLE = {
    "events": [
        {"customerId": "cust-a", "subject": "revenue", "value": 500, "time": "2026-07-15T00:00:00Z"},
        {"customerId": "cust-a", "subject": "tokens", "value": 1000000, "time": "2026-07-15T00:00:00Z"},
    ]
}

def test_import_openmeter_revenue():
    r = fakeredis.FakeRedis(decode_responses=True)
    from intelligence.connectors.openmeter import import_openmeter_events
    stats = import_openmeter_events(r, SAMPLE, period="2026-07")
    assert stats["revenue_rows"] == 1
    assert get_revenue(r, "cust-a", "2026-07")["revenue_usd"] == 500.0
```

- [ ] **Step 2–4: Implement import + store overlay metadata key `intel:overlay:openmeter:{period}:imported_at`**

- [ ] **Step 5: Commit**

---

### Task 8: REST API routes

**Files:**
- Create: `api/intelligence/routes.py`
- Modify: `api/main.py` (include router, version → 3.0.0)
- Test: `tests/test_intelligence_api.py`

- [ ] **Step 1: Write failing API test**

```python
# tests/test_intelligence_api.py — use TestClient with auth disabled or test keys from conftest
def test_root_cause_endpoint(client, seeded_redis):
    resp = client.get("/intelligence/root-cause", params={
        "period": "2026-07", "baseline_period": "2026-06", "scope": "global"
    })
    assert resp.status_code == 200
    body = resp.json()
    assert "summary" in body
    assert "contributors" in body
```

- [ ] **Step 2: Implement routes**

```python
# api/intelligence/routes.py
router = APIRouter(prefix="/intelligence", tags=["intelligence"])

@router.get("/root-cause", response_model=RootCauseReport)
@router.get("/unit-economics", response_model=list[CustomerEconomics])
@router.post("/simulate", response_model=SimulationResult)
@router.post("/revenue/{customer_id}")  # admin
@router.post("/import/openmeter")  # admin
@router.get("/summary")  # P1: prescriptive one-pager JSON for Finance
```

Mount in `main.py`:
```python
from intelligence.routes import router as intelligence_router
app.include_router(intelligence_router)
```

- [ ] **Step 3: Run `pytest tests/test_intelligence_api.py -v` — PASS**

- [ ] **Step 4: Commit**

---

### Task 9: OpenAPI + docs

**Files:**
- Modify: `spec/openapi/openapi.yaml` (intelligence paths)
- Create: `docs/intelligence-api.md`
- Create: `docs/landing-intelligence-copy.md`
- Modify: `docs/api-reference.md` (link)

- [ ] **Step 1: Document endpoints with curl examples**

- [ ] **Step 2: Landing copy from approved draft (Headline, Hero bullets, CTA)**

- [ ] **Step 3: Run `scripts/validate-spec.sh` if present**

- [ ] **Step 4: Commit**

---

### Task 10: Version bump + tracking

**Files:**
- Modify: `build.gradle` → `3.0.0`
- Modify: `changLog.md` → `[3.0.0]` section
- Modify: `progress.md` → mark Phase 5 items Done as shipped
- Modify: `README.md` → dual-pillar one-liner (minimal)

- [ ] **Step 1: Bump versions**

- [ ] **Step 2: changLog entry — narrative shift + Intelligence MVP features**

- [ ] **Step 3: Run full test suite**

Run: `make test-unit`  
Expected: all pass

- [ ] **Step 4: Commit**

---

## Spec coverage checklist

| Spec requirement | Task |
|------------------|------|
| Root Cause Analysis | Task 4, 8 |
| Unit Economics | Task 2, 5, 8 |
| Scenario Simulation (≥3 types) | Task 6, 8 |
| Dual-source: native | Task 1, 3 |
| Dual-source: OpenMeter overlay | Task 7, 8 |
| Prescriptive summary | Task 8 `/summary`, Task 9 |
| Landing alignment | Task 9 copy doc |
| Metering maintained | Task 1 extends lite ingest; no engine removal |

## Out of scope (defer Phase 6)

- Langfuse live API polling (document manual export → future connector)
- Pricing Optimizer ML
- NL agent queries
- Hosted SaaS deployment

## Verification gate (before claiming 3.0.0)

```bash
make test-unit
pytest tests/test_intelligence_*.py -v
curl -s "http://localhost:8000/intelligence/root-cause?period=2026-07&baseline_period=2026-06&scope=global" | jq .summary
```

Demo script (optional): `demos/intelligence_mvp_demo.py` — seed two months of data, print root cause + unit economics + one simulation.
