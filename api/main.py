"""FluxMeter Query API — real-time usage and budget queries backed by Redis."""

from __future__ import annotations

import os
import time
from typing import Optional

import redis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(
    title="FluxMeter API",
    description="Real-time token usage and budget queries",
    version="0.6.1",
)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "token-events")

pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# --- Layer 1: In-process budget cache (always available, 0.01ms) ---
_budget_cache: dict[str, dict] = {}  # {customer_id: {"balance": float, "ts": float, "max_rpm": int}}
CACHE_TTL_SEC = 30  # Stale after 30 seconds


def cache_get(customer_id: str) -> Optional[dict]:
    """Get cached budget. Returns None if not cached or expired."""
    entry = _budget_cache.get(customer_id)
    if entry and (time.time() - entry["ts"]) < CACHE_TTL_SEC:
        return entry
    return None


def cache_set(customer_id: str, balance: float, max_rpm: int = 0):
    """Update cache with fresh Redis data."""
    _budget_cache[customer_id] = {"balance": balance, "max_rpm": max_rpm, "ts": time.time()}


# Kafka producer for HTTP ingest
_kafka_producer = None


def get_kafka_producer():
    global _kafka_producer
    if _kafka_producer is None:
        from confluent_kafka import Producer
        _kafka_producer = Producer({
            "bootstrap.servers": KAFKA_BROKERS,
            "linger.ms": 5,
            "compression.type": "lz4",
            "acks": "all",
        })
    return _kafka_producer


def get_redis() -> redis.Redis:
    return redis.Redis(connection_pool=pool)


# --- Response Models ---


class GlobalUsage(BaseModel):
    total_events: int
    total_tokens: int
    input_tokens: int
    output_tokens: int
    total_cost_usd: float
    last_window_end: Optional[int] = None


class CustomerUsage(BaseModel):
    customer_id: str
    total_tokens: int
    input_tokens: int
    output_tokens: int
    cache_read_tokens: int
    reasoning_tokens: int
    event_count: int
    cost_usd: float


class CustomerBudget(BaseModel):
    customer_id: str
    balance_usd: float
    total_spent_usd: float
    alert_threshold_usd: Optional[float] = None
    is_exhausted: bool


class BudgetSetRequest(BaseModel):
    balance_usd: float
    alert_threshold_usd: Optional[float] = None
    max_rpm: Optional[int] = None  # Max requests per minute (rate limit)


class ModelUsage(BaseModel):
    model_id: str
    total_tokens: int
    input_tokens: int
    output_tokens: int
    cost_usd: float


# --- Endpoints ---


@app.get("/health")
def health():
    r = get_redis()
    r.ping()
    return {"status": "ok"}


# --- Ingest Endpoints ---


class IngestEvent(BaseModel):
    customerId: str
    modelId: str
    provider: str = "openai"
    inputTokens: int = 0
    outputTokens: int = 0
    cacheReadTokens: int = 0
    cacheWriteTokens: int = 0
    reasoningTokens: int = 0
    embeddingTokens: int = 0
    eventId: Optional[str] = None
    requestId: Optional[str] = None
    spanId: Optional[str] = None
    parentSpanId: Optional[str] = None
    sessionId: Optional[str] = None
    latencyMs: int = 0
    environment: Optional[str] = None
    timestamp: Optional[int] = None


@app.post("/ingest", status_code=202)
def ingest_event(event: IngestEvent):
    """Ingest a single token usage event via HTTP.

    Alternative to the Python SDK or direct Kafka producer.
    Use this when you can't run a Kafka client (serverless, edge, simple integrations).

    The event is produced to Kafka and processed by Flink identically
    to events sent via the SDK.
    """
    import json, uuid

    producer = get_kafka_producer()
    event_dict = event.model_dump(exclude_none=True)
    if "eventId" not in event_dict:
        event_dict["eventId"] = str(uuid.uuid4())
    if "timestamp" not in event_dict:
        event_dict["timestamp"] = int(time.time() * 1000)

    value = json.dumps(event_dict).encode("utf-8")
    producer.produce(KAFKA_TOPIC, key=event.customerId.encode("utf-8"), value=value)
    producer.poll(0)

    return {"status": "accepted", "eventId": event_dict["eventId"]}


@app.post("/ingest/batch", status_code=202)
def ingest_batch(events: list[IngestEvent]):
    """Ingest multiple events in one HTTP call (max 1000 per batch).

    More efficient than calling /ingest repeatedly — single HTTP round-trip
    for up to 1000 events.
    """
    import json, uuid

    if len(events) > 1000:
        raise HTTPException(status_code=400, detail="Max 1000 events per batch")

    producer = get_kafka_producer()
    event_ids = []

    for event in events:
        event_dict = event.model_dump(exclude_none=True)
        if "eventId" not in event_dict:
            event_dict["eventId"] = str(uuid.uuid4())
        if "timestamp" not in event_dict:
            event_dict["timestamp"] = int(time.time() * 1000)

        value = json.dumps(event_dict).encode("utf-8")
        producer.produce(KAFKA_TOPIC, key=event.customerId.encode("utf-8"), value=value)
        event_ids.append(event_dict["eventId"])

    producer.flush(timeout=5)
    return {"status": "accepted", "count": len(events), "event_ids": event_ids}


@app.get("/usage/global", response_model=GlobalUsage)
def get_global_usage():
    """Global aggregated usage across all customers."""
    r = get_redis()
    return GlobalUsage(
        total_events=int(r.get("global:total_events") or 0),
        total_tokens=int(r.get("global:total_tokens") or 0),
        input_tokens=int(r.get("global:input_tokens") or 0),
        output_tokens=int(r.get("global:output_tokens") or 0),
        total_cost_usd=float(r.get("global:total_cost_usd") or 0),
        last_window_end=_int_or_none(r.get("global:last_window_end")),
    )


@app.get("/usage/customer/{customer_id}", response_model=CustomerUsage)
def get_customer_usage(customer_id: str):
    """Usage for a specific customer."""
    r = get_redis()
    key = f"customer:{customer_id}"
    total_tokens = r.get(f"{key}:total_tokens")
    if total_tokens is None:
        raise HTTPException(status_code=404, detail=f"Customer {customer_id} not found")
    return CustomerUsage(
        customer_id=customer_id,
        total_tokens=int(total_tokens),
        input_tokens=int(r.get(f"{key}:input_tokens") or 0),
        output_tokens=int(r.get(f"{key}:output_tokens") or 0),
        cache_read_tokens=int(r.get(f"{key}:cache_read_tokens") or 0),
        reasoning_tokens=int(r.get(f"{key}:reasoning_tokens") or 0),
        event_count=int(r.get(f"{key}:event_count") or 0),
        cost_usd=float(r.get(f"{key}:cost_usd") or 0),
    )


@app.get("/usage/customer/{customer_id}/model/{model_id}", response_model=ModelUsage)
def get_customer_model_usage(customer_id: str, model_id: str):
    """Per-model usage breakdown for a customer."""
    r = get_redis()
    key = f"customer:{customer_id}:model:{model_id}"
    total_tokens = r.get(f"{key}:total_tokens")
    if total_tokens is None:
        raise HTTPException(status_code=404, detail=f"No usage for {customer_id}/{model_id}")
    return ModelUsage(
        model_id=model_id,
        total_tokens=int(total_tokens),
        input_tokens=int(r.get(f"{key}:input_tokens") or 0),
        output_tokens=int(r.get(f"{key}:output_tokens") or 0),
        cost_usd=float(r.get(f"{key}:cost_usd") or 0),
    )


@app.get("/budget/{customer_id}", response_model=CustomerBudget)
def get_customer_budget(customer_id: str):
    """Get customer's current budget status."""
    r = get_redis()
    budget_key = f"budget:{customer_id}"
    balance = r.get(f"{budget_key}:balance_usd")
    if balance is None:
        raise HTTPException(status_code=404, detail=f"No budget set for {customer_id}")
    balance_val = float(balance)
    return CustomerBudget(
        customer_id=customer_id,
        balance_usd=balance_val,
        total_spent_usd=float(r.get(f"customer:{customer_id}:cost_usd") or 0),
        alert_threshold_usd=_float_or_none(r.get(f"{budget_key}:alert_threshold_usd")),
        is_exhausted=balance_val <= 0,
    )


@app.post("/budget/{customer_id}", response_model=CustomerBudget)
def set_customer_budget(customer_id: str, req: BudgetSetRequest):
    """Set or reset a customer's prepaid budget."""
    r = get_redis()
    budget_key = f"budget:{customer_id}"
    r.set(f"{budget_key}:balance_usd", str(req.balance_usd))
    r.set(f"{budget_key}:initial_balance_usd", str(req.balance_usd))  # For threshold calculation
    if req.alert_threshold_usd is not None:
        r.set(f"{budget_key}:alert_threshold_usd", str(req.alert_threshold_usd))
    if req.max_rpm is not None:
        r.set(f"{budget_key}:max_rpm", str(req.max_rpm))
    return get_customer_budget(customer_id)


@app.get("/budget/{customer_id}/check")
def check_budget(customer_id: str, estimated_cost_usd: float = 0.0):
    """Pre-request guardrail gate. Call BEFORE sending an LLM request.

    Three-layer resilience:
    - Layer 1: In-process cache (0.01ms, always available)
    - Layer 2: Redis GET (1-5ms, 99.9% available)
    - Layer 3: Flink pipeline (10-15s, exact — runs in background)

    If Redis is down, the cache provides a recent-enough answer for 30 seconds.
    After cache expires, behavior depends on fail_policy:
    - "open" (default): allow the request (revenue-preserving)
    - "closed": deny the request (cost-preserving)

    Returns:
        {"allowed": true/false, "balance_usd": ..., "reason": ..., "source": "redis|cache|policy"}
    """
    # --- Try Layer 2: Redis (authoritative, 1-5ms) ---
    try:
        r = get_redis()
        budget_key = f"budget:{customer_id}"

        # Rate limit check
        rate_limit_key = f"ratelimit:{customer_id}:{int(time.time()) // 60}"
        requests_this_minute = int(r.get(rate_limit_key) or 0)
        max_rpm = r.get(f"budget:{customer_id}:max_rpm")
        max_rpm_val = int(max_rpm) if max_rpm else 0

        if max_rpm_val > 0 and requests_this_minute >= max_rpm_val:
            return {
                "allowed": False, "balance_usd": None,
                "reason": "rate_limited",
                "requests_this_minute": requests_this_minute,
                "max_rpm": max_rpm_val, "source": "redis",
            }

        # Budget check
        balance = r.get(f"{budget_key}:balance_usd")

        if balance is None:
            pipe = r.pipeline()
            pipe.incr(rate_limit_key)
            pipe.expire(rate_limit_key, 120)
            pipe.execute()
            return {"allowed": True, "balance_usd": None, "reason": "no_budget_configured",
                    "requests_this_minute": requests_this_minute + 1, "source": "redis"}

        balance_val = float(balance)

        # Update cache with fresh data
        cache_set(customer_id, balance_val, max_rpm_val)

        if balance_val <= 0:
            return {"allowed": False, "balance_usd": balance_val, "reason": "budget_exhausted",
                    "requests_this_minute": requests_this_minute, "source": "redis"}

        if estimated_cost_usd > 0 and balance_val < estimated_cost_usd:
            return {"allowed": False, "balance_usd": balance_val, "reason": "insufficient_balance",
                    "requests_this_minute": requests_this_minute, "source": "redis"}

        # All passed — increment rate counter
        pipe = r.pipeline()
        pipe.incr(rate_limit_key)
        pipe.expire(rate_limit_key, 120)
        pipe.execute()

        return {"allowed": True, "balance_usd": balance_val, "reason": "ok",
                "requests_this_minute": requests_this_minute + 1, "source": "redis"}

    except Exception:
        # --- Layer 1 fallback: In-process cache (0.01ms) ---
        cached = cache_get(customer_id)
        if cached:
            balance_val = cached["balance"]
            if balance_val <= 0:
                return {"allowed": False, "balance_usd": balance_val,
                        "reason": "budget_exhausted", "source": "cache"}
            if estimated_cost_usd > 0 and balance_val < estimated_cost_usd:
                return {"allowed": False, "balance_usd": balance_val,
                        "reason": "insufficient_balance", "source": "cache"}
            return {"allowed": True, "balance_usd": balance_val,
                    "reason": "ok", "source": "cache"}

        # --- Cache expired or never populated: apply fail policy ---
        # Default: fail-open (allow). Configurable per customer in Redis
        # (budget:{id}:fail_policy = "open" | "closed") — but Redis is down,
        # so we use the global default.
        fail_policy = os.getenv("BUDGET_FAIL_POLICY", "open")
        if fail_policy == "closed":
            return {"allowed": False, "balance_usd": None,
                    "reason": "redis_unavailable_fail_closed", "source": "policy"}
        return {"allowed": True, "balance_usd": None,
                "reason": "redis_unavailable_fail_open", "source": "policy"}


@app.post("/budget/{customer_id}/topup")
def topup_customer_budget(customer_id: str, amount_usd: float):
    """Add credits to a customer's balance."""
    if amount_usd <= 0:
        raise HTTPException(status_code=400, detail="amount_usd must be positive")
    r = get_redis()
    budget_key = f"budget:{customer_id}"
    balance = r.get(f"{budget_key}:balance_usd")
    if balance is None:
        raise HTTPException(status_code=404, detail=f"No budget set for {customer_id}")
    new_balance = r.incrbyfloat(f"{budget_key}:balance_usd", amount_usd)
    return {"customer_id": customer_id, "new_balance_usd": new_balance, "added_usd": amount_usd}


class SpanUsage(BaseModel):
    span_id: str
    customer_id: Optional[str] = None
    total_tokens: int
    call_count: int
    cost_usd: float
    duration_ms: int


@app.get("/usage/span/{span_id}", response_model=SpanUsage)
def get_span_usage(span_id: str):
    """Get aggregated cost and usage for an agent span (group of related LLM calls)."""
    r = get_redis()
    key = f"span:{span_id}"
    cost = r.get(f"{key}:cost_usd")
    if cost is None:
        raise HTTPException(status_code=404, detail=f"Span {span_id} not found")
    return SpanUsage(
        span_id=span_id,
        customer_id=r.get(f"{key}:customer_id"),
        total_tokens=int(r.get(f"{key}:total_tokens") or 0),
        call_count=int(r.get(f"{key}:call_count") or 0),
        cost_usd=float(cost),
        duration_ms=int(r.get(f"{key}:duration_ms") or 0),
    )


@app.get("/usage/customer/{customer_id}/spans")
def get_customer_top_spans(customer_id: str, limit: int = 10):
    """Top N most expensive agent spans for a customer (sorted by cost)."""
    r = get_redis()
    spans = r.zrevrange(f"customer:{customer_id}:spans", 0, limit - 1, withscores=True)
    if not spans:
        return []
    return [{"span_id": span_id, "cost_usd": score} for span_id, score in spans]


class ReRateRequest(BaseModel):
    model_id: str
    old_input_price: float  # per million tokens
    new_input_price: float
    old_output_price: float
    new_output_price: float
    start_timestamp: Optional[int] = None  # epoch ms, optional filter
    end_timestamp: Optional[int] = None


@app.post("/rerate/preview")
def preview_rerate(req: ReRateRequest):
    """Preview retroactive re-rating adjustment without applying.

    Computes the cost delta for all customers who used the specified model,
    based on the difference between old and new pricing. Uses existing
    Redis counters (no event replay needed).
    """
    r = get_redis()
    # Find all customers with usage on this model
    pattern = f"customer:*:model:{req.model_id}:input_tokens"
    adjustments = []
    total_adjustment = 0.0

    for key in r.scan_iter(match=pattern, count=1000):
        # Extract customer_id from key pattern
        parts = key.split(":")
        customer_id = parts[1]
        model_key = f"customer:{customer_id}:model:{req.model_id}"

        input_tokens = int(r.get(f"{model_key}:input_tokens") or 0)
        output_tokens = int(r.get(f"{model_key}:output_tokens") or 0)

        input_delta = (input_tokens / 1_000_000) * (req.new_input_price - req.old_input_price)
        output_delta = (output_tokens / 1_000_000) * (req.new_output_price - req.old_output_price)
        adjustment = input_delta + output_delta

        if abs(adjustment) > 0.001:  # Skip negligible adjustments
            adjustments.append({
                "customer_id": customer_id,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "adjustment_usd": round(adjustment, 6),
            })
            total_adjustment += adjustment

    return {
        "model_id": req.model_id,
        "customers_affected": len(adjustments),
        "total_adjustment_usd": round(total_adjustment, 4),
        "adjustments": sorted(adjustments, key=lambda x: x["adjustment_usd"])[:50],
    }


@app.post("/rerate/apply", status_code=202)
def apply_rerate(req: ReRateRequest):
    """Apply retroactive re-rating: adjust cost_usd for all affected customers.
    NOTE: For large customer bases (>10K), this runs synchronously but should
    be behind a task queue in production. Returns 202 to signal async semantics.

    Atomically updates each customer's cost_usd and the global total.
    If the customer has a budget, adjusts the balance accordingly
    (price decrease = credit back to balance).
    """
    r = get_redis()
    pattern = f"customer:*:model:{req.model_id}:input_tokens"
    applied = 0
    total_adjustment = 0.0

    for key in r.scan_iter(match=pattern, count=1000):
        parts = key.split(":")
        customer_id = parts[1]
        model_key = f"customer:{customer_id}:model:{req.model_id}"

        input_tokens = int(r.get(f"{model_key}:input_tokens") or 0)
        output_tokens = int(r.get(f"{model_key}:output_tokens") or 0)

        input_delta = (input_tokens / 1_000_000) * (req.new_input_price - req.old_input_price)
        output_delta = (output_tokens / 1_000_000) * (req.new_output_price - req.old_output_price)
        adjustment = input_delta + output_delta

        if abs(adjustment) < 0.001:
            continue

        pipe = r.pipeline()
        # Adjust per-model cost
        pipe.incrbyfloat(f"{model_key}:cost_usd", adjustment)
        # Adjust per-customer cost
        pipe.incrbyfloat(f"customer:{customer_id}:cost_usd", adjustment)
        # Adjust global cost
        pipe.incrbyfloat("global:total_cost_usd", adjustment)
        # If customer has budget, credit back (price decrease = positive balance adjustment)
        budget_key = f"budget:{customer_id}:balance_usd"
        if r.exists(budget_key):
            # Price decrease (negative adjustment) → credit back (add to balance)
            pipe.incrbyfloat(budget_key, -adjustment)
        pipe.execute()

        applied += 1
        total_adjustment += adjustment

    return {
        "model_id": req.model_id,
        "customers_adjusted": applied,
        "total_adjustment_usd": round(total_adjustment, 4),
        "status": "applied",
    }


@app.post("/budget/{customer_id}/reserve")
def reserve_budget(customer_id: str, estimated_cost_usd: float):
    """Pessimistic pre-deduction for streaming responses.

    Deducts estimated_cost from balance BEFORE the LLM call starts.
    After the call completes, call /budget/{id}/reconcile with actual cost
    to credit back the difference.

    Use when: streaming responses where you can't know final cost upfront.
    Flow: reserve(estimate) → LLM call → track(actual) → reconcile(estimate, actual)
    """
    if estimated_cost_usd <= 0:
        raise HTTPException(status_code=400, detail="estimated_cost_usd must be positive")
    r = get_redis()
    budget_key = f"budget:{customer_id}:balance_usd"
    balance = r.get(budget_key)
    if balance is None:
        raise HTTPException(status_code=404, detail=f"No budget set for {customer_id}")

    balance_val = float(balance)
    if balance_val < estimated_cost_usd:
        return {"allowed": False, "balance_usd": balance_val, "reason": "insufficient_balance"}

    # Pessimistic deduction
    new_balance = r.incrbyfloat(budget_key, -estimated_cost_usd)
    return {
        "allowed": True,
        "balance_usd": new_balance,
        "reserved_usd": estimated_cost_usd,
        "reason": "reserved",
    }


@app.post("/budget/{customer_id}/reconcile")
def reconcile_budget(customer_id: str, reserved_usd: float, actual_usd: float):
    """Reconcile a pre-deduction after streaming response completes.

    Credits back the difference between reserved and actual cost.
    If actual > reserved (underestimated), deducts the extra.
    """
    r = get_redis()
    budget_key = f"budget:{customer_id}:balance_usd"
    balance = r.get(budget_key)
    if balance is None:
        raise HTTPException(status_code=404, detail=f"No budget set for {customer_id}")

    # Credit back: reserved - actual (positive if overestimated)
    credit_back = reserved_usd - actual_usd
    new_balance = r.incrbyfloat(budget_key, credit_back)
    return {
        "customer_id": customer_id,
        "balance_usd": new_balance,
        "reserved_usd": reserved_usd,
        "actual_usd": actual_usd,
        "credit_back_usd": credit_back,
    }


def _int_or_none(val) -> Optional[int]:
    return int(val) if val else None


def _float_or_none(val) -> Optional[float]:
    return float(val) if val else None
