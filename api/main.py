"""FluxMeter Query API — real-time usage and budget queries backed by Redis."""

from __future__ import annotations

import asyncio
import json
import os
import re
import time
import uuid
from typing import Optional

import redis
from fastapi import BackgroundTasks, Depends, FastAPI, Header, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel

from auth import (
    create_customer_api_key,
    record_api_key_spend,
    require_admin_key,
    require_api_key,
    require_customer_access,
    resolve_customer_from_key,
    resolve_key_context,
    revoke_customer_api_key,
    set_api_key_budget,
)
from budget_gate import check_hierarchy_cap, run_budget_check
from budget_ops import get_effective_balance, reconcile_hold, reserve_hold
from lite_aggregate_lua import LiteAggregator
from webhook_deliver import deliver_lite_alerts
from billing_export import billing_export_loop, link_customer_platform, link_customer_stripe
from pricing_loader import billing_period_month, get_catalog, reload_catalog
from rollup_worker import rollup_loop
from intelligence.intel_alert_worker import intel_alert_loop
from usage_buckets import read_session, read_usage_bucket, rollup_day_key, rollup_month_key
from billing_dims import increment_dims, read_dim_usage, validate_metadata

app = FastAPI(
    title="FluxMeter API",
    description="Real-time token usage and budget queries",
    version="3.2.0",
)

from intelligence.routes import router as intelligence_router

app.include_router(intelligence_router)

LITE_MODE = os.getenv("FLUXMETER_LITE_MODE", "false").lower() == "true"
SPEC_OPENAPI_PATH = os.getenv(
    "FLUXMETER_OPENAPI_PATH",
    os.path.join(os.path.dirname(__file__), "..", "spec", "openapi", "openapi.yaml"),
)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "token-events")

pool = redis.ConnectionPool(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,
    decode_responses=True,
)

_lite_aggregator = None


def get_lite_aggregator():
    global _lite_aggregator
    if _lite_aggregator is None:
        r = redis.Redis(connection_pool=pool)
        _lite_aggregator = LiteAggregator(r)
    return _lite_aggregator


@app.on_event("startup")
async def start_background_tasks():
    r = redis.Redis(connection_pool=pool)
    reload_catalog(redis_client=r)
    if LITE_MODE:
        asyncio.create_task(rollup_loop(r))
    if os.getenv("STRIPE_API_KEY"):
        asyncio.create_task(billing_export_loop(r))
    if os.getenv("FLUXMETER_INTEL_WEBHOOK_URL") or r.get("intel:webhook:url"):
        asyncio.create_task(intel_alert_loop(r))


# --- Layer 1: In-process budget cache (always available, 0.01ms) ---
# Implemented in budget_gate.py (cache_get / cache_set)

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
    held_usd: float = 0.0
    effective_balance_usd: float = 0.0
    debt_usd: float = 0.0
    total_spent_usd: float
    alert_threshold_usd: Optional[float] = None
    is_exhausted: bool


class WebhookConfig(BaseModel):
    webhook_url: str
    webhook_secret: Optional[str] = None


class HierarchyCapConfig(BaseModel):
    """Hard spend cap for a span (agent run) or session."""
    kind: str  # "span" | "session"
    id: str
    max_cost_usd: float


_check_hierarchy_cap = check_hierarchy_cap  # backwards compat for tests importing main


class BudgetSetRequest(BaseModel):
    balance_usd: float
    alert_threshold_usd: Optional[float] = None
    max_rpm: Optional[int] = None  # Max requests per minute (rate limit)


class PackageSetRequest(BaseModel):
    tokens: int  # Prepaid token allowance (drawn down on lite ingest)


class ModelUsage(BaseModel):
    model_id: str
    total_tokens: int
    input_tokens: int
    output_tokens: int
    cost_usd: float


class BucketUsage(BaseModel):
    """Time-bucketed usage (calendar month or day)."""
    customer_id: str
    bucket: str
    total_tokens: int
    input_tokens: int
    output_tokens: int
    cache_read_tokens: int = 0
    reasoning_tokens: int = 0
    event_count: int
    cost_usd: float


class SessionUsage(BaseModel):
    session_id: str
    customer_id: Optional[str] = None
    total_tokens: int
    input_tokens: int
    output_tokens: int
    cache_read_tokens: int = 0
    reasoning_tokens: int = 0
    event_count: int
    cost_usd: float


# --- Endpoints ---


@app.get("/health")
def health():
    r = get_redis()
    r.ping()
    return {"status": "ok", "mode": "lite" if LITE_MODE else "full"}


@app.get("/openapi.yaml")
def get_openapi_spec():
    """Serve canonical OpenAPI spec from spec/openapi/openapi.yaml."""
    try:
        with open(SPEC_OPENAPI_PATH, encoding="utf-8") as f:
            return Response(content=f.read(), media_type="application/yaml")
    except OSError:
        raise HTTPException(status_code=404, detail="OpenAPI spec file not found")


# --- Ingest Endpoints ---


class IngestEvent(BaseModel):
    customerId: str
    modelId: str
    tenantId: Optional[str] = None
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
    metadata: Optional[dict[str, str]] = None


class ApiKeyBudgetRequest(BaseModel):
    daily_budget_usd: Optional[float] = None
    monthly_budget_usd: Optional[float] = None


@app.post("/ingest", status_code=202)
def ingest_event(
    event: IngestEvent,
    background_tasks: BackgroundTasks,
    _: None = Depends(require_api_key),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
):
    """Ingest a single token usage event via HTTP.

    Alternative to the Python SDK or direct Kafka producer.
    Use this when you can't run a Kafka client (serverless, edge, simple integrations).

    The event is produced to Kafka and processed by Flink identically
    to events sent via the SDK.
    """
    require_customer_access(event.customerId, x_api_key)

    try:
        validated_meta = validate_metadata(event.metadata)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    event_dict = event.model_dump(exclude_none=True)
    if validated_meta:
        event_dict["metadata"] = validated_meta
    if "timestamp" not in event_dict:
        event_dict["timestamp"] = int(time.time() * 1000)

    if LITE_MODE:
        agg = get_lite_aggregator()
        result = agg.aggregate(event_dict)
        if result.get("status") == "ok":
            r = get_redis()
            cost = float(result.get("cost_usd") or 0)
            if validated_meta and cost > 0:
                increment_dims(r, validated_meta, cost_usd=cost, event_ts_ms=event_dict["timestamp"])
            _, key_id = resolve_key_context(x_api_key)
            if key_id and cost > 0:
                record_api_key_spend(r, key_id, cost)
        # Lite: fire webhook without Kafka (BUDGET_LOW / BUDGET_EXHAUSTED)
        background_tasks.add_task(
            deliver_lite_alerts, get_redis(), event.customerId, result, event.modelId
        )
        return Response(status_code=202, content=json.dumps(result),
                        media_type="application/json")

    if "eventId" not in event_dict:
        event_dict["eventId"] = str(uuid.uuid4())

    producer = get_kafka_producer()
    value = json.dumps(event_dict).encode("utf-8")
    producer.produce(KAFKA_TOPIC, key=event.customerId.encode("utf-8"), value=value)
    producer.poll(0)

    return {"status": "accepted", "eventId": event_dict["eventId"]}


@app.post("/ingest/batch", status_code=202)
def ingest_batch(
    events: list[IngestEvent],
    background_tasks: BackgroundTasks,
    _: None = Depends(require_api_key),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
):
    """Ingest multiple events in one HTTP call (max 1000 per batch).

    More efficient than calling /ingest repeatedly — single HTTP round-trip
    for up to 1000 events.
    """
    if len(events) > 1000:
        raise HTTPException(status_code=400, detail="Max 1000 events per batch")

    for event in events:
        require_customer_access(event.customerId, x_api_key)

    event_ids = []

    if LITE_MODE:
        agg = get_lite_aggregator()
        event_dicts = []
        for event in events:
            try:
                validated_meta = validate_metadata(event.metadata)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            event_dict = event.model_dump(exclude_none=True)
            if validated_meta:
                event_dict["metadata"] = validated_meta
            if "timestamp" not in event_dict:
                event_dict["timestamp"] = int(time.time() * 1000)
            event_dicts.append(event_dict)
        results = agg.aggregate_batch(event_dicts)
        r = get_redis()
        _, key_id = resolve_key_context(x_api_key)
        for event_dict, result in zip(event_dicts, results):
            if result.get("status") == "ok":
                cost = float(result.get("cost_usd") or 0)
                meta = event_dict.get("metadata")
                if meta and cost > 0:
                    increment_dims(r, meta, cost_usd=cost, event_ts_ms=event_dict["timestamp"])
                if key_id and cost > 0:
                    record_api_key_spend(r, key_id, cost)
        for event, result in zip(events, results):
            background_tasks.add_task(
                deliver_lite_alerts, r, event.customerId, result, event.modelId
            )
        return Response(status_code=202, content=json.dumps({"results": results}),
                        media_type="application/json")

    producer = get_kafka_producer()
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


@app.get("/usage/global", response_model=GlobalUsage, dependencies=[Depends(require_api_key)])
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


@app.get("/usage/customer/{customer_id}", response_model=CustomerUsage, dependencies=[Depends(require_api_key)])
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


@app.get("/usage/customer/{customer_id}/model/{model_id}", response_model=ModelUsage, dependencies=[Depends(require_api_key)])
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


def _bucket_usage_response(customer_id: str, bucket: str, data: dict) -> BucketUsage:
    return BucketUsage(
        customer_id=customer_id,
        bucket=bucket,
        total_tokens=data["total_tokens"],
        input_tokens=data["input_tokens"],
        output_tokens=data["output_tokens"],
        cache_read_tokens=data.get("cache_read_tokens", 0),
        reasoning_tokens=data.get("reasoning_tokens", 0),
        event_count=data["event_count"],
        cost_usd=data["cost_usd"],
    )


@app.get(
    "/usage/customer/{customer_id}/period/{period}",
    response_model=BucketUsage,
    dependencies=[Depends(require_api_key)],
)
def get_customer_period_usage(customer_id: str, period: str):
    """Calendar-month usage for a customer (UTC). Lite: rollup worker; Full: Flink RedisSink."""
    if not re.fullmatch(r"\d{4}-\d{2}", period):
        raise HTTPException(status_code=400, detail="period must be YYYY-MM")
    r = get_redis()
    data = read_usage_bucket(r, rollup_month_key(customer_id, period))
    if data is None:
        raise HTTPException(status_code=404, detail=f"No usage for {customer_id} in {period}")
    return _bucket_usage_response(customer_id, period, data)


@app.get(
    "/usage/customer/{customer_id}/day/{date}",
    response_model=BucketUsage,
    dependencies=[Depends(require_api_key)],
)
def get_customer_day_usage(customer_id: str, date: str):
    """Daily usage for a customer (UTC)."""
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date):
        raise HTTPException(status_code=400, detail="date must be YYYY-MM-DD")
    r = get_redis()
    data = read_usage_bucket(r, rollup_day_key(customer_id, date))
    if data is None:
        raise HTTPException(status_code=404, detail=f"No usage for {customer_id} on {date}")
    return _bucket_usage_response(customer_id, date, data)


@app.get("/usage/session/{session_id}", response_model=SessionUsage)
def get_session_usage(
    session_id: str,
    _: None = Depends(require_api_key),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
):
    """Aggregated usage for a conversation/project session (lite ingest with sessionId)."""
    r = get_redis()
    data = read_session(r, session_id)
    if data is None:
        raise HTTPException(status_code=404, detail=f"Session {session_id} not found")
    customer_id = data.get("customer_id")
    if customer_id:
        require_customer_access(customer_id, x_api_key)
    return SessionUsage(
        session_id=session_id,
        customer_id=customer_id,
        total_tokens=data["total_tokens"],
        input_tokens=data["input_tokens"],
        output_tokens=data["output_tokens"],
        cache_read_tokens=data.get("cache_read_tokens", 0),
        reasoning_tokens=data.get("reasoning_tokens", 0),
        event_count=data["event_count"],
        cost_usd=data["cost_usd"],
    )


@app.get("/usage/dim/{dim_key}/{dim_value}", dependencies=[Depends(require_api_key)])
def get_dim_usage(
    dim_key: str,
    dim_value: str,
    period: Optional[str] = None,
):
    """Usage for a whitelisted metadata dimension (room_id, feature, etc.)."""
    if period and not re.fullmatch(r"\d{4}-\d{2}", period):
        raise HTTPException(status_code=400, detail="period must be YYYY-MM")
    r = get_redis()
    data = read_dim_usage(r, dim_key, dim_value, period=period)
    if data is None:
        raise HTTPException(
            status_code=404,
            detail=f"No usage for dim {dim_key}={dim_value}",
        )
    return data


def _fetch_customer_budget(customer_id: str) -> CustomerBudget:
    """Load budget from Redis. Callers must enforce auth before invoking."""
    r = get_redis()
    budget_key = f"budget:{customer_id}"
    balance = r.get(f"{budget_key}:balance_usd")
    if balance is None:
        raise HTTPException(status_code=404, detail=f"No budget set for {customer_id}")
    balance_val = float(balance)
    held_val = float(r.get(f"{budget_key}:held_usd") or 0)
    debt_val = float(r.get(f"{budget_key}:debt_usd") or 0)
    return CustomerBudget(
        customer_id=customer_id,
        balance_usd=balance_val,
        held_usd=held_val,
        effective_balance_usd=balance_val - held_val,
        debt_usd=debt_val,
        total_spent_usd=float(r.get(f"customer:{customer_id}:cost_usd") or 0),
        alert_threshold_usd=_float_or_none(r.get(f"{budget_key}:alert_threshold_usd")),
        is_exhausted=balance_val <= 0,
    )


@app.get("/budget/{customer_id}", response_model=CustomerBudget)
def get_customer_budget(
    customer_id: str,
    _: None = Depends(require_api_key),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
):
    """Get customer's current budget status."""
    require_customer_access(customer_id, x_api_key)
    return _fetch_customer_budget(customer_id)


@app.post("/budget/{customer_id}", response_model=CustomerBudget, dependencies=[Depends(require_admin_key)])
def set_customer_budget(customer_id: str, req: BudgetSetRequest):
    """Set or reset a customer's prepaid budget."""
    r = get_redis()
    budget_key = f"budget:{customer_id}"
    r.set(f"{budget_key}:balance_usd", str(req.balance_usd))
    r.set(f"{budget_key}:initial_balance_usd", str(req.balance_usd))
    r.set(f"{budget_key}:held_usd", "0")
    r.set(f"{budget_key}:debt_usd", "0")
    r.set(f"{budget_key}:total_deducted_usd", "0")
    r.set(f"{budget_key}:total_topup_usd", "0")
    # Reset soft-alert debounce so ladder can fire again after top-up/reset
    r.delete(f"{budget_key}:webhook_low_sent")
    for pct in (70, 90):
        r.delete(f"{budget_key}:webhook_warn_{pct}_sent")
    if req.alert_threshold_usd is not None:
        r.set(f"{budget_key}:alert_threshold_usd", str(req.alert_threshold_usd))
    if req.max_rpm is not None:
        r.set(f"{budget_key}:max_rpm", str(req.max_rpm))
    return _fetch_customer_budget(customer_id)


@app.get("/budget/{customer_id}/check")
def check_budget(
    customer_id: str,
    estimated_cost_usd: float = 0.0,
    parent_span_id: Optional[str] = None,
    session_id: Optional[str] = None,
    _: None = Depends(require_api_key),
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
):
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
    require_customer_access(customer_id, x_api_key)

    _, key_id = resolve_key_context(x_api_key)
    return run_budget_check(
        get_redis(),
        customer_id,
        estimated_cost_usd,
        parent_span_id=parent_span_id,
        session_id=session_id,
        key_id=key_id,
    )


@app.post("/budget/{customer_id}/topup", dependencies=[Depends(require_admin_key)])
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
    r.incrbyfloat(f"{budget_key}:total_topup_usd", amount_usd)
    return {"customer_id": customer_id, "new_balance_usd": new_balance, "added_usd": amount_usd}


@app.post("/budget/{customer_id}/package", dependencies=[Depends(require_admin_key)])
def set_customer_package(customer_id: str, req: PackageSetRequest):
    """Set prepaid token package allowance (lite ingest drawdown)."""
    if req.tokens < 0:
        raise HTTPException(status_code=400, detail="tokens must be >= 0")
    r = get_redis()
    r.set(f"package:{customer_id}:tokens_remaining", str(req.tokens))
    return {"customer_id": customer_id, "tokens_remaining": req.tokens}


@app.get("/budget/{customer_id}/package", dependencies=[Depends(require_api_key)])
def get_customer_package(
    customer_id: str,
    _auth: str = Depends(require_customer_access),
):
    """Remaining prepaid token package balance."""
    r = get_redis()
    remaining = int(r.get(f"package:{customer_id}:tokens_remaining") or 0)
    return {"customer_id": customer_id, "tokens_remaining": remaining}


class SpanUsage(BaseModel):
    span_id: str
    customer_id: Optional[str] = None
    total_tokens: int
    call_count: int
    cost_usd: float
    duration_ms: int


@app.get("/usage/span/{span_id}", response_model=SpanUsage, dependencies=[Depends(require_api_key)])
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


@app.get("/usage/customer/{customer_id}/spans", dependencies=[Depends(require_api_key)])
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


def _assert_flat_rerate(model_id: str) -> None:
    """Re-rating uses aggregate counters — valid for flat models only."""
    mode = get_catalog().model_pricing(model_id).pricing_mode
    if mode != "flat":
        raise HTTPException(
            status_code=422,
            detail=(
                f"Re-rating applies to flat-priced models only; '{model_id}' "
                f"uses pricing_mode={mode}. Replay events from Kafka for tier changes."
            ),
        )


@app.post("/rerate/preview", dependencies=[Depends(require_admin_key)])
def preview_rerate(req: ReRateRequest):
    """Preview retroactive re-rating adjustment without applying.

    Computes the cost delta for all customers who used the specified model,
    based on the difference between old and new pricing. Uses existing
    Redis counters (no event replay needed). Flat-priced models only.
    """
    _assert_flat_rerate(req.model_id)
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


@app.post("/rerate/apply", status_code=202, dependencies=[Depends(require_admin_key)])
def apply_rerate(req: ReRateRequest):
    """Apply retroactive re-rating: adjust cost_usd for all affected customers.
    NOTE: For large customer bases (>10K), this runs synchronously but should
    be behind a task queue in production. Returns 202 to signal async semantics.

    Atomically updates each customer's cost_usd and the global total.
    If the customer has a budget, adjusts the balance accordingly
    (price decrease = credit back to balance). Flat-priced models only.
    """
    _assert_flat_rerate(req.model_id)
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


@app.post("/budget/{customer_id}/reserve", dependencies=[Depends(require_admin_key)])
def reserve_budget(
    customer_id: str,
    estimated_cost_usd: float,
    parent_span_id: Optional[str] = None,
):
    """Reserve budget hold for streaming (does not deduct balance — Sink deducts actual).

    Flow: reserve(estimate) → LLM call → track(actual) → reconcile(estimate)
    Optional parent_span_id: also reserve against span cap (requires POST /budget/{id}/cap).
    """
    if estimated_cost_usd <= 0:
        raise HTTPException(status_code=400, detail="estimated_cost_usd must be positive")
    r = get_redis()
    if not r.exists(f"budget:{customer_id}:balance_usd"):
        raise HTTPException(status_code=404, detail=f"No budget set for {customer_id}")
    return reserve_hold(r, customer_id, estimated_cost_usd, parent_span_id=parent_span_id)


@app.post("/budget/{customer_id}/reconcile", dependencies=[Depends(require_admin_key)])
def reconcile_budget(
    customer_id: str,
    reserved_usd: float,
    actual_usd: float = 0.0,
    parent_span_id: Optional[str] = None,
):
    """Release hold after streaming. Balance unchanged (Flink Sink deducted actual)."""
    r = get_redis()
    if not r.exists(f"budget:{customer_id}:balance_usd"):
        raise HTTPException(status_code=404, detail=f"No budget set for {customer_id}")
    result = reconcile_hold(r, customer_id, reserved_usd, parent_span_id=parent_span_id)
    result["actual_usd"] = actual_usd
    return result


@app.post("/budget/{customer_id}/cap", dependencies=[Depends(require_admin_key)])
def set_hierarchy_cap(customer_id: str, config: HierarchyCapConfig):
    """Set a hard spend cap on a span (agent run) or session. Enforced at /check."""
    if config.kind not in ("span", "session"):
        raise HTTPException(status_code=400, detail="kind must be 'span' or 'session'")
    if config.max_cost_usd < 0:
        raise HTTPException(status_code=400, detail="max_cost_usd must be >= 0")
    r = get_redis()
    # Bind cap to customer for audit; enforcement keys match usage_buckets counters
    r.set(f"{config.kind}:{config.id}:max_cost_usd", str(config.max_cost_usd))
    r.set(f"{config.kind}:{config.id}:cap_customer_id", customer_id)
    return {
        "customer_id": customer_id,
        "kind": config.kind,
        "id": config.id,
        "max_cost_usd": config.max_cost_usd,
    }


@app.post("/budget/{customer_id}/webhook", dependencies=[Depends(require_admin_key)])
def set_budget_webhook(customer_id: str, config: WebhookConfig):
    """Configure HTTPS webhook for BUDGET_LOW / BUDGET_EXHAUSTED alerts."""
    r = get_redis()
    budget_key = f"budget:{customer_id}"
    if not r.exists(f"{budget_key}:balance_usd"):
        raise HTTPException(status_code=404, detail=f"No budget set for {customer_id}")
    r.set(f"{budget_key}:webhook_url", config.webhook_url)
    if config.webhook_secret:
        r.set(f"{budget_key}:webhook_secret", config.webhook_secret)
    return {"customer_id": customer_id, "webhook_url": config.webhook_url}


@app.get("/budget/{customer_id}/webhook", dependencies=[Depends(require_admin_key)])
def get_budget_webhook(customer_id: str):
    r = get_redis()
    url = r.get(f"budget:{customer_id}:webhook_url")
    if not url:
        raise HTTPException(status_code=404, detail="Webhook not configured")
    return {"customer_id": customer_id, "webhook_url": url}


class BillingLinkRequest(BaseModel):
    platform: str  # stripe | metronome | orb
    external_customer_id: str


@app.post("/admin/billing/{customer_id}/link", dependencies=[Depends(require_admin_key)])
def link_billing_platform(customer_id: str, body: BillingLinkRequest):
    """Link a FluxMeter customer to Stripe / Metronome / Orb for periodic export."""
    platform = body.platform.lower()
    if platform not in ("stripe", "metronome", "orb"):
        raise HTTPException(400, detail="platform must be stripe, metronome, or orb")
    if not body.external_customer_id:
        raise HTTPException(400, detail="external_customer_id required")
    r = get_redis()
    link_customer_platform(r, customer_id, platform, body.external_customer_id)
    return {
        "linked": True,
        "customer_id": customer_id,
        "platform": platform,
        "external_customer_id": body.external_customer_id,
    }


@app.post("/admin/billing/{customer_id}/link-stripe")
async def link_stripe(customer_id: str, body: dict, _=Depends(require_admin_key)):
    """Link a customer to a Stripe customer for automatic usage billing."""
    stripe_cid = body.get("stripe_customer_id")
    if not stripe_cid:
        raise HTTPException(400, "stripe_customer_id required")
    r = get_redis()
    link_customer_stripe(r, customer_id, stripe_cid)
    return {"linked": True, "customer_id": customer_id, "stripe_customer_id": stripe_cid}


@app.post("/admin/customers/{customer_id}/api-keys", dependencies=[Depends(require_admin_key)])
def create_api_key(customer_id: str):
    """Create a customer-scoped API key (ingest/check for this customer only)."""
    return create_customer_api_key(customer_id)


@app.post(
    "/admin/customers/{customer_id}/api-keys/{key_id}/budget",
    dependencies=[Depends(require_admin_key)],
)
def set_api_key_budget_endpoint(
    customer_id: str,
    key_id: str,
    body: ApiKeyBudgetRequest,
):
    """Set daily/monthly spend caps on a reseller API key."""
    r = get_redis()
    meta_raw = r.get(f"apikey:meta:{key_id}")
    if not meta_raw:
        raise HTTPException(status_code=404, detail="API key not found")
    meta = json.loads(meta_raw)
    if meta.get("customer_id") != customer_id:
        raise HTTPException(status_code=404, detail="API key not found for customer")
    return set_api_key_budget(
        key_id,
        daily_budget_usd=body.daily_budget_usd,
        monthly_budget_usd=body.monthly_budget_usd,
    )


@app.delete("/admin/api-keys/{key_id}", dependencies=[Depends(require_admin_key)])
def delete_api_key(key_id: str):
    if not revoke_customer_api_key(key_id):
        raise HTTPException(status_code=404, detail="API key not found")
    return {"key_id": key_id, "revoked": True}


PRICING_FILE = os.getenv(
    "PRICING_FILE",
    os.path.join(os.path.dirname(__file__), "..", "config", "pricing.json"),
)


@app.get("/pricing", dependencies=[Depends(require_api_key)])
def get_pricing():
    """Current pricing catalog (Redis snapshot or file)."""
    import json

    r = get_redis()
    snap = r.get("pricing:current")
    if snap:
        return json.loads(snap)
    try:
        with open(PRICING_FILE, encoding="utf-8") as f:
            return json.load(f)
    except OSError:
        raise HTTPException(status_code=404, detail="Pricing file not found")


@app.put("/admin/pricing", dependencies=[Depends(require_admin_key)])
def update_pricing(body: dict):
    """Hot-update pricing in Redis (Flink polls via PRICING_FILE or restart)."""
    _validate_pricing_body(body)
    import json

    r = get_redis()
    r.set("pricing:current", json.dumps(body))
    return {"status": "updated", "version": body.get("version", "unknown")}


def _validate_pricing_body(body: dict) -> None:
    """Raise HTTPException on invalid pricing catalog structure."""
    required = ("models", "defaults")
    for field in required:
        if field not in body:
            raise HTTPException(status_code=400, detail=f"Missing field: {field}")

    volume_scope = body.get("volume_scope", "customer_model")
    if volume_scope != "customer_model":
        raise HTTPException(status_code=400, detail=f"Unsupported volume_scope: {volume_scope}")

    billing_period = body.get("billing_period", "calendar_month")
    if billing_period != "calendar_month":
        raise HTTPException(status_code=400, detail=f"Unsupported billing_period: {billing_period}")

    valid_modes = {"flat", "volume", "graduated"}
    for model_id, model in body.get("models", {}).items():
        tiers = model.get("tiers") or []
        mode = model.get("pricing_mode")
        if mode is not None and mode not in valid_modes:
            raise HTTPException(
                status_code=400,
                detail=f"models.{model_id}: invalid pricing_mode '{mode}'",
            )
        if mode == "flat" and tiers:
            raise HTTPException(
                status_code=400,
                detail=f"models.{model_id}: pricing_mode=flat cannot have tiers",
            )
        if mode in ("volume", "graduated") and not tiers:
            raise HTTPException(
                status_code=400,
                detail=f"models.{model_id}: pricing_mode={mode} requires tiers",
            )
        if tiers and mode is None:
            mode = "volume"
        if tiers:
            prev_up_to = -1
            for i, tier in enumerate(tiers):
                up_to = tier.get("up_to_tokens_m")
                if up_to is not None:
                    if not isinstance(up_to, int) or up_to <= prev_up_to:
                        raise HTTPException(
                            status_code=400,
                            detail=(
                                f"models.{model_id}: tiers[{i}] up_to_tokens_m "
                                "must be strictly increasing"
                            ),
                        )
                    prev_up_to = up_to
            if tiers[-1].get("up_to_tokens_m") is not None:
                raise HTTPException(
                    status_code=400,
                    detail=f"models.{model_id}: last tier must have up_to_tokens_m: null",
                )


@app.post("/admin/pricing/validate", dependencies=[Depends(require_admin_key)])
def validate_pricing(body: dict):
    """Validate pricing JSON structure."""
    _validate_pricing_body(body)
    return {"status": "valid", "models": len(body.get("models", {}))}


@app.get("/admin/reconciliation", dependencies=[Depends(require_admin_key)])
def get_reconciliation_snapshot():
    """Last reconciliation job results stored in Redis."""
    import json

    r = get_redis()
    snap = r.get("reconciliation:last")
    if not snap:
        return {"status": "no_data"}
    return json.loads(snap)


def _int_or_none(val) -> Optional[int]:
    return int(val) if val else None


def _float_or_none(val) -> Optional[float]:
    return float(val) if val else None
