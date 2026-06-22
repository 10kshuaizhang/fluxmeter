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

app = FastAPI(title="FluxMeter Control Plane", version="2.2.1")

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
