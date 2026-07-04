"""Pydantic models for the control plane."""

from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


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


class CheckoutRequest(BaseModel):
    plan: PlanTier
    success_url: str = "http://localhost:8001/checkout/success"
    cancel_url: str = "http://localhost:8001/checkout/cancel"
