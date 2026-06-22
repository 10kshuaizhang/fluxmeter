"""Unit tests for SaaS control plane models (no Docker)."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_CP = Path(__file__).resolve().parents[1] / "services" / "control-plane"
if str(_CP) not in sys.path:
    sys.path.insert(0, str(_CP))

from models import PLAN_LIMITS, PlanTier, TenantCreate  # noqa: E402


class TestPlanLimits:
    def test_free_tier_limits(self):
        limits = PLAN_LIMITS[PlanTier.free]
        assert limits["max_events_per_month"] == 100_000
        assert limits["max_eps"] == 100

    def test_growth_tier_limits(self):
        limits = PLAN_LIMITS[PlanTier.growth]
        assert limits["max_events_per_month"] == 10_000_000
        assert limits["max_eps"] == 10_000

    def test_enterprise_unlimited(self):
        limits = PLAN_LIMITS[PlanTier.enterprise]
        assert limits["max_events_per_month"] == -1
        assert limits["max_eps"] == -1

    def test_tenant_create_defaults_to_free(self):
        t = TenantCreate(name="Acme", email="a@acme.example")
        assert t.plan == PlanTier.free
