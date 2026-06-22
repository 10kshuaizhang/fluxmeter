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
