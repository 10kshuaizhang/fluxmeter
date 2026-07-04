"""Stripe subscription + Checkout for SaaS tenants."""

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


def _price_id(plan: str) -> Optional[str]:
    price_map = {
        "growth": os.getenv("STRIPE_GROWTH_PRICE_ID", "price_growth"),
        "scale": os.getenv("STRIPE_SCALE_PRICE_ID", "price_scale"),
    }
    return price_map.get(plan)


def create_subscription(stripe_customer_id: str, plan: str) -> Optional[str]:
    """Create a Stripe subscription for a tenant. Returns subscription ID."""
    if not stripe:
        return None
    price_id = _price_id(plan)
    if not price_id:
        return None
    sub = stripe.Subscription.create(
        customer=stripe_customer_id,
        items=[{"price": price_id}],
    )
    return sub.id


def create_checkout_session(
    stripe_customer_id: str,
    plan: str,
    success_url: str,
    cancel_url: str,
) -> Optional[str]:
    """Create Stripe Checkout session URL for plan subscription."""
    if not stripe:
        return None
    price_id = _price_id(plan)
    if not price_id:
        return None
    session = stripe.checkout.Session.create(
        mode="subscription",
        customer=stripe_customer_id,
        line_items=[{"price": price_id, "quantity": 1}],
        success_url=success_url,
        cancel_url=cancel_url,
    )
    return session.url
