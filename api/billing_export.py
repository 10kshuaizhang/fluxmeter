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
