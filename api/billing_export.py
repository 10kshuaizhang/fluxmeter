"""Stripe billing export — reports aggregated usage to Stripe Meters API.

Env:
  STRIPE_EXPORT_MODE=events|cost   (default: events)
  BILLING_EXPORT_PERIOD=hourly|monthly   (default: hourly)
  STRIPE_METER_NAME                  meter event name (events mode)
  STRIPE_COST_METER_NAME             meter event name (cost mode, default: token_cost_usd_cents)
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Optional

import redis

from pricing_loader import billing_period_month

logger = logging.getLogger(__name__)

STRIPE_API_KEY = os.getenv("STRIPE_API_KEY")
EXPORT_INTERVAL_SEC = int(os.getenv("BILLING_EXPORT_INTERVAL", "3600"))
STRIPE_EXPORT_MODE = os.getenv("STRIPE_EXPORT_MODE", "events").lower()
BILLING_EXPORT_PERIOD = os.getenv("BILLING_EXPORT_PERIOD", "hourly").lower()
METER_EVENT_NAME = os.getenv("STRIPE_METER_NAME", "token_events_processed")
COST_METER_EVENT_NAME = os.getenv("STRIPE_COST_METER_NAME", "token_cost_usd_cents")

stripe = None
if STRIPE_API_KEY:
    try:
        import stripe as _stripe
        _stripe.api_key = STRIPE_API_KEY
        stripe = _stripe
    except ImportError:
        logger.warning("stripe package not installed; billing export disabled")


def _export_period_key() -> str:
    return f"billing:export:last_period:{BILLING_EXPORT_PERIOD}"


def should_run_export_cycle(r: redis.Redis) -> bool:
    """For monthly mode, export at most once per UTC calendar month."""
    if BILLING_EXPORT_PERIOD != "monthly":
        return True
    current = billing_period_month(int(time.time() * 1000))
    last = r.get(_export_period_key())
    return last != current


def mark_export_cycle(r: redis.Redis) -> None:
    if BILLING_EXPORT_PERIOD == "monthly":
        r.set(_export_period_key(), billing_period_month(int(time.time() * 1000)))


def collect_customer_usage(r: redis.Redis, customer_id: str) -> Optional[dict]:
    """Collect usage delta for one customer since last report."""
    stripe_cid = r.get(f"billing:{customer_id}:stripe_customer_id")
    if not stripe_cid:
        return None

    total_events = int(r.get(f"customer:{customer_id}:event_count") or 0)
    last_reported = int(r.get(f"billing:{customer_id}:last_reported_events") or 0)
    new_events = max(0, total_events - last_reported)
    cost_usd = float(r.get(f"customer:{customer_id}:cost_usd") or 0)
    last_cost = float(r.get(f"billing:{customer_id}:last_reported_cost_usd") or 0)
    new_cost_usd = max(0.0, cost_usd - last_cost)

    return {
        "customer_id": customer_id,
        "stripe_customer_id": stripe_cid,
        "new_events": new_events,
        "total_events": total_events,
        "total_cost_usd": cost_usd,
        "new_cost_usd": new_cost_usd,
    }


def export_value(usage: dict) -> int:
    """Meter value for Stripe (events count or cost in USD cents)."""
    if STRIPE_EXPORT_MODE == "cost":
        return int(round(usage["new_cost_usd"] * 100))
    return usage["new_events"]


def meter_event_name() -> str:
    return COST_METER_EVENT_NAME if STRIPE_EXPORT_MODE == "cost" else METER_EVENT_NAME


def report_to_stripe(stripe_customer_id: str, event_name: str, value: int, timestamp: int):
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
    """Background loop: report usage to Stripe every hour (or monthly gate)."""
    if not STRIPE_API_KEY:
        logger.info("STRIPE_API_KEY not set; billing export disabled")
        return

    logger.info(
        "Billing export started (interval=%ds, mode=%s, period=%s, meter=%s)",
        EXPORT_INTERVAL_SEC,
        STRIPE_EXPORT_MODE,
        BILLING_EXPORT_PERIOD,
        meter_event_name(),
    )

    while True:
        try:
            await asyncio.sleep(EXPORT_INTERVAL_SEC)
            if not should_run_export_cycle(r):
                continue

            now = int(time.time())
            customers = discover_billable_customers(r)
            reported = 0
            event_name = meter_event_name()

            for cid in customers:
                usage = collect_customer_usage(r, cid)
                if not usage:
                    continue
                value = export_value(usage)
                if value <= 0:
                    continue

                report_to_stripe(
                    stripe_customer_id=usage["stripe_customer_id"],
                    event_name=event_name,
                    value=value,
                    timestamp=now,
                )

                r.set(f"billing:{cid}:last_reported_events", str(usage["total_events"]))
                r.set(f"billing:{cid}:last_reported_cost_usd", str(usage["total_cost_usd"]))
                reported += 1

            if reported > 0:
                mark_export_cycle(r)
                logger.info("Reported usage for %d customers to Stripe", reported)

        except Exception as e:
            logger.error("Billing export error: %s", e)
            await asyncio.sleep(30)
