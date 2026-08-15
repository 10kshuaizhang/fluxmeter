"""Gateway reservation expiry contract."""

import fakeredis

from budget_ops import (
    reap_expired_reservations,
    refresh_gateway_reservation,
    register_gateway_reservation,
    settle_gateway_reservation,
)


def test_expired_gateway_reservation_releases_hold_and_alerts():
    r = fakeredis.FakeRedis(decode_responses=True)
    r.set("budget:cust_expired:held_usd", "0.5")
    register_gateway_reservation(
        r,
        "reservation_expired",
        customer_id="cust_expired",
        reserved_usd=0.2,
        parent_span_id=None,
        expires_at=1,
    )

    assert reap_expired_reservations(r, now=2) == 1
    assert float(r.get("budget:cust_expired:held_usd")) == 0.3
    assert r.llen("gateway:reservation:alerts") == 1


def test_long_stream_refresh_extends_reservation_deadline():
    r = fakeredis.FakeRedis(decode_responses=True)
    register_gateway_reservation(
        r,
        "reservation_long",
        customer_id="cust_long",
        reserved_usd=0.5,
        parent_span_id=None,
        expires_at=10,
    )

    refresh_gateway_reservation(r, "reservation_long", expires_at=100)

    assert r.zscore("gateway:reservations:pending", "reservation_long") == 100


def test_failed_upstream_settlement_cannot_release_a_future_hold():
    r = fakeredis.FakeRedis(decode_responses=True)
    r.set("budget:cust_failed:held_usd", "0.5")
    register_gateway_reservation(
        r,
        "reservation_failed",
        customer_id="cust_failed",
        reserved_usd=0.2,
        parent_span_id=None,
    )

    assert settle_gateway_reservation(r, "reservation_failed") == 0.2
    r.incrbyfloat("budget:cust_failed:held_usd", 0.4)
    assert settle_gateway_reservation(r, "reservation_failed") == 0.0
    assert float(r.get("budget:cust_failed:held_usd")) == 0.7
