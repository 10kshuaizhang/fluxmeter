"""Gateway reservation expiry contract."""

import fakeredis

from budget import Budget
from reservation import Reservation


def test_expired_gateway_reservation_releases_hold_and_alerts():
    r = fakeredis.FakeRedis(decode_responses=True)
    Budget(r, cache={}).configure("cust_expired", 5)
    reservations = Reservation(r)
    reservations.open(
        "reservation_expired",
        customer_id="cust_expired",
        estimated_cost_usd=0.2,
        expires_at=1,
    )

    assert reservations.expire_due(now=2) == 1
    assert float(r.get("budget:cust_expired:held_usd")) == 0
    assert r.llen("gateway:reservation:alerts") == 1


def test_long_stream_refresh_extends_reservation_deadline():
    r = fakeredis.FakeRedis(decode_responses=True)
    Budget(r, cache={}).configure("cust_long", 5)
    reservations = Reservation(r)
    reservations.open(
        "reservation_long",
        customer_id="cust_long",
        estimated_cost_usd=0.5,
        expires_at=10,
    )

    reservations.refresh("reservation_long", expires_at=100)

    assert r.zscore("gateway:reservations:pending", "reservation_long") == 100


def test_failed_upstream_settlement_cannot_release_a_future_hold():
    r = fakeredis.FakeRedis(decode_responses=True)
    Budget(r, cache={}).configure("cust_failed", 5)
    reservations = Reservation(r)
    reservations.open(
        "reservation_failed",
        customer_id="cust_failed",
        estimated_cost_usd=0.2,
    )

    assert reservations.settle("reservation_failed") == 0.2
    r.incrbyfloat("budget:cust_failed:held_usd", 0.4)
    assert reservations.settle("reservation_failed") == 0.0
    assert float(r.get("budget:cust_failed:held_usd")) == 0.4
