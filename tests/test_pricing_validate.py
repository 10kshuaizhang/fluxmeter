"""Unit tests for pricing catalog validation — no Redis/Docker."""

from __future__ import annotations

import sys

import pytest
from fastapi import HTTPException

sys.path.insert(0, "api")

from main import _validate_pricing_body  # noqa: E402


def _minimal_catalog(**overrides):
    body = {
        "models": {"gpt-4o": {"input_per_m": 2.5, "output_per_m": 10.0}},
        "defaults": {"input_per_m": 1.0, "output_per_m": 3.0},
    }
    body.update(overrides)
    return body


class TestPricingValidation:
    def test_valid_minimal(self):
        _validate_pricing_body(_minimal_catalog())

    def test_valid_tiered_example(self):
        import json
        from pathlib import Path

        body = json.loads(Path("contrib/pricing/tiered-example.json").read_text())
        _validate_pricing_body(body)

    def test_rejects_non_monotonic_tiers(self):
        body = _minimal_catalog(
            models={
                "m": {
                    "pricing_mode": "volume",
                    "input_per_m": 1.0,
                    "output_per_m": 1.0,
                    "tiers": [
                        {"up_to_tokens_m": 10, "input_per_m": 1.0, "output_per_m": 1.0},
                        {"up_to_tokens_m": 5, "input_per_m": 0.5, "output_per_m": 0.5},
                    ],
                }
            }
        )
        with pytest.raises(HTTPException) as exc:
            _validate_pricing_body(body)
        assert exc.value.status_code == 400

    def test_last_tier_must_be_open_ended(self):
        body = _minimal_catalog(
            models={
                "m": {
                    "pricing_mode": "graduated",
                    "input_per_m": 1.0,
                    "output_per_m": 1.0,
                    "tiers": [
                        {"up_to_tokens_m": 1, "input_per_m": 1.0, "output_per_m": 1.0},
                        {"up_to_tokens_m": 10, "input_per_m": 0.5, "output_per_m": 0.5},
                    ],
                }
            }
        )
        with pytest.raises(HTTPException) as exc:
            _validate_pricing_body(body)
        assert "last tier" in exc.value.detail
