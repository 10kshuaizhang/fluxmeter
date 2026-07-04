"""Re-rating guards for tiered pricing — no Redis."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
from fastapi import HTTPException

sys.path.insert(0, "api")

from main import ReRateRequest, _assert_flat_rerate  # noqa: E402
from pricing_loader import PricingCatalog, reload_catalog  # noqa: E402


class TestRerateFlatOnly:
    def setup_method(self):
        reload_catalog(PricingCatalog.load_from_file())

    def test_allows_flat_model(self):
        _assert_flat_rerate("gpt-4o")

    def test_rejects_volume_model(self):
        tiered = PricingCatalog(json.loads(
            Path("contrib/pricing/tiered-example.json").read_text()
        ))
        reload_catalog(tiered)
        with pytest.raises(HTTPException) as exc:
            _assert_flat_rerate("gpt-4o-mini")
        assert exc.value.status_code == 422
        assert "pricing_mode=volume" in exc.value.detail

    def test_rejects_graduated_model(self):
        tiered = PricingCatalog(json.loads(
            Path("contrib/pricing/tiered-example.json").read_text()
        ))
        reload_catalog(tiered)
        with pytest.raises(HTTPException) as exc:
            _assert_flat_rerate("claude-sonnet-4")
        assert exc.value.status_code == 422
