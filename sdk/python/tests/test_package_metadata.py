"""Guard PyPI metadata for the 2.0.2 SDK release."""

from pathlib import Path

import fluxmeter

ROOT = Path(__file__).resolve().parents[1]
README = (ROOT / "README.md").read_text(encoding="utf-8")
PYPROJECT = (ROOT / "pyproject.toml").read_text(encoding="utf-8")


def test_version_is_2_0_2():
    assert fluxmeter.__version__ == "2.0.2"
    assert 'version = "2.0.2"' in PYPROJECT


def test_delivery_error_is_exported():
    assert hasattr(fluxmeter, "DeliveryError")
    from fluxmeter import DeliveryError

    assert issubclass(DeliveryError, RuntimeError)


def test_long_description_does_not_claim_sub_10ms_checks():
    for source in (README.lower(), PYPROJECT.lower()):
        assert "sub-10ms" not in source
        assert "sub 10ms" not in source


def test_readme_uses_deployment_dependent_wording():
    assert "deployment-dependent" in README
    assert "pip install \"fluxmeter>=2.0.2\"" in README
    assert "pip install fluxmeter==2.0.2" in README
    assert "DeliveryError" in README


def test_summary_mentions_deployment_dependent_latency():
    assert "deployment-dependent" in PYPROJECT
