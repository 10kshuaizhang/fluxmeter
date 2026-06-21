# PyPI Release Guide

How to publish the FluxMeter Python SDK (`fluxmeter` on PyPI).

Package lives in `sdk/python/`. Version is defined in:

- `sdk/python/pyproject.toml` (`project.version`)
- `sdk/python/fluxmeter/__init__.py` (`__version__`)

Keep both in sync before every release.

## Prerequisites

- PyPI account: https://pypi.org/account/register/
- TestPyPI account (recommended): https://test.pypi.org/account/register/
- API token from PyPI → Account settings → API tokens

For CI releases, configure **Trusted Publishers** on the PyPI project (do not commit API tokens):

1. PyPI project → Publishing → Add a new pending publisher
2. Owner: your GitHub org/user, repo: `fluxmeter`, workflow: `pypi-publish.yml`, environment: (optional)

Until Trusted Publisher is set up, use a local `twine upload` with a short-lived API token (never commit tokens to git).

## Local release (manual)

```bash
cd sdk/python

# 1. Bump version in pyproject.toml and fluxmeter/__init__.py
# 2. Run tests
pip install -e ".[dev]"
pytest tests/ -v

# 3. Build
python3 -m pip install --upgrade build twine
python3 -m build

# 4. TestPyPI (dry run)
python3 -m twine upload --repository testpypi dist/*
# Username: __token__
# Password: your TestPyPI API token

pip install --index-url https://test.pypi.org/simple/ \
  --extra-index-url https://pypi.org/simple/ \
  fluxmeter==<version>

# 5. Production PyPI
python3 -m twine upload dist/*
```

Use environment variables instead of typing the token:

```bash
export TWINE_USERNAME=__token__
export TWINE_PASSWORD=pypi-<your-token>
python3 -m twine upload dist/*
```

## CI release (GitHub Actions)

Workflow: `.github/workflows/pypi-publish.yml`

1. Ensure Trusted Publisher is configured on PyPI (see above).
2. Create a GitHub Release (tag e.g. `sdk-v1.0.0` or any tag that triggers your release policy).
3. Publish the release — the workflow builds, tests, and uploads to PyPI.

Manual trigger: Actions → **Publish Python SDK to PyPI** → **Run workflow**.

## After release

- Verify: `pip install fluxmeter==<version>`
- Update root `changLog.md` and `progress.md`
- Confirm README `pip install fluxmeter` works for new users

## Troubleshooting

| Error | Fix |
|-------|-----|
| `403 File already exists` | Bump version; PyPI versions are immutable |
| `confluent-kafka` install fails on user machine | Document librdkafka / use a platform with prebuilt wheels |
| Workflow publish fails | Check Trusted Publisher owner/repo/workflow name match |

PyPI versions cannot be deleted; use **Yank** on PyPI if a bad release must be hidden.
