# Contributing to FluxMeter

## Getting Started

```bash
git clone https://github.com/10kshuaizhang/fluxmeter.git
cd fluxmeter
make demo  # Verify everything works
```

## Development Setup

**Requirements:**
- Java 17 (Flink engine)
- Python 3.9+ (SDK + API)
- Docker & Docker Compose (infrastructure)

**Build:**
```bash
./gradlew shadowJar  # Build Flink JAR
cd sdk/python && pip install -e ".[dev]"  # Install SDK in dev mode
```

**Run tests:**
```bash
# Unit tests (Python SDK)
cd sdk/python && pytest tests/ -v

# Integration tests (requires running stack)
make start
sleep 15
make submit-job
pytest tests/test_integration.py -v
make stop
```

## Making Changes

1. Fork the repo
2. Create a feature branch (`git checkout -b feat/your-feature`)
3. Make your changes
4. Run tests (both unit and integration)
5. Commit with a descriptive message
6. Open a Pull Request

## Commit Messages

Format: `type: description`

Types:
- `feat:` — new feature
- `fix:` — bug fix
- `docs:` — documentation only
- `test:` — tests only
- `refactor:` — code change that neither fixes a bug nor adds a feature

## Code Style

**Java:** Standard Java conventions. No specific formatter enforced.

**Python:** Follow existing patterns. Run `ruff check` before committing.

## Areas Where Help is Wanted

- [ ] Tiered pricing and volume discounts
- [ ] Webhook delivery for budget alerts (in addition to Kafka)
- [ ] Multi-tenant auth middleware
- [ ] Node.js / Go / Rust SDK
- [ ] Helm chart for Kubernetes deployment
- [ ] Grafana dashboard improvements
- [ ] More provider integrations (Azure OpenAI, AWS Bedrock, Cohere)

## Reporting Issues

Open a GitHub issue with:
- What you expected
- What actually happened
- Steps to reproduce
- FluxMeter version and environment (OS, Docker version)

## Questions?

Open a Discussion on GitHub. We're happy to help.
