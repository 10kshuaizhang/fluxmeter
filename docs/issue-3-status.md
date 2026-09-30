# Issue #3: implementation and remaining acceptance

Review baseline: `6573af8ceae190647b7132a3a2272dca08c67621`; documentation update:
2026-09-30, engine/API 4.8.4. This records repository behavior and evidence; it does
not close or rewrite [Issue #3](https://github.com/10kshuaizhang/fluxmeter/issues/3).

| Requirement | Repository state | Acceptance |
| --- | --- | --- |
| One HTTP → Kafka → Flink → Redis architecture | Base Compose and SDKs use HTTP custody; Kafka stays internal | Implemented architecture; not evidence that all issue gates passed |
| Durable receipt and retry identity | `202` after Kafka ACK and identity finalization; pending/uncertain failures use retryable `503`, overload uses `429` | Current contract is more specific than the original ACK-only wording |
| Whole-batch validation before any publish | Current `api/main.py` validates per row and still submits valid rows when other rows are rejected | **Contract adjusted in ADR-024 (2026-08-17).** The original issue still asks for whole-batch validation; reconciling that acceptance text remains open |
| Batch per-event failure wire format | `failed` represents internal unavailable/uncertain outcomes; all rejected returns `207` with top-level `rejected` | API reference/OpenAPI corrected; per-item retryable flags govern recovery |
| SDK delivery errors | Direct track calls raise typed errors; wrapper returns provider output, now warns and supports an error callback (Python SDK 2.0.1) | Compatibility behavior is explicit; callers must install their recovery/alerting path |
| Reproducible first trial | `make demo` waits for readiness and checks a unique HTTP event's tokens and cost | Local Docker `make demo` and `make demo-proof` passed on 2026-09-30; production and sustained-capacity acceptance remain separate |
| 10K single / 100K batch HTTP gates | Existing evidence separates custody, short full-stack samples, and internal bursts | **Open.** No new sustained-performance claim |
| Production readiness / feature parity | Updated examples match environment-based config, `/ready`, and repository images | Target-environment HA, restore, load and parity acceptance still required |

See [benchmark evidence](load-testing.md), [API reference](api-reference.md),
[local trial](quickstart.md), and [production guide](production-deploy.md).
Do not infer completion of the issue from architecture consolidation or a version bump.
