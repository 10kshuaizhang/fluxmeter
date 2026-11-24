# Examples

Self-contained snippets that talk to a running FluxMeter stack over HTTP.
They depend on the published Python SDK (`fluxmeter>=2.0.2`), not this
repository's source tree.

## wrap_runaway.py

Reproduce a runaway agent loop through `wrap(OpenAI())` with a local mock
upstream (no provider key). The script sets a $0.30 prepaid budget itself;
each mock call reports gpt-4o 4000 input + 2000 output tokens (catalog $0.03).

```bash
make demo
pip install "fluxmeter>=2.0.2" openai
python examples/wrap_runaway.py
```

Optional: `python examples/wrap_runaway.py 1` waits one second between calls.

This is a reproduction helper, not a zero-overspend guarantee.
