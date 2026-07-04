FROM python:3.11-slim
WORKDIR /app
COPY api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY api/main.py api/auth.py api/lite_aggregate.py api/lite_aggregate_lua.py api/pricing_loader.py api/tenant_keys.py api/budget_ops.py api/webhook_worker.py api/rollup_worker.py api/billing_export.py api/usage_buckets.py .
COPY spec /spec
COPY config /config
ENV FLUXMETER_OPENAPI_PATH=/spec/openapi/openapi.yaml
ENV PRICING_FILE=/config/pricing.json
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
