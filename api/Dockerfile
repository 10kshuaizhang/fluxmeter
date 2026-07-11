FROM python:3.11-slim
WORKDIR /app
COPY api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY api/ /app/
COPY spec /spec
COPY config /config
ENV FLUXMETER_OPENAPI_PATH=/spec/openapi/openapi.yaml
ENV PRICING_FILE=/config/pricing.json
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
