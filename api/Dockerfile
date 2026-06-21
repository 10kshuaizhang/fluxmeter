FROM python:3.11-slim
WORKDIR /app
COPY api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY api/main.py api/auth.py api/lite_aggregate.py .
COPY spec /spec
ENV FLUXMETER_OPENAPI_PATH=/spec/openapi/openapi.yaml
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
