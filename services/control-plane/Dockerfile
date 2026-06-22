FROM python:3.11-slim
WORKDIR /app
COPY services/control-plane/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY services/control-plane/main.py services/control-plane/models.py services/control-plane/stripe_billing.py ./
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]
