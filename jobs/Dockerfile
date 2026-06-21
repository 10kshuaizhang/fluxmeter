FROM python:3.11-slim
WORKDIR /app
COPY api/requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY jobs/reconcile_balances.py .
CMD ["python", "reconcile_balances.py"]
