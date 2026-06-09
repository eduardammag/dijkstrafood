FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY realtime_metrics_service/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY realtime_metrics_service/ .
COPY dashboard/app.py ./app.py

EXPOSE 8010

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8010"]
