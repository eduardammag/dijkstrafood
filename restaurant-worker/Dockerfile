FROM python:3.11-slim

WORKDIR /app

COPY common /app/common
COPY restaurant-worker /app/restaurant-worker

RUN pip install --no-cache-dir -r /app/common/requirements.txt

CMD ["python", "/app/restaurant-worker/restaurant_worker.py"]