FROM python:3.11-slim

WORKDIR /app

COPY common /app/common
COPY courier-worker /app/courier-worker

RUN pip install --no-cache-dir -r /app/common/requirements.txt

CMD ["python", "/app/courier-worker/courier_worker.py"]