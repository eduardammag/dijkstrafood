FROM python:3.11-slim

WORKDIR /app

COPY common /app/common
COPY test-publisher /app/test-publisher

RUN pip install --no-cache-dir -r /app/common/requirements.txt

CMD ["python", "/app/test-publisher/test_publish.py"]