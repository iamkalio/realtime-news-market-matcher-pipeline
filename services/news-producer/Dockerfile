FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .

RUN apt-get update && \
	apt-get install -y bash netcat-openbsd && \
	rm -rf /var/lib/apt/lists/* && \
	pip install --no-cache-dir -r requirements.txt

COPY wait-for-it.sh wait-for-it.sh
COPY producer.py producer.py

RUN chmod +x wait-for-it.sh

ENV PYTHONUNBUFFERED=1

CMD ["./wait-for-it.sh", "kafka:9092", "--", "python", "-u", "producer.py"]