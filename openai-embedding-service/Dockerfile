FROM python:3.10-slim

WORKDIR /app

# Install system dependencies (curl for healthcheck)
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy service code
COPY service.py .

# Expose port
EXPOSE 8002

# Run the service
CMD ["uvicorn", "service:app", "--host", "0.0.0.0", "--port", "8002"]










