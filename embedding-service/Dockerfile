FROM python:3.10-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy service code
COPY service.py .

# Expose port
EXPOSE 8001

# Run the service
CMD ["uvicorn", "service:app", "--host", "0.0.0.0", "--port", "8001"]

