FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy test files
COPY . .

# Expose Locust web UI port
EXPOSE 8089

# Default command starts Locust in web mode
CMD ["locust", "-f", "locustfile.py", "--web-host", "0.0.0.0"]
