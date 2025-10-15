FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create data directory for persistence
RUN mkdir -p /data

# Start the application
CMD ["python", "mexc_tracker.py"]