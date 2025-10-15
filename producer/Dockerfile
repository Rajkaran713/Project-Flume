# Stage 1: The Build Stage
FROM python:3.11-slim AS builder

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY producer.py .

# Stage 2: The Final, Smaller Runtime Image
FROM python:3.11-alpine

# Set the working directory
WORKDIR /app

# Create a non-root user for security
RUN adduser -D appuser
RUN chown -R appuser:appuser /app
USER appuser

# Copy the installed packages and application files from the builder stage
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /app /app

# Set the entrypoint to run the script
CMD ["python", "producer.py"]