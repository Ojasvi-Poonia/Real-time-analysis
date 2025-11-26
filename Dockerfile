# ==============================================================================
# CASSANDRA REAL-TIME ANALYTICS - APPLICATION IMAGE
# ==============================================================================
# This Dockerfile creates the application container that:
# 1. Runs the transaction stream producer (demo.py)
# 2. Runs the Streamlit dashboard (dashboard.py)
# ==============================================================================

FROM python:3.11-slim

# Install system dependencies for cassandra-driver
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        libc6-dev \
        libffi-dev \
        libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements first for better layer caching
COPY requirements.txt /app/requirements.txt

# Install Python dependencies
RUN pip install --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r /app/requirements.txt

# Copy application code
COPY . /app

# Make run script executable
RUN chmod +x /app/run.sh

# Expose Streamlit port
EXPOSE 8501

# Environment variables
ENV PYTHONUNBUFFERED=1
ENV CASSANDRA_HOST=cassandra
ENV STREAM_DELAY=0.5
ENV MAX_ROWS=50000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:8501/_stcore/health || exit 1

# Start application
CMD ["/app/run.sh"]
