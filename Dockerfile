# ---------------------------------------------------------------------------
# Dockerfile
# PySpark Property Search Pipeline
# ---------------------------------------------------------------------------

# Base image: Python 3.11 slim
FROM python:3.11-slim

# ---------------------------------------------------------------------------
# System dependencies — Java is required to run PySpark
# ---------------------------------------------------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------------
# Java environment
# ---------------------------------------------------------------------------
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# ---------------------------------------------------------------------------
# Working directory inside the container
# ---------------------------------------------------------------------------
WORKDIR /app

# ---------------------------------------------------------------------------
# Install Python dependencies first (Docker layer cache optimisation)
# Copying requirements separately means this layer is only rebuilt
# when requirements.txt changes, not on every code change.
# ---------------------------------------------------------------------------
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ---------------------------------------------------------------------------
# Copy project source code
# ---------------------------------------------------------------------------
COPY main.py       .
COPY config.py     .
COPY test_main.py  .
COPY pytest.ini    .

# ---------------------------------------------------------------------------
# Create directories that the pipeline writes to at runtime
# ---------------------------------------------------------------------------
RUN mkdir -p data/output/final_output \
             data/output/unmatched_details \
             logs

# ---------------------------------------------------------------------------
# Default command — run the pipeline
# Override at runtime to run tests instead (see docker-compose.yml)
# ---------------------------------------------------------------------------
CMD ["python3", "main.py"]