# Shared image used by every service in docker-compose.yml.
# Kept intentionally simple: one image, different commands per service.
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# System deps: build tools for psutil/duckdb wheels fallback.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps first for layer caching.
COPY pyproject.toml ./
RUN pip install --upgrade pip && pip install ".[dev]"

# Project source.
COPY . .

# Dagster needs a writable home for run/event storage.
ENV DAGSTER_HOME=/opt/dagster/dagster_home
RUN mkdir -p /opt/dagster/dagster_home

EXPOSE 3000 8501

CMD ["python", "-c", "print('Specify a command in docker-compose.yml')"]
