# Shared image for the four Python services (gateway, ingestor, maintenance, api).
# Non-root, pinned base, slim (design doc section 8: security basics).
FROM python:3.12-slim AS base

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

# librdkafka for confluent-kafka.
RUN apt-get update \
    && apt-get install -y --no-install-recommends librdkafka1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
COPY contracts ./contracts

RUN pip install --upgrade pip \
    && pip install ".[adapters,api]"

# Drop privileges.
RUN useradd --create-home --uid 10001 appuser
USER appuser

# Default command is overridden per service in docker-compose.yml.
CMD ["python", "-m", "mini_lakehouse.ingestor"]
