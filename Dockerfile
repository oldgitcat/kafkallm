FROM python:3.12-slim-bookworm

LABEL org.opencontainers.image.title="Kafkallm" \
      org.opencontainers.image.description="Kafka-backed goose jobs in isolated microVMs" \
      org.opencontainers.image.source="https://github.com/oldgitcat/kafkallm" \
      org.opencontainers.image.licenses="Apache-2.0"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
COPY agents ./agents

RUN pip install --no-cache-dir . \
    && useradd --create-home --uid 10001 appuser

USER appuser

CMD ["kafkallm", "worker"]
