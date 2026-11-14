FROM docker.io/library/python:3.13-slim-bookworm@sha256:f3dac913ab53b78c64f26d5518fcd7043d5a1a46522d6a493b598c3ed12f62a6

LABEL org.opencontainers.image.title="Kafkallm" \
      org.opencontainers.image.description="Kafka-backed goose jobs in isolated microVMs" \
      org.opencontainers.image.source="https://github.com/oldgitcat/kafkallm"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md constraints.txt ./
COPY src ./src
COPY gaense/agents ./gaense/agents

RUN pip install --no-cache-dir -c constraints.txt . \
    && useradd --create-home --uid 10001 appuser

USER appuser

CMD ["kafkallm", "worker"]
