FROM docker.io/library/python:3.12-slim-bookworm@sha256:0f086103b026048cc50cb5da25ec435c023c5241a83fc44daea593377abc61c4

LABEL org.opencontainers.image.title="Kafkallm" \
      org.opencontainers.image.description="Kafka-backed goose jobs in isolated microVMs" \
      org.opencontainers.image.source="https://github.com/oldgitcat/kafkallm"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md constraints.txt ./
COPY src ./src
COPY tools ./tools
COPY gaense/agents ./gaense/agents

RUN pip install --no-cache-dir -c constraints.txt . \
    && useradd --create-home --uid 10001 appuser

USER appuser

CMD ["kafkallm", "worker"]
