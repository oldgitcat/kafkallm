# Standalone GUI image. The build stage installs the application and its
# dependencies into /deps; the runtime stage is distroless Python 3.13.
FROM docker.io/library/python:3.13-slim-bookworm@sha256:f3dac913ab53b78c64f26d5518fcd7043d5a1a46522d6a493b598c3ed12f62a6 AS build

WORKDIR /app

COPY pyproject.toml README.md constraints.txt ./
COPY src ./src
COPY tools ./tools

RUN python -m pip install --no-cache-dir --target /deps \
        -c constraints.txt .

FROM gcr.io/distroless/python3-debian13@sha256:973adebb0daedf75c0542081edea2d45bd60ba413a9522011b478523f76b0d80

COPY --from=build /deps /deps
COPY gaense/agents /app/gaense/agents

ENV PYTHONPATH=/deps \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HOME=/tmp

WORKDIR /app

USER 10001:10001

ENTRYPOINT ["/usr/bin/python3", "-m", "kafkallm.cli", "gui"]
