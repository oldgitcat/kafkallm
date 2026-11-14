# Allowlisting proxy for the isolated job network. This is a separate,
# minimal distroless Python image. It runs as a normal container next to the
# job microVM and forwards only the Goose gateway endpoints.
FROM gcr.io/distroless/python3-debian12@sha256:0f8ca62dea61023c1fe02e445bd154ee02b2d97aec377f965ae3c641ec838e61

WORKDIR /app

COPY infra/llm-proxy/llm_proxy.py /app/llm_proxy.py

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

USER 65532:65532

ENTRYPOINT ["/usr/bin/python3", "/app/llm_proxy.py"]
