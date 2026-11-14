# MLflow tracking server with the built-in AI Gateway and Postgres backend.
#
# MLflow 3.18 ships the gateway inside `mlflow server`; the endpoint is stored
# in the tracking database. This image only adds the PostgreSQL driver, the
# idempotent endpoint bootstrap, and an opt-in TLS verification bypass.
FROM ghcr.io/mlflow/mlflow:v3.18.0@sha256:19f9de44741318515fc3a992429f177420c32d40d971a25f6ae4621a1b9c8516

USER root
RUN pip install --no-cache-dir psycopg2-binary==2.9.10

ENV PYTHONPATH=/opt/gaensestall \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY infra/mlflow/bootstrap_gateway.py /opt/gaensestall/bootstrap_gateway.py
COPY infra/mlflow/sitecustomize.py /opt/gaensestall/sitecustomize.py
COPY infra/mlflow/entrypoint.sh /opt/gaensestall/entrypoint.sh
RUN chmod 755 /opt/gaensestall/entrypoint.sh

WORKDIR /mlflow
ENTRYPOINT ["/opt/gaensestall/entrypoint.sh"]
