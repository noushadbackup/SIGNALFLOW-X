# syntax=docker/dockerfile:1
# Superset 5.0 ("lean" image) plus the two drivers it needs here: Trino (the lake) and psycopg2 (Superset's own
# metadata database in Postgres).
ARG SUPERSET_IMAGE=mirror.gcr.io/apache/superset:5.0.0
FROM ${SUPERSET_IMAGE}
USER root
# The optional build secret is a CA bundle for networks with a TLS-intercepting proxy (never stored in a layer).
RUN --mount=type=secret,id=build_ca,required=false \
    if [ -f /run/secrets/build_ca ]; then export SSL_CERT_FILE=/run/secrets/build_ca UV_NATIVE_TLS=1; fi; \
    uv pip install --python /app/.venv/bin/python --no-cache "trino[sqlalchemy]==0.340.0" "psycopg2-binary==2.9.10"
COPY docker/superset/superset_config.py /app/pythonpath/superset_config.py
COPY --chmod=755 docker/superset/init.sh /app/docker/signalflow-init.sh
USER superset
