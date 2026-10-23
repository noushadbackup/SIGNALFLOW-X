# syntax=docker/dockerfile:1
# The metrics exporter only needs psycopg: no dbt, Spark or Trino client in this image.
ARG PYTHON_IMAGE=mirror.gcr.io/library/python:3.12-slim
FROM ${PYTHON_IMAGE}
RUN --mount=type=secret,id=build_ca,required=false \
    if [ -f /run/secrets/build_ca ]; then export PIP_CERT=/run/secrets/build_ca; fi; \
    pip install --no-cache-dir "psycopg[binary]==3.2.13" \
    && useradd --uid 10001 --no-create-home exporter
WORKDIR /app
COPY src/signalflow/__init__.py src/signalflow/log.py ./signalflow/
COPY src/signalflow/observability ./signalflow/observability
USER exporter
ENV PYTHONPATH=/app SIGNALFLOW_EXPORTER_HOST=0.0.0.0
EXPOSE 9101
ENTRYPOINT ["python", "-m", "signalflow.observability.exporter"]
