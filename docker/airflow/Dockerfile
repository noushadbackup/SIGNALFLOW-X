# syntax=docker/dockerfile:1
# Airflow 3.1 with the SignalFlow CLI in its own virtualenv. The CLI pulls in dbt, PySpark and the Trino client,
# whose pins would fight Airflow's constraints if installed into Airflow's environment, so tasks call
# /opt/signalflow/venv/bin/signalflow instead (the DAGs read it from SIGNALFLOW_CLI).
ARG AIRFLOW_IMAGE=mirror.gcr.io/apache/airflow:3.1.0-python3.12
ARG JRE_IMAGE=mirror.gcr.io/library/eclipse-temurin:17-jre
FROM ${JRE_IMAGE} AS jre

FROM ${AIRFLOW_IMAGE}
USER root
COPY --from=jre /opt/java/openjdk /opt/java/openjdk
RUN mkdir -p /opt/signalflow /opt/airflow/auth && chown airflow: /opt/signalflow /opt/airflow/auth
USER airflow
ENV JAVA_HOME=/opt/java/openjdk \
    PATH=/opt/java/openjdk/bin:$PATH
WORKDIR /opt/signalflow
COPY --chown=airflow pyproject.toml README.md ./
COPY --chown=airflow src ./src
COPY --chown=airflow contracts ./contracts
COPY --chown=airflow sql ./sql
COPY --chown=airflow dbt ./dbt
# The optional build secret is a CA bundle for networks with a TLS-intercepting proxy (never stored in a layer).
RUN --mount=type=secret,id=build_ca,required=false,uid=50000 \
    if [ -f /run/secrets/build_ca ]; then export PIP_CERT=/run/secrets/build_ca; fi; \
    python -m venv /opt/signalflow/venv \
    && /opt/signalflow/venv/bin/pip install --no-cache-dir -e ".[lake]" \
    && rm -rf dbt/target dbt/logs
COPY --chown=airflow .cache/artifacts/lake-jars /opt/signalflow/lake-jars
COPY --chown=airflow airflow/dags /opt/airflow/dags
ENV SIGNALFLOW_CLI=/opt/signalflow/venv/bin/signalflow \
    SIGNALFLOW_LAKE_JARS=/opt/signalflow/lake-jars \
    PYSPARK_PYTHON=/opt/signalflow/venv/bin/python \
    PATH=/opt/signalflow/venv/bin:/opt/java/openjdk/bin:$PATH
WORKDIR /opt/airflow
