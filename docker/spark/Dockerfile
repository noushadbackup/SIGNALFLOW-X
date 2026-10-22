# syntax=docker/dockerfile:1
# Spark Structured Streaming and lake jobs (local mode). PySpark from PyPI bundles Spark 4.0.3; the connector jars
# are fetched and checksum-verified by scripts/fetch_artifacts.sh before the build.
# The JRE is copied from the official Temurin image instead of apt-get, so the build needs only the Python index.
ARG PYTHON_IMAGE=mirror.gcr.io/library/python:3.12-slim-bookworm
ARG JRE_IMAGE=mirror.gcr.io/library/eclipse-temurin:17-jre
FROM ${JRE_IMAGE} AS jre

FROM ${PYTHON_IMAGE}
COPY --from=jre /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk \
    PATH=/opt/java/openjdk/bin:$PATH
RUN useradd --create-home --uid 1000 app && mkdir /state && chown app /state
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
COPY contracts ./contracts
COPY sql ./sql
# Editable install keeps REPO_ROOT at /app so the contract registry is found. The optional build secret is a CA
# bundle for networks with a TLS-intercepting proxy; it is mounted for this step only and never stored in a layer.
RUN --mount=type=secret,id=build_ca,required=false \
    if [ -f /run/secrets/build_ca ]; then export PIP_CERT=/run/secrets/build_ca; fi; \
    pip install --no-cache-dir -e ".[stream]"
COPY .cache/artifacts/jars /opt/spark-jars
ENV SIGNALFLOW_SPARK_JARS=/opt/spark-jars \
    PYSPARK_PYTHON=/usr/local/bin/python
USER app
ENTRYPOINT ["python", "-m", "signalflow.streaming.spark_silver"]
