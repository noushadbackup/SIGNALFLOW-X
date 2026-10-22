# Spark Structured Streaming job (local mode) for the `stream` profile. PySpark from PyPI bundles Spark 4.0.3;
# the Kafka connector jars are fetched and checksum-verified by scripts/fetch_artifacts.sh before the build.
ARG PYTHON_IMAGE=mirror.gcr.io/library/python:3.12-slim-bookworm
FROM ${PYTHON_IMAGE}

RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*
RUN useradd --create-home --uid 1000 app && mkdir /state && chown app /state
WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
COPY contracts ./contracts
COPY sql ./sql
# Editable install keeps REPO_ROOT at /app so the contract registry is found.
RUN pip install --no-cache-dir -e ".[stream]"
COPY .cache/artifacts/jars /opt/spark-jars
ENV SIGNALFLOW_SPARK_JARS=/opt/spark-jars \
    PYSPARK_PYTHON=/usr/local/bin/python
USER app
ENTRYPOINT ["python", "-m", "signalflow.streaming.spark_silver"]
