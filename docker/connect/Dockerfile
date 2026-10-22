# Kafka Connect (from the same Apache Kafka 4.1.1 image as the broker) with the Debezium PostgreSQL connector plugin.
# Debezium's own 3.x images are published only on quay.io; building from the official Kafka image plus the plugin
# tarball from Maven Central gives the same connector on a Connect runtime that matches the broker version.
# The tarball is fetched and SHA-1 verified by scripts/fetch_artifacts.sh before the build (see ADR-0005).
ARG KAFKA_IMAGE=mirror.gcr.io/apache/kafka:4.1.1
FROM ${KAFKA_IMAGE}

ARG DEBEZIUM_VERSION=3.6.3.Final
USER root
RUN mkdir -p /opt/connect/plugins && chown -R appuser /opt/connect
COPY .cache/artifacts/debezium/debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz /tmp/debezium.tgz
RUN tar -xzf /tmp/debezium.tgz -C /opt/connect/plugins && rm /tmp/debezium.tgz && chown -R appuser /opt/connect
COPY docker/connect/worker.properties /opt/connect/worker.properties
USER appuser
EXPOSE 8083
ENTRYPOINT ["/opt/kafka/bin/connect-distributed.sh", "/opt/connect/worker.properties"]
