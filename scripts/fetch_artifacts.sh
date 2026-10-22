#!/usr/bin/env bash
# Download the pinned JVM artifacts the stream and lake profiles need, verifying each against Maven Central's SHA-1.
# Retries with backoff because Maven Central rate-limits (HTTP 429). Idempotent: verified files are skipped.
set -euo pipefail

DEST="${1:-.cache/artifacts}"
MAVEN="${MAVEN_MIRROR:-https://repo1.maven.org/maven2}"
mkdir -p "$DEST/jars" "$DEST/lake-jars" "$DEST/debezium"

SPARK_VERSION=4.0.3
KAFKA_CLIENTS_VERSION=3.9.1        # the version spark-sql-kafka 4.0.3 is built against
DEBEZIUM_VERSION=3.6.3.Final
ICEBERG_VERSION=1.10.1             # runtime for Spark 4.0 / Scala 2.13 (ADR-0005, ADR-0009)
POSTGRES_JDBC_VERSION=42.7.8

JARS=(
  "org/apache/spark/spark-sql-kafka-0-10_2.13/$SPARK_VERSION/spark-sql-kafka-0-10_2.13-$SPARK_VERSION.jar"
  "org/apache/spark/spark-token-provider-kafka-0-10_2.13/$SPARK_VERSION/spark-token-provider-kafka-0-10_2.13-$SPARK_VERSION.jar"
  "org/apache/kafka/kafka-clients/$KAFKA_CLIENTS_VERSION/kafka-clients-$KAFKA_CLIENTS_VERSION.jar"
  "org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar"
)
LAKE_JARS=(
  "org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/$ICEBERG_VERSION/iceberg-spark-runtime-4.0_2.13-$ICEBERG_VERSION.jar"
  "org/apache/iceberg/iceberg-aws-bundle/$ICEBERG_VERSION/iceberg-aws-bundle-$ICEBERG_VERSION.jar"
  "org/postgresql/postgresql/$POSTGRES_JDBC_VERSION/postgresql-$POSTGRES_JDBC_VERSION.jar"
)
DEBEZIUM="io/debezium/debezium-connector-postgres/$DEBEZIUM_VERSION/debezium-connector-postgres-$DEBEZIUM_VERSION-plugin.tar.gz"

fetch() {  # fetch <maven path> <target file>
  local path="$1" out="$2" delay=2
  if [[ -f "$out" && -f "$out.sha1" ]] && [[ "$(sha1sum "$out" | cut -d' ' -f1)" == "$(cut -c1-40 "$out.sha1")" ]]; then
    return 0
  fi
  for attempt in 1 2 3 4 5 6; do
    if curl -fsSL "$MAVEN/$path.sha1" -o "$out.sha1" && curl -fsSL "$MAVEN/$path" -o "$out.part"; then
      if [[ "$(sha1sum "$out.part" | cut -d' ' -f1)" == "$(cut -c1-40 "$out.sha1")" ]]; then
        mv "$out.part" "$out"; echo "fetched $(basename "$out")"; return 0
      fi
      echo "checksum mismatch for $path" >&2
    fi
    echo "retry $attempt for $path in ${delay}s" >&2; sleep "$delay"; delay=$((delay * 2))
  done
  echo "failed to fetch $path" >&2; return 1
}

for j in "${JARS[@]}"; do fetch "$j" "$DEST/jars/$(basename "$j")"; done
for j in "${LAKE_JARS[@]}"; do fetch "$j" "$DEST/lake-jars/$(basename "$j")"; done
fetch "$DEBEZIUM" "$DEST/debezium/$(basename "$DEBEZIUM")"
echo "artifacts ready in $DEST"
