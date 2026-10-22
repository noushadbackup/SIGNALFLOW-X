#!/usr/bin/env bash
# One-shot Superset initialisation: metadata database, schema migrations, admin user, roles. Idempotent.
set -euo pipefail
python - <<'PY'
import os
import psycopg2

conn = psycopg2.connect(host="postgres", dbname="postgres", user=os.environ["PGUSER"], password=os.environ["PGPASSWORD"])
conn.autocommit = True
cur = conn.cursor()
cur.execute("SELECT 1 FROM pg_database WHERE datname = 'signalflow_superset'")
if cur.fetchone() is None:
    cur.execute("CREATE DATABASE signalflow_superset")
PY
superset db upgrade
superset fab create-admin --username admin --firstname SignalFlow --lastname Admin \
  --email admin@signalflow.invalid --password "${SUPERSET_ADMIN_PASSWORD}" || true   # exists on re-runs
superset init
