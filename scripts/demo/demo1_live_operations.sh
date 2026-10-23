#!/usr/bin/env bash
# Demo 1: normal operations with continuously updating data (lite profile).
#
# Builds a fresh database (default signalflow_live), then feeds the arrival files one at a time: each file is loaded
# into Bronze, validated into Silver, the Leak Hunter runs, and Gold is rebuilt every few files. Open the control
# tower against the same database and turn on "Live refresh" in the sidebar to watch the numbers move.
#
#   scripts/demo/demo1_live_operations.sh            # 6 days, 1,500 subscribers, one file every 2 s
#   PACE=1 GOLD_EVERY=10 scripts/demo/demo1_live_operations.sh
#
# Then, in a second terminal:   make ui PGDATABASE=signalflow_live
set -euo pipefail
cd "$(dirname "$0")/../.."
. scripts/demo/_env.sh

DB="${SIGNALFLOW_LIVE_DB:-signalflow_live}"
DATA="${DATA:-data/live}"
PACE="${PACE:-2}"
GOLD_EVERY="${GOLD_EVERY:-6}"
PY=.venv/bin/python

# The live database is dropped and recreated, so refuse any name the platform uses for real work.
"$PY" - "$DB" <<'EOF'
import sys
import psycopg
from psycopg import sql
from signalflow.chaos.lab import protected_databases
name = sys.argv[1]
if name in protected_databases() or "live" not in name or not name.replace("_", "").isalnum():
    sys.exit(f"refusing to recreate {name!r}: the demo database name must contain 'live' and must not be a "
             "database the platform uses")
with psycopg.connect(dbname="postgres", autocommit=True) as admin:
    admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name)))
    admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
print(f"recreated database {name}")
EOF

.venv/bin/signalflow generate --out "$DATA" --days "${DAYS:-6}" --subscribers "${SUBSCRIBERS:-1500}" --overwrite >/dev/null
echo "dataset: $(ls "$DATA"/arrivals | wc -l) arrival files in $DATA"
echo "control tower:  make ui PGDATABASE=$DB   (then toggle 'Live refresh' in the sidebar)"
echo "feeding one arrival file every ${PACE}s, Gold rebuilt every ${GOLD_EVERY} files; Ctrl-C stops the feed"
PGDATABASE="$DB" .venv/bin/signalflow run --data "$DATA" --pace-seconds "$PACE" --gold-every "$GOLD_EVERY"
