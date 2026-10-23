#!/usr/bin/env bash
# Demo 3: processing failure, recovery, replay and correctness verification (lite profile, lab database).
#
#   1. Chaos Lab incident D: the Silver processor is killed with SIGKILL mid-backlog, restarted, and the result is
#      compared with a clean run of the same data (no row past the checkpoint, nothing lost or doubled).
#   2. Chaos Lab incident B: a late surge arrives after the watermark; KPIs are restated and Gold rebuilt.
#   3. Data Time Machine: damage Silver in the lab database, replay the window from Bronze into an isolated preview,
#      promote it in one transaction, rebuild the affected Gold models and reconcile.
#
#   OPERATOR=tanvir scripts/demo/demo3_failure_recovery.sh
# The operator's password is read from SIGNALFLOW_OPERATOR_PASSWORD or prompted for; create the account with
#   make operator OPERATOR=tanvir ROLE=operator NAME="Tanvir Ahmed"
# Everything destructive happens in the lab database (SIGNALFLOW_LAB_DB, default signalflow_lab), never in the demo
# database.
set -euo pipefail
cd "$(dirname "$0")/../.."
. scripts/demo/_env.sh

OPERATOR="${OPERATOR:?set OPERATOR to an account with the operator role}"
LAB="${SIGNALFLOW_LAB_DB:-signalflow_lab}"
SF=.venv/bin/signalflow
step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
if [ -z "${SIGNALFLOW_OPERATOR_PASSWORD:-}" ]; then
  read -rsp "password for $OPERATOR: " SIGNALFLOW_OPERATOR_PASSWORD; echo
  export SIGNALFLOW_OPERATOR_PASSWORD
fi

step "1. Incident D: SIGKILL the Silver processor mid-backlog, restart, verify against a clean run"
$SF chaos run D --user "$OPERATOR"

step "2. Incident B: late surge after the watermark, restatement, Gold rebuilt"
$SF chaos run B --user "$OPERATOR"
$SF chaos list --limit 5

# The lab now holds the dataset as the last incident left it. Chaos Lab rebuilds it on every run, so the
# operator account is added there again before the Time Machine steps.
step "3. Time Machine on the lab database"
SIGNALFLOW_NEW_OPERATOR_PASSWORD="$SIGNALFLOW_OPERATOR_PASSWORD" \
    $SF operators add "$OPERATOR" --database "$LAB" --role operator --name "$OPERATOR (lab)" >/dev/null
START="2026-10-02T00:00:00+06:00"; END="2026-10-03T00:00:00+06:00"
$SF replay defect --database "$LAB" --stream telecom --start "$START" --end "$END" --kind inflate_amounts --rows 20 \
    --user "$OPERATOR"
# A faulty release would also have built Gold from the damaged rows; do the same so the repair shows a Gold diff.
PGDATABASE="$LAB" .venv/bin/dbt run --project-dir dbt --profiles-dir dbt --quiet \
    --select fct_recharge mart_telecom_revenue_daily
$SF replay inspect --database "$LAB" --stream telecom --start "$START" --end "$END"
PREVIEW=$($SF replay preview --database "$LAB" --stream telecom --start "$START" --end "$END" --user "$OPERATOR")
echo "$PREVIEW"
REPLAY=$(echo "$PREVIEW" | .venv/bin/python -c 'import json, sys; print(json.load(sys.stdin)["replay_id"])')
$SF replay promote --database "$LAB" --replay "$REPLAY" --user "$OPERATOR"
$SF replay list --database "$LAB" --limit 3
echo
echo "Open the control tower (make ui): Chaos Lab shows both incident reports, Time Machine shows replay $REPLAY."
