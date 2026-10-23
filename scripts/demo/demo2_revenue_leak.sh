#!/usr/bin/env bash
# Demo 2: telecom revenue discrepancy, from detection to an audited analyst decision.
#
# Uses the demo database (PGDATABASE, built by `make generate run`). Steps:
#   1. run the Leak Hunter and list the telecom cases it holds;
#   2. open the most severe confirmed telecom case: rule, evidence, raw Bronze records, case history;
#   3. as an analyst: add a note, assign the case and resolve it (each action is versioned and audited);
#   4. evaluate the rules offline against the generator's scenario ledger (precision/recall per rule).
#
#   ANALYST=nadia scripts/demo/demo2_revenue_leak.sh
# The analyst's password is read from SIGNALFLOW_OPERATOR_PASSWORD or prompted for; create the account first with
#   make operator OPERATOR=nadia ROLE=analyst NAME="Nadia Rahman"
set -euo pipefail
cd "$(dirname "$0")/../.."
. scripts/demo/_env.sh

DATA="${DATA:-data/seed7}"
ANALYST="${ANALYST:-}"
SF=.venv/bin/signalflow
step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
if [ -n "$ANALYST" ] && [ -z "${SIGNALFLOW_OPERATOR_PASSWORD:-}" ]; then
  read -rsp "password for $ANALYST: " SIGNALFLOW_OPERATOR_PASSWORD; echo
  export SIGNALFLOW_OPERATOR_PASSWORD
fi

step "1. Leak Hunter run, then the telecom cases"
$SF assurance hunt
$SF assurance cases --rule TEL-01 --limit 5
$SF assurance cases --rule TEL-02 --limit 5

step "2. The most severe confirmed telecom case"
CASE=$($SF assurance cases --status CONFIRMED_BY_RULE --limit 200 | .venv/bin/python -c '
import json, sys
cases = sorted((c for c in json.load(sys.stdin)["cases"] if c["rule_id"].startswith("TEL-")),
               key=lambda c: (c["rule_id"] != "TEL-01", -float(c["exposure_bdt"] or 0)))
print(cases[0]["case_id"] if cases else "")')
if [ -z "$CASE" ]; then echo "no confirmed telecom case yet (run make run first)"; exit 1; fi
$SF assurance show --case "$CASE"

if [ -n "$ANALYST" ]; then
  step "3. Analyst actions on $CASE (audited)"
  $SF assurance note --case "$CASE" --user "$ANALYST" --note "Evidence checked against the raw Bronze records; raised with the owning team."
  $SF assurance assign --case "$CASE" --user "$ANALYST" --assignee "$ANALYST"
  $SF assurance resolve --case "$CASE" --user "$ANALYST" --resolution RECOVERED \
      --note "Charge re-issued by the billing team (demo)."
  $SF assurance show --case "$CASE"
else
  step "3. Skipped analyst actions (set ANALYST=<username> to run them)"
fi

step "4. Offline evaluation against the scenario ledger"
.venv/bin/python -m signalflow_eval.leak_hunter --data "$DATA"
