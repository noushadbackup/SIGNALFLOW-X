# Sourced by the demo scripts: load .env without overriding what the caller set, so that
# `PGDATABASE=signalflow_live scripts/demo/demo2_revenue_leak.sh` really uses signalflow_live.
_caller_env=$(export -p)
set -a; [ -f .env ] && . ./.env; set +a
eval "$_caller_env"
unset _caller_env
