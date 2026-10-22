"""Superset settings for the local SignalFlow X stack. Everything secret comes from the environment."""

import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://{os.environ['PGUSER']}:{os.environ['PGPASSWORD']}@postgres:5432/signalflow_superset"
)
# Local single-node deployment behind 127.0.0.1: no Celery, no async queries, a simple in-process cache.
CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300}
DATA_CACHE_CONFIG = CACHE_CONFIG
ROW_LIMIT = 50_000
FEATURE_FLAGS = {"DASHBOARD_RBAC": False, "ENABLE_TEMPLATE_PROCESSING": False}

# The validated SignalFlow categorical palette (same order as the control tower).
EXTRA_CATEGORICAL_COLOR_SCHEMES = [{
    "id": "signalflow",
    "description": "SignalFlow X categorical palette",
    "label": "SignalFlow X",
    "isDefault": True,
    "colors": ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"],
}]
