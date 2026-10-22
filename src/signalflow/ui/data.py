"""Read-only queries behind the control tower. Each function maps to one panel; no number is computed in the UI
that is not in the database."""

from __future__ import annotations

from typing import Any

import pandas as pd
import psycopg

from ..db import connect


def query(sql: str, params: dict[str, Any] | None = None) -> pd.DataFrame:
    with connect() as conn, conn.cursor() as cur:
        cur.execute(sql, params or {})
        cols = [d.name for d in cur.description or []]
        return pd.DataFrame(cur.fetchall(), columns=cols)


def table_exists(name: str) -> bool:
    try:
        return bool(query("SELECT to_regclass(%(n)s) IS NOT NULL AS ok", {"n": name}).iloc[0]["ok"])
    except psycopg.OperationalError:
        return False


PIPELINE_COUNTS = """
SELECT (SELECT count(*) FROM bronze.raw_events) AS bronze_rows,
       (SELECT count(*) FROM silver.events) AS silver_events,
       (SELECT count(*) FROM silver.quarantine) AS quarantined,
       (SELECT count(*) FROM ops.duplicate_events) AS duplicates,
       (SELECT count(*) FROM bronze.raw_events
         WHERE bronze_id > coalesce((SELECT max(last_bronze_id) FROM ops.checkpoints), 0)) AS pending,
       (SELECT max(finished_at) FROM ops.processing_runs) AS last_batch_at
"""

STREAMS = """
SELECT stream, max_event_time, watermark, allowed_lateness::text AS allowed_lateness, updated_at
FROM ops.stream_state ORDER BY stream
"""

QUARANTINE_BY_REASON = "SELECT reason_code, count(*) AS records FROM silver.quarantine GROUP BY 1 ORDER BY 2 DESC"

QUARANTINE_SAMPLE = """
SELECT q.bronze_id, q.reason_code, q.event_type, q.details::text AS details, b.source_position, q.quarantined_at
FROM silver.quarantine q JOIN bronze.raw_events b USING (bronze_id) ORDER BY q.bronze_id
"""

ARRIVAL_CLASSES = """
SELECT stream, arrival_class, count(*) AS events FROM silver.events GROUP BY 1, 2 ORDER BY 1, 2
"""

PROCESSING_RUNS = """
SELECT run_id, to_bronze_id, rows_read, rows_accepted, rows_quarantined, rows_duplicate,
       extract(epoch FROM finished_at - started_at) * 1000 AS duration_ms, finished_at
FROM ops.processing_runs ORDER BY run_id
"""

RESTATEMENTS = """
SELECT stream, business_date, late_event_count, last_detected_at FROM ops.kpi_restatements ORDER BY 1, 2
"""

REVENUE_DAILY = """
SELECT business_date, channel, sum(gross_recharge_bdt) AS gross_bdt, sum(successful_recharges) AS successful,
       sum(failed_recharges) AS failed, min(kpi_status) AS kpi_status
FROM gold.mart_telecom_revenue_daily GROUP BY 1, 2 ORDER BY 1, 2
"""

REVENUE_BY_REGION = """
SELECT region, sum(gross_recharge_bdt) AS gross_bdt, sum(successful_recharges) AS successful
FROM gold.mart_telecom_revenue_daily GROUP BY 1 ORDER BY 2 DESC
"""

DAY_STATUS = """
SELECT business_date, max(kpi_status) AS kpi_status, sum(gross_recharge_bdt) AS gross_bdt,
       sum(successful_recharges) AS successful, sum(failed_recharges) AS failed,
       round(sum(successful_recharges)::numeric / nullif(sum(recharge_attempts), 0), 4) AS success_rate
FROM gold.mart_telecom_revenue_daily GROUP BY 1 ORDER BY 1
"""

ACTIVE_SUBSCRIBERS = """
SELECT business_date, home_region, sum(active_subscribers) AS active
FROM gold.mart_telecom_subscribers_daily GROUP BY 1, 2 ORDER BY 1, 2
"""

USAGE_BY_REGION = """
SELECT serving_region, sum(total_quantity) / 1024.0 AS data_gb
FROM gold.mart_telecom_usage_daily WHERE usage_type = 'DATA' GROUP BY 1 ORDER BY 2 DESC
"""

INVENTORY = """
SELECT warehouse_id, sku, category, on_hand_qty, outbound_7d_qty, avg_daily_outbound, days_of_cover, stockout_risk,
       unauthorized_adjustments
FROM gold.mart_inventory_position ORDER BY days_of_cover NULLS LAST, warehouse_id, sku
"""

MFG_SALES = """
SELECT business_date, category, sum(ordered_value_bdt) AS ordered_bdt, sum(orders) AS orders,
       sum(backordered_orders) AS backordered, min(kpi_status) AS kpi_status
FROM gold.mart_mfg_sales_daily GROUP BY 1, 2 ORDER BY 1, 2
"""

PRODUCTION = """
SELECT business_date, factory_id, sum(units) AS units, max(disruption_reason) AS disruption
FROM gold.mart_mfg_production_daily GROUP BY 1, 2 ORDER BY 1, 2
"""

INVOICES = """
SELECT settlement_status, count(*) AS invoices, sum(amount_bdt) AS invoiced_bdt, sum(outstanding_bdt) AS outstanding_bdt
FROM gold.fct_invoice GROUP BY 1 ORDER BY 1
"""

SIGNALS = """
SELECT 'Channel settlement differs from expected' AS signal, 'telecom' AS domain, count(*) AS records,
       coalesce(sum(abs(difference_bdt)), 0) AS amount_bdt
FROM gold.fct_channel_settlement WHERE difference_bdt <> 0
UNION ALL
SELECT 'Successful recharge with no billing record', 'telecom', count(*), coalesce(sum(amount_bdt), 0)
FROM gold.fct_recharge WHERE is_success AND billing_records = 0
UNION ALL
SELECT 'Bundle activated, payment capture failed', 'telecom', count(*), coalesce(sum(price_bdt), 0)
FROM gold.fct_package_purchase WHERE activation_status = 'ACTIVATED' AND capture_status = 'FAILED'
UNION ALL
SELECT 'Invoice partially paid', 'manufacturing', count(*), coalesce(sum(outstanding_bdt), 0)
FROM gold.fct_invoice WHERE settlement_status = 'PARTIALLY_PAID'
UNION ALL
SELECT 'Return refunded more than once', 'manufacturing', count(*), coalesce(sum(refunded_bdt - approved_refund_bdt), 0)
FROM gold.fct_return WHERE refund_count > 1
UNION ALL
SELECT 'Inventory adjustment without approval', 'manufacturing', count(*), NULL
FROM gold.fct_inventory_movement WHERE authorization_status = 'NO_APPROVAL_FOUND'
"""


# --- lakehouse (Phase 3) -------------------------------------------------------------------------------------------
LAKE_PUBLISH_RUNS = """
SELECT published_at, table_name, mode, rows_written, hwm_before, hwm_after, skipped, seconds
FROM ops.lake_publish_runs ORDER BY run_id DESC LIMIT 60
"""

LAKE_LAST_CHECK = """
SELECT check_id, model, rows_postgres, rows_lake, identical, checked_at
FROM ops.lake_differential_checks
WHERE check_id = (SELECT max(check_id) FROM ops.lake_differential_checks)
ORDER BY model
"""


def lake_query(sql: str) -> pd.DataFrame:
    """Query Trino (catalog `lake`). Raises if Trino is not running; the page says so."""
    from ..lakehouse.settings import LakeSettings
    from ..lakehouse.trino_sql import query as trino_query

    cols, rows = trino_query(LakeSettings.from_env(), sql)
    return pd.DataFrame(rows, columns=cols)


def lake_tables() -> pd.DataFrame:
    from ..lakehouse.settings import LakeSettings
    from ..lakehouse.tables import TABLES

    settings = LakeSettings.from_env()
    parts = [f"""SELECT '{t.key}' AS table_name, '{t.mode}' AS mode,
                    (SELECT count(*) FROM lake.{settings.namespace(t.layer)}."{t.name}") AS rows,
                    count(*) AS snapshots, max(committed_at) AS last_commit
                 FROM lake.{settings.namespace(t.layer)}."{t.name}$snapshots" """ for t in TABLES]
    return lake_query(" UNION ALL ".join(parts) + " ORDER BY 1")


def lake_snapshots(layer: str, table: str) -> pd.DataFrame:
    from ..lakehouse.settings import LakeSettings

    ns = LakeSettings.from_env().namespace(layer)
    return lake_query(f"""
        SELECT committed_at, snapshot_id, operation,
               CAST(coalesce(element_at(summary, 'added-records'), '0') AS bigint) AS added_records,
               CAST(coalesce(element_at(summary, 'total-records'), '0') AS bigint) AS total_records,
               element_at(summary, 'signalflow.hwm') AS hwm
        FROM lake.{ns}."{table}$snapshots" ORDER BY committed_at""")
