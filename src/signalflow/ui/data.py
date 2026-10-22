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


# --- Revenue Leak Hunter (Phase 4) ---------------------------------------------------------------------------------
LEAK_LAST_RUN = """
SELECT run_id, as_of ->> 'telecom' AS telecom_clock, as_of ->> 'mfg' AS mfg_clock, finished_at,
       (SELECT count(*) FROM assurance.hunter_runs) AS runs
FROM assurance.hunter_runs ORDER BY run_id DESC LIMIT 1
"""

LEAK_TOTALS = """
SELECT count(*) FILTER (WHERE status = 'PENDING') AS pending,
       count(*) FILTER (WHERE status = 'SUSPICIOUS') AS suspicious,
       count(*) FILTER (WHERE status = 'CONFIRMED_BY_RULE') AS confirmed,
       count(*) FILTER (WHERE resolution = 'AUTO_CLEARED') AS auto_cleared,
       count(*) FILTER (WHERE status = 'RESOLVED' AND resolution <> 'AUTO_CLEARED') AS analyst_resolved,
       coalesce(sum(exposure_bdt) FILTER (WHERE status = 'CONFIRMED_BY_RULE'), 0) AS confirmed_exposure,
       coalesce(sum(exposure_bdt) FILTER (WHERE status = 'SUSPICIOUS'), 0) AS suspicious_exposure,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM resolved_as_of - first_detected_as_of) / 3600)
           FILTER (WHERE resolution = 'AUTO_CLEARED') AS median_hours_to_clear
FROM assurance.cases
"""

# Case state per business date of the anchoring transaction; resolved cases split by who resolved them.
LEAK_TREND = """
SELECT business_date, domain,
       CASE WHEN status <> 'RESOLVED' THEN status
            WHEN resolution = 'AUTO_CLEARED' THEN 'CLEARED_BY_DATA' ELSE 'RESOLVED_BY_ANALYST' END AS state,
       count(*) AS cases
FROM assurance.cases GROUP BY 1, 2, 3 ORDER BY 1, 2, 3
"""

LEAK_BY_UNIT = """
SELECT business_unit, domain, status, count(*) AS cases, sum(exposure_bdt) AS exposure_bdt
FROM assurance.cases WHERE status IN ('SUSPICIOUS', 'CONFIRMED_BY_RULE')
GROUP BY 1, 2, 3 ORDER BY 1
"""

LEAK_RULES = """
SELECT r.rule_id, r.domain, r.title, r.exposure_kind, r.expected_within::text AS expected_within,
       r.confirm_after::text AS confirm_after,
       count(c.case_id) FILTER (WHERE c.status = 'PENDING') AS pending,
       count(c.case_id) FILTER (WHERE c.status = 'SUSPICIOUS') AS suspicious,
       count(c.case_id) FILTER (WHERE c.status = 'CONFIRMED_BY_RULE') AS confirmed,
       count(c.case_id) FILTER (WHERE c.resolution = 'AUTO_CLEARED') AS auto_cleared,
       count(c.case_id) FILTER (WHERE c.status = 'RESOLVED' AND c.resolution <> 'AUTO_CLEARED') AS analyst_resolved,
       coalesce(sum(c.exposure_bdt) FILTER (WHERE c.status IN ('SUSPICIOUS', 'CONFIRMED_BY_RULE')), 0) AS exposure_bdt,
       percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM c.resolved_as_of - c.first_detected_as_of) / 3600)
           FILTER (WHERE c.resolution = 'AUTO_CLEARED') AS median_hours_to_clear,
       max(extract(epoch FROM c.resolved_as_of - c.first_detected_as_of) / 3600)
           FILTER (WHERE c.resolution = 'AUTO_CLEARED') AS max_hours_to_clear
FROM assurance.rules r LEFT JOIN assurance.cases c USING (rule_id)
GROUP BY 1, 2, 3, 4, 5, 6 ORDER BY 1
"""

# Largest lateness actually observed per stream, to check against the late-data horizon.
OBSERVED_LATENESS = """
SELECT stream, max(lateness_seconds) / 3600.0 AS max_lateness_hours, count(*) AS late_events
FROM silver.events WHERE arrival_class <> 'ON_TIME' GROUP BY 1 ORDER BY 1
"""

LEAK_CASES = """
SELECT case_id, rule_id, domain, status, resolution, business_unit, business_date, anchor_time, exposure_bdt,
       summary, assignee, version, first_detected_as_of, confirmed_as_of, resolved_as_of, last_evaluated_as_of
FROM assurance.cases
WHERE (%(domain)s::text IS NULL OR domain = %(domain)s)
  AND (cardinality(%(states)s::text[]) = 0
       OR (CASE WHEN status <> 'RESOLVED' THEN status
                WHEN resolution = 'AUTO_CLEARED' THEN 'CLEARED_BY_DATA' ELSE 'RESOLVED_BY_ANALYST' END)
          = ANY(%(states)s::text[]))
  AND (%(rule)s::text IS NULL OR rule_id = %(rule)s)
ORDER BY CASE status WHEN 'CONFIRMED_BY_RULE' THEN 0 WHEN 'SUSPICIOUS' THEN 1 WHEN 'PENDING' THEN 2 ELSE 3 END,
         exposure_bdt DESC, case_id
LIMIT 500
"""

LEAK_CASE = "SELECT * FROM assurance.cases WHERE case_id = %(case_id)s"

LEAK_CASE_EVENTS = """
SELECT case_event_id, action, from_status, to_status, actor, as_of, at, note, details::text AS details
FROM assurance.case_events WHERE case_id = %(case_id)s ORDER BY case_event_id
"""

# Raw Bronze lines behind the evidence, so an analyst can see exactly what each source system sent.
EVIDENCE_RAW = """
SELECT e.event_id::text AS event_id, e.event_type, e.arrival_class, b.source_stream, b.source_position,
       b.arrival_time, b.raw_record
FROM silver.events e JOIN bronze.raw_events b USING (bronze_id)
WHERE e.event_id = ANY(%(ids)s::uuid[]) ORDER BY e.event_time
"""

LEAK_LAST_EVALUATION = """
SELECT evaluation_id, dataset, results, benign_checks, passed, created_at
FROM assurance.evaluation_runs ORDER BY evaluation_id DESC LIMIT 1
"""

AUDIT_RECENT = """
SELECT at, actor, actor_role, action, target_type, target_id, outcome FROM ops.audit_log
ORDER BY audit_id DESC LIMIT 25
"""

# --- Digital twin (Phase 4): live state from processed events ------------------------------------------------------
TWIN_TELECOM_LAST24H = """
WITH clock AS (SELECT max_event_time AS t FROM ops.stream_state WHERE stream = 'telecom'),
recent AS (
    SELECT date_trunc('hour', e.event_time) AS hour, e.event_type, e.payload ->> 'status' AS status,
           e.payload ->> 'usage_type' AS usage_type,
           e.payload ->> 'amount_bdt' AS amount, e.payload ->> 'quantity' AS qty
    FROM silver.events e, clock
    WHERE e.stream = 'telecom' AND e.event_time > clock.t - interval '24 hours' AND e.event_time <= clock.t
)
SELECT hour,
       count(*) FILTER (WHERE event_type = 'telecom.recharge' AND status = 'SUCCESS') AS recharges,
       count(*) FILTER (WHERE event_type = 'telecom.recharge' AND status = 'FAILED') AS failed,
       coalesce(sum(amount::numeric) FILTER (WHERE event_type = 'telecom.recharge' AND status = 'SUCCESS'), 0)
           AS recharge_bdt,
       coalesce(sum(qty::numeric) FILTER (WHERE event_type = 'telecom.usage_record' AND usage_type = 'DATA'), 0)
           / 1024.0 AS data_gb
FROM recent GROUP BY 1 ORDER BY 1
"""

TWIN_CHANNEL_SUCCESS = """
SELECT channel, sum(successful_recharges) AS successful, sum(recharge_attempts) AS attempts,
       round(sum(successful_recharges)::numeric / nullif(sum(recharge_attempts), 0), 4) AS success_rate
FROM gold.mart_telecom_revenue_daily GROUP BY 1 ORDER BY 1
"""

TWIN_PACKAGE_REVENUE = """
SELECT p.package_code, d.package_name, d.category, count(*) AS activations, sum(p.price_bdt) AS revenue_bdt
FROM gold.fct_package_purchase p JOIN gold.dim_package d USING (package_code)
WHERE p.activation_status = 'ACTIVATED' AND p.capture_status = 'CAPTURED'
GROUP BY 1, 2, 3 ORDER BY revenue_bdt DESC
"""

TWIN_SHIPMENTS = """
WITH clock AS (SELECT max_event_time AS t FROM ops.stream_state WHERE stream = 'mfg')
SELECT s.warehouse_id,
       count(*) FILTER (WHERE s.current_status = 'DELIVERED') AS delivered,
       count(*) FILTER (WHERE s.current_status = 'IN_TRANSIT' AND clock.t - s.dispatched_at <= interval '60 hours')
           AS in_transit,
       count(*) FILTER (WHERE s.current_status = 'IN_TRANSIT' AND clock.t - s.dispatched_at > interval '60 hours')
           AS delayed,
       round(avg(s.transit_hours), 1) AS avg_transit_hours
FROM gold.fct_shipment s, clock GROUP BY 1 ORDER BY 1
"""
