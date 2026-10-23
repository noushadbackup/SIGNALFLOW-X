"""The declared part of the lineage model: jobs that move data, and the KPIs the business reads.

Everything else in the graph is derived from code or artifacts (contract registry, topic router, dbt manifest, rule
SQL, dashboard code). What is declared here is checked against what is derived: a KPI's model and column must exist
in the dbt manifest, and a KPI is connected to a dashboard only when that dashboard's code reads the model and
mentions the column (see ``graph.build``).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

Profile = Literal["lite", "stream"]
PROFILES: tuple[Profile, ...] = ("lite", "stream")


@dataclass(frozen=True)
class Job:
    id: str
    label: str
    profiles: tuple[Profile, ...]
    detail: str


JOBS: dict[str, Job] = {j.id: j for j in (
    Job("telecom_producer", "Telecom producer", ("stream",),
        "Publishes telecom events to one Kafka topic per event type and major version (streaming/replay.py)."),
    Job("debezium_outbox", "Debezium outbox connector", ("stream",),
        "Streams erp.outbox inserts to Kafka with the EventRouter transform (ADR-0003)."),
    Job("debezium_master", "Debezium master-data CDC", ("stream",),
        "Captures ERP master tables to erpcdc topics; kept in Bronze as evidence."),
    Job("spark_silver", "Spark Structured Streaming", ("stream",),
        "Kafka to Bronze, Silver, quarantine and stream clocks in one Postgres transaction per micro-batch."),
    Job("bronze_loader", "Bronze loader", ("lite",),
        "Loads arrival files into bronze.raw_events, one row per line."),
    Job("silver_processor", "Silver processor", ("lite",),
        "Validates Bronze against the contracts; writes Silver, quarantine, duplicates and lateness evidence."),
    Job("reference_loader", "Reference data loader", ("lite", "stream"),
        "Loads the reference CSVs (regions, packages, products, factories, warehouses, dealers)."),
    Job("dbt", "dbt (Postgres)", ("lite", "stream"), "Builds staging views and the Gold star schema in Postgres."),
    Job("leak_hunter", "Revenue Leak Hunter", ("lite", "stream"),
        "Runs the assurance rules over Silver and maintains cases (ADR-0010)."),
    Job("lake_publisher", "Lake publisher", ("lite", "stream"),
        "Publishes Bronze, Silver, ops and ref tables from Postgres to Iceberg (ADR-0009)."),
    Job("dbt_trino", "dbt-trino (Iceberg)", ("lite", "stream"),
        "Builds the same Gold models on the Iceberg tables through Trino."),
    Job("control_tower", "Control tower", ("lite", "stream"), "Streamlit pages reading Postgres."),
    Job("superset", "Superset", ("lite", "stream"), "Dashboard charts reading the Iceberg Gold tables through Trino."),
)}


@dataclass(frozen=True)
class Kpi:
    id: str
    label: str
    domain: str
    model: str            # dbt model (or "rule:<domain>" for leak exposure, "table:silver.quarantine")
    column: str           # the column the KPI is computed from (also used to match dashboards)
    definition: str


KPIS: tuple[Kpi, ...] = (
    Kpi("gross_recharge_revenue", "Gross recharge revenue (BDT)", "telecom", "mart_telecom_revenue_daily",
        "gross_recharge_bdt", "Sum of successful recharge amounts per business day (Asia/Dhaka)."),
    Kpi("recharge_success_rate", "Recharge success rate", "telecom", "mart_telecom_revenue_daily",
        "successful_recharges", "Successful recharges / recharge attempts."),
    Kpi("recharges_without_billing", "Successful recharges without billing", "telecom", "mart_telecom_revenue_daily",
        "successful_without_billing", "Successful recharges with no billing charge yet (a candidate leak)."),
    Kpi("late_arrivals", "Late arrivals", "telecom", "mart_telecom_revenue_daily", "late_arrivals",
        "Recharges that arrived after their event-time window closed."),
    Kpi("kpi_finality", "KPI finality", "telecom", "mart_telecom_revenue_daily", "kpi_status",
        "PROVISIONAL until the stream watermark passes the business day, then FINAL or RESTATED."),
    Kpi("active_subscribers", "Active subscribers", "telecom", "mart_telecom_subscribers_daily",
        "active_subscribers", "Subscribers active at the end of each business day (SCD2 dimension)."),
    Kpi("rated_usage", "Rated usage (BDT)", "telecom", "mart_telecom_usage_daily", "rated_bdt",
        "Rated value of voice, data and SMS usage per business day."),
    Kpi("dealer_ordered_value", "Dealer ordered value (BDT)", "mfg", "mart_mfg_sales_daily", "ordered_value_bdt",
        "Value of dealer orders per business day, region and category."),
    Kpi("production_units", "Production units", "mfg", "mart_mfg_production_daily", "units",
        "Units produced per factory, category and business day."),
    Kpi("stockout_risk", "Stockout risk", "mfg", "mart_inventory_position", "stockout_risk",
        "Days of cover per warehouse and SKU, bucketed into risk levels."),
    Kpi("stock_uncertainty", "Stock positions marked uncertain", "mfg", "mart_inventory_position",
        "position_status", "Positions with an adjustment lacking a matching, unused approval since the last count."),
    Kpi("telecom_leak_exposure", "Telecom leak exposure (BDT)", "telecom", "rule:telecom", "exposure_bdt",
        "Open Leak Hunter cases of the telecom rules, summed by exposure."),
    Kpi("mfg_leak_exposure", "Manufacturing leak exposure (BDT)", "mfg", "rule:mfg", "exposure_bdt",
        "Open Leak Hunter cases of the manufacturing rules, summed by exposure."),
    Kpi("quarantine_volume", "Quarantined records", "platform", "table:silver.quarantine", "reason_code",
        "Records rejected by the contracts, by reason."),
)

# Columns that say how far a model's data reaches ("data through"), used for freshness. Postgres keeps no
# modification time per table, so freshness is measured on the data, not on the last dbt run.
DATA_THROUGH: dict[str, str] = {
    "mart_telecom_revenue_daily": "business_date",
    "mart_telecom_subscribers_daily": "business_date",
    "mart_telecom_usage_daily": "business_date",
    "mart_mfg_sales_daily": "business_date",
    "mart_mfg_production_daily": "business_date",
    "mart_inventory_position": "as_of_time",
}
