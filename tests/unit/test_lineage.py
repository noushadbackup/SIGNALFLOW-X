"""Lineage graph: derived from contracts, topics, the dbt manifest, rule SQL and dashboard code; impact per profile."""

from __future__ import annotations

import pytest

from signalflow import CONTRACTS_DIR
from signalflow.contracts import ContractRegistry
from signalflow.lineage import graph as G
from signalflow.lineage.catalog import JOBS, KPIS
from signalflow.lineage.status import alert_starts


@pytest.fixture(scope="module")
def g() -> G.Graph:
    return G.build(topic_prefix="signalflow")


def test_every_event_type_has_its_slices_and_every_staging_model_reads_exactly_one(g: G.Graph) -> None:
    types = ContractRegistry.load(CONTRACTS_DIR).event_types()
    assert {n.label for n in g.of_kind("event")} == types
    for et in types:
        assert {f"bronze:{et}", f"silver:{et}"} <= set(g.nodes)
    for model in (n for n in g.of_kind("model") if n.label.startswith("stg_")):
        feeds = [e.src for e in g.in_edges(model.id, "lite") if e.src.startswith("silver:")]
        assert len(feeds) == 1, (model.id, feeds)


def test_recharge_reaches_the_revenue_dashboard_through_kafka_spark_dbt_and_the_lake(g: G.Graph) -> None:
    reach = G.downstream(g, "event:telecom.recharge", "stream")
    chart = reach["dashboard:superset/Gross recharge revenue (BDT)"]
    assert "topic:signalflow.telecom.recharge.v1" in chart.path and "silver:telecom.recharge" in chart.path
    assert {"telecom_producer", "spark_silver", "dbt", "lake_publisher", "dbt_trino", "superset"} <= set(chart.jobs)
    kpi = reach["kpi:gross_recharge_revenue"]
    assert kpi.path == ("event:telecom.recharge", "topic:signalflow.telecom.recharge.v1", "bronze:telecom.recharge",
                        "silver:telecom.recharge", "model:stg_tel_recharge", "model:fct_recharge",
                        "model:mart_telecom_revenue_daily", "kpi:gross_recharge_revenue")
    # The lite profile has no Kafka: the same KPI is reached through the Bronze loader instead.
    lite = G.downstream(g, "event:telecom.recharge", "lite")["kpi:gross_recharge_revenue"]
    assert not any(n.startswith("topic:") for n in lite.path) and "bronze_loader" in lite.jobs


def test_erp_inventory_movements_reach_the_stockout_dashboard_through_the_outbox(g: G.Graph) -> None:
    reach = G.downstream(g, "event:mfg.inventory_movement", "stream")
    chart = reach["dashboard:superset/Stockout risk by warehouse (SKU positions)"]
    assert "model:mart_inventory_position" in chart.path and "debezium_outbox" in chart.jobs
    assert "kpi:stock_uncertainty" in reach
    # Dealer orders feed the sales mart, not the stock position: the graph must not invent that link.
    orders = G.downstream(g, "event:mfg.dealer_order", "stream")
    assert "kpi:dealer_ordered_value" in orders and "model:mart_inventory_position" not in orders


def test_upstream_of_a_kpi_names_its_sources_and_nothing_unrelated(g: G.Graph) -> None:
    up = G.upstream(g, "kpi:stockout_risk", "lite")
    events = {n for n in up if n.startswith("event:")}
    assert events == {"event:mfg.inventory_movement", "event:mfg.stock_count_approval"}
    assert not any(n.startswith("event:telecom") for n in up)


def test_job_failure_impact_depends_on_the_profile(g: G.Graph) -> None:
    assert G.job_failure(g, "spark_silver", "lite") == {}
    spark = G.impact_summary(g, G.job_failure(g, "spark_silver", "stream"))
    assert {f"kpi:{k.id}" for k in KPIS} <= set(spark["kpi"])
    lake = G.job_failure(g, "lake_publisher", "lite")
    assert lake and all(n.startswith("dashboard:superset/") for n in lake)  # the control tower reads Postgres


def test_kpis_link_only_to_dashboards_that_read_their_model_and_show_their_column(g: G.Graph) -> None:
    for kpi in KPIS:
        for e in (e for e in g.edges if e.src == f"kpi:{kpi.id}"):
            assert g.nodes[e.dst].kind == "dashboard"
    assert any(e.src == "kpi:gross_recharge_revenue" and e.dst == "dashboard:ui/Telecom" for e in g.edges)
    assert not any(e.src == "kpi:stockout_risk" and e.dst == "dashboard:ui/Telecom" for e in g.edges)
    assert "model:mart_telecom_revenue_daily" in [e.src for e in g.in_edges("dashboard:ui/Telecom", "lite")]


def test_alert_subjects_map_to_nodes_or_to_nothing(g: G.Graph) -> None:
    assert alert_starts(g, "CONTRACT_VIOLATIONS", "telecom.recharge/1.0 / CONTRACT_VIOLATION") == (
        ["bronze:telecom.recharge"], None)
    assert alert_starts(g, "PROCESSING_STALLED", "silver.spark") == ([], "spark_silver")
    assert alert_starts(g, "LATE_BEYOND_WATERMARK", "mfg 2026-10-02") == (["table:ops.kpi_restatements#mfg"], None)
    assert alert_starts(g, "CONTRACT_VIOLATIONS", "unknown / UNPARSEABLE") == ([], None)


def test_graph_is_honest_about_its_granularity_and_every_edge_names_a_known_job(g: G.Graph) -> None:
    assert G.COLUMN_LEVEL is False and any("Column-level lineage is not captured" in n for n in g.notes)
    for e in g.edges:
        assert e.via is None or e.via in JOBS
        assert all(j in JOBS for j in e.requires)
    dot = G.to_dot(g, G.downstream(g, "event:telecom.recharge", "lite"), "lite", start="event:telecom.recharge")
    assert dot.startswith("digraph") and '"kpi:gross_recharge_revenue"' in dot
