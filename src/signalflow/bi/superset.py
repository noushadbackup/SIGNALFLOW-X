"""Superset assets as code: the Trino connection, datasets on lake Gold, charts and one dashboard.

Run after `docker compose --profile superset up` and a lake refresh (`signalflow bi bootstrap`). Idempotent:
objects are looked up by name and updated in place. Every chart is a query against the lake; nothing here holds a
number.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any

import requests

from ..log import get_logger

log = get_logger(__name__)

DATABASE_NAME = "SignalFlow lake (Trino)"
DASHBOARD_SLUG = "signalflow-lakehouse"
STATUS_COLORS = {  # validated status palette; identity also carried by the label text
    "FINAL": "#0ca30c", "PROVISIONAL": "#fab219", "RESTATED": "#ec835a",
    "LOW": "#0ca30c", "MEDIUM": "#fab219", "HIGH": "#ec835a", "OUT_OF_STOCK": "#d03b3b", "NO_RECENT_DEMAND": "#8a8a8a",
}


def metric(sql: str, label: str) -> dict[str, Any]:
    return {"expressionType": "SQL", "sqlExpression": sql, "label": label}


@dataclass
class Chart:
    name: str
    dataset: tuple[str, str]   # (schema layer, table)
    viz_type: str
    params: dict[str, Any]
    width: int = 6
    height: int = 50
    row: int = 0
    id: int | None = field(default=None, compare=False)


def charts() -> list[Chart]:
    revenue = ("gold", "mart_telecom_revenue_daily")
    return [
        Chart("Gross recharge revenue (BDT)", revenue, "big_number_total",
              {"metric": metric("SUM(gross_recharge_bdt)", "Gross recharge (BDT)"),
               "subheader": "successful recharges, all days in the lake", "y_axis_format": ",.2f"}, 3, 30, 0),
        Chart("Recharge success rate", revenue, "big_number_total",
              {"metric": metric("CAST(SUM(successful_recharges) AS DOUBLE) / SUM(recharge_attempts)",
                                "Success rate"), "y_axis_format": ".1%", "subheader": "successful / attempted"},
              3, 30, 0),
        Chart("Successful recharges without a billing record", revenue, "big_number_total",
              {"metric": metric("SUM(successful_without_billing)", "Without billing"), "y_axis_format": ",d",
               "subheader": "candidate revenue leaks (Phase 4 investigates)"}, 3, 30, 0),
        Chart("Late arrivals", revenue, "big_number_total",
              {"metric": metric("SUM(late_arrivals)", "Late arrivals"), "y_axis_format": ",d",
               "subheader": "recharges that arrived after their event-time window"}, 3, 30, 0),
        Chart("Gross recharge by business day and channel", revenue, "echarts_timeseries_bar",
              {"x_axis": "business_date", "time_grain_sqla": "P1D",
               "metrics": [metric("SUM(gross_recharge_bdt)", "Gross recharge (BDT)")], "groupby": ["channel"],
               "stack": "Stack", "row_limit": 10000, "show_legend": True, "legendOrientation": "top",
               "y_axis_format": ",.0f", "x_axis_time_format": "%d %b", "rich_tooltip": True, "zoomable": False},
              8, 55, 1),
        Chart("KPI finality per business day", revenue, "table",
              {"query_mode": "aggregate", "groupby": ["business_date", "kpi_status"],
               "metrics": [metric("SUM(gross_recharge_bdt)", "Gross recharge (BDT)")],
               "order_desc": True, "row_limit": 100, "table_timestamp_format": "%Y-%m-%d"}, 4, 55, 1),
        Chart("Dealer orders by region", ("gold", "mart_mfg_sales_daily"), "echarts_timeseries_bar",
              {"x_axis": "dealer_region", "metrics": [metric("SUM(ordered_value_bdt)", "Ordered value (BDT)")],
               "groupby": ["category"], "stack": "Stack", "row_limit": 10000, "show_legend": True,
               "legendOrientation": "top", "y_axis_format": ",.0f",
               "x_axis_sort_series": "name", "x_axis_sort_series_ascending": True}, 6, 55, 2),
        Chart("Stockout risk by warehouse (SKU positions)", ("gold", "mart_inventory_position"),
              "echarts_timeseries_bar",
              {"x_axis": "warehouse_id", "metrics": [metric("COUNT(*)", "Positions")], "groupby": ["stockout_risk"],
               "stack": "Stack", "row_limit": 100, "show_legend": True, "legendOrientation": "top",
               "y_axis_format": ",d"}, 3, 55, 2),
        Chart("Quarantined records by reason", ("silver", "quarantine"), "echarts_timeseries_bar",
              {"x_axis": "reason_code", "metrics": [metric("COUNT(*)", "Records")], "groupby": [],
               "row_limit": 100, "show_legend": False, "y_axis_format": ",d", "orientation": "horizontal"},
              3, 55, 2),
    ]


class SupersetClient:
    def __init__(self, url: str, username: str, password: str):
        self.url = url.rstrip("/")
        self.s = requests.Session()
        token = self._ok(self.s.post(f"{self.url}/api/v1/security/login", json={
            "username": username, "password": password, "provider": "db", "refresh": True}, timeout=30))
        self.s.headers["Authorization"] = f"Bearer {token['access_token']}"
        csrf = self._ok(self.s.get(f"{self.url}/api/v1/security/csrf_token/", timeout=30))["result"]
        self.s.headers.update({"X-CSRFToken": csrf, "Referer": self.url})

    @staticmethod
    def _ok(resp: requests.Response) -> Any:
        if resp.status_code >= 400:
            raise RuntimeError(f"{resp.request.method} {resp.url} -> {resp.status_code}: {resp.text[:500]}")
        return resp.json()

    def list(self, resource: str) -> list[dict[str, Any]]:
        return list(self._ok(self.s.get(f"{self.url}/api/v1/{resource}/", params={"q": "(page_size:100)"},
                                        timeout=30))["result"])

    def upsert(self, resource: str, match: dict[str, Any], body: dict[str, Any]) -> int:
        for item in self.list(resource):
            if all(item.get(k) == v for k, v in match.items()):
                self._ok(self.s.put(f"{self.url}/api/v1/{resource}/{item['id']}", json=body, timeout=60))
                return int(item["id"])
        return int(self._ok(self.s.post(f"{self.url}/api/v1/{resource}/", json=body, timeout=60))["id"])


def _position(chart_list: list[Chart]) -> dict[str, Any]:
    pos: dict[str, Any] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
        "HEADER_ID": {"id": "HEADER_ID", "type": "HEADER", "meta": {"text": "SignalFlow X: lakehouse overview"}},
    }
    for r in sorted({c.row for c in chart_list}):
        row_id = f"ROW-{r}"
        pos["GRID_ID"]["children"].append(row_id)
        pos[row_id] = {"type": "ROW", "id": row_id, "children": [], "parents": ["ROOT_ID", "GRID_ID"],
                       "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        for c in (c for c in chart_list if c.row == r):
            cid = f"CHART-{c.id}"
            pos[row_id]["children"].append(cid)
            pos[cid] = {"type": "CHART", "id": cid, "children": [], "parents": ["ROOT_ID", "GRID_ID", row_id],
                        "meta": {"chartId": c.id, "width": c.width, "height": c.height, "sliceName": c.name}}
    return pos


def bootstrap(url: str | None = None, password: str | None = None, lake_prefix: str = "") -> dict[str, Any]:
    url = url or os.environ.get("SIGNALFLOW_SUPERSET_URL", "http://127.0.0.1:58088")
    password = password or os.environ["SUPERSET_ADMIN_PASSWORD"]
    api = SupersetClient(url, "admin", password)
    trino_uri = os.environ.get("SIGNALFLOW_SUPERSET_TRINO_URI", "trino://signalflow@trino:8080/lake")
    db_id = api.upsert("database", {"database_name": DATABASE_NAME}, {
        "database_name": DATABASE_NAME, "sqlalchemy_uri": trino_uri, "expose_in_sqllab": True,
        "allow_run_async": False, "allow_dml": False})

    datasets: dict[tuple[str, str], int] = {}
    existing = {(d["schema"], d["table_name"]): int(d["id"]) for d in api.list("dataset")}
    chart_list = charts()
    for layer, table in sorted({c.dataset for c in chart_list}):
        schema = f"{lake_prefix}{layer}"
        if (schema, table) in existing:
            datasets[(layer, table)] = existing[(schema, table)]
            api._ok(api.s.put(f"{api.url}/api/v1/dataset/{existing[(schema, table)]}/refresh", timeout=60))
        else:
            datasets[(layer, table)] = int(api._ok(api.s.post(f"{api.url}/api/v1/dataset/", json={
                "database": db_id, "schema": schema, "table_name": table}, timeout=60))["id"])

    dash_id = api.upsert("dashboard", {"slug": DASHBOARD_SLUG}, {
        "dashboard_title": "SignalFlow X: lakehouse overview", "slug": DASHBOARD_SLUG, "published": True})
    for c in chart_list:
        ds = datasets[c.dataset]
        params = {"viz_type": c.viz_type, "datasource": f"{ds}__table", "adhoc_filters": [],
                  "color_scheme": "signalflow", **c.params}
        c.id = api.upsert("chart", {"slice_name": c.name}, {
            "slice_name": c.name, "viz_type": c.viz_type, "datasource_id": ds, "datasource_type": "table",
            "params": json.dumps(params), "dashboards": [dash_id]})
    position = _position(chart_list)
    # Superset applies json_metadata.positions to the layout and to the dashboard's chart list, so the layout goes
    # in both places (an empty "positions" would detach every chart).
    metadata = {"color_scheme": "signalflow", "label_colors": STATUS_COLORS, "refresh_frequency": 0,
                "positions": position, "cross_filters_enabled": False}
    api._ok(api.s.put(f"{api.url}/api/v1/dashboard/{dash_id}", json={
        "position_json": json.dumps(position), "json_metadata": json.dumps(metadata)}, timeout=60))
    result = {"database_id": db_id, "datasets": len(datasets), "charts": len(chart_list), "dashboard_id": dash_id,
              "dashboard_url": f"{url}/superset/dashboard/{DASHBOARD_SLUG}/"}
    log.info("superset assets bootstrapped", extra=result)
    return result
