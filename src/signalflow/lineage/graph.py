"""Table-level lineage from source systems to KPIs and dashboards, with impact analysis.

The graph is derived, not drawn by hand:

* event types and their Kafka topics from the contract registry and the topic router,
* dbt models and their dependencies from the dbt manifest (``dbt parse``); a staging model's event type from its
  ``event_type = '...'`` filter,
* Leak Hunter rules and the event types they read from the rule SQL,
* dashboard reads from the dashboard code: string literals and the ``data.*`` queries each control tower page
  reaches, and ``bi.superset.charts()`` for Superset,
* KPIs are declared in ``catalog.KPIS`` and checked against the manifest; a KPI is linked to a dashboard only when
  the dashboard reads its model and mentions its column.

Granularity is the table, and the event type inside the shared Bronze and Silver tables. Column-level lineage is
not captured: a KPI names the column it is computed from, but no column-to-column path is traced.

Edges carry the job that moves the data. A job runs in one or both profiles (lite: files and Python; stream: Kafka,
Debezium and Spark), so impact is always computed for one profile.
"""

from __future__ import annotations

import ast
import heapq
import json
import os
import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .. import CONTRACTS_DIR, REPO_ROOT
from ..contracts import ContractRegistry
from ..streaming.topics import MASTER_DATA_TABLES, event_topic, master_data_topic
from .catalog import JOBS, KPIS, PROFILES, Profile

DBT_DIR = REPO_ROOT / "dbt"
RULES_DIR = REPO_ROOT / "sql" / "assurance"
UI_DIR = Path(__file__).resolve().parents[1] / "ui"
LINEAGE_TARGET = "target/lineage"  # its own dbt target path: lab and lake runs overwrite target/manifest.json
EVENT_FILTER = re.compile(r"event_type\s*=\s*'([a-z_]+\.[a-z_]+)'")
TABLE_REF = re.compile(r"\b(bronze|silver|gold|ops|assurance|ref)\.([a-z_][a-z0-9_]*)\b")
RULE_HEADER = re.compile(r"^--\s*([A-Z]+-\d+)\s+(.+)$", re.M)
KPI_STATUS_STREAM = re.compile(r"kpi_status\(\s*'(telecom|mfg)'")
PER_STREAM = ("ops.stream_state", "ops.late_events", "ops.kpi_restatements")  # one row set per stream
CONTEXT_WEIGHT = 4  # clock / evidence edges: real dependencies, but not the path a reader expects first
PAGES = {  # control tower page functions (module, function) -> page title, as registered in ui/app.py
    ("app", "page_overview"): "Overview", ("app", "page_telecom"): "Telecom",
    ("app", "page_manufacturing"): "Manufacturing", ("app", "page_pipeline"): "Pipeline observatory",
    ("app", "page_lakehouse"): "Lakehouse", ("leak_hunter", "page"): "Revenue Leak Hunter",
}
COLUMN_LEVEL = False


@dataclass(frozen=True)
class Node:
    id: str
    kind: str        # source, event, topic, bronze, silver, table, model, rule, kpi, dashboard
    label: str
    domain: str = ""
    detail: str = ""
    evidence: str = ""                  # where this node comes from
    locations: tuple[str, ...] = ()     # physical places holding it


@dataclass(frozen=True)
class Edge:
    src: str
    dst: str
    via: str | None                     # job id (catalog.JOBS) or None for a logical dependency
    evidence: str
    requires: tuple[str, ...] = ()      # further jobs the dependency needs (e.g. the lake publisher for Superset)
    weight: int = 1                     # path preference only; reachability ignores it

    def active(self, profile: Profile) -> bool:
        return all(profile in JOBS[j].profiles for j in ((self.via,) if self.via else ()) + self.requires)


@dataclass
class Graph:
    nodes: dict[str, Node] = field(default_factory=dict)
    edges: list[Edge] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def add(self, node: Node) -> str:
        self.nodes.setdefault(node.id, node)
        return node.id

    def link(self, src: str, dst: str, via: str | None, evidence: str, requires: tuple[str, ...] = (),
             weight: int = 1) -> None:
        if src not in self.nodes or dst not in self.nodes:
            raise KeyError(f"edge {src} -> {dst}: unknown node")
        edge = Edge(src, dst, via, evidence, requires, weight)
        if edge not in self.edges:
            self.edges.append(edge)

    def out_edges(self, node: str, profile: Profile) -> list[Edge]:
        return [e for e in self.edges if e.src == node and e.active(profile)]

    def in_edges(self, node: str, profile: Profile) -> list[Edge]:
        return [e for e in self.edges if e.dst == node and e.active(profile)]

    def of_kind(self, kind: str) -> list[Node]:
        return sorted((n for n in self.nodes.values() if n.kind == kind), key=lambda n: n.id)


# --------------------------------------------------------------------------------------------------- inputs

def manifest(refresh: bool = False) -> dict[str, Any]:
    """The dbt manifest from ``dbt parse`` into its own target path, re-parsed when a model file is newer."""
    path = DBT_DIR / LINEAGE_TARGET / "manifest.json"
    inputs = [DBT_DIR / "dbt_project.yml", *DBT_DIR.glob("models/**/*"), *DBT_DIR.glob("macros/**/*")]
    newest = max(p.stat().st_mtime for p in inputs if p.is_file())
    if refresh or not path.exists() or path.stat().st_mtime < newest:
        from ..pipeline import run_dbt

        proc = run_dbt("parse", ["--target-path", LINEAGE_TARGET])
        if proc.returncode != 0:
            raise RuntimeError("dbt parse failed:\n" + proc.stdout[-2000:])
    return json.loads(path.read_text())  # type: ignore[no-any-return]


def rule_files() -> list[tuple[str, str, str, set[str]]]:
    """(rule id, title, domain, event types read) for every assurance rule."""
    out = []
    for path in sorted(RULES_DIR.glob("*.sql")):
        text = path.read_text()
        m = RULE_HEADER.search(text)
        if not m:
            raise ValueError(f"{path.name}: missing '-- RULE-ID  title' header")
        types = set(re.findall(r"'((?:telecom|mfg)\.[a-z_]+)'", text))
        out.append((m.group(1), m.group(2).strip(), "telecom" if m.group(1).startswith("TEL") else "mfg", types))
    return out


class _CodeReads:
    """Table names reachable from a UI function: its string literals, the module constants and functions it
    references (also through ``data.X``), followed recursively inside the UI package."""

    def __init__(self, ui_dir: Path):
        self.modules: dict[str, dict[str, ast.AST]] = {}
        self.aliases: dict[str, dict[str, str]] = {}
        for path in ui_dir.glob("*.py"):
            tree = ast.parse(path.read_text())
            defs: dict[str, ast.AST] = {}
            aliases: dict[str, str] = {}
            for stmt in tree.body:
                if isinstance(stmt, ast.FunctionDef):
                    defs[stmt.name] = stmt
                elif isinstance(stmt, ast.Assign) and len(stmt.targets) == 1 and isinstance(stmt.targets[0], ast.Name):
                    defs[stmt.targets[0].id] = stmt.value
                elif isinstance(stmt, ast.ImportFrom):
                    for a in stmt.names:
                        aliases[a.asname or a.name] = a.name
            self.modules[path.stem] = defs
            self.aliases[path.stem] = aliases

    def strings(self, module: str, name: str) -> list[str]:
        seen: set[tuple[str, str]] = set()
        out: list[str] = []
        todo = [(module, name)]
        while todo:
            mod, nm = todo.pop()
            if (mod, nm) in seen or nm not in self.modules.get(mod, {}):
                continue
            seen.add((mod, nm))
            for sub in ast.walk(self.modules[mod][nm]):
                if isinstance(sub, ast.Constant) and isinstance(sub.value, str):
                    out.append(sub.value)
                elif isinstance(sub, ast.Name) and sub.id in self.modules[mod]:
                    todo.append((mod, sub.id))
                elif isinstance(sub, ast.Attribute) and isinstance(sub.value, ast.Name):
                    target = self.aliases[mod].get(sub.value.id)
                    if target in self.modules:
                        todo.append((target, sub.attr))
        return out


# --------------------------------------------------------------------------------------------------- build

def build(dbt_manifest: dict[str, Any] | None = None, registry: ContractRegistry | None = None,
          topic_prefix: str | None = None) -> Graph:
    registry = registry or ContractRegistry.load(CONTRACTS_DIR)
    dbt_manifest = dbt_manifest if dbt_manifest is not None else manifest()
    prefix = topic_prefix or os.environ.get("SIGNALFLOW_TOPIC_PREFIX", "signalflow")
    g = Graph()
    _sources_events_topics(g, registry, prefix)
    models = _dbt(g, dbt_manifest)
    _rules(g)
    _kpis(g, models)
    _dashboards(g)
    g.notes.append("Lineage is table-level (event-type level inside Bronze and Silver). Column-level lineage is "
                   "not captured; a KPI names the column it is computed from, nothing more.")
    return g


def _sources_events_topics(g: Graph, registry: ContractRegistry, prefix: str) -> None:
    g.add(Node("source:telecom", "source", "Telecom source systems", "telecom",
               "Synthetic recharge, billing, payment, usage, lifecycle and settlement producers.", "generator"))
    g.add(Node("source:erp", "source", "ERP (simulated)", "mfg",
               "Manufacturer ERP; each event commits a business row and an outbox row together.", "erp.py",
               ("postgres: erp.outbox",)))
    g.add(Node("source:erp_master", "source", "ERP master tables", "mfg", "Dealers, products, warehouses.",
               "erp.py", tuple(f"postgres: erp.{t}" for t in MASTER_DATA_TABLES)))
    g.add(Node("source:reference", "source", "Reference data", "shared", "Reference CSVs of the dataset.",
               "db.load_reference"))
    for name, d in (("ops.stream_state", "Event-time clock and watermark"),
                    ("ops.late_events", "Events accepted after the watermark"),
                    ("ops.kpi_restatements", "Business days restated by late events")):
        for domain in ("telecom", "mfg"):
            g.add(Node(f"table:{name}#{domain}", "table", f"{name} ({domain})", domain, d, "migrations",
                       (f"postgres: {name} where stream = '{domain}'", f"iceberg: lake.{name}")))
    for name, d in (("silver.events", "Accepted events of every type (one row per event)"),
                    ("silver.quarantine", "Rejected records with reason code and details"),
                    ("ops.duplicate_events", "Duplicate deliveries (first arrival wins)"),
                    ("ops.processing_runs", "One row per processing batch")):
        g.add(Node(f"table:{name}", "table", name, "platform", d, "migrations", (f"postgres: {name}",
                                                                                  f"iceberg: lake.{name}")))
    for ref_table in ("regions", "packages", "products", "factories", "warehouses", "dealers"):
        nid = g.add(Node(f"table:ref.{ref_table}", "table", f"ref.{ref_table}", "shared", "Reference dimension "
                         "input", "db.load_reference",
                         (f"postgres: ref.{ref_table}", f"iceberg: lake.ref.{ref_table}")))
        g.link("source:reference", nid, "reference_loader", "db.load_reference")
    g.add(Node("table:bronze.master_data_changes", "table", "bronze.master_data_changes", "mfg",
               "Master-data change events, kept as evidence (dimensions load from reference data).", "sink.py",
               ("postgres: bronze.master_data_changes",)))
    for t in MASTER_DATA_TABLES:
        topic = g.add(Node(f"topic:{master_data_topic(prefix, t)}", "topic", master_data_topic(prefix, t), "mfg",
                           f"CDC of erp.{t}", "streaming/topics.py"))
        g.link("source:erp_master", topic, "debezium_master", "streaming/connect.py master_data_connector")
        g.link(topic, "table:bronze.master_data_changes", "spark_silver", "sink.commit_master_changes")

    majors: dict[str, set[str]] = {}
    for et, version in registry.contracts:
        majors.setdefault(et, set()).add(version)
    for et in sorted(majors):
        domain = et.split(".", 1)[0]
        source = "source:telecom" if domain == "telecom" else "source:erp"
        ev = g.add(Node(f"event:{et}", "event", et, domain, f"Contract versions {', '.join(sorted(majors[et]))}",
                        "contract registry"))
        br = g.add(Node(f"bronze:{et}", "bronze", f"Bronze {et}", domain, "Raw records of this type, as received",
                        "bronze.raw_events.event_type",
                        ("postgres: bronze.raw_events", "iceberg: lake.bronze.raw_events")))
        sv = g.add(Node(f"silver:{et}", "silver", f"Silver {et}", domain, "Validated, deduplicated events",
                        "silver.events.event_type", (f"postgres: silver.events where event_type = '{et}'",
                                                     f"iceberg: lake.silver.events where event_type = '{et}'")))
        g.link(source, ev, None, "generator / ERP emit")
        g.link(ev, br, "bronze_loader", "arrival files -> ingest.bronze_loader")
        for topic_name in sorted({event_topic(prefix, et, v) for v in majors[et]}):
            tp = g.add(Node(f"topic:{topic_name}", "topic", topic_name, domain, "Kafka topic (one per major "
                            "contract version)", "streaming/topics.event_topic"))
            g.link(ev, tp, "telecom_producer" if domain == "telecom" else "debezium_outbox", "streaming/topics.Router")
            g.link(tp, br, "spark_silver", "spark_silver + sink.commit_batch")
        for job in ("silver_processor", "spark_silver"):
            g.link(br, sv, job, "processing/silver.py apply_validated")
            g.link(br, "table:silver.quarantine", job, "contract validation")
            g.link(br, "table:ops.duplicate_events", job, "first arrival wins", weight=CONTEXT_WEIGHT)
            g.link(br, "table:ops.processing_runs", job, "processing/silver.record_run", weight=CONTEXT_WEIGHT)
            for t in PER_STREAM:
                g.link(sv, f"table:{t}#{domain}", job, "same transaction as the Silver row",
                       weight=CONTEXT_WEIGHT)
        g.link(sv, "table:silver.events", None, "slice of silver.events", weight=CONTEXT_WEIGHT)


def _dbt(g: Graph, m: dict[str, Any]) -> set[str]:
    models = {n["unique_id"]: n for n in m["nodes"].values() if n["resource_type"] == "model"}
    for n in models.values():
        layer = "staging" if n["name"].startswith("stg_") else "gold"
        cols = ", ".join(list(n.get("columns", {}))[:12])
        g.add(Node(f"model:{n['name']}", "model", n["name"], _model_domain(n["name"]),
                   (n.get("description") or "").strip() or f"dbt {layer} model", "dbt manifest",
                   (f"postgres: {n['schema']}.{n['name']}",
                    f"iceberg: lake.gold.{n['name']}" if layer == "gold" else "trino view (lake)")
                   + ((f"documented columns: {cols}",) if cols else ())))
    for n in models.values():
        dst = f"model:{n['name']}"
        event_types = set(EVENT_FILTER.findall(n["raw_code"]))
        for dep in n["depends_on"]["nodes"]:
            if dep.startswith("model."):
                g.link(f"model:{models[dep]['name']}", dst, "dbt", "dbt manifest depends_on")
            elif dep.startswith("source."):
                _, _, schema, table = dep.split(".", 3)
                if (schema, table) == ("silver", "events") and event_types:
                    for et in sorted(event_types):
                        g.link(f"silver:{et}", dst, "dbt", f"dbt source + filter event_type = '{et}'")
                elif f"{schema}.{table}" in PER_STREAM:
                    streams = sorted(set(KPI_STATUS_STREAM.findall(n["raw_code"]))) or ["telecom", "mfg"]
                    for stream in streams:
                        g.link(f"table:{schema}.{table}#{stream}", dst, "dbt",
                               f"kpi_status('{stream}') reads {schema}.{table}", weight=CONTEXT_WEIGHT)
                else:
                    if f"table:{schema}.{table}" not in g.nodes:
                        g.add(Node(f"table:{schema}.{table}", "table", f"{schema}.{table}", "platform",
                                   "dbt source", "dbt manifest", (f"postgres: {schema}.{table}",)))
                    g.link(f"table:{schema}.{table}", dst, "dbt", "dbt manifest depends_on (source)")
    return {n["name"] for n in models.values()}


def _model_domain(name: str) -> str:
    if name.startswith(("stg_tel", "mart_telecom")) or name in {"fct_recharge", "fct_billing_charge", "fct_usage",
                                                                   "fct_channel_settlement", "fct_package_purchase",
                                                                   "dim_package", "dim_subscriber_scd2"}:
        return "telecom"
    if name in {"dim_date", "dim_region"}:
        return "shared"
    return "mfg"


def _rules(g: Graph) -> None:
    for t in ("assurance.cases", "assurance.case_events", "assurance.hunter_runs"):
        g.add(Node(f"table:{t}", "table", t, "assurance", "Leak Hunter state", "migrations", (f"postgres: {t}",)))
    for rule_id, title, domain, types in rule_files():
        rid = g.add(Node(f"rule:{rule_id}", "rule", f"{rule_id} {title}", domain, title, "sql/assurance",
                         (f"postgres: assurance.cases where rule_id = '{rule_id}'",)))
        for et in sorted(types):
            g.link(f"silver:{et}", rid, "leak_hunter", "rule SQL reads silver.events of this type")
        for t in ("assurance.cases", "assurance.case_events", "assurance.hunter_runs"):
            g.link(rid, f"table:{t}", "leak_hunter", "assurance/hunter.py")


def _kpis(g: Graph, models: set[str]) -> None:
    for k in KPIS:
        kid = g.add(Node(f"kpi:{k.id}", "kpi", k.label, k.domain, k.definition, "catalog.KPIS (declared)",
                         (f"column: {k.column}",)))
        if k.model.startswith("rule:"):
            for rule in g.of_kind("rule"):
                if rule.domain == k.model.split(":", 1)[1]:
                    g.link(rule.id, kid, "leak_hunter", "open cases of this rule, summed by exposure")
        elif k.model.startswith("table:"):
            g.link(k.model, kid, None, f"computed from {k.column}")
        else:
            if k.model not in models:
                raise ValueError(f"KPI {k.id}: model {k.model} is not in the dbt manifest")
            g.link(f"model:{k.model}", kid, None, f"computed from {k.model}.{k.column}")


def _table_node(g: Graph, schema: str, table: str) -> list[str]:
    """Graph nodes a code reference ``schema.table`` resolves to."""
    if schema == "gold":
        return [f"model:{table}"] if f"model:{table}" in g.nodes else []
    if (schema, table) == ("bronze", "raw_events"):
        return [n.id for n in g.of_kind("bronze")]
    if f"{schema}.{table}" in PER_STREAM:
        return [f"table:{schema}.{table}#{d}" for d in ("telecom", "mfg")]
    nid = f"table:{schema}.{table}"
    return [nid] if nid in g.nodes else []


def _dashboards(g: Graph) -> None:
    from ..bi.superset import charts

    reads = _CodeReads(UI_DIR)
    kpis = [(k, f"kpi:{k.id}") for k in KPIS]
    for (module, func), title in PAGES.items():
        text = "\n".join(reads.strings(module, func))
        did = g.add(Node(f"dashboard:ui/{title}", "dashboard", f"Control tower: {title}", "",
                         "Streamlit page", f"ui/{module}.py:{func}"))
        read = sorted({nid for s, t in TABLE_REF.findall(text) for nid in _table_node(g, s, t)})
        for nid in read:
            g.link(nid, did, "control_tower", f"query text reachable from {module}.{func}")
        for k, kid in kpis:
            model = k.model if ":" in k.model else f"model:{k.model}"
            readers = ([n.id for n in g.of_kind("rule") if n.domain == k.model.split(":", 1)[1]]
                       if k.model.startswith("rule:") else [model])
            if any(r in read for r in readers) and re.search(rf"\b{k.column}\b", text):
                g.link(kid, did, "control_tower", f"page reads the source and shows {k.column}")
    for chart in charts():
        layer, table = chart.dataset
        cid = g.add(Node(f"dashboard:superset/{chart.name}", "dashboard", f"Superset: {chart.name}", "",
                         f"{chart.viz_type} on lake.{layer}.{table}", "bi/superset.py charts()"))
        requires = ("lake_publisher", "dbt_trino") if layer == "gold" else ("lake_publisher",)
        targets = _table_node(g, layer, table)
        params = json.dumps(chart.params)
        for nid in targets:
            g.link(nid, cid, "superset", f"chart dataset lake.{layer}.{table}", requires)
        for k, kid in kpis:
            model = k.model if ":" in k.model else f"model:{k.model}"
            if model in targets and re.search(rf"\b{k.column}\b", params):
                g.link(kid, cid, "superset", f"chart metric uses {k.column}", requires)


# --------------------------------------------------------------------------------------------------- analysis

@dataclass(frozen=True)
class Reach:
    node: str
    distance: int
    path: tuple[str, ...]     # node ids from the start to this node
    jobs: tuple[str, ...]     # jobs along the path


def _walk(g: Graph, starts: Iterable[str], profile: Profile, forward: bool) -> dict[str, Reach]:
    """Reachable nodes, each with its preferred path (lowest total edge weight; context edges cost more)."""
    best: dict[str, tuple[int, Reach]] = {}
    heap: list[tuple[int, int, Reach]] = []
    tie = 0
    for s in starts:
        if s not in g.nodes:
            raise KeyError(f"unknown node {s}")
        best[s] = (0, Reach(s, 0, (s,), ()))
        heapq.heappush(heap, (0, tie, best[s][1]))
        tie += 1
    done: set[str] = set()
    while heap:
        cost, _, cur = heapq.heappop(heap)
        if cur.node in done:
            continue
        done.add(cur.node)
        for e in (g.out_edges(cur.node, profile) if forward else g.in_edges(cur.node, profile)):
            nxt = e.dst if forward else e.src
            if nxt in done or (nxt in best and best[nxt][0] <= cost + e.weight):
                continue
            jobs = cur.jobs + tuple(j for j in ((e.via,) if e.via else ()) + e.requires if j not in cur.jobs)
            reach = Reach(nxt, cur.distance + 1, cur.path + (nxt,), jobs)
            best[nxt] = (cost + e.weight, reach)
            heapq.heappush(heap, (cost + e.weight, tie, reach))
            tie += 1
    return {n: r for n, (_, r) in best.items()}


def downstream(g: Graph, node: str | Iterable[str], profile: Profile = "lite") -> dict[str, Reach]:
    """Everything that depends on ``node`` (or on any of several nodes) in ``profile``, with one shortest path."""
    return _walk(g, [node] if isinstance(node, str) else node, profile, forward=True)


def upstream(g: Graph, node: str, profile: Profile = "lite") -> dict[str, Reach]:
    """Everything ``node`` depends on: the candidate root causes when a figure looks wrong."""
    return _walk(g, [node], profile, forward=False)


def job_failure(g: Graph, job: str, profile: Profile = "lite") -> dict[str, Reach]:
    """What a stopped job leaves stale: every node fed through an edge that needs the job, and their dependents."""
    if job not in JOBS:
        raise KeyError(f"unknown job {job}")
    if profile not in JOBS[job].profiles:
        return {}
    starts = sorted({e.dst for e in g.edges if e.active(profile) and (e.via == job or job in e.requires)})
    return {k: v for k, v in downstream(g, starts, profile).items()}


def impact_summary(g: Graph, reach: dict[str, Reach]) -> dict[str, list[str]]:
    """Affected node ids grouped by kind, for display."""
    out: dict[str, list[str]] = {}
    for nid in sorted(reach, key=lambda n: (reach[n].distance, n)):
        out.setdefault(g.nodes[nid].kind, []).append(nid)
    return out


def to_dot(g: Graph, nodes: Iterable[str], profile: Profile, highlight: Iterable[str] = (),
           start: str | None = None, edge_labels: bool = True) -> str:
    """Graphviz DOT for the subgraph on ``nodes`` (left to right, one column per kind)."""
    keep = set(nodes)
    hot = set(highlight)
    style = {"source": ("#E8EEF7", "#3B5B92"), "event": ("#EEF6EE", "#3D7A3D"), "topic": ("#F4EEF8", "#6B4A8A"),
             "bronze": ("#F7F0E6", "#8A5A1F"), "silver": ("#EEF2F5", "#4A5E70"), "table": ("#F3F3F3", "#666666"),
             "model": ("#E9F4F6", "#2A6F7A"), "rule": ("#FBEDEC", "#A23B32"), "kpi": ("#FFF6DD", "#8A6A00"),
             "dashboard": ("#EDEDF7", "#4A4A8A")}
    lines = ["digraph lineage {", 'rankdir=LR; bgcolor="transparent"; nodesep=0.18; ranksep=0.55;',
             'node [shape=box, style="rounded,filled", fontname="Helvetica", fontsize=10, penwidth=1.2];',
             'edge [color="#9AA3AD", arrowsize=0.6, fontname="Helvetica", fontsize=8, fontcolor="#6B7280"];']
    for nid in sorted(keep):
        n = g.nodes[nid]
        fill, line = style.get(n.kind, ("#FFFFFF", "#444444"))
        if nid in hot:
            fill, line = "#FDE2E1", "#C0392B"
        width = 2.4 if nid == start else 1.2
        label = n.label if len(n.label) <= 38 else n.label[:36] + "…"
        lines.append(f'"{nid}" [label="{n.kind}\\n{label}", fillcolor="{fill}", color="{line}", penwidth={width}];')
    for e in g.edges:
        if e.src in keep and e.dst in keep and e.active(profile):
            label = JOBS[e.via].label if e.via and edge_labels else ""
            hot_edge = e.src in hot and e.dst in hot
            color = ', color="#C0392B"' if hot_edge else ""
            lines.append(f'"{e.src}" -> "{e.dst}" [label="{label}"{color}];')
    lines.append("}")
    return "\n".join(lines)


__all__ = ["COLUMN_LEVEL", "PROFILES", "Edge", "Graph", "Node", "Reach", "build", "downstream", "impact_summary",
           "job_failure", "manifest", "to_dot", "upstream"]
