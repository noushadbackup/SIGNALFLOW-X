"""Exporter text format, alert rule catalogue, and the separation between detection and the chaos tooling."""

from __future__ import annotations

import ast
from pathlib import Path

from signalflow import REPO_ROOT
from signalflow.observability import alerts
from signalflow.observability.exporter import Metric, render


def test_render_follows_the_prometheus_text_format_and_escapes_labels() -> None:
    m = Metric("signalflow_x", "gauge", "Help text")
    m.add(3, rule='say "hi"\\now')
    m.add(None, rule="skipped")  # absent values are not invented as zero
    text = render([m])
    assert text.splitlines() == ["# HELP signalflow_x Help text", "# TYPE signalflow_x gauge",
                                 'signalflow_x{rule="say \\"hi\\"\\\\now"} 3.0']


def test_alert_rules_are_named_once_and_scoped_by_the_window() -> None:
    names = [r.name for r in alerts.RULES]
    assert len(names) == len(set(names)) == 4
    assert all("%(window)s" in r.sql or r.name == "PROCESSING_STALLED" for r in alerts.RULES)


def _imports(path: Path) -> set[str]:
    out: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.ImportFrom) and node.module:
            out.add(node.module)
        elif isinstance(node, ast.Import):
            out.update(a.name for a in node.names)
    return out


def test_detection_and_processing_never_import_chaos_or_the_time_machine() -> None:
    for pkg in ("assurance", "processing", "ingest", "streaming", "lakehouse"):
        for path in (REPO_ROOT / "src" / "signalflow" / pkg).rglob("*.py"):
            bad = {m for m in _imports(path) if "chaos" in m or "timemachine" in m}
            assert not bad, f"{path}: {bad}"
