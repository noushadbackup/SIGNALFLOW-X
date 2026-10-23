"""Measurement helpers: percentiles, the machine description, Postgres counters, process resource usage."""

from __future__ import annotations

import os
import platform
import resource
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import Any

import psycopg

from signalflow import REPO_ROOT


def percentiles(values: list[float], weights: list[int] | None = None) -> dict[str, float | int | None]:
    """Nearest-rank p50/p95/p99 (plus min, mean, max) of ``values``, each counted ``weights[i]`` times."""
    if weights is None:
        weights = [1] * len(values)
    pairs = sorted((v, w) for v, w in zip(values, weights, strict=True) if w > 0)
    total = sum(w for _, w in pairs)
    if not total:
        return {"n": 0, "min": None, "p50": None, "p95": None, "p99": None, "max": None, "mean": None}
    out: dict[str, float | int | None] = {"n": total, "min": round(pairs[0][0], 4)}
    for q in (50, 95, 99):
        rank, seen = q / 100 * total, 0
        for v, w in pairs:
            seen += w
            if seen >= rank:
                out[f"p{q}"] = round(v, 4)
                break
    out["max"] = round(pairs[-1][0], 4)
    out["mean"] = round(sum(v * w for v, w in pairs) / total, 4)
    return out


def environment(conn: psycopg.Connection) -> dict[str, Any]:
    """What the numbers were measured on. Results from different machines are not comparable."""
    def setting(name: str) -> str:
        row = conn.execute("SELECT current_setting(%s)", (name,)).fetchone()
        return str(row[0]) if row else ""

    mem_kb = 0
    try:
        with open("/proc/meminfo") as fh:
            mem_kb = next(int(line.split()[1]) for line in fh if line.startswith("MemTotal:"))
    except (OSError, StopIteration):
        pass
    cpu_model = ""
    try:
        with open("/proc/cpuinfo") as fh:
            cpu_model = next((line.split(":", 1)[1].strip() for line in fh if line.startswith("model name")), "")
    except OSError:
        pass
    git = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=REPO_ROOT, capture_output=True, text=True)
    dirty = subprocess.run(["git", "status", "--porcelain", "--untracked-files=no"], cwd=REPO_ROOT,
                           capture_output=True, text=True)
    return {
        "cpus": os.cpu_count(), "cpus_usable": len(os.sched_getaffinity(0)), "cpu_model": cpu_model,
        "memory_gb": round(mem_kb / 1024 / 1024, 1), "platform": platform.platform(),
        "python": sys.version.split()[0], "psycopg": psycopg.__version__,
        "postgres": setting("server_version"),
        "postgres_settings": {k: setting(k) for k in ("shared_buffers", "work_mem", "max_wal_size",
                                                      "synchronous_commit", "max_parallel_workers_per_gather")},
        "git_commit": git.stdout.strip() or None, "git_dirty": bool(dirty.stdout.strip()),
        "postgres_container": os.environ.get("SIGNALFLOW_BENCH_PG_CONTAINER") or None,
        "containers_running": _containers(),
        "load_average_at_start": [round(v, 2) for v in os.getloadavg()],
    }


def _containers() -> list[str] | None:
    """Other services running on the host compete for CPU; the record says which were up."""
    try:
        proc = subprocess.run(["docker", "ps", "--format", "{{.Names}}"], capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.TimeoutExpired):
        return None
    return sorted(proc.stdout.split()) if proc.returncode == 0 else None


PG_COUNTERS = """SELECT xact_commit, blks_read, blks_hit, tup_inserted, tup_updated, tup_deleted, temp_bytes,
                        pg_current_wal_lsn()
                 FROM pg_stat_database WHERE datname = current_database()"""


def pg_counters(conn: psycopg.Connection) -> dict[str, Any]:
    conn.execute("SELECT pg_stat_clear_snapshot()")
    row = conn.execute(PG_COUNTERS).fetchone()
    assert row is not None
    names = ("xact_commit", "blks_read", "blks_hit", "tup_inserted", "tup_updated", "tup_deleted", "temp_bytes")
    return {**dict(zip(names, (int(v) for v in row[:7]), strict=True)), "wal_lsn": str(row[7])}


def pg_delta(conn: psycopg.Connection, before: dict[str, Any]) -> dict[str, Any]:
    """Counter deltas since ``before`` for the connection's database. WAL is cluster-wide, so it includes any
    other activity on the server during the interval."""
    after = pg_counters(conn)
    out = {k: after[k] - before[k] for k in before if k != "wal_lsn"}
    row = conn.execute("SELECT pg_wal_lsn_diff(%s, %s)", (after["wal_lsn"], before["wal_lsn"])).fetchone()
    out["wal_bytes"] = int(row[0]) if row else None
    hits = out["blks_hit"] + out["blks_read"]
    out["cache_hit_ratio"] = round(out["blks_hit"] / hits, 4) if hits else None
    return out


def container_cpu_seconds() -> float | None:
    """CPU seconds the Postgres container has used, when SIGNALFLOW_BENCH_PG_CONTAINER names it.
    Reads cgroup v2 ``cpu.stat`` (microseconds) or, on cgroup v1 hosts, ``cpuacct.usage`` (nanoseconds)."""
    name = os.environ.get("SIGNALFLOW_BENCH_PG_CONTAINER")
    if not name:
        return None
    proc = subprocess.run(["docker", "exec", name, "sh", "-c", "cat /sys/fs/cgroup/cpu.stat 2>/dev/null || "
                           "echo v1 $(cat /sys/fs/cgroup/cpuacct/cpuacct.usage)"],
                          capture_output=True, text=True, timeout=30)
    for line in proc.stdout.splitlines():
        if line.startswith("usage_usec "):
            return int(line.split()[1]) / 1e6
        if line.startswith("v1 ") and line[3:].strip().isdigit():
            return int(line[3:]) / 1e9
    return None


@dataclass
class Clock:
    """Wall time plus this process's CPU time and the Postgres container's CPU time over an interval."""
    wall: float = 0.0
    cpu: float = 0.0
    pg_cpu: float | None = None

    @classmethod
    def start(cls) -> Clock:
        return cls(time.perf_counter(), time.process_time(), container_cpu_seconds())

    def stop(self) -> dict[str, Any]:
        wall = time.perf_counter() - self.wall
        pg_now = container_cpu_seconds()
        pg = None if pg_now is None or self.pg_cpu is None else pg_now - self.pg_cpu
        return {"seconds": round(wall, 3), "python_cpu_seconds": round(time.process_time() - self.cpu, 3),
                "python_cores": round((time.process_time() - self.cpu) / wall, 2) if wall else None,
                "postgres_cpu_seconds": None if pg is None else round(pg, 3),
                "postgres_cores": None if pg is None or not wall else round(pg / wall, 2)}


def run_worker(database: str, chunk_size: int, log_path: str, validator: str = "compiled") -> dict[str, Any]:
    """Drain Bronze with the Silver processor in its own OS process; return its wall time and resource usage."""
    env = {**os.environ, "PGDATABASE": database, "SIGNALFLOW_VALIDATOR": validator}
    pg0 = container_cpu_seconds()
    t0 = time.perf_counter()
    with open(log_path, "a") as log:
        proc = subprocess.Popen([sys.executable, "-m", "signalflow.chaos.worker", "--chunk-size", str(chunk_size)],
                                env=env, stdout=log, stderr=log)
        _, status, usage = os.wait4(proc.pid, 0)
    wall = time.perf_counter() - t0
    proc.returncode = os.waitstatus_to_exitcode(status)
    pg1 = container_cpu_seconds()
    return {"returncode": proc.returncode, "seconds": round(wall, 3),
            "worker_cpu_seconds": round(usage.ru_utime + usage.ru_stime, 3),
            "worker_cores": round((usage.ru_utime + usage.ru_stime) / wall, 2) if wall else None,
            "worker_max_rss_mb": round(usage.ru_maxrss / 1024, 1),
            "postgres_cpu_seconds": None if pg0 is None or pg1 is None else round(pg1 - pg0, 3),
            "postgres_cores": None if pg0 is None or pg1 is None or not wall else round((pg1 - pg0) / wall, 2)}


def max_rss_mb() -> float:
    return round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)
