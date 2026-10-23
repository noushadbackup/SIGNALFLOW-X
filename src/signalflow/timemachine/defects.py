"""Lab-only defect simulator: damage Silver in a window the way a faulty processor release would, so the Time
Machine can be shown repairing it from Bronze. Refuses to run outside a Chaos Lab database."""

from __future__ import annotations

from typing import Any

import psycopg

from ..chaos.lab import lab_database_name
from ..security import Operator, audit, authorize
from .replay import Window

KINDS = {
    "inflate_amounts": "multiply amount_bdt by 10 in some successful recharges (payload changed, hash untouched)",
    "drop_events": "delete some accepted events of the window from Silver",
}


def inject(conn: psycopg.Connection, operator: Operator | None, window: Window, kind: str,
           rows: int = 20) -> dict[str, Any]:
    database = conn.info.dbname
    lab_database_name(database)  # raises unless this is a lab database
    if kind not in KINDS:
        raise ValueError(f"unknown defect {kind!r}; one of {sorted(KINDS)}")
    operator = authorize(conn, operator, "chaos.run", "silver_defect", f"{database}:{kind}")
    p = {"stream": window.stream, "start": window.start, "end": window.end, "n": rows}
    with conn.transaction():
        if kind == "inflate_amounts":
            ids = [str(e) for (e,) in conn.execute(
                """UPDATE silver.events SET payload = jsonb_set(payload, '{amount_bdt}',
                          to_jsonb(round((payload ->> 'amount_bdt')::numeric * 10, 2)))
                   WHERE event_id IN (SELECT event_id FROM silver.events
                                      WHERE stream = %(stream)s AND event_time >= %(start)s AND event_time < %(end)s
                                        AND payload ? 'amount_bdt' AND payload ->> 'status' = 'SUCCESS'
                                      ORDER BY event_id LIMIT %(n)s)
                   RETURNING event_id""", p)]
        else:
            ids = [str(e) for (e,) in conn.execute(
                """DELETE FROM silver.events
                   WHERE event_id IN (SELECT event_id FROM silver.events
                                      WHERE stream = %(stream)s AND event_time >= %(start)s AND event_time < %(end)s
                                      ORDER BY md5(event_id::text) LIMIT %(n)s)
                   RETURNING event_id""", p)]
        audit(conn, operator.username, operator.role, "replay.defect_injected", "OK", "silver_defect", database,
              {"kind": kind, "rows": len(ids), "window": [window.start.isoformat(), window.end.isoformat()]})
    return {"kind": kind, "description": KINDS[kind], "database": database, "event_ids": sorted(ids)}
