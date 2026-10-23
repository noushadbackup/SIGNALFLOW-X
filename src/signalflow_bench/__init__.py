"""SignalFlow X benchmark suite (Phase 6).

Runs the real pipeline code (Bronze loader, Silver processor, dbt, Leak Hunter, Time Machine) against generated
workloads of several sizes in a dedicated benchmark database, measures it, and checks every result against the
generator's truth manifest. Like ``signalflow_eval``, it lives outside ``signalflow`` because it reads ground truth,
which the production package must never see.

Every number it records was measured during the run that recorded it. Nothing is estimated from file sizes.
"""
