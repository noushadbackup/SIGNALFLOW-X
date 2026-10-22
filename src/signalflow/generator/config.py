"""Generator configuration. Every knob that changes output is here, so a (config, seed) pair fully determines it."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Any


@dataclass(frozen=True)
class Disruption:
    factory_id: str
    day_index: int  # 0-based day of the simulation
    reason: str = "POWER_OUTAGE"
    hours: int = 18
    severity: str = "HIGH"


@dataclass(frozen=True)
class GeneratorConfig:
    seed: int = 7
    start_date: date = date(2026, 10, 1)  # first business day, Asia/Dhaka
    days: int = 3

    # Telecom
    subscribers: int = 2_000
    recharge_rate: float = 1.0  # multiplier on per-segment daily recharge probability
    package_purchase_prob: float = 0.12
    usage_scale: float = 1.0

    # Manufacturing
    dealers: int = 150
    warehouses: int = 4
    order_prob: float = 0.35
    return_prob: float = 0.05
    disruptions: tuple[Disruption, ...] = (Disruption("FAC-01", 1),)

    # Transport effects (pipeline must cope with these)
    late_within_prob: float = 0.01
    late_beyond_prob: float = 0.002
    duplicate_prob: float = 0.005
    schema_break_count: int = 12
    event_id_conflict_count: int = 2
    unparseable_line_count: int = 1
    v11_share: float = 0.3  # share of wallet recharges emitted as recharge v1.1 from day 1

    # Business anomalies (Revenue Leak Hunter targets, Phase 4)
    missing_billing_prob: float = 0.004
    duplicate_payment_prob: float = 0.002
    activation_without_capture_prob: float = 0.006
    settlement_mismatch_count: int = 1
    shipment_without_invoice_prob: float = 0.01
    dealer_short_payment_prob: float = 0.05
    double_refund_count: int = 1
    unauthorized_adjustment_count: int = 2
    dealer_non_payment_prob: float = 0.01   # invoice never settled
    # Benign timing (must never be confirmed as a leak): delayed but complete
    delayed_billing_prob: float = 0.003     # billing record arrives 2-20 h after the recharge
    late_payment_share: float = 0.25        # dealers paying after the due date, at most LATE_PAYMENT_MAX later

    extra: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        data = asdict(self)
        data["start_date"] = self.start_date.isoformat()
        return data
