"""Revenue Leak Hunter rule catalogue (ADR-0010).

Each rule is one SQL file in ``sql/assurance`` that lists the discrepancies it observes *now* in Silver, one row per
subject, with the columns in ``OUTPUT_COLUMNS``. Python only decides how long a discrepancy has existed, measured on
the stream's event-time clock, and maps that age to a status:

    age < expected_within   PENDING            normal processing time; the counterpart is probably on its way
    age < confirm_after     SUSPICIOUS         later than normal, but late data could still close it
    otherwise               CONFIRMED_BY_RULE  survived the late-data horizon; still an estimate, not an audit

``confirm_after`` is at least the stream's late-data horizon, which is longer than the largest delay the platform
tolerates for that stream (telecom 30 h, manufacturing 48 h; see the lateness policy in ADR-0004). A delayed but
complete transaction therefore clears its case before the rule can confirm it.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import timedelta
from functools import cached_property
from pathlib import Path

from .. import REPO_ROOT

RULES_DIR = REPO_ROOT / "sql" / "assurance"
OUTPUT_COLUMNS = ("subject_key", "anchor_time", "business_date", "business_unit", "region_code", "exposure_bdt",
                  "summary", "evidence")
STATUSES = ("PENDING", "SUSPICIOUS", "CONFIRMED_BY_RULE")

# Late-data horizon per stream: the maximum delay we accept before treating an absence as real, plus margin.
HORIZON = {"telecom": timedelta(hours=36), "mfg": timedelta(hours=52)}


@dataclass(frozen=True)
class Rule:
    rule_id: str
    domain: str              # also the stream whose clock measures age
    title: str
    description: str
    exposure_kind: str
    expected_within: timedelta
    confirm_after: timedelta
    sql_file: str

    @cached_property
    def sql(self) -> str:
        return (RULES_DIR / self.sql_file).read_text()

    @property
    def sql_path(self) -> Path:
        return RULES_DIR / self.sql_file

    @property
    def sql_sha256(self) -> str:
        return hashlib.sha256(self.sql.encode()).hexdigest()

    def status_for(self, age: timedelta) -> str:
        if age < self.expected_within:
            return "PENDING"
        if age < self.confirm_after:
            return "SUSPICIOUS"
        return "CONFIRMED_BY_RULE"

    def case_id(self, subject_key: str) -> str:
        return f"{self.rule_id}-{hashlib.sha1(subject_key.encode()).hexdigest()[:10].upper()}"


TEL, MFG = HORIZON["telecom"], HORIZON["mfg"]
RULES: tuple[Rule, ...] = (
    Rule("TEL-01", "telecom", "Recharge without billing record",
         "A successful recharge has no billing charge. Billing normally follows within a minute.",
         "REVENUE_NOT_BILLED", timedelta(minutes=15), TEL, "tel_01_missing_billing.sql"),
    Rule("TEL-02", "telecom", "Payment charged twice",
         "Two or more successful recharges carry the same payment reference.",
         "CUSTOMER_OVERCHARGE", timedelta(0), TEL, "tel_02_duplicate_payment_charge.sql"),
    Rule("TEL-03", "telecom", "Bundle activated without payment capture",
         "A package was activated but its payment was not captured.",
         "UNCOLLECTED_REVENUE", timedelta(minutes=15), TEL, "tel_03_activation_without_capture.sql"),
    Rule("TEL-04", "telecom", "Channel settlement differs from expected revenue",
         "A channel's settled amount or count for a business day differs from its successful recharges.",
         "SETTLEMENT_SHORTFALL", timedelta(0), TEL, "tel_04_settlement_mismatch.sql"),
    Rule("TEL-05", "telecom", "Billing charge without a successful recharge",
         "A billing charge whose recharge has not arrived or did not succeed. Normally a temporary gap caused by "
         "a delayed recharge event.",
         "UNSUPPORTED_CHARGE", timedelta(minutes=15), TEL, "tel_05_billing_without_recharge.sql"),
    Rule("MFG-01", "mfg", "Shipment without invoice",
         "Goods were dispatched but no invoice exists. Invoices normally follow within an hour.",
         "UNINVOICED_GOODS", timedelta(hours=2), MFG, "mfg_01_shipment_without_invoice.sql"),
    Rule("MFG-02", "mfg", "Dealer settlement differs from invoice",
         "Settlements for an invoice do not add up to its amount, or name an invoice not yet received.",
         "SETTLEMENT_SHORTFALL", timedelta(0), MFG, "mfg_02_settlement_mismatch.sql"),
    Rule("MFG-03", "mfg", "Return refunded twice",
         "A return has more than one refund, or refunds above the amount due.",
         "OVERPAID_REFUND", timedelta(0), MFG, "mfg_03_double_refund.sql"),
    Rule("MFG-04", "mfg", "Inventory adjustment without approval",
         "A stock adjustment cites no matching approved stock count.",
         "INVENTORY_VALUE", timedelta(hours=1), MFG, "mfg_04_unauthorized_adjustment.sql"),
    Rule("MFG-05", "mfg", "Invoice unpaid past its reconciliation window",
         "No settlement after the due date. Normal late payers settle within three days of it.",
         "OVERDUE_RECEIVABLE", timedelta(days=3), timedelta(days=3) + MFG, "mfg_05_invoice_overdue.sql"),
)
BY_ID = {r.rule_id: r for r in RULES}
