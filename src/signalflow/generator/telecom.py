"""Telecom simulation: subscribers, recharges, billing, bundles, usage and channel settlements."""

from __future__ import annotations

import random
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta

from .config import GeneratorConfig
from .core import (
    DAY_MS,
    HOUR_MS,
    Emitter,
    at_local_time,
    between,
    business_date,
    dhaka_midnight_ms,
    poisha_to_bdt,
)
from .reference import (
    CHANNELS,
    FAILURE_REASONS,
    PACKAGES,
    RECHARGE_DENOMINATIONS,
    REGION_ABBR,
    REGION_CODES,
    REGION_WEIGHTS,
    SEGMENTS,
    TELECOM_HOURLY,
    TELECOM_WEEKDAY,
)

SEGMENT_NAMES = [s[0] for s in SEGMENTS]
SEGMENT_SHARE = [s[1] for s in SEGMENTS]
SEGMENT_RECHARGE_PROB = {s[0]: s[2] for s in SEGMENTS}
CHANNEL_NAMES = [c[0] for c in CHANNELS]
CHANNEL_SHARE = [c[1] for c in CHANNELS]
CHANNEL_FAIL = {c[0]: c[2] for c in CHANNELS}
DENOMS = [d[0] for d in RECHARGE_DENOMINATIONS]
DENOM_W = [d[1] for d in RECHARGE_DENOMINATIONS]


@dataclass
class Subscriber:
    subscriber_id: str
    segment: str
    home_region: str
    plan_type: str
    active: bool = True
    activated_ms: int = 0


class TelecomSimulator:
    def __init__(self, cfg: GeneratorConfig, em: Emitter, rng: random.Random):
        self.cfg, self.em, self.rng = cfg, em, rng
        self.subs: list[Subscriber] = []
        # truth: (business_date, channel, region) -> [count, poisha] for valid successful recharges
        self.revenue_truth: dict[tuple[str, str, str], list[int]] = defaultdict(lambda: [0, 0])
        self.settle_basis: dict[tuple[date, str], list[int]] = defaultdict(lambda: [0, 0])
        self.recharge_events: list[dict[str, object]] = []

    # -- subscribers -------------------------------------------------------------------------------------------
    def _new_subscriber(self) -> Subscriber:
        n = len(self.subs) + 1
        if n > 9_999_999:
            raise ValueError("subscriber id space exhausted")
        sub = Subscriber(
            f"SUB-{n:07d}",
            self.rng.choices(SEGMENT_NAMES, SEGMENT_SHARE)[0],
            self.rng.choices(REGION_CODES, REGION_WEIGHTS)[0],
            "POSTPAID" if self.rng.random() < 0.08 else "PREPAID",
        )
        self.subs.append(sub)
        return sub

    def _lifecycle(self, sub: Subscriber, action: str, ms: int, arrival_ms: int | None = None) -> None:
        self.em.emit(
            "telecom.subscriber_lifecycle", "tel-crm", ms,
            {"subscriber_id": sub.subscriber_id, "action": action, "segment": sub.segment,
             "home_region": sub.home_region, "plan_type": sub.plan_type},
            arrival_ms=arrival_ms,
        )

    def initial_snapshot(self, start_ms: int) -> None:
        """Existing subscriber base, delivered as one backfill batch an hour before the stream starts."""
        for _ in range(self.cfg.subscribers):
            sub = self._new_subscriber()
            activated = start_ms - self.rng.randint(30, 3 * 365) * DAY_MS - self.rng.randrange(DAY_MS)
            sub.activated_ms = activated
            self._lifecycle(sub, "ACTIVATED", activated, arrival_ms=start_ms - HOUR_MS + self.rng.randrange(60_000))

    # -- daily activity ----------------------------------------------------------------------------------------
    def simulate_day(self, day_index: int, day: date) -> None:
        day_ms = dhaka_midnight_ms(day)
        weekday = TELECOM_WEEKDAY[day.weekday()]
        self._lifecycle_changes(day_ms)
        for sub in list(self.subs):
            if not sub.active:
                continue
            if sub.plan_type == "PREPAID":
                p = SEGMENT_RECHARGE_PROB[sub.segment] * self.cfg.recharge_rate * weekday
                while p > 0:
                    if self.rng.random() < min(p, 1.0):
                        ms = at_local_time(day_ms, self.rng, TELECOM_HOURLY)
                        if ms > sub.activated_ms:  # no activity before activation
                            self._recharge(sub, ms, day_index)
                    p -= 1.0
                if self.rng.random() < self.cfg.package_purchase_prob * weekday:
                    ms = at_local_time(day_ms, self.rng, TELECOM_HOURLY)
                    if ms > sub.activated_ms:
                        self._package(sub, ms)
            self._usage(sub, day_ms)

    def _lifecycle_changes(self, day_ms: int) -> None:
        for _ in range(max(1, int(self.cfg.subscribers * 0.002))):
            sub = self._new_subscriber()
            sub.activated_ms = at_local_time(day_ms, self.rng, TELECOM_HOURLY)
            self._lifecycle(sub, "ACTIVATED", sub.activated_ms)
        for sub in self.subs:
            if not sub.active:
                continue
            r = self.rng.random()
            ms = at_local_time(day_ms, self.rng, TELECOM_HOURLY)
            if r < 0.0008:
                sub.active = False
                self._lifecycle(sub, "TERMINATED", ms)
            elif r < 0.0028:
                sub.home_region = self.rng.choices(REGION_CODES, REGION_WEIGHTS)[0]
                self._lifecycle(sub, "REGION_CHANGED", ms)
            elif r < 0.0038:
                sub.segment = self.rng.choices(SEGMENT_NAMES, SEGMENT_SHARE)[0]
                self._lifecycle(sub, "SEGMENT_CHANGED", ms)

    def _region_for(self, sub: Subscriber) -> str:
        if self.rng.random() < 0.9:
            return sub.home_region
        return self.rng.choices(REGION_CODES, REGION_WEIGHTS)[0]

    def _recharge(self, sub: Subscriber, ms: int, day_index: int, payment_ref: str | None = None,
                  amount_poisha: int | None = None, channel: str | None = None) -> dict[str, object] | None:
        channel = channel or self.rng.choices(CHANNEL_NAMES, CHANNEL_SHARE)[0]
        amount = amount_poisha if amount_poisha is not None else self.rng.choices(DENOMS, DENOM_W)[0] * 100
        failed = payment_ref is None and self.rng.random() < CHANNEL_FAIL[channel]
        payload: dict[str, object] = {
            "recharge_id": self.em.business_id("RCH"),
            "subscriber_id": sub.subscriber_id,
            "amount_bdt": poisha_to_bdt(amount),
            "channel": channel,
            "payment_txn_ref": payment_ref or self.em.business_id("PAY"),
            "status": "FAILED" if failed else "SUCCESS",
            "failure_reason": self.rng.choice(FAILURE_REASONS) if failed else None,
            "region": self._region_for(sub),
        }
        version = "1.0"
        if channel == "MOBILE_WALLET" and day_index >= 1 and self.rng.random() < self.cfg.v11_share:
            version = "1.1"  # schema evolution example: producer upgraded mid-stream
            payload["promo_code"] = self.rng.choice([None, "PUJA26", "DATA2X", "NIGHTPACK"])
        event = self.em.emit("telecom.recharge", "tel-ocs", ms, payload, version)
        if event is None:
            return None
        self.recharge_events.append(event)
        if failed:
            return event
        bdate = business_date(ms)
        truth = self.revenue_truth[(bdate.isoformat(), channel, str(payload["region"]))]
        truth[0] += 1
        truth[1] += amount
        basis = self.settle_basis[(bdate, channel)]
        basis[0] += 1
        basis[1] += amount

        if payment_ref is None and self.em.roll(self.rng, "MISSING_BILLING", self.cfg.missing_billing_prob):
            self.em.anomaly("MISSING_BILLING", "telecom", {"recharge_id": payload["recharge_id"]},
                            "successful recharge has no billing_charge")
        else:
            charge_ms = between(self.rng, ms, timedelta(seconds=2), timedelta(seconds=40))
            delayed = payment_ref is None and self.em.roll(self.rng, "DELAYED_BILLING", self.cfg.delayed_billing_prob)
            arrival = between(self.rng, charge_ms, timedelta(hours=2), timedelta(hours=20)) if delayed else None
            charge = self.em.emit("telecom.billing_charge", "tel-billing", charge_ms,
                                  {"charge_id": self.em.business_id("CHG"), "recharge_id": payload["recharge_id"],
                                   "subscriber_id": sub.subscriber_id, "amount_bdt": poisha_to_bdt(amount),
                                   "charge_type": "RECHARGE_CREDIT"}, arrival_ms=arrival)
            if charge is not None and arrival is not None:
                self.em.anomaly("DELAYED_BILLING", "telecom",
                                {"recharge_id": payload["recharge_id"], "event_id": charge["event_id"],
                                 "delay_seconds": (arrival - charge_ms) // 1000},
                                "benign: billing arrives late; a temporary gap that must never be confirmed")
        if payment_ref is None and self.em.roll(self.rng, "DUPLICATE_PAYMENT_CHARGE",
                                                   self.cfg.duplicate_payment_prob):
            dup = self._recharge(sub, between(self.rng, ms, timedelta(seconds=30), timedelta(minutes=3)), day_index,
                                 payment_ref=str(payload["payment_txn_ref"]), amount_poisha=amount, channel=channel)
            if dup is not None:
                self.em.anomaly("DUPLICATE_PAYMENT_CHARGE", "telecom",
                                {"payment_txn_ref": payload["payment_txn_ref"],
                                 "recharge_ids": [payload["recharge_id"], dup["payload"]["recharge_id"]]},  # type: ignore[index]
                                "one payment reference charged twice")
        return event

    def _package(self, sub: Subscriber, ms: int) -> None:
        code, _, _, price, _, _, _ = self.rng.choice(PACKAGES)
        pay_ref = self.em.business_id("PAY")
        capture_failed = self.rng.random() < 0.03
        anomaly = not capture_failed and self.em.roll(self.rng, "ACTIVATION_WITHOUT_CAPTURE",
                                                      self.cfg.activation_without_capture_prob)
        cap = self.em.emit("telecom.payment_capture", "tel-payment-gw", ms,
                           {"capture_id": self.em.business_id("CAP"), "payment_txn_ref": pay_ref,
                            "subscriber_id": sub.subscriber_id, "amount_bdt": float(price),
                            "status": "FAILED" if capture_failed or anomaly else "CAPTURED", "purpose": "PACKAGE"})
        act = self.em.emit("telecom.package_activation", "tel-provisioning",
                           between(self.rng, ms, timedelta(seconds=1), timedelta(seconds=10)),
                           {"activation_id": self.em.business_id("ACT"), "subscriber_id": sub.subscriber_id,
                            "package_code": code, "price_bdt": float(price), "payment_txn_ref": pay_ref,
                            "status": "REJECTED" if capture_failed else "ACTIVATED"})
        if anomaly and cap is not None and act is not None:
            self.em.anomaly("ACTIVATION_WITHOUT_CAPTURE", "telecom",
                            {"activation_id": act["payload"]["activation_id"], "payment_txn_ref": pay_ref},
                            "bundle activated although payment capture failed")

    def _usage(self, sub: Subscriber, day_ms: int) -> None:
        scale = self.cfg.usage_scale
        for usage_type, unit, sessions_w in (("DATA", "MB", [0.2, 0.3, 0.3, 0.2]),
                                             ("VOICE", "SECOND", [0.3, 0.35, 0.25, 0.1]),
                                             ("SMS", "MESSAGE", [0.7, 0.3])):
            sessions = self.rng.choices(range(len(sessions_w)), sessions_w)[0]
            for _ in range(round(sessions * scale)):
                ms = at_local_time(day_ms, self.rng, TELECOM_HOURLY)
                if ms <= sub.activated_ms:
                    continue
                region = self._region_for(sub)
                if usage_type == "DATA":
                    qty = round(self.rng.lognormvariate(4.3, 1.0), 2)
                    rated = 0 if self.rng.random() < 0.7 else round(qty * 25)  # poisha; 0.25 BDT/MB PAYG
                elif usage_type == "VOICE":
                    qty = self.rng.randint(15, 900)
                    rated = 0 if self.rng.random() < 0.5 else qty * 2  # 0.02 BDT/second
                else:
                    qty = self.rng.randint(1, 5)
                    rated = qty * 50
                self.em.emit("telecom.usage_record", "tel-mediation", ms,
                             {"usage_id": self.em.business_id("USG"), "subscriber_id": sub.subscriber_id,
                              "usage_type": usage_type, "quantity": qty, "unit": unit, "serving_region": region,
                              "cell_area_code": f"{REGION_ABBR[region]}-{self.rng.randint(1, 120):03d}",
                              "rated_amount_bdt": poisha_to_bdt(rated)})

    # -- settlements -------------------------------------------------------------------------------------------
    def settlements(self, days: list[date]) -> None:
        keys = [(d, ch) for d in days for ch in CHANNEL_NAMES]
        # The last day settles after the simulated window closes, so mismatches are drawn from earlier days.
        candidates = [i for i, (d, _) in enumerate(keys) if d < days[-1]]
        mismatched = set(self.rng.sample(candidates, min(self.cfg.settlement_mismatch_count, len(candidates))))
        for i, (d, channel) in enumerate(keys):
            count, poisha = self.settle_basis.get((d, channel), [0, 0])
            settled = poisha
            if i in mismatched and poisha > 0:
                settled = poisha - max(100, poisha * self.rng.randint(5, 30) // 1000)
            ms = dhaka_midnight_ms(d) + DAY_MS + 2 * HOUR_MS + self.rng.randrange(2 * HOUR_MS)
            event = self.em.emit("telecom.channel_settlement", "tel-payment-gw", ms,
                                 {"settlement_id": f"STL-{channel}-{d:%Y%m%d}", "channel": channel,
                                  "business_date": d.isoformat(), "transaction_count": count,
                                  "settled_amount_bdt": poisha_to_bdt(settled)})
            if event is not None and settled != poisha:
                self.em.anomaly("SETTLEMENT_MISMATCH", "telecom",
                                {"settlement_id": event["payload"]["settlement_id"], "expected_bdt":
                                 poisha_to_bdt(poisha), "settled_bdt": poisha_to_bdt(settled)},
                                "channel settled less than successful recharges")
