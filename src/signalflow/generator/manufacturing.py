"""Manufacturing simulation: production, inventory, dealer orders, shipments, invoices, returns, disruptions."""

from __future__ import annotations

import random
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta

from .config import GeneratorConfig
from .core import DAY_MS, HOUR_MS, Emitter, between, business_date, dhaka_midnight_ms, poisha_to_bdt
from .reference import CATEGORY_DEMAND, DEALER_WEEKDAY, FACTORIES, PRODUCTS, Dealer, Warehouse

TIER_DISCOUNT_BP = {"A": 300, "B": 150, "C": 0}  # basis points off dealer price
LATE_PAYMENT_MAX = timedelta(days=3)  # normal late payers settle within this time after the due date
CHEAP_CATEGORIES = {"FAN", "SMARTPHONE"}


@dataclass
class Shipment:
    shipment_id: str
    order_id: str
    dealer: Dealer
    warehouse_id: str
    sku: str
    quantity: int
    unit_poisha: int
    dispatched_ms: int
    invoice_id: str | None = None
    delivered_ms: int | None = None


class ManufacturingSimulator:
    def __init__(self, cfg: GeneratorConfig, em: Emitter, rng: random.Random, warehouses: list[Warehouse],
                 dealers: list[Dealer]):
        self.cfg, self.em, self.rng = cfg, em, rng
        self.warehouses, self.dealers = warehouses, dealers
        self.wh_by_region = {w.region: w.warehouse_id for w in warehouses}
        self.central = next(w.warehouse_id for w in warehouses if w.is_central)
        self.stock: dict[tuple[str, str], int] = defaultdict(int)  # availability tracker for order decisions
        self.shipments: list[Shipment] = []
        self.sku_weights = [CATEGORY_DEMAND[p[2]] for p in PRODUCTS]

    def _movement(self, ms: int, warehouse_id: str, sku: str, delta: int, movement_type: str, source_ref: str) -> None:
        self.em.emit("mfg.inventory_movement", "mfg-wms", ms,
                     {"movement_id": self.em.business_id("MOV"), "warehouse_id": warehouse_id, "sku": sku,
                      "quantity_delta": delta, "movement_type": movement_type, "source_ref": source_ref})

    def opening_balances(self, start: date) -> None:
        start_ms = dhaka_midnight_ms(start)
        for wh in self.warehouses:
            for sku, *_ in PRODUCTS:
                qty = self.rng.randint(120, 200) if wh.is_central else self.rng.randint(20, 60)
                self.stock[(wh.warehouse_id, sku)] += qty
                ms = start_ms - HOUR_MS
                self.em.emit("mfg.inventory_movement", "mfg-wms", ms,
                             {"movement_id": self.em.business_id("MOV"), "warehouse_id": wh.warehouse_id, "sku": sku,
                              "quantity_delta": qty, "movement_type": "OPENING_BALANCE",
                              "source_ref": f"OPEN-{start:%Y%m%d}"},
                             arrival_ms=ms + self.rng.randint(60_000, 120_000))

    def simulate_day(self, day_index: int, day: date) -> None:
        day_ms = dhaka_midnight_ms(day)
        self._production(day_index, day_ms)
        self._orders(day, day_ms)
        self._cycle_counts(day_ms)
        self._transfers(day_ms)

    def _production(self, day_index: int, day_ms: int) -> None:
        down = {d.factory_id: d for d in self.cfg.disruptions if d.day_index == day_index}
        for factory_id, _, _, dest in FACTORIES:
            dest = dest if dest in {w.warehouse_id for w in self.warehouses} else self.central
            if factory_id in down:
                d = down[factory_id]
                self.em.emit("mfg.supply_disruption", "mfg-mes", day_ms + 6 * HOUR_MS + self.rng.randrange(HOUR_MS),
                             {"disruption_id": self.em.business_id("DSR"), "factory_id": factory_id,
                              "reason": d.reason, "expected_duration_hours": d.hours, "severity": d.severity})
                continue
            for sku, _, category, _, fac in PRODUCTS:
                if fac != factory_id:
                    continue
                qty = self.rng.randint(25, 70) if category in CHEAP_CATEGORIES else self.rng.randint(8, 30)
                ms = day_ms + 14 * HOUR_MS + self.rng.randrange(4 * HOUR_MS)
                batch_id = self.em.business_id("BAT")
                self.em.emit("mfg.production_batch", "mfg-mes", ms,
                             {"batch_id": batch_id, "factory_id": factory_id, "sku": sku, "quantity": qty,
                              "destination_warehouse_id": dest})
                receipt_ms = between(self.rng, ms, timedelta(minutes=2), timedelta(minutes=30))
                self._movement(receipt_ms, dest, sku, qty, "PRODUCTION_RECEIPT", batch_id)
                self.stock[(dest, sku)] += qty

    def _orders(self, day: date, day_ms: int) -> None:
        factor = DEALER_WEEKDAY[day.weekday()]
        for dealer in self.dealers:
            if self.rng.random() >= self.cfg.order_prob * factor:
                continue
            idx = self.rng.choices(range(len(PRODUCTS)), self.sku_weights)[0]
            sku, _, category, price, _ = PRODUCTS[idx]
            qty = self.rng.randint(5, 30) if category in CHEAP_CATEGORIES else self.rng.randint(1, 6)
            unit_poisha = price * 100 * (10_000 - TIER_DISCOUNT_BP[dealer.tier]) // 10_000
            order_ms = day_ms + 10 * HOUR_MS + self.rng.randrange(9 * HOUR_MS)
            order_id = self.em.business_id("ORD")
            if self.em.emit("mfg.dealer_order", "mfg-erp", order_ms,
                            {"order_id": order_id, "dealer_id": dealer.dealer_id, "sku": sku, "quantity": qty,
                             "unit_price_bdt": poisha_to_bdt(unit_poisha)}) is None:
                continue
            wh = self.wh_by_region.get(dealer.region, self.central)
            if self.stock[(wh, sku)] < qty and self.stock[(self.central, sku)] >= qty:
                wh = self.central
            if self.stock[(wh, sku)] < qty:
                self.em.emit("mfg.order_status", "mfg-erp",
                             between(self.rng, order_ms, timedelta(minutes=5), timedelta(minutes=30)),
                             {"order_id": order_id, "status": "BACKORDERED", "reason": "INSUFFICIENT_STOCK"})
                continue
            self.stock[(wh, sku)] -= qty
            self._ship(order_id, dealer, wh, sku, qty, unit_poisha,
                       between(self.rng, order_ms, timedelta(hours=2), timedelta(hours=8)))

    def _ship(self, order_id: str, dealer: Dealer, wh: str, sku: str, qty: int, unit_poisha: int, ms: int) -> None:
        shp = Shipment(self.em.business_id("SHP"), order_id, dealer, wh, sku, qty, unit_poisha, ms)
        if self.em.emit("mfg.shipment", "mfg-wms", ms,
                        {"shipment_id": shp.shipment_id, "order_id": order_id, "warehouse_id": wh, "sku": sku,
                         "quantity": qty, "status": "DISPATCHED"}) is None:
            return
        self.shipments.append(shp)
        self._movement(between(self.rng, ms, timedelta(minutes=1), timedelta(minutes=5)), wh, sku, -qty,
                       "SHIPMENT_OUT", shp.shipment_id)
        amount = unit_poisha * qty
        if self.em.roll(self.rng, "SHIPMENT_WITHOUT_INVOICE", self.cfg.shipment_without_invoice_prob):
            self.em.anomaly("SHIPMENT_WITHOUT_INVOICE", "mfg", {"shipment_id": shp.shipment_id, "order_id": order_id,
                            "expected_amount_bdt": poisha_to_bdt(amount)}, "dispatched goods never invoiced")
        else:
            inv_ms = between(self.rng, ms, timedelta(minutes=5), timedelta(minutes=60))
            due = (business_date(inv_ms) + timedelta(days=dealer.credit_days)).isoformat()
            invoice_id = self.em.business_id("INV")
            if self.em.emit("mfg.invoice", "mfg-erp", inv_ms,
                            {"invoice_id": invoice_id, "shipment_id": shp.shipment_id, "order_id": order_id,
                             "dealer_id": dealer.dealer_id, "amount_bdt": poisha_to_bdt(amount), "due_date": due}):
                shp.invoice_id = invoice_id
                self._settlement(shp, inv_ms, amount, due)
        delivered = between(self.rng, ms, timedelta(hours=18), timedelta(hours=60))
        if self.em.emit("mfg.shipment", "mfg-wms", delivered,
                        {"shipment_id": shp.shipment_id, "order_id": order_id, "warehouse_id": wh, "sku": sku,
                         "quantity": qty, "status": "DELIVERED"}):
            shp.delivered_ms = delivered
            return_ms = between(self.rng, delivered, timedelta(hours=2), timedelta(hours=36))
            # Only returns whose refund can complete inside the window count as an opportunity.
            if (shp.invoice_id and return_ms + 8 * HOUR_MS < self.em.end_ms
                    and self.em.roll(self.rng, "PRODUCT_RETURN", self.cfg.return_prob)):
                self._return(shp, return_ms)

    def _settlement(self, shp: Shipment, inv_ms: int, amount: int, due: str) -> None:
        if self.em.roll(self.rng, "DEALER_NON_PAYMENT", self.cfg.dealer_non_payment_prob):
            self.em.anomaly("DEALER_NON_PAYMENT", "mfg", {"invoice_id": shp.invoice_id, "due_date": due,
                            "amount_bdt": poisha_to_bdt(amount)}, "invoice is never settled")
            return
        # Due at the end of the due date (Asia/Dhaka). Most dealers pay within terms, the rest up to
        # LATE_PAYMENT_MAX after the due date; payments after the simulated window are simply not emitted.
        due_end_ms = dhaka_midnight_ms(date.fromisoformat(due)) + DAY_MS
        late = self.rng.random() < self.cfg.late_payment_share
        if late:
            pay_ms = between(self.rng, due_end_ms, timedelta(minutes=1), LATE_PAYMENT_MAX)
        else:
            pay_ms = self.rng.randint(inv_ms + 4 * HOUR_MS, max(inv_ms + 4 * HOUR_MS, due_end_ms - 60_000))
        if pay_ms >= self.em.end_ms:
            return  # not paid yet within the simulated window
        paid = amount
        short = self.em.roll(self.rng, "DEALER_SETTLEMENT_MISMATCH", self.cfg.dealer_short_payment_prob)
        if short:
            paid = amount - max(100, amount * self.rng.randint(10, 50) // 1000)
        event = self.em.emit("mfg.dealer_settlement", "mfg-erp", pay_ms,
                             {"settlement_id": self.em.business_id("DST"), "invoice_id": shp.invoice_id,
                              "dealer_id": shp.dealer.dealer_id, "amount_bdt": poisha_to_bdt(paid)})
        if event is not None and short:
            self.em.anomaly("DEALER_SETTLEMENT_MISMATCH", "mfg",
                            {"invoice_id": shp.invoice_id, "settlement_id": event["payload"]["settlement_id"],
                             "invoice_bdt": poisha_to_bdt(amount), "settled_bdt": poisha_to_bdt(paid)},
                            "dealer settlement differs from invoice amount")
        if event is not None and late:
            self.em.anomaly("DELAYED_DEALER_PAYMENT", "mfg",
                            {"invoice_id": shp.invoice_id, "settlement_id": event["payload"]["settlement_id"],
                             "hours_after_due": (pay_ms - due_end_ms) // HOUR_MS},
                            "benign: paid after the due date; must never be confirmed as non-payment")

    def _return(self, shp: Shipment, ms: int) -> None:
        return_id = self.em.business_id("RET")
        refund = shp.unit_poisha
        if self.em.emit("mfg.product_return", "mfg-erp", ms,
                        {"return_id": return_id, "invoice_id": shp.invoice_id, "dealer_id": shp.dealer.dealer_id,
                         "sku": shp.sku, "quantity": 1, "refund_amount_bdt": poisha_to_bdt(refund),
                         "warehouse_id": shp.warehouse_id}) is None:
            return
        self._movement(between(self.rng, ms, timedelta(minutes=30), timedelta(hours=2)), shp.warehouse_id, shp.sku, 1,
                       "RETURN_IN", return_id)
        refund_ms = between(self.rng, ms, timedelta(hours=1), timedelta(hours=6))
        self._refund(return_id, shp.dealer.dealer_id, refund_ms, refund)

    def _refund(self, return_id: str, dealer_id: str, ms: int, refund: int) -> dict[str, object] | None:
        return self.em.emit("mfg.refund", "mfg-erp", ms,
                            {"refund_id": self.em.business_id("RFD"), "return_id": return_id,
                             "dealer_id": dealer_id, "amount_bdt": poisha_to_bdt(refund)})

    def _cycle_counts(self, day_ms: int) -> None:
        for _ in range(2):
            wh = self.rng.choice(self.warehouses).warehouse_id
            sku = self.rng.choice(PRODUCTS)[0]
            system_qty = self.stock[(wh, sku)]
            counted = max(0, system_qty + self.rng.choice([-2, -1, -1, 0, 0, 1]))
            ms = day_ms + 8 * HOUR_MS + self.rng.randrange(2 * HOUR_MS)
            approval_id = self.em.business_id("SCA")
            self.em.emit("mfg.stock_count_approval", "mfg-wms", ms,
                         {"approval_id": approval_id, "warehouse_id": wh, "sku": sku, "counted_quantity": counted,
                          "system_quantity": system_qty,
                          "approved_by_role": self.rng.choice(["INVENTORY_CONTROLLER", "WAREHOUSE_MANAGER"])})
            if counted != system_qty:
                self._movement(between(self.rng, ms, timedelta(minutes=5), timedelta(minutes=40)), wh, sku,
                               counted - system_qty, "ADJUSTMENT", approval_id)
                self.stock[(wh, sku)] = counted

    def _transfers(self, day_ms: int) -> None:
        for wh in self.warehouses:
            if wh.is_central:
                continue
            for sku, *_ in PRODUCTS:
                if self.stock[(wh.warehouse_id, sku)] >= 15 or self.stock[(self.central, sku)] < 40:
                    continue
                qty = 20
                transfer_id = self.em.business_id("TRF")
                out_ms = day_ms + 20 * HOUR_MS + self.rng.randrange(HOUR_MS)
                self._movement(out_ms, self.central, sku, -qty, "TRANSFER_OUT", transfer_id)
                self._movement(between(self.rng, out_ms, timedelta(hours=6), timedelta(hours=20)), wh.warehouse_id,
                               sku, qty, "TRANSFER_IN", transfer_id)
                self.stock[(self.central, sku)] -= qty
                self.stock[(wh.warehouse_id, sku)] += qty

    # -- injected anomalies that need the whole history ---------------------------------------------------------
    def inject_end_of_run_anomalies(self) -> None:
        returned = [r for r in self.em.records if r.event and r.event["event_type"] == "mfg.refund"]
        for rec in self.rng.sample(returned, min(self.cfg.double_refund_count, len(returned))):
            assert rec.event is not None
            original = rec.event["payload"]
            first_ms = int(rec.event["_event_ms"])
            # second payment lands 10 minutes to 20 hours later, but always inside the simulated window
            second_ms = self.rng.randint(first_ms + 600_000, max(first_ms + 600_001,
                                                                min(first_ms + 20 * HOUR_MS, self.em.end_ms - 60_000)))
            second = self._refund(original["return_id"], original["dealer_id"], second_ms,
                                  int(round(original["amount_bdt"] * 100)))
            if second is not None:
                self.em.anomaly("DOUBLE_REFUND", "mfg",
                                {"return_id": original["return_id"],
                                 "refund_ids": [original["refund_id"], second["payload"]["refund_id"]]},  # type: ignore[index]
                                "return refunded twice")
        for _ in range(self.cfg.unauthorized_adjustment_count):
            wh = self.rng.choice(self.warehouses).warehouse_id
            sku = self.rng.choice(PRODUCTS)[0]
            fake_ref = self.em.business_id("SCA")  # an approval id that is never emitted
            # anywhere from the second day to two hours before the end, so long runs can confirm them
            first_ms = min(dhaka_midnight_ms(self.cfg.start_date) + DAY_MS, self.em.end_ms - 40 * HOUR_MS)
            ms = self.rng.randint(first_ms, self.em.end_ms - 2 * HOUR_MS)
            delta = -self.rng.randint(1, 5)
            self._movement(ms, wh, sku, delta, "ADJUSTMENT", fake_ref)
            self.em.anomaly("UNAUTHORIZED_ADJUSTMENT", "mfg",
                            {"warehouse_id": wh, "sku": sku, "source_ref": fake_ref, "quantity_delta": delta},
                            "inventory adjustment references no approved stock count")
