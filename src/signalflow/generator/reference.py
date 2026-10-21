"""Reference (master) data for the two fictional enterprises. All names are invented."""

from __future__ import annotations

import random
from dataclasses import dataclass

# Eight administrative divisions of Bangladesh; weights approximate relative market size (illustrative, not sourced).
REGIONS: list[tuple[str, str, str, float]] = [
    ("DHAKA", "Dhaka", "DHK", 0.32),
    ("CHATTOGRAM", "Chattogram", "CTG", 0.20),
    ("RAJSHAHI", "Rajshahi", "RAJ", 0.10),
    ("KHULNA", "Khulna", "KHL", 0.09),
    ("BARISHAL", "Barishal", "BAR", 0.05),
    ("SYLHET", "Sylhet", "SYL", 0.08),
    ("RANGPUR", "Rangpur", "RNG", 0.09),
    ("MYMENSINGH", "Mymensingh", "MYM", 0.07),
]
REGION_CODES = [r[0] for r in REGIONS]
REGION_WEIGHTS = [r[3] for r in REGIONS]
REGION_ABBR = {r[0]: r[2] for r in REGIONS}

# Telecom bundles: (code, name, category, price BDT, validity days, data MB, voice minutes)
PACKAGES: list[tuple[str, str, str, int, int, int, int]] = [
    ("DATA_1GB_3D", "Data 1GB 3 days", "DATA", 49, 3, 1024, 0),
    ("DATA_1GB_7D", "Data 1GB 7 days", "DATA", 99, 7, 1024, 0),
    ("DATA_5GB_30D", "Data 5GB 30 days", "DATA", 399, 30, 5120, 0),
    ("SOCIAL_2GB_7D", "Social pack 2GB 7 days", "DATA", 59, 7, 2048, 0),
    ("VOICE_100MIN_7D", "Voice 100 minutes 7 days", "VOICE", 89, 7, 0, 100),
    ("COMBO_10GB_300MIN_30D", "Combo 10GB + 300 min 30 days", "COMBO", 699, 30, 10240, 300),
]

# Fictional electronics catalogue: (sku, name, category, dealer price BDT, factory)
PRODUCTS: list[tuple[str, str, str, int, str]] = [
    ("RF-230L", "Frost 230L refrigerator", "REFRIGERATOR", 42000, "FAC-01"),
    ("RF-340L", "Frost 340L refrigerator", "REFRIGERATOR", 58000, "FAC-01"),
    ("RF-180D", "Frost 180L direct-cool refrigerator", "REFRIGERATOR", 31000, "FAC-01"),
    ("TV-32H", "Vista 32in HD TV", "TELEVISION", 19500, "FAC-02"),
    ("TV-43U", "Vista 43in 4K TV", "TELEVISION", 38000, "FAC-02"),
    ("TV-55U", "Vista 55in 4K TV", "TELEVISION", 62000, "FAC-02"),
    ("AC-10T", "Breeze 1 ton inverter AC", "AIR_CONDITIONER", 48000, "FAC-01"),
    ("AC-15T", "Breeze 1.5 ton inverter AC", "AIR_CONDITIONER", 65000, "FAC-01"),
    ("SP-A15", "Nova A15 smartphone", "SMARTPHONE", 14500, "FAC-02"),
    ("SP-X20", "Nova X20 smartphone", "SMARTPHONE", 23000, "FAC-02"),
    ("FN-56C", "Aero 56in ceiling fan", "FAN", 3200, "FAC-01"),
    ("WM-7KG", "Aqua 7kg washing machine", "WASHING_MACHINE", 36000, "FAC-01"),
]
# Relative dealer demand per category (October: AC demand tapering, fans steady).
CATEGORY_DEMAND = {
    "REFRIGERATOR": 1.0, "TELEVISION": 1.0, "AIR_CONDITIONER": 0.5, "SMARTPHONE": 1.6, "FAN": 1.3,
    "WASHING_MACHINE": 0.4,
}

FACTORIES: list[tuple[str, str, str, str]] = [
    ("FAC-01", "North assembly plant", "DHAKA", "WH-DHK-01"),
    ("FAC-02", "Port electronics plant", "CHATTOGRAM", "WH-CTG-01"),
]

WAREHOUSE_ORDER = ["DHAKA", "CHATTOGRAM", "RAJSHAHI", "KHULNA", "SYLHET", "RANGPUR", "BARISHAL", "MYMENSINGH"]

# Dhaka local-hour demand curve for consumer telecom activity (morning ramp, evening peak).
TELECOM_HOURLY = [0.6, 0.4, 0.3, 0.25, 0.25, 0.35, 0.6, 0.9, 1.1, 1.3, 1.4, 1.4,
                  1.3, 1.2, 1.2, 1.2, 1.3, 1.4, 1.6, 1.8, 1.9, 1.8, 1.4, 0.9]
# Weekday factor, Monday=0 .. Sunday=6. Friday is the main weekly holiday in Bangladesh.
TELECOM_WEEKDAY = [1.0, 1.0, 1.0, 1.0, 0.9, 0.95, 1.0]
DEALER_WEEKDAY = [1.0, 1.0, 1.0, 1.0, 0.15, 0.8, 1.0]

RECHARGE_DENOMINATIONS = [(20, 0.15), (30, 0.10), (50, 0.25), (100, 0.20), (200, 0.12), (300, 0.06),
                          (500, 0.08), (1000, 0.04)]
CHANNELS = [("MOBILE_WALLET", 0.55, 0.03), ("RETAIL_AGENT", 0.30, 0.01), ("BANK_CARD", 0.08, 0.06),
            ("INTERNET_BANKING", 0.07, 0.05)]
FAILURE_REASONS = ["INSUFFICIENT_FUNDS", "GATEWAY_TIMEOUT", "CHANNEL_DECLINED"]
SEGMENTS = [("MASS", 0.62, 0.35), ("YOUTH", 0.25, 0.50), ("PREMIUM", 0.10, 0.25), ("ENTERPRISE", 0.03, 0.15)]


@dataclass(frozen=True)
class Warehouse:
    warehouse_id: str
    name: str
    region: str
    is_central: bool


@dataclass(frozen=True)
class Dealer:
    dealer_id: str
    name: str
    region: str
    tier: str
    credit_days: int


def build_warehouses(count: int) -> list[Warehouse]:
    if not 2 <= count <= len(WAREHOUSE_ORDER):
        raise ValueError(f"warehouses must be between 2 and {len(WAREHOUSE_ORDER)}")
    return [
        Warehouse(f"WH-{REGION_ABBR[r]}-01", f"{r.title()} distribution centre", r, r == "DHAKA")
        for r in WAREHOUSE_ORDER[:count]
    ]


def build_dealers(count: int, rng: random.Random) -> list[Dealer]:
    if not 1 <= count <= 9999:
        raise ValueError("dealers must be between 1 and 9999")
    dealers = []
    for i in range(1, count + 1):
        region = rng.choices(REGION_CODES, REGION_WEIGHTS)[0]
        tier = rng.choices(["A", "B", "C"], [0.15, 0.35, 0.5])[0]
        dealers.append(
            Dealer(f"DLR-{i:04d}", f"{region.title()} Electronics Outlet {i:04d}", region, tier,
                   {"A": 45, "B": 30, "C": 15}[tier])
        )
    return dealers
