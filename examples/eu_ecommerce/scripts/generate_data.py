"""Deterministic data generator for the Fjord & Fable EU e-commerce demo.

Produces seven CSV seed files under ``seeds/`` (or ``--output-dir``). No
external deps beyond the standard library: uses :mod:`random` seeded from
``--seed`` (default 42) and hard-coded name/token lists.

Scales:

* ``small``   5 000 customers /   100 products /    20 000 orders /  15 campaigns   (CI)
* ``medium`` 50 000 customers /   300 products /   200 000 orders /  80 campaigns   (default)
* ``full``  500 000 customers /   500 products / 3 000 000 orders / 160 campaigns   (stress)

Usage::

    python scripts/generate_data.py --scale medium
    python scripts/generate_data.py --scale small --output-dir /tmp/seeds
"""

from __future__ import annotations

import argparse
import csv
import logging
import random
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

log = logging.getLogger(__name__)

# Fictional EU e-commerce "Fjord & Fable". All data is synthetic.
COUNTRIES = [
    ("CZ", "EUR", ["Prague", "Brno", "Ostrava", "Plzen", "Liberec"]),
    ("DE", "EUR", ["Berlin", "Munich", "Hamburg", "Cologne", "Frankfurt"]),
    ("FR", "EUR", ["Paris", "Lyon", "Marseille", "Toulouse", "Nice"]),
    ("NL", "EUR", ["Amsterdam", "Rotterdam", "The Hague", "Utrecht", "Eindhoven"]),
]
# CZK used as a secondary currency for a fraction of Czech orders to exercise
# currency-aware aggregations. EUR rate used for conversion downstream.
CZK_PER_EUR = 24.5

LANGUAGES = {"CZ": "cs", "DE": "de", "FR": "fr", "NL": "nl"}

# Campaign metadata; channels drive marketing-mix reporting downstream.
CHANNELS = ["email", "display", "social", "affiliate"]

# Order status mix; pending orders are intentionally excluded from several
# marts so downstream filters ("only closed orders") are meaningful.
ORDER_STATUSES = ["completed", "refunded", "cancelled", "pending"]
ORDER_STATUS_WEIGHTS = [0.80, 0.06, 0.04, 0.10]

# Names are Latin-only to avoid CSV encoding gotchas across OSes.
FIRST_NAMES = [
    "Anna",
    "Petr",
    "Jan",
    "Eva",
    "Martin",
    "Karel",
    "Pavel",
    "Lucie",
    "Tereza",
    "Jana",
    "Markus",
    "Laura",
    "Sven",
    "Sophie",
    "Lukas",
    "Hannah",
    "Noah",
    "Lena",
    "Paul",
    "Emma",
    "Pierre",
    "Camille",
    "Louis",
    "Marie",
    "Antoine",
    "Chloe",
    "Hugo",
    "Manon",
    "Adrien",
    "Lea",
    "Daan",
    "Sanne",
    "Bram",
    "Fleur",
    "Sem",
    "Noor",
    "Finn",
    "Lotte",
    "Luuk",
    "Mila",
    "Tomas",
    "Kristyna",
    "David",
    "Katerina",
    "Michal",
    "Barbora",
    "Vaclav",
    "Simona",
    "Jakub",
    "Helena",
]
LAST_NAMES = [
    "Svoboda",
    "Novak",
    "Cerna",
    "Horak",
    "Dvorak",
    "Polakova",
    "Becker",
    "Fischer",
    "Muller",
    "Wagner",
    "Schmidt",
    "Schneider",
    "Weber",
    "Richter",
    "Hoffmann",
    "Dupont",
    "Bernard",
    "Thomas",
    "Robert",
    "Petit",
    "Durand",
    "Richard",
    "Moreau",
    "Laurent",
    "Simon",
    "de Jong",
    "Jansen",
    "Bakker",
    "Visser",
    "Smit",
    "Meijer",
    "Kooij",
    "Dekker",
    "Bosch",
    "van Dijk",
    "Mares",
    "Hrubes",
    "Krejci",
    "Pokorna",
    "Benes",
]

# Product taxonomy: four departments, ~15 categories, many SKUs per category.
DEPARTMENTS = {
    "Home": [
        "Bedding",
        "Kitchen Essentials",
        "Lighting",
        "Storage",
    ],
    "Outdoor": [
        "Camping",
        "Hiking Gear",
        "Garden Tools",
    ],
    "Kitchen": [
        "Cookware",
        "Small Appliances",
        "Tableware",
    ],
    "Apparel": [
        "Outerwear",
        "Footwear",
        "Accessories",
        "Basics",
        "Activewear",
    ],
}

PRODUCT_NAME_TOKENS = {
    "Bedding": ["Fjord", "Nordic", "Arctic", "Sunrise", "Cloud"],
    "Kitchen Essentials": ["Viking", "Forge", "Hearth", "Nordic", "Rustic"],
    "Lighting": ["Aurora", "Polar", "Lumen", "Ember", "Beacon"],
    "Storage": ["Stack", "Keep", "Vault", "Haven", "Nest"],
    "Camping": ["Trail", "Ridge", "Summit", "Aurora", "Wild"],
    "Hiking Gear": ["Peak", "Alpine", "Trail", "Summit", "Outland"],
    "Garden Tools": ["Grove", "Bloom", "Terra", "Harvest", "Root"],
    "Cookware": ["Forge", "Iron", "Heritage", "Classic", "Copper"],
    "Small Appliances": ["Nova", "Prime", "Home", "Craft", "Pulse"],
    "Tableware": ["Fable", "Heritage", "Meadow", "Seaside", "Linen"],
    "Outerwear": ["Fjord", "Borealis", "Summit", "Vigor", "Urban"],
    "Footwear": ["Trail", "Voyager", "Stride", "Terra", "Comet"],
    "Accessories": ["Luxe", "Harbor", "Voyager", "Stack", "Fable"],
    "Basics": ["Daily", "Core", "Essentials", "Classic", "Harbor"],
    "Activewear": ["Pulse", "Flex", "Motion", "Stride", "Trail"],
}
PRODUCT_TYPES = {
    "Bedding": ["Duvet", "Pillowcase", "Sheet Set", "Blanket", "Throw"],
    "Kitchen Essentials": ["Cutting Board", "Mixing Bowl", "Utensil Set", "Trivet", "Apron"],
    "Lighting": ["Desk Lamp", "Floor Lamp", "Pendant", "Candle", "String Lights"],
    "Storage": ["Bin", "Basket", "Shelf", "Organizer", "Box"],
    "Camping": ["Tent", "Sleeping Bag", "Stove", "Lantern", "Cookset"],
    "Hiking Gear": ["Backpack", "Trekking Poles", "Boots", "Jacket", "Socks"],
    "Garden Tools": ["Trowel", "Pruner", "Rake", "Watering Can", "Gloves"],
    "Cookware": ["Skillet", "Saucepan", "Dutch Oven", "Wok", "Baking Sheet"],
    "Small Appliances": ["Kettle", "Toaster", "Blender", "Coffee Maker", "Grinder"],
    "Tableware": ["Plate", "Bowl", "Mug", "Glass", "Flatware"],
    "Outerwear": ["Parka", "Jacket", "Vest", "Windbreaker", "Shell"],
    "Footwear": ["Boots", "Sneakers", "Sandals", "Loafers", "Runners"],
    "Accessories": ["Scarf", "Hat", "Belt", "Gloves", "Bag"],
    "Basics": ["T-Shirt", "Sweater", "Pants", "Henley", "Shorts"],
    "Activewear": ["Leggings", "Top", "Shorts", "Hoodie", "Tank"],
}

TRAFFIC_SOURCES = ["organic", "paid_search", "email", "social", "direct", "referral"]
DEVICES = ["desktop", "mobile", "tablet"]


@dataclass(frozen=True)
class Scale:
    """Scale parameters: row counts for each entity."""

    name: str
    customers: int
    products: int
    orders: int
    campaigns: int
    sessions_per_customer: float


SCALES: dict[str, Scale] = {
    "small": Scale("small", 5_000, 100, 20_000, 15, 0.4),
    "medium": Scale("medium", 50_000, 300, 200_000, 80, 0.8),
    "full": Scale("full", 500_000, 500, 3_000_000, 160, 1.2),
}


# Window of 2 years, inclusive-exclusive.
HISTORY_START = date(2024, 1, 1)
HISTORY_END = date(2026, 1, 1)
HISTORY_DAYS = (HISTORY_END - HISTORY_START).days  # 731


def _ensure_output_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def _iso(value: date | datetime) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return value.strftime("%Y-%m-%d")


def _random_date(rng: random.Random, start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=rng.randint(0, max(delta - 1, 0)))


def _random_datetime(rng: random.Random, start: date, end: date) -> datetime:
    d = _random_date(rng, start, end)
    return datetime.combine(d, datetime.min.time()) + timedelta(
        hours=rng.randint(0, 23),
        minutes=rng.randint(0, 59),
        seconds=rng.randint(0, 59),
    )


def write_product_categories(out_dir: Path) -> list[tuple[int, str, str]]:
    """Write ``product_categories.csv`` and return rows as tuples."""
    rows: list[tuple[int, str, str]] = []
    category_id = 1
    for department, categories in DEPARTMENTS.items():
        for category_name in categories:
            rows.append((category_id, category_name, department))
            category_id += 1

    with (out_dir / "product_categories.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["category_id", "category_name", "department"])
        writer.writerows(rows)
    log.info("product_categories.csv: %d rows", len(rows))
    return rows


def write_products(
    out_dir: Path,
    rng: random.Random,
    scale: Scale,
    categories: list[tuple[int, str, str]],
) -> list[tuple[int, str, str, int, str, float, float, str, bool]]:
    """Write ``products.csv`` and return the generated rows."""
    rows: list[tuple[int, str, str, int, str, float, float, str, bool]] = []
    sku_seen: set[str] = set()
    for product_id in range(1, scale.products + 1):
        category_id, category_name, department = rng.choice(categories)
        tokens = PRODUCT_NAME_TOKENS[category_name]
        types = PRODUCT_TYPES[category_name]
        token = rng.choice(tokens)
        kind = rng.choice(types)
        # Short descriptive suffix so SKUs stay unique.
        variant = rng.choice(["Classic", "Pro", "Lite", "Plus", "Select", "Edition"])
        name = f"{token} {kind} {variant}"
        sku_root = f"{department[:2].upper()}-{category_id:02d}-{product_id:04d}"
        sku = sku_root
        while sku in sku_seen:
            sku = f"{sku_root}-{rng.randint(10, 99)}"
        sku_seen.add(sku)
        # Prices: lognormal-ish via uniform-of-log to avoid numpy.
        price = round(math_expm1_uniform(rng, 2.5, 6.2), 2)  # 11.23 EUR to ~480 EUR
        margin_ratio = rng.uniform(0.25, 0.65)
        cost = round(price * (1 - margin_ratio), 2)
        launched_at = _random_date(rng, date(2023, 1, 1), HISTORY_END)
        is_active = rng.random() > 0.07  # ~7 % retired
        # A pre-launched product would never be bought; enforce launch before
        # history_end for simplicity.
        rows.append(
            (
                product_id,
                sku,
                name,
                category_id,
                # Denormalised subcategory label (= category_name) for simpler
                # demo queries; keeps schema flat.
                category_name,
                price,
                cost,
                _iso(launched_at),
                is_active,
            )
        )

    with (out_dir / "products.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            [
                "product_id",
                "sku",
                "name",
                "category_id",
                "subcategory",
                "price_eur",
                "cost_eur",
                "launched_at",
                "is_active",
            ]
        )
        writer.writerows(rows)
    log.info("products.csv: %d rows", len(rows))
    return rows


def math_expm1_uniform(rng: random.Random, lo_log: float, hi_log: float) -> float:
    """Draw a value log-uniformly between ``exp(lo_log)`` and ``exp(hi_log)``.

    We intentionally avoid numpy. ``random.expovariate`` is too skewed for a
    price distribution; sampling log-uniform gives a reasonable catalogue
    price spread (cheap accessories to pricier outerwear).
    """
    import math

    return math.exp(rng.uniform(lo_log, hi_log))


def write_customers(
    out_dir: Path,
    rng: random.Random,
    scale: Scale,
) -> list[tuple[int, str, str, str, str, str, bool]]:
    """Write ``customers.csv`` and return the rows."""
    rows: list[tuple[int, str, str, str, str, str, bool]] = []
    for customer_id in range(1, scale.customers + 1):
        country_code, _currency, cities = rng.choice(COUNTRIES)
        city = rng.choice(cities)
        first = rng.choice(FIRST_NAMES)
        last = rng.choice(LAST_NAMES)
        # Force uniqueness by appending the customer id.
        email = f"{first.lower()}.{last.lower().replace(' ', '')}{customer_id}@example.test"
        signup_date = _random_date(rng, HISTORY_START, HISTORY_END)
        language = LANGUAGES[country_code]
        marketing_consent = rng.random() > 0.35  # ~65 % opt-in
        rows.append(
            (
                customer_id,
                email,
                country_code,
                city,
                _iso(signup_date),
                language,
                marketing_consent,
            )
        )

    with (out_dir / "customers.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            [
                "customer_id",
                "email",
                "country_code",
                "city",
                "signup_date",
                "preferred_language",
                "marketing_consent",
            ]
        )
        writer.writerows(rows)
    log.info("customers.csv: %d rows", len(rows))
    return rows


def write_campaigns(
    out_dir: Path,
    rng: random.Random,
    scale: Scale,
) -> list[tuple[int, str, str, str, str, float, str]]:
    """Write ``campaigns.csv`` and return the rows.

    Target ~20 campaigns per quarter at full scale over 8 quarters; smaller
    scales simply run fewer campaigns per quarter. Each campaign runs 3 to
    28 days.
    """
    rows: list[tuple[int, str, str, str, str, float, str]] = []
    quarters = []
    q_start = HISTORY_START
    while q_start < HISTORY_END:
        q_end = q_start + timedelta(days=91)
        quarters.append((q_start, min(q_end, HISTORY_END)))
        q_start = q_end

    # Distribute the full campaign budget across quarters. A naive
    # `campaigns // quarters` truncates at small scales; using divmod lets us
    # front-load remainder campaigns into the first quarters so the total
    # lands on ``scale.campaigns`` exactly.
    base, remainder = divmod(scale.campaigns, len(quarters))
    campaign_id = 1
    for q_idx, (q_start, q_end) in enumerate(quarters):
        per_this_quarter = base + (1 if q_idx < remainder else 0)
        for _ in range(per_this_quarter):
            if campaign_id > scale.campaigns:
                break
            channel = rng.choice(CHANNELS)
            country_code, _currency, _cities = rng.choice(COUNTRIES)
            started_at = _random_date(rng, q_start, q_end)
            duration = rng.randint(3, 28)
            ended_at = min(started_at + timedelta(days=duration), q_end)
            # Budget scales by channel; display is expensive, email is cheap.
            budget_ranges = {
                "email": (500.0, 3_000.0),
                "display": (5_000.0, 30_000.0),
                "social": (2_000.0, 15_000.0),
                "affiliate": (1_000.0, 8_000.0),
            }
            lo, hi = budget_ranges[channel]
            budget = round(rng.uniform(lo, hi), 2)
            name = f"{channel.title()} Q{q_idx % 4 + 1} {country_code} #{campaign_id}"
            rows.append(
                (
                    campaign_id,
                    name,
                    channel,
                    _iso(started_at),
                    _iso(ended_at),
                    budget,
                    country_code,
                )
            )
            campaign_id += 1

    with (out_dir / "campaigns.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            [
                "campaign_id",
                "name",
                "channel",
                "started_at",
                "ended_at",
                "budget_eur",
                "target_country",
            ]
        )
        writer.writerows(rows)
    log.info("campaigns.csv: %d rows", len(rows))
    return rows


def write_orders_and_items(
    out_dir: Path,
    rng: random.Random,
    scale: Scale,
    customers: list[tuple[int, str, str, str, str, str, bool]],
    products: list[tuple[int, str, str, int, str, float, float, str, bool]],
    campaigns: list[tuple[int, str, str, str, str, float, str]],
) -> tuple[int, int]:
    """Write ``orders.csv`` and ``order_items.csv``; return row counts.

    Customer order count is geometric(1-0.35) clipped to [1, 18]; this gives
    most customers 1-3 orders and a long tail of repeat buyers.
    """
    # Customer -> country lookup for currency selection.
    customer_country: dict[int, str] = {row[0]: row[2] for row in customers}
    # Active products only (feels realistic; retired skus rarely ordered).
    active_products = [p for p in products if p[-1]]
    product_price: dict[int, float] = {p[0]: p[5] for p in active_products}
    product_ids = [p[0] for p in active_products]

    # Campaign lookup: campaigns -> (started, ended, target_country).
    campaign_spans: list[tuple[int, date, date, str]] = []
    for c in campaigns:
        started = datetime.strptime(c[3], "%Y-%m-%d").date()
        ended = datetime.strptime(c[4], "%Y-%m-%d").date()
        campaign_spans.append((c[0], started, ended, c[6]))

    orders_path = out_dir / "orders.csv"
    items_path = out_dir / "order_items.csv"

    # Write incrementally to keep memory flat at full scale (3M orders).
    total_orders = 0
    total_items = 0
    order_id = 100_000  # opaque numeric ids starting well above 0 to exercise
    # downstream casts.
    orders_target = scale.orders

    with (
        orders_path.open("w", newline="", encoding="utf-8") as orders_fh,
        items_path.open("w", newline="", encoding="utf-8") as items_fh,
    ):
        order_writer = csv.writer(orders_fh)
        items_writer = csv.writer(items_fh)
        order_writer.writerow(
            [
                "order_id",
                "customer_id",
                "placed_at",
                "status",
                "currency",
                "total_amount",
                "shipping_cost",
                "campaign_id",
            ]
        )
        items_writer.writerow(
            [
                "order_id",
                "line_no",
                "product_id",
                "quantity",
                "unit_price",
                "discount_pct",
            ]
        )

        # Distribute the orders budget across customers by drawing an
        # order-count per customer until we hit the target. Geometric gives
        # a long tail and lets us hit the exact orders target.
        customer_ids = [c[0] for c in customers]
        rng.shuffle(customer_ids)
        cursor = 0

        while total_orders < orders_target:
            # Cycle back through customers if budget exceeds geometric draws.
            if cursor >= len(customer_ids):
                rng.shuffle(customer_ids)
                cursor = 0
            customer_id = customer_ids[cursor]
            cursor += 1
            # Geometric draw clipped to [1, 18].
            order_count = 1
            while rng.random() < 0.55 and order_count < 18:
                order_count += 1

            for _ in range(order_count):
                if total_orders >= orders_target:
                    break
                order_id += 1
                placed_at = _random_datetime(rng, HISTORY_START, HISTORY_END)
                status = rng.choices(ORDER_STATUSES, ORDER_STATUS_WEIGHTS, k=1)[0]
                country = customer_country[customer_id]
                # ~15 % of CZ orders priced in CZK; everyone else in EUR.
                currency = "CZK" if (country == "CZ" and rng.random() < 0.15) else "EUR"
                # 1-5 line items per order, geometric-ish.
                lines = 1
                while rng.random() < 0.45 and lines < 5:
                    lines += 1

                campaign_id: int | str = ""
                # 40 % of orders are tied to a campaign that was active on the
                # placed_at date and targets the customer's country.
                if rng.random() < 0.40:
                    placed_day = placed_at.date()
                    eligible = [
                        cid for cid, s, e, tgt in campaign_spans if s <= placed_day <= e and tgt == country
                    ]
                    if eligible:
                        campaign_id = rng.choice(eligible)

                total_amount = 0.0
                line_rows: list[tuple[int, int, int, int, float, float]] = []
                for line_no in range(1, lines + 1):
                    product_id = rng.choice(product_ids)
                    unit_price = product_price[product_id]
                    qty = 1 + (rng.choice([0, 0, 0, 1, 1, 2, 3]) if rng.random() < 0.7 else 0)
                    discount_pct = rng.choice([0.0, 0.0, 0.0, 0.05, 0.10, 0.15, 0.20, 0.30])
                    net = round(qty * unit_price * (1 - discount_pct), 2)
                    total_amount += net
                    line_rows.append((order_id, line_no, product_id, qty, unit_price, discount_pct))
                shipping_cost = round(rng.choice([0.0, 2.99, 4.99, 6.99, 9.99]), 2)
                total_amount = round(total_amount + shipping_cost, 2)
                if currency == "CZK":
                    total_amount = round(total_amount * CZK_PER_EUR, 2)
                    shipping_cost = round(shipping_cost * CZK_PER_EUR, 2)
                order_writer.writerow(
                    [
                        order_id,
                        customer_id,
                        _iso(placed_at),
                        status,
                        currency,
                        total_amount,
                        shipping_cost,
                        campaign_id,
                    ]
                )
                items_writer.writerows(line_rows)
                total_orders += 1
                total_items += len(line_rows)

    log.info("orders.csv: %d rows", total_orders)
    log.info("order_items.csv: %d rows", total_items)
    return total_orders, total_items


def write_web_sessions(
    out_dir: Path,
    rng: random.Random,
    scale: Scale,
    customers: list[tuple[int, str, str, str, str, str, bool]],
) -> int:
    """Write ``web_sessions.csv``; return row count.

    Sessions scale with customer count; ``sessions_per_customer`` acts as a
    multiplier to keep CSV sizes bounded at the ``small`` scale.
    """
    total_sessions = int(scale.customers * scale.sessions_per_customer)
    customer_ids = [c[0] for c in customers]
    path = out_dir / "web_sessions.csv"
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(
            [
                "session_id",
                "customer_id",
                "started_at",
                "device",
                "traffic_source",
                "pageviews",
                "bounced",
            ]
        )
        for session_id in range(1, total_sessions + 1):
            # ~30 % anonymous (no customer_id yet).
            customer_id = "" if rng.random() < 0.30 else str(rng.choice(customer_ids))
            started_at = _random_datetime(rng, HISTORY_START, HISTORY_END)
            device = rng.choices(DEVICES, [0.40, 0.50, 0.10], k=1)[0]
            source = rng.choice(TRAFFIC_SOURCES)
            pageviews = max(1, int(rng.expovariate(1 / 4.5)))
            bounced = pageviews == 1
            writer.writerow(
                [
                    session_id,
                    customer_id,
                    _iso(started_at),
                    device,
                    source,
                    pageviews,
                    bounced,
                ]
            )
    log.info("web_sessions.csv: %d rows", total_sessions)
    return total_sessions


def generate(scale_name: str, output_dir: Path, seed: int) -> dict[str, int]:
    """Main entry point; returns a dict of ``filename -> row_count``."""
    if scale_name not in SCALES:
        raise ValueError(f"unknown scale {scale_name!r}; expected one of {sorted(SCALES)}")
    scale = SCALES[scale_name]
    rng = random.Random(seed)

    _ensure_output_dir(output_dir)

    categories = write_product_categories(output_dir)
    products = write_products(output_dir, rng, scale, categories)
    customers = write_customers(output_dir, rng, scale)
    campaigns = write_campaigns(output_dir, rng, scale)
    orders_count, items_count = write_orders_and_items(output_dir, rng, scale, customers, products, campaigns)
    sessions_count = write_web_sessions(output_dir, rng, scale, customers)

    return {
        "product_categories.csv": len(categories),
        "products.csv": len(products),
        "customers.csv": len(customers),
        "campaigns.csv": len(campaigns),
        "orders.csv": orders_count,
        "order_items.csv": items_count,
        "web_sessions.csv": sessions_count,
    }


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scale",
        choices=sorted(SCALES),
        default="medium",
        help="Dataset scale (small for CI, medium default, full for stress tests).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Destination dir for CSVs (default: seeds/ relative to this script).",
    )
    parser.add_argument("--seed", type=int, default=42, help="RNG seed (default 42).")
    args = parser.parse_args()

    output_dir: Path = args.output_dir or (Path(__file__).resolve().parent.parent / "seeds")
    counts = generate(args.scale, output_dir, args.seed)
    log.info("Done. Seeds written to %s", output_dir)
    for name, n in counts.items():
        log.info("  %-28s %8d rows", name, n)


if __name__ == "__main__":
    main()
