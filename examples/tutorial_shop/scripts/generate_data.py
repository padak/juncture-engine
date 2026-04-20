"""Deterministic synthetic-data generator for the tutorial_shop example.

Produces two CSV seeds compatible with the schema documented in
``docs/TUTORIAL.md``:

* ``customers.csv`` -- customer_id, email, country, signed_up_at
* ``orders.csv``    -- order_id, customer_id, order_ts, amount_eur, status

Scales (customers / orders)::

    tiny      8 /    20   -- same volume as the hand-written seeds
    small    50 /   200   -- default, enough to see joins / aggregates matter
    medium  500 /  2000   -- stress the executor without waiting for coffee

No third-party dependencies: uses ``random`` seeded from ``--seed`` (default
42) so successive runs are byte-identical.

Usage::

    # cd into your project first; default output is ``./seeds`` (CWD).
    cd my_shop
    python /path/to/juncture-engine/examples/tutorial_shop/scripts/generate_data.py
    python /path/to/juncture-engine/examples/tutorial_shop/scripts/generate_data.py --scale medium

    # Or explicit, from anywhere:
    python .../generate_data.py --output-dir /tmp/seeds
"""

from __future__ import annotations

import argparse
import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

# Four-country mix matches the hand-written seeds; keep ASCII-only names to
# avoid CSV encoding gotchas across OSes.
COUNTRIES = ["CZ", "DE", "FR", "NL"]

FIRST_NAMES = [
    "alice",
    "bob",
    "charlie",
    "dana",
    "erik",
    "fatima",
    "george",
    "hannah",
    "irena",
    "jakub",
    "karel",
    "lucia",
    "milos",
    "nina",
    "ondra",
    "petra",
    "radka",
    "sarka",
    "tomas",
    "ursula",
    "viktor",
    "wenda",
    "xenie",
    "yvona",
    "zuzana",
]

SCALES: dict[str, dict[str, int]] = {
    "tiny": {"customers": 8, "orders": 20},
    "small": {"customers": 50, "orders": 200},
    "medium": {"customers": 500, "orders": 2000},
}

# Matches the hand-written dataset's ~90 % completed / ~10 % refunded mix.
STATUS_CHOICES = ["completed", "refunded"]
STATUS_WEIGHTS = [0.92, 0.08]

# Window of interest: matches the hand-written seeds (Nov 2025 - Mar 2026).
SIGNUP_START = date(2025, 11, 1)
ORDER_LATEST = date(2026, 3, 31)


def generate_customers(n: int, rng: random.Random) -> list[dict[str, str | int]]:
    rows: list[dict[str, str | int]] = []
    signup_span_days = (ORDER_LATEST - SIGNUP_START).days
    for i in range(n):
        base_name = FIRST_NAMES[i % len(FIRST_NAMES)]
        suffix = "" if i < len(FIRST_NAMES) else str(i // len(FIRST_NAMES))
        email = f"{base_name}{suffix}@example.com"
        signup = SIGNUP_START + timedelta(days=rng.randint(0, signup_span_days))
        rows.append(
            {
                "customer_id": 101 + i,
                "email": email,
                "country": rng.choice(COUNTRIES),
                "signed_up_at": signup.isoformat(),
            }
        )
    return rows


def generate_orders(
    n: int,
    customers: list[dict[str, str | int]],
    rng: random.Random,
) -> list[dict[str, str | int]]:
    rows: list[dict[str, str | int]] = []
    for oid in range(1, n + 1):
        customer = rng.choice(customers)
        signup = date.fromisoformat(str(customer["signed_up_at"]))
        span_days = max(1, (ORDER_LATEST - signup).days)
        order_day = signup + timedelta(days=rng.randint(0, span_days))
        order_ts = datetime.combine(order_day, datetime.min.time()) + timedelta(
            hours=rng.randint(8, 20),
            minutes=rng.randint(0, 59),
        )
        # Rough log-ish distribution skewed to small-ticket purchases:
        # 70 % under 200 EUR, rest up to 1500 EUR.
        amount = rng.uniform(10, 200) if rng.random() < 0.7 else rng.uniform(200, 1500)
        status = rng.choices(STATUS_CHOICES, weights=STATUS_WEIGHTS, k=1)[0]
        rows.append(
            {
                "order_id": oid,
                "customer_id": customer["customer_id"],
                "order_ts": order_ts.strftime("%Y-%m-%d %H:%M:%S"),
                "amount_eur": f"{amount:.2f}",
                "status": status,
            }
        )
    return rows


def write_csv(path: Path, rows: list[dict[str, str | int]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    # Resolve the default lazily so users who cd into a project get data
    # written next to them, not into the repo where this script lives.
    default_out = Path.cwd() / "seeds"

    parser = argparse.ArgumentParser(
        description="Generate synthetic seeds compatible with docs/TUTORIAL.md.",
    )
    parser.add_argument("--scale", choices=list(SCALES), default="small")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=default_out,
        help="Target directory for customers.csv / orders.csv (default: ./seeds in CWD).",
    )
    args = parser.parse_args()

    cfg = SCALES[args.scale]
    rng = random.Random(args.seed)

    customers = generate_customers(cfg["customers"], rng)
    orders = generate_orders(cfg["orders"], customers, rng)

    write_csv(args.output_dir / "customers.csv", customers)
    write_csv(args.output_dir / "orders.csv", orders)

    print(
        f"Wrote {len(customers)} customers and {len(orders)} orders "
        f"to {args.output_dir} (scale={args.scale}, seed={args.seed})"
    )


if __name__ == "__main__":
    main()
