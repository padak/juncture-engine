"""Customer Lifetime Value (CLV) estimate.

Simple closed-form CLV:

    CLV = AOV * frequency * expected_lifetime_months / 12

where:
   * AOV  = average order value (EUR)
   * frequency = historical orders per customer
   * expected_lifetime_months scales with RFM tier (champions stay longer).

The model ingests both a SQL upstream (``int_order_facts``) and a Python
upstream (``rfm_scores``) — the canonical "SQL and Python in the same DAG"
demo (VISION #8). RFM tiers drive the expected_lifetime_months parameter,
so business owners can tune retention-based CLV without touching SQL.
"""

from __future__ import annotations

from juncture import transform

# Expected future buying-relationship length by RFM tier, in months. These
# are business inputs, not data-derived — in a real stack they'd live in a
# small YAML under seeds/ and flow through a seed. Keeping them inline for
# the demo makes the relationship between the knob and the output obvious.
_LIFETIME_BY_TIER = {
    "champion": 36,
    "loyal": 24,
    "potential": 18,
    "at_risk": 9,
    "hibernating": 3,
}


@transform(
    name="customer_ltv",
    depends_on=["int_order_facts", "rfm_scores"],
    description="Closed-form CLV estimate per customer blending SQL facts + Python RFM.",
)
def customer_ltv(ctx):  # type: ignore[no-untyped-def]
    facts = ctx.ref("int_order_facts").to_pandas()
    rfm = ctx.ref("rfm_scores").to_pandas()

    per_customer = (
        facts[facts["status"] == "completed"]
        .groupby("customer_id", as_index=False)
        .agg(
            order_count=("order_id", "count"),
            total_revenue_eur=("total_amount_eur", "sum"),
            total_margin_eur=("items_margin_eur", "sum"),
            first_order=("placed_date", "min"),
            last_order=("placed_date", "max"),
        )
    )
    per_customer["aov_eur"] = per_customer["total_revenue_eur"] / per_customer["order_count"]

    joined = per_customer.merge(
        rfm[["customer_id", "rfm_tier", "rfm_score"]],
        on="customer_id",
        how="left",
    )
    joined["rfm_tier"] = joined["rfm_tier"].fillna("hibernating")
    joined["expected_lifetime_months"] = joined["rfm_tier"].map(_LIFETIME_BY_TIER).fillna(3)
    joined["annual_order_rate"] = joined["order_count"]  # treat whole 2-year window as 1 "year" signal
    # CLV = AOV * frequency * lifetime (in years). Scale frequency as
    # orders-per-year by halving the 2-year window.
    joined["orders_per_year"] = joined["order_count"] / 2.0
    joined["ltv_eur"] = (
        joined["aov_eur"] * joined["orders_per_year"] * (joined["expected_lifetime_months"] / 12.0)
    )

    return joined[
        [
            "customer_id",
            "rfm_tier",
            "rfm_score",
            "order_count",
            "aov_eur",
            "orders_per_year",
            "expected_lifetime_months",
            "total_revenue_eur",
            "total_margin_eur",
            "ltv_eur",
        ]
    ].copy()
