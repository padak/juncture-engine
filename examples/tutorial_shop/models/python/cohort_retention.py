"""Cohort retention computed in Python.

SQL is fine for rolling aggregations; pivot-shaped cohort matrices are
much easier in pandas. This is the point of Python-in-the-DAG: pick
the right tool per model.

The function runs through the same executor as every SQL model:
``ctx.ref()`` bridges to upstream SQL seamlessly, ``ctx.vars()`` reads
the same external parameters used by SQL models via ``{{ var() }}``.
"""

from __future__ import annotations

from juncture import transform


@transform(depends_on=["stg_orders", "customers"])
def cohort_retention(ctx):
    """Month-of-first-order x month-of-subsequent-order retention grid."""
    import pandas as pd

    # ctx.ref() returns an Arrow Table from the DuckDB adapter;
    # .to_pandas() hands it over to pandas without a second copy.
    orders = ctx.ref("stg_orders").to_pandas()
    customers = ctx.ref("customers").to_pandas()

    # Cap the analysis at the `as_of` date so a tutorial reader can
    # back-fill the same view for any point in time via --var.
    # ctx.vars(key, default) is a single-key accessor, not a dict.
    as_of = pd.to_datetime(ctx.vars("as_of", "2026-03-31"))
    orders = orders[orders["order_ts"] <= as_of].copy()

    orders["order_month"] = orders["order_ts"].dt.to_period("M").astype(str)
    first_order = orders.groupby("customer_id")["order_ts"].min().reset_index()
    first_order["cohort_month"] = first_order["order_ts"].dt.to_period("M").astype(str)

    joined = orders.merge(first_order[["customer_id", "cohort_month"]], on="customer_id")
    grid = (
        joined.groupby(["cohort_month", "order_month"])["customer_id"]
        .nunique()
        .reset_index(name="active_customers")
    )
    cohort_size = (
        first_order.groupby("cohort_month")["customer_id"].nunique().reset_index(name="cohort_size")
    )
    grid = grid.merge(cohort_size, on="cohort_month")
    grid["retention_pct"] = (grid["active_customers"] / grid["cohort_size"] * 100).round(1)
    # Tack country on so BI can slice the grid without another join.
    grid = grid.merge(
        customers[["customer_id"]].drop_duplicates().merge(
            first_order[["customer_id", "cohort_month"]], on="customer_id"
        )[["cohort_month"]].drop_duplicates(),
        on="cohort_month",
        how="left",
    )
    return grid.sort_values(["cohort_month", "order_month"]).reset_index(drop=True)
