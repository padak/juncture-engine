"""Simple RFM-style segmentation as a Python transformation.

This model is expressed in Python because the segmentation rules mix non-trivial
conditionals and the explicit tie-break priority reads better than nested CASE
expressions in SQL. The DAG is still fully visible: the decorator declares the
upstream ``fct_completed_orders`` dependency.
"""

from __future__ import annotations

import pandas as pd

from juncture import transform


@transform(
    name="customer_segment",
    depends_on=["fct_completed_orders", "dim_customers"],
    description="Segment customers into vip/regular/new based on order history.",
)
def customer_segment(ctx):  # type: ignore[no-untyped-def]
    orders = ctx.ref("fct_completed_orders").to_pandas()
    customers = ctx.ref("dim_customers").to_pandas()

    agg = (
        orders.groupby("customer_id", as_index=False)
        .agg(order_count=("order_id", "count"), lifetime_value=("amount", "sum"))
    )
    joined = customers.merge(agg, on="customer_id", how="left").fillna(
        {"order_count": 0, "lifetime_value": 0}
    )

    def _segment(row: pd.Series) -> str:
        if row["lifetime_value"] >= 500:
            return "vip"
        if row["order_count"] >= 2:
            return "regular"
        if row["order_count"] == 1:
            return "occasional"
        return "new"

    joined["segment"] = joined.apply(_segment, axis=1)
    return joined[["customer_id", "name", "country", "order_count", "lifetime_value", "segment"]]
