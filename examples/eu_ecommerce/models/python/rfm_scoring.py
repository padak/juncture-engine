"""RFM quintile scoring, expressed in Python.

Recency / Frequency / Monetary scoring is natural in Python with
``pandas.qcut`` and trivially painful in portable SQL (``NTILE`` works but
silently collapses on small distinct value counts). Keeping the quintile
logic in Python makes the rule one line and fully testable.

The SQL upstream (``int_rfm_inputs``) does the heavy per-customer
aggregation; this model only buckets the aggregate values — which is the
whole point of mixing Python and SQL in one DAG (VISION #8).
"""

from __future__ import annotations

import pandas as pd

from juncture import transform


@transform(
    name="rfm_scores",
    depends_on=["int_rfm_inputs"],
    description="Quintile RFM scoring per customer.",
    schedule_cron="0 3 * * 1",  # Monday 03:00 UTC (metadata only, Phase 1)
)
def rfm_scores(ctx):  # type: ignore[no-untyped-def]
    arrow = ctx.ref("int_rfm_inputs")
    df = arrow.to_pandas()

    # pandas.qcut with duplicates="drop" handles the rare case where enough
    # customers share the same frequency/monetary that a quintile edge
    # collapses; we then re-label bins 1..n so consumers always see a
    # dense 1..5 scale (or less if the distribution is truly tiny).
    def _score(series: pd.Series, *, ascending: bool = True) -> pd.Series:
        try:
            codes = pd.qcut(series, 5, labels=False, duplicates="drop")
        except ValueError:
            # All values identical: everyone gets the middle score.
            return pd.Series([3] * len(series), index=series.index, dtype="int64")
        if not ascending:
            max_code = int(codes.max()) if len(codes) else 0
            codes = max_code - codes
        return (codes + 1).astype("int64")

    df = df.copy()
    # Recency: lower is better, so higher quintile = more recent buyer.
    df["r_score"] = _score(df["recency_days"], ascending=False)
    df["f_score"] = _score(df["frequency"])
    df["m_score"] = _score(df["monetary_eur"])
    df["rfm_score"] = df["r_score"] * 100 + df["f_score"] * 10 + df["m_score"]
    df["rfm_tier"] = df["rfm_score"].apply(_rfm_tier)

    return df[
        [
            "customer_id",
            "recency_days",
            "frequency",
            "monetary_eur",
            "avg_order_value_eur",
            "first_order_date",
            "last_order_date",
            "r_score",
            "f_score",
            "m_score",
            "rfm_score",
            "rfm_tier",
        ]
    ]


def _rfm_tier(score: int) -> str:
    """Coarse tier bucket from the three-digit score (recency/frequency/monetary)."""
    if score >= 454:
        return "champion"
    if score >= 344:
        return "loyal"
    if score >= 234:
        return "potential"
    if score >= 123:
        return "at_risk"
    return "hibernating"
