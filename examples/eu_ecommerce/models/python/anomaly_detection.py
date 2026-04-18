"""Flag anomalous revenue days via a rolling-window z-score.

Consumes the SQL mart ``daily_revenue`` and emits every (revenue_date,
country_code, channel_group) triple whose gross_revenue_eur deviates by
more than ``z_threshold`` sigma from a 14-day rolling mean.

Doing this in Python keeps the statistics simple and testable: no portable
SQL for rolling std across grouped time series.
"""

from __future__ import annotations

import pandas as pd

from juncture import transform

_WINDOW_DAYS = 14
_Z_THRESHOLD = 2.0


@transform(
    name="daily_revenue_anomalies",
    depends_on=["daily_revenue"],
    description="Rolling-window revenue anomaly detector (> 2 sigma deviations).",
)
def daily_revenue_anomalies(ctx):  # type: ignore[no-untyped-def]
    arrow = ctx.ref("daily_revenue")
    df = arrow.to_pandas()
    if df.empty:
        # Keep the schema stable so downstream consumers don't have to guess
        # at column names when the source is empty.
        return pd.DataFrame(
            columns=[
                "revenue_date",
                "country_code",
                "channel_group",
                "gross_revenue_eur",
                "rolling_mean",
                "rolling_std",
                "z_score",
                "is_anomaly",
            ]
        )

    df["revenue_date"] = pd.to_datetime(df["revenue_date"])
    df = df.sort_values(["country_code", "channel_group", "revenue_date"])

    grouped = df.groupby(["country_code", "channel_group"], group_keys=False)
    df["rolling_mean"] = grouped["gross_revenue_eur"].transform(
        lambda s: s.rolling(window=_WINDOW_DAYS, min_periods=3).mean()
    )
    df["rolling_std"] = grouped["gross_revenue_eur"].transform(
        lambda s: s.rolling(window=_WINDOW_DAYS, min_periods=3).std()
    )

    df["z_score"] = (df["gross_revenue_eur"] - df["rolling_mean"]) / df["rolling_std"]
    df["z_score"] = df["z_score"].fillna(0.0)
    df["is_anomaly"] = df["z_score"].abs() >= _Z_THRESHOLD

    out = df[
        [
            "revenue_date",
            "country_code",
            "channel_group",
            "gross_revenue_eur",
            "rolling_mean",
            "rolling_std",
            "z_score",
            "is_anomaly",
        ]
    ].copy()
    out["revenue_date"] = out["revenue_date"].dt.date
    return out
