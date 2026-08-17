"""Aggregation of raw ticks into OHLC bars.

Ticks arrive at least once, so the same trade can land in the raw layer more than
once. Deduplication by (pair, trade_id) happens here, where a whole day is visible,
rather than in the consumer, which only ever sees one batch.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

BAR_COLUMNS = [
    "pair", "bar_start", "open", "high", "low", "close",
    "volume", "trade_count", "volatility",
]

FREQUENCIES = {"1m": "1min", "1h": "1h"}


def deduplicate(ticks: pd.DataFrame) -> pd.DataFrame:
    """Drop repeated trades, keeping one row per (pair, trade_id)."""
    if ticks.empty:
        return ticks
    return (
        ticks.sort_values(["pair", "trade_id"])
        .drop_duplicates(subset=["pair", "trade_id"], keep="first")
        .reset_index(drop=True)
    )


def _realised_volatility(prices: pd.Series) -> float:
    """Standard deviation of tick-to-tick log returns inside a bar."""
    if len(prices) < 2:
        return 0.0
    returns = np.diff(np.log(prices.to_numpy(dtype="float64")))
    return float(np.std(returns, ddof=1)) if len(returns) > 1 else 0.0


def aggregate_ohlc(ticks: pd.DataFrame, freq: str = "1m") -> pd.DataFrame:
    """Aggregate ticks into OHLC bars at the requested frequency."""
    if freq not in FREQUENCIES:
        raise ValueError(f"unsupported frequency {freq!r}, expected one of {list(FREQUENCIES)}")
    if ticks.empty:
        return pd.DataFrame(columns=BAR_COLUMNS)

    frame = deduplicate(ticks).copy()
    frame["traded_at"] = pd.to_datetime(frame["traded_at"], utc=True)
    frame = frame.sort_values(["pair", "traded_at", "trade_id"])

    grouped = frame.groupby(
        ["pair", pd.Grouper(key="traded_at", freq=FREQUENCIES[freq])], sort=True
    )

    bars = grouped.agg(
        open=("price", "first"),
        high=("price", "max"),
        low=("price", "min"),
        close=("price", "last"),
        volume=("quantity", "sum"),
        trade_count=("trade_id", "size"),
        volatility=("price", _realised_volatility),
    ).reset_index()

    bars = bars.rename(columns={"traded_at": "bar_start"})
    bars["trade_count"] = bars["trade_count"].astype("int64")
    return bars[BAR_COLUMNS].sort_values(["pair", "bar_start"]).reset_index(drop=True)
