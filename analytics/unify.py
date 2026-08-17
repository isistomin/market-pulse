"""One daily series per instrument, whatever contour it came from.

Stocks arrive as daily bars from a scheduled batch; crypto arrives as ticks that
were aggregated into hourly bars. Both collapse to the same shape here, which is
what lets a single set of metrics cover the two contours.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from ingestion.crypto.storage import read_bars
from ingestion.stocks.storage import read_partitions

DAILY_COLUMNS = ["instrument_id", "instrument_type", "market", "date", "close", "volume"]


def _daily_from_stocks(root: Path | str) -> pd.DataFrame:
    bars = read_partitions(root)
    if bars.empty:
        return pd.DataFrame(columns=DAILY_COLUMNS)

    daily = bars.rename(columns={"ticker": "instrument_id"})
    daily["instrument_type"] = "stock"
    daily["date"] = pd.to_datetime(daily["date"]).dt.tz_localize(None).dt.normalize()
    return daily[DAILY_COLUMNS]


def _daily_from_crypto(root: Path | str) -> pd.DataFrame:
    bars = read_bars(root, freq="1h")
    if bars.empty:
        return pd.DataFrame(columns=DAILY_COLUMNS)

    bars = bars.copy()
    bars["date"] = pd.to_datetime(bars["bar_start"], utc=True).dt.tz_convert(None).dt.normalize()

    # A daily close is the last hourly close of the day; volume adds up.
    daily = (
        bars.sort_values(["pair", "bar_start"])
        .groupby(["pair", "date"], as_index=False)
        .agg(close=("close", "last"), volume=("volume", "sum"))
        .rename(columns={"pair": "instrument_id"})
    )
    daily["instrument_type"] = "crypto"
    daily["market"] = "CRYPTO"
    return daily[DAILY_COLUMNS]


def build_daily_series(root: Path | str) -> pd.DataFrame:
    """Unified daily close series for every instrument, sorted by instrument and date."""
    frames = [_daily_from_stocks(root), _daily_from_crypto(root)]
    combined = pd.concat([f for f in frames if not f.empty], ignore_index=True) \
        if any(not f.empty for f in frames) else pd.DataFrame(columns=DAILY_COLUMNS)

    if combined.empty:
        return combined

    return (
        combined.drop_duplicates(subset=["instrument_id", "date"], keep="last")
        .sort_values(["instrument_id", "date"])
        .reset_index(drop=True)
    )
