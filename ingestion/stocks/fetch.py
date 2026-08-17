"""Daily bar download from yfinance, normalised to a long format.

A batch request sometimes returns truncated history for part of the tickers: a
handful of bars instead of several hundred, with no error and no gaps inside the
range. Those tickers are refetched one by one after the batch.
"""

from __future__ import annotations

import logging

import pandas as pd
import yfinance as yf

from ingestion.stocks.universe import market_of

log = logging.getLogger(__name__)

COLUMNS = ["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]

_RENAME = {
    "Open": "open",
    "High": "high",
    "Low": "low",
    "Close": "close",
    "Adj Close": "adj_close",
    "Volume": "volume",
}

# Share of the median bar count below which history is treated as truncated.
SHORT_HISTORY_RATIO = 0.5


def _to_long(raw: pd.DataFrame, tickers: list[str]) -> pd.DataFrame:
    """Reshape a yfinance response into ticker/date/OHLCV rows."""
    frames = []
    for ticker in tickers:
        if isinstance(raw.columns, pd.MultiIndex):
            if ticker not in raw.columns.get_level_values(0):
                continue
            df = raw[ticker]
        else:
            df = raw
        df = df.dropna(how="all")
        if df.empty:
            continue
        df = df.rename(columns=_RENAME).reset_index()
        df = df.rename(columns={df.columns[0]: "date"})
        df["ticker"] = ticker
        frames.append(df)

    if not frames:
        return pd.DataFrame(columns=COLUMNS)

    out = pd.concat(frames, ignore_index=True)
    out["date"] = pd.to_datetime(out["date"]).dt.tz_localize(None).dt.normalize()
    for column in [c for c in COLUMNS if c not in out.columns]:
        out[column] = pd.NA
    return out[COLUMNS]


def _short_history(counts: pd.Series) -> list[str]:
    if counts.empty:
        return []
    threshold = counts.median() * SHORT_HISTORY_RATIO
    return sorted(counts[counts < threshold].index.tolist())


def fetch_daily_bars(tickers: list[str], start: str, end: str) -> pd.DataFrame:
    """Daily OHLCV for the half-open interval [start, end).

    Returns long-format rows with COLUMNS, sorted by ticker and date.
    """
    if not tickers:
        return pd.DataFrame(columns=COLUMNS)

    raw = yf.download(
        tickers, start=start, end=end, auto_adjust=False, group_by="ticker",
        progress=False, threads=True,
    )
    bars = _to_long(raw, tickers)

    counts = bars.groupby("ticker").size() if not bars.empty else pd.Series(dtype=int)
    suspect = set(_short_history(counts)) | {t for t in tickers if t not in counts.index}
    for ticker in sorted(suspect):
        log.warning("truncated history after batch, refetching on its own: %s", ticker)
        single = yf.download(
            ticker, start=start, end=end, auto_adjust=False,
            progress=False, threads=False,
        )
        refetched = _to_long(single, [ticker])
        if len(refetched) > int(counts.get(ticker, 0)):
            bars = bars[bars["ticker"] != ticker]
            bars = pd.concat([bars, refetched], ignore_index=True)

    return bars.sort_values(["ticker", "date"]).reset_index(drop=True)


def add_market(bars: pd.DataFrame) -> pd.DataFrame:
    out = bars.copy()
    out["market"] = out["ticker"].map(market_of)
    return out
