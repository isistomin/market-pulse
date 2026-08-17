from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from ingestion.stocks.universe import market_of


def make_bars(tickers: list[str], days: int, start: str = "2026-01-05") -> pd.DataFrame:
    """Build synthetic OHLCV rows on consecutive business days."""
    rows = []
    for offset, ticker in enumerate(tickers):
        dates = pd.bdate_range(start=start, periods=days)
        base = 100.0 + offset
        rows.append(
            pd.DataFrame(
                {
                    "ticker": ticker,
                    "date": dates,
                    "open": base,
                    "high": base + 2.0,
                    "low": base - 2.0,
                    "close": base + 1.0,
                    "adj_close": base + 1.0,
                    "volume": np.arange(days, dtype="int64") + 1000,
                }
            )
        )
    bars = pd.concat(rows, ignore_index=True)
    return bars.sort_values(["ticker", "date"]).reset_index(drop=True)


@pytest.fixture
def bars() -> pd.DataFrame:
    return make_bars(["AAPL", "MSFT", "SAN.MC"], days=20)


@pytest.fixture
def bars_with_market(bars: pd.DataFrame) -> pd.DataFrame:
    out = bars.copy()
    out["market"] = out["ticker"].map(market_of)
    return out
