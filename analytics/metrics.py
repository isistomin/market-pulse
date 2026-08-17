"""Return, volatility, drawdown and benchmark metrics over the unified daily series.

Every function takes the long frame produced by `unify.build_daily_series` and
returns a frame keyed the same way, so they compose without a shared object.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

TRADING_DAYS_PER_YEAR = 252
VOLATILITY_WINDOWS = (20, 60)

BENCHMARKS = {
    "stock": {"IBEX": "^IBEX", "US": "^GSPC"},
    "crypto": {"CRYPTO": "BTCUSDT"},
}


def daily_returns(series: pd.DataFrame) -> pd.DataFrame:
    """Day-over-day simple return per instrument."""
    if series.empty:
        return series.assign(daily_return=pd.Series(dtype="float64"))

    out = series.sort_values(["instrument_id", "date"]).copy()
    out["daily_return"] = out.groupby("instrument_id")["close"].pct_change()
    return out.reset_index(drop=True)


def rolling_volatility(
    returns: pd.DataFrame, windows: tuple[int, ...] = VOLATILITY_WINDOWS
) -> pd.DataFrame:
    """Annualised rolling standard deviation of daily returns, one column per window."""
    out = returns.sort_values(["instrument_id", "date"]).copy()
    grouped = out.groupby("instrument_id")["daily_return"]

    for window in windows:
        # Annualised so the number is comparable to how volatility is usually quoted.
        out[f"volatility_{window}d"] = (
            grouped.transform(lambda s, w=window: s.rolling(w, min_periods=w).std())
            * np.sqrt(TRADING_DAYS_PER_YEAR)
        )
    return out.reset_index(drop=True)


def drawdown(series: pd.DataFrame) -> pd.DataFrame:
    """Drop from the running maximum, as a negative fraction."""
    out = series.sort_values(["instrument_id", "date"]).copy()
    running_max = out.groupby("instrument_id")["close"].cummax()
    out["drawdown"] = out["close"] / running_max - 1.0
    return out.reset_index(drop=True)


def _benchmark_for(instrument_type: str, market: str) -> str | None:
    return BENCHMARKS.get(instrument_type, {}).get(market)


def benchmark_comparison(returns: pd.DataFrame) -> pd.DataFrame:
    """Excess return of each instrument over the benchmark of its market.

    Stocks are compared to their index, crypto to BTC, which is the sector's de
    facto benchmark. Instruments without a benchmark series get no excess column.
    """
    if returns.empty:
        return returns.assign(benchmark_id=None, excess_return=np.nan)

    out = returns.copy()
    out["benchmark_id"] = [
        _benchmark_for(t, m) for t, m in zip(out["instrument_type"], out["market"], strict=True)
    ]

    benchmark_returns = (
        out[["instrument_id", "date", "daily_return"]]
        .rename(columns={"instrument_id": "benchmark_id", "daily_return": "benchmark_return"})
    )

    merged = out.merge(benchmark_returns, on=["benchmark_id", "date"], how="left")
    merged["excess_return"] = merged["daily_return"] - merged["benchmark_return"]
    return merged.reset_index(drop=True)


def build_metrics(series: pd.DataFrame) -> pd.DataFrame:
    """Full metric set for the unified series."""
    if series.empty:
        return series

    enriched = rolling_volatility(daily_returns(series))
    enriched = enriched.merge(
        drawdown(series)[["instrument_id", "date", "drawdown"]],
        on=["instrument_id", "date"],
        how="left",
    )
    return benchmark_comparison(enriched)


def latest_per_instrument(metrics: pd.DataFrame) -> pd.DataFrame:
    """Most recent row per instrument, which is what the dashboard table shows."""
    if metrics.empty:
        return metrics
    return (
        metrics.sort_values(["instrument_id", "date"])
        .groupby("instrument_id", as_index=False)
        .tail(1)
        .sort_values("instrument_id")
        .reset_index(drop=True)
    )
