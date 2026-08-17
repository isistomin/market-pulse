"""Checks applied to raw bars before they reach the raw layer.

The point of this layer is to catch a silent source failure. An empty response is
obvious; truncated history looks like valid data and travels into the marts
unnoticed, which is the failure mode actually seen with yfinance batch requests.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

REQUIRED_COLUMNS = ["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]

# A ticker covering less than this share of the median bar count is a source failure.
MIN_COVERAGE_RATIO = 0.5


class ValidationError(Exception):
    """Raised on a violation that must stop the pipeline."""


@dataclass
class ValidationReport:
    rows: int
    tickers: int
    date_min: pd.Timestamp | None
    date_max: pd.Timestamp | None
    warnings: list[str] = field(default_factory=list)

    def __str__(self) -> str:
        return (
            f"rows={self.rows} tickers={self.tickers} "
            f"range={self.date_min}..{self.date_max} warnings={len(self.warnings)}"
        )


def validate_bars(
    bars: pd.DataFrame,
    expected_tickers: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
) -> ValidationReport:
    """Validate bars and return a report. Raises ValidationError on hard violations."""
    if bars.empty:
        raise ValidationError("source returned no rows")

    missing_columns = [c for c in REQUIRED_COLUMNS if c not in bars.columns]
    if missing_columns:
        raise ValidationError(f"missing required columns: {missing_columns}")

    duplicates = int(bars.duplicated(subset=["ticker", "date"]).sum())
    if duplicates:
        raise ValidationError(f"duplicate rows for key (ticker, date): {duplicates}")

    null_close = int(bars["close"].isna().sum())
    if null_close:
        raise ValidationError(f"null close in {null_close} rows")

    inconsistent = bars[(bars["high"] < bars["low"]) | (bars["close"] <= 0)]
    if not inconsistent.empty:
        raise ValidationError(f"inconsistent OHLC in {len(inconsistent)} rows")

    if start is not None and bars["date"].min() < pd.Timestamp(start):
        raise ValidationError(f"bars dated before the requested start {start}")
    if end is not None and bars["date"].max() >= pd.Timestamp(end):
        raise ValidationError(f"bars dated on or after the requested end {end}")

    warnings: list[str] = []
    counts = bars.groupby("ticker").size()

    threshold = counts.median() * MIN_COVERAGE_RATIO
    truncated = sorted(counts[counts < threshold].index.tolist())
    if truncated:
        warnings.append(
            f"truncated history, under {MIN_COVERAGE_RATIO:.0%} of the median "
            f"{int(counts.median())} bars: {truncated}"
        )

    if expected_tickers:
        absent = sorted(set(expected_tickers) - set(counts.index))
        if absent:
            warnings.append(f"tickers with no data: {absent}")

    negative_volume = int((bars["volume"] < 0).sum())
    if negative_volume:
        warnings.append(f"negative volume in {negative_volume} rows")

    return ValidationReport(
        rows=len(bars),
        tickers=int(counts.size),
        date_min=bars["date"].min(),
        date_max=bars["date"].max(),
        warnings=warnings,
    )
