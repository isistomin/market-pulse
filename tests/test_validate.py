import pandas as pd
import pytest

from ingestion.stocks.validate import ValidationError, validate_bars
from tests.conftest import make_bars


def test_clean_bars_pass(bars):
    report = validate_bars(bars, expected_tickers=["AAPL", "MSFT", "SAN.MC"])
    assert report.rows == len(bars)
    assert report.tickers == 3
    assert report.warnings == []


def test_empty_input_is_rejected():
    with pytest.raises(ValidationError, match="no rows"):
        validate_bars(pd.DataFrame())


def test_missing_column_is_rejected(bars):
    with pytest.raises(ValidationError, match="missing required columns"):
        validate_bars(bars.drop(columns=["adj_close"]))


def test_duplicate_key_is_rejected(bars):
    doubled = pd.concat([bars, bars.head(1)], ignore_index=True)
    with pytest.raises(ValidationError, match="duplicate rows"):
        validate_bars(doubled)


def test_null_close_is_rejected(bars):
    broken = bars.copy()
    broken.loc[0, "close"] = None
    with pytest.raises(ValidationError, match="null close"):
        validate_bars(broken)


def test_high_below_low_is_rejected(bars):
    broken = bars.copy()
    broken.loc[0, "high"] = broken.loc[0, "low"] - 1
    with pytest.raises(ValidationError, match="inconsistent OHLC"):
        validate_bars(broken)


def test_bars_outside_requested_range_are_rejected(bars):
    with pytest.raises(ValidationError, match="before the requested start"):
        validate_bars(bars, start="2026-06-01")


# The yfinance failure this whole layer exists for: a batch response comes back with
# a handful of bars for some tickers instead of the full history, and nothing errors.
def test_truncated_history_is_reported(bars):
    truncated = pd.concat(
        [
            bars[bars["ticker"] != "SAN.MC"],
            make_bars(["SAN.MC"], days=2),
        ],
        ignore_index=True,
    )
    report = validate_bars(truncated)
    assert len(report.warnings) == 1
    assert "SAN.MC" in report.warnings[0]
    assert "truncated history" in report.warnings[0]


def test_absent_ticker_is_reported(bars):
    report = validate_bars(bars, expected_tickers=["AAPL", "MSFT", "SAN.MC", "TEF.MC"])
    assert any("TEF.MC" in w for w in report.warnings)
