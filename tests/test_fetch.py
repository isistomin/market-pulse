import pandas as pd
import pytest

from ingestion.stocks.fetch import COLUMNS, _short_history, _to_long, add_market


def _yahoo_shaped(tickers: list[str], days: int) -> pd.DataFrame:
    """Mimic the wide MultiIndex frame yfinance returns for a multi-ticker request."""
    index = pd.bdate_range("2026-01-05", periods=days, name="Date")
    fields = ["Open", "High", "Low", "Close", "Adj Close", "Volume"]
    columns = pd.MultiIndex.from_product([tickers, fields])
    data = {(t, f): range(days) for t in tickers for f in fields}
    return pd.DataFrame(data, index=index, columns=columns)


def test_to_long_flattens_multiindex():
    raw = _yahoo_shaped(["AAPL", "SAN.MC"], days=5)
    long = _to_long(raw, ["AAPL", "SAN.MC"])

    assert list(long.columns) == COLUMNS
    assert len(long) == 10
    assert set(long["ticker"]) == {"AAPL", "SAN.MC"}
    assert long["date"].dt.tz is None


def test_to_long_handles_single_ticker_frame():
    raw = _yahoo_shaped(["AAPL"], days=5)["AAPL"]
    long = _to_long(raw, ["AAPL"])

    assert len(long) == 5
    assert set(long["ticker"]) == {"AAPL"}


def test_to_long_skips_tickers_absent_from_response():
    raw = _yahoo_shaped(["AAPL"], days=5)
    long = _to_long(raw, ["AAPL", "DELISTED"])

    assert set(long["ticker"]) == {"AAPL"}


def test_short_history_flags_tickers_below_half_the_median():
    counts = pd.Series({"AAPL": 750, "MSFT": 752, "ITX.MC": 764, "SAN.MC": 8})
    assert _short_history(counts) == ["SAN.MC"]


def test_short_history_flags_nothing_when_coverage_is_even():
    counts = pd.Series({"AAPL": 750, "MSFT": 752, "ITX.MC": 764})
    assert _short_history(counts) == []


def test_add_market_maps_suffix():
    bars = pd.DataFrame({"ticker": ["AAPL", "SAN.MC"]})
    assert list(add_market(bars)["market"]) == ["US", "IBEX"]


@pytest.mark.network
def test_fetch_returns_bars_for_both_markets():
    from ingestion.stocks.fetch import fetch_daily_bars

    bars = fetch_daily_bars(["AAPL", "ITX.MC"], start="2026-07-01", end="2026-08-01")

    assert not bars.empty
    assert set(bars["ticker"]) == {"AAPL", "ITX.MC"}
    assert not bars.duplicated(subset=["ticker", "date"]).any()
