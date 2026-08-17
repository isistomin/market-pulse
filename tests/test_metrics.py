import numpy as np
import pandas as pd
import pytest

from analytics.metrics import (
    TRADING_DAYS_PER_YEAR,
    benchmark_comparison,
    build_metrics,
    daily_returns,
    drawdown,
    latest_per_instrument,
    rolling_volatility,
)


def series(rows: list[tuple[str, str, str, str, float]]) -> pd.DataFrame:
    frame = pd.DataFrame(
        rows, columns=["instrument_id", "instrument_type", "market", "date", "close"]
    )
    frame["date"] = pd.to_datetime(frame["date"])
    frame["volume"] = 1.0
    return frame


def walk(instrument_id, kind, market, closes, start="2026-01-01"):
    dates = pd.bdate_range(start, periods=len(closes)).strftime("%Y-%m-%d")
    return [(instrument_id, kind, market, d, c) for d, c in zip(dates, closes, strict=True)]


def test_daily_return_is_the_percentage_change():
    frame = daily_returns(series(walk("AAPL", "stock", "US", [100.0, 110.0, 99.0])))

    assert pd.isna(frame.loc[0, "daily_return"])
    assert frame.loc[1, "daily_return"] == pytest.approx(0.10)
    assert frame.loc[2, "daily_return"] == pytest.approx(-0.10)


def test_returns_do_not_leak_between_instruments():
    rows = walk("AAPL", "stock", "US", [100.0, 110.0]) + walk("MSFT", "stock", "US", [50.0, 55.0])
    frame = daily_returns(series(rows))

    first_rows = frame[frame["date"] == frame["date"].min()]
    assert first_rows["daily_return"].isna().all()


def test_volatility_needs_a_full_window():
    frame = rolling_volatility(
        daily_returns(series(walk("AAPL", "stock", "US", [100.0 + i for i in range(30)]))),
        windows=(20,),
    )
    assert frame["volatility_20d"].isna().sum() == 20
    assert frame["volatility_20d"].notna().sum() == 10


def test_volatility_is_annualised():
    closes = [100.0 * (1.01 if i % 2 else 0.99) ** 1 for i in range(25)]
    frame = rolling_volatility(
        daily_returns(series(walk("AAPL", "stock", "US", closes))), windows=(20,)
    )
    last = frame.dropna(subset=["volatility_20d"]).iloc[-1]

    raw = frame["daily_return"].tail(20).std()
    assert last["volatility_20d"] == pytest.approx(raw * np.sqrt(TRADING_DAYS_PER_YEAR), rel=0.2)


def test_flat_prices_have_zero_volatility():
    frame = rolling_volatility(
        daily_returns(series(walk("AAPL", "stock", "US", [100.0] * 25))), windows=(20,)
    )
    assert frame["volatility_20d"].dropna().eq(0.0).all()


# Hand-checked: peak 120, trough 90, so the deepest drawdown is -25%.
def test_drawdown_measures_from_the_running_peak():
    frame = drawdown(series(walk("AAPL", "stock", "US", [100.0, 120.0, 90.0, 110.0])))

    assert frame.loc[0, "drawdown"] == pytest.approx(0.0)
    assert frame.loc[1, "drawdown"] == pytest.approx(0.0)
    assert frame.loc[2, "drawdown"] == pytest.approx(-0.25)
    assert frame.loc[3, "drawdown"] == pytest.approx(110 / 120 - 1, abs=1e-9)


def test_drawdown_is_never_positive():
    frame = drawdown(series(walk("AAPL", "stock", "US", [10.0, 20.0, 30.0, 40.0])))
    assert (frame["drawdown"] <= 1e-12).all()


def test_benchmark_is_assigned_per_market():
    rows = (
        walk("AAPL", "stock", "US", [100.0, 101.0])
        + walk("SAN.MC", "stock", "IBEX", [10.0, 10.5])
        + walk("ETHUSDT", "crypto", "CRYPTO", [2000.0, 2100.0])
    )
    frame = benchmark_comparison(daily_returns(series(rows)))
    mapping = dict(zip(frame["instrument_id"], frame["benchmark_id"], strict=True))

    assert mapping["AAPL"] == "^GSPC"
    assert mapping["SAN.MC"] == "^IBEX"
    assert mapping["ETHUSDT"] == "BTCUSDT"


def test_excess_return_subtracts_the_benchmark():
    rows = (
        walk("ETHUSDT", "crypto", "CRYPTO", [100.0, 110.0])
        + walk("BTCUSDT", "crypto", "CRYPTO", [100.0, 104.0])
    )
    frame = benchmark_comparison(daily_returns(series(rows)))
    eth = frame[(frame["instrument_id"] == "ETHUSDT")].iloc[-1]

    assert eth["excess_return"] == pytest.approx(0.10 - 0.04)


def test_benchmark_against_itself_is_zero_excess():
    frame = benchmark_comparison(
        daily_returns(series(walk("BTCUSDT", "crypto", "CRYPTO", [100.0, 110.0])))
    )
    assert frame.iloc[-1]["excess_return"] == pytest.approx(0.0)


def test_build_metrics_produces_every_column():
    frame = build_metrics(series(walk("AAPL", "stock", "US", [100.0 + i for i in range(70)])))

    for column in ["daily_return", "volatility_20d", "volatility_60d", "drawdown",
                   "benchmark_id", "excess_return"]:
        assert column in frame.columns


def test_latest_per_instrument_takes_the_last_row():
    rows = walk("AAPL", "stock", "US", [100.0, 101.0, 102.0]) + walk("MSFT", "stock", "US", [1.0])
    latest = latest_per_instrument(build_metrics(series(rows)))

    assert len(latest) == 2
    assert latest[latest["instrument_id"] == "AAPL"].iloc[0]["close"] == 102.0


def test_empty_input_stays_empty():
    empty = pd.DataFrame(
        columns=["instrument_id", "instrument_type", "market", "date", "close", "volume"]
    )
    assert build_metrics(empty).empty
