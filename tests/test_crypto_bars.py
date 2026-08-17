import pandas as pd
import pytest

from ingestion.crypto.bars import aggregate_ohlc, deduplicate
from ingestion.crypto.storage import read_bars, write_bars

# Aligned to a minute boundary so bar membership in the tests is unambiguous.
BASE_MS = 1785999960000  # 2026-08-06T07:06:00Z
SECOND_MS = 1_000


def tick(pair: str, trade_id: int, price: float, offset_ms: int, quantity: float = 1.0) -> dict:
    return {
        "pair": pair,
        "trade_id": trade_id,
        "price": price,
        "quantity": quantity,
        "traded_at": pd.to_datetime(BASE_MS + offset_ms, unit="ms", utc=True),
        "buyer_is_maker": False,
    }


def frame(ticks: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(ticks)


def test_deduplicate_keeps_one_row_per_trade():
    ticks = frame([tick("BTCUSDT", 1, 100.0, 0), tick("BTCUSDT", 1, 100.0, 0)])
    assert len(deduplicate(ticks)) == 1


def test_deduplicate_keeps_same_id_on_different_pairs():
    ticks = frame([tick("BTCUSDT", 1, 100.0, 0), tick("ETHUSDT", 1, 50.0, 0)])
    assert len(deduplicate(ticks)) == 2


# Hand-checked bar: four trades inside one minute, so OHLC is first/max/min/last.
def test_ohlc_matches_a_hand_computed_bar():
    ticks = frame([
        tick("BTCUSDT", 1, 100.0, 0, quantity=0.5),
        tick("BTCUSDT", 2, 105.0, 10 * SECOND_MS, quantity=1.0),
        tick("BTCUSDT", 3, 95.0, 20 * SECOND_MS, quantity=2.0),
        tick("BTCUSDT", 4, 102.0, 30 * SECOND_MS, quantity=0.5),
    ])
    bars = aggregate_ohlc(ticks, freq="1m")

    assert len(bars) == 1
    bar = bars.iloc[0]
    assert bar["open"] == 100.0
    assert bar["high"] == 105.0
    assert bar["low"] == 95.0
    assert bar["close"] == 102.0
    assert bar["volume"] == 4.0
    assert bar["trade_count"] == 4


def test_trades_split_across_minute_boundaries():
    ticks = frame([
        tick("BTCUSDT", 1, 100.0, 0),
        tick("BTCUSDT", 2, 110.0, 90 * SECOND_MS),
    ])
    bars = aggregate_ohlc(ticks, freq="1m")

    assert len(bars) == 2
    assert list(bars["close"]) == [100.0, 110.0]


def test_hourly_bars_collapse_the_same_trades():
    ticks = frame([
        tick("BTCUSDT", 1, 100.0, 0),
        tick("BTCUSDT", 2, 110.0, 90 * SECOND_MS),
    ])
    assert len(aggregate_ohlc(ticks, freq="1h")) == 1


def test_pairs_are_aggregated_independently():
    ticks = frame([
        tick("BTCUSDT", 1, 100.0, 0),
        tick("ETHUSDT", 1, 50.0, 0),
    ])
    bars = aggregate_ohlc(ticks, freq="1m")

    assert set(bars["pair"]) == {"BTCUSDT", "ETHUSDT"}
    assert len(bars) == 2


def test_duplicated_ticks_do_not_inflate_volume():
    ticks = frame([
        tick("BTCUSDT", 1, 100.0, 0, quantity=1.0),
        tick("BTCUSDT", 1, 100.0, 0, quantity=1.0),
        tick("BTCUSDT", 2, 101.0, SECOND_MS, quantity=1.0),
    ])
    bar = aggregate_ohlc(ticks, freq="1m").iloc[0]

    assert bar["volume"] == 2.0
    assert bar["trade_count"] == 2


def test_flat_prices_give_zero_volatility():
    ticks = frame([tick("BTCUSDT", i, 100.0, i * SECOND_MS) for i in range(1, 6)])
    assert aggregate_ohlc(ticks, freq="1m").iloc[0]["volatility"] == 0.0


def test_moving_prices_give_positive_volatility():
    ticks = frame([tick("BTCUSDT", i, 100.0 + i * i, i * SECOND_MS) for i in range(1, 6)])
    assert aggregate_ohlc(ticks, freq="1m").iloc[0]["volatility"] > 0.0


def test_single_trade_bar_has_zero_volatility():
    bars = aggregate_ohlc(frame([tick("BTCUSDT", 1, 100.0, 0)]), freq="1m")
    assert bars.iloc[0]["volatility"] == 0.0


def test_empty_input_gives_no_bars():
    assert aggregate_ohlc(pd.DataFrame(), freq="1m").empty


def test_unsupported_frequency_is_rejected():
    with pytest.raises(ValueError, match="unsupported frequency"):
        aggregate_ohlc(frame([tick("BTCUSDT", 1, 100.0, 0)]), freq="5s")


def test_bars_roundtrip_through_partitions(tmp_path):
    ticks = frame([tick("BTCUSDT", i, 100.0 + i, i * SECOND_MS) for i in range(1, 4)])
    bars = aggregate_ohlc(ticks, freq="1m")

    write_bars(bars, tmp_path, freq="1m")
    back = read_bars(tmp_path, freq="1m")

    assert len(back) == len(bars)
    assert list(back["pair"]) == list(bars["pair"])


# Bars are derived, so recomputing a day replaces the partition instead of doubling it.
def test_rerun_replaces_the_bar_partition(tmp_path):
    ticks = frame([tick("BTCUSDT", i, 100.0 + i, i * SECOND_MS) for i in range(1, 4)])
    bars = aggregate_ohlc(ticks, freq="1m")

    write_bars(bars, tmp_path, freq="1m")
    write_bars(bars, tmp_path, freq="1m")

    assert len(read_bars(tmp_path, freq="1m")) == len(bars)
