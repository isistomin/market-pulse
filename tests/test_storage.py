import pandas as pd
import pytest

from ingestion.stocks.storage import (
    partition_path,
    read_partitions,
    write_daily_partitions,
)


def test_partition_path_layout(tmp_path):
    path = partition_path(tmp_path, "2026-08-13", "IBEX")
    assert path == tmp_path / "stocks" / "date=2026-08-13" / "market=IBEX"


def test_write_creates_one_partition_per_date_and_market(tmp_path, bars_with_market):
    written = write_daily_partitions(bars_with_market, tmp_path)

    dates = bars_with_market["date"].dt.normalize().nunique()
    markets = bars_with_market["market"].nunique()
    assert len(written) == dates * markets
    assert all((p / "bars.parquet").exists() for p in written)


def test_roundtrip_preserves_rows(tmp_path, bars_with_market):
    write_daily_partitions(bars_with_market, tmp_path)
    back = read_partitions(tmp_path)

    assert len(back) == len(bars_with_market)
    assert set(back["ticker"]) == set(bars_with_market["ticker"])
    assert set(back["market"]) == set(bars_with_market["market"])


# Idempotency: the DAG reruns the same logical date on retry and during catchup.
def test_rerun_replaces_partition_instead_of_appending(tmp_path, bars_with_market):
    write_daily_partitions(bars_with_market, tmp_path)
    write_daily_partitions(bars_with_market, tmp_path)

    back = read_partitions(tmp_path)
    assert len(back) == len(bars_with_market)
    assert not back.duplicated(subset=["ticker", "date"]).any()


def test_rerun_with_corrected_data_wins(tmp_path, bars_with_market):
    write_daily_partitions(bars_with_market, tmp_path)

    corrected = bars_with_market.copy()
    corrected["close"] = 999.0
    write_daily_partitions(corrected, tmp_path)

    back = read_partitions(tmp_path)
    assert (back["close"] == 999.0).all()
    assert len(back) == len(bars_with_market)


def test_market_column_is_required(tmp_path, bars):
    with pytest.raises(ValueError, match="market column"):
        write_daily_partitions(bars, tmp_path)


def test_empty_input_writes_nothing(tmp_path):
    assert write_daily_partitions(pd.DataFrame(), tmp_path) == []
    assert read_partitions(tmp_path).empty
