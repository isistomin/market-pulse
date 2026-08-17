import pandas as pd

from ingestion.crypto.storage import partition_path, read_ticks, to_frame, write_ticks

DAY_ONE_MS = 1786000000000  # 2026-08-04T04:26:40Z
MINUTE_MS = 60_000
DAY_MS = 86_400_000


def ticks(pair: str, count: int, first_id: int = 1, base_ms: int = DAY_ONE_MS) -> list[dict]:
    return [
        {
            "pair": pair,
            "trade_id": first_id + i,
            "price": 100.0 + i,
            "quantity": 0.5,
            "traded_at": base_ms + i * MINUTE_MS,
            "buyer_is_maker": bool(i % 2),
        }
        for i in range(count)
    ]


def test_partition_path_layout(tmp_path):
    path = partition_path(tmp_path, "2026-08-13", "BTCUSDT")
    assert path == tmp_path / "crypto_ticks" / "date=2026-08-13" / "pair=BTCUSDT"


def test_epoch_millis_become_timestamps():
    frame = to_frame(ticks("BTCUSDT", 3))
    assert str(frame["traded_at"].dtype).startswith("datetime64")
    assert frame["traded_at"].dt.tz is not None


def test_writes_one_file_per_pair(tmp_path):
    written = write_ticks(ticks("BTCUSDT", 5) + ticks("ETHUSDT", 5, first_id=900), tmp_path)

    assert len(written) == 2
    assert {f.parent.name for f in written} == {"pair=BTCUSDT", "pair=ETHUSDT"}


def test_splits_across_day_boundary(tmp_path):
    batch = ticks("BTCUSDT", 2) + ticks("BTCUSDT", 2, first_id=50, base_ms=DAY_ONE_MS + DAY_MS)
    written = write_ticks(batch, tmp_path)

    assert len({f.parent.parent.name for f in written}) == 2


def test_file_name_carries_the_trade_id_range(tmp_path):
    written = write_ticks(ticks("BTCUSDT", 4, first_id=1000), tmp_path)
    assert written[0].name == "part-1000-1003.parquet"


# Replaying an identical batch must not leave a second copy behind.
def test_replaying_the_same_batch_overwrites(tmp_path):
    batch = ticks("BTCUSDT", 4)
    write_ticks(batch, tmp_path)
    write_ticks(batch, tmp_path)

    back = read_ticks(tmp_path)
    assert len(back) == 4
    assert not back.duplicated(subset=["pair", "trade_id"]).any()


def test_later_batches_append_rather_than_replace(tmp_path):
    write_ticks(ticks("BTCUSDT", 3, first_id=1), tmp_path)
    write_ticks(ticks("BTCUSDT", 3, first_id=100), tmp_path)

    back = read_ticks(tmp_path)
    assert len(back) == 6


def test_empty_batch_writes_nothing(tmp_path):
    assert write_ticks([], tmp_path) == []
    assert read_ticks(tmp_path).empty


def test_roundtrip_preserves_values(tmp_path):
    write_ticks(ticks("BTCUSDT", 3), tmp_path)
    back = read_ticks(tmp_path)

    assert list(back["price"]) == [100.0, 101.0, 102.0]
    assert list(back["trade_id"]) == [1, 2, 3]
    assert pd.api.types.is_bool_dtype(back["buyer_is_maker"])
