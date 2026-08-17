import json
from datetime import UTC, datetime

import pandas as pd
import pytest

pytest.importorskip("fastapi", reason="fastapi not installed")

from fastapi.testclient import TestClient  # noqa: E402

from analytics.snapshot import (  # noqa: E402
    build_snapshot,
    read_snapshot,
    snapshot_path,
    write_snapshot,
)
from api.main import create_app  # noqa: E402
from ingestion.crypto.bars import aggregate_ohlc  # noqa: E402
from ingestion.crypto.storage import write_bars  # noqa: E402
from ingestion.stocks.storage import write_daily_partitions  # noqa: E402

GENERATED_AT = datetime(2026, 8, 13, 2, 0, tzinfo=UTC)


def seed_stocks(root, tickers=("AAPL", "^GSPC"), days=70):
    dates = pd.bdate_range("2026-01-01", periods=days)
    rows = []
    for offset, ticker in enumerate(tickers):
        for i, date in enumerate(dates):
            price = 100.0 + offset * 10 + i
            rows.append({
                "ticker": ticker, "date": date, "open": price, "high": price + 1,
                "low": price - 1, "close": price, "adj_close": price,
                "volume": 1000, "market": "US",
            })
    write_daily_partitions(pd.DataFrame(rows), root)


def seed_crypto(root, pairs=("BTCUSDT", "ETHUSDT"), hours=48):
    start = pd.Timestamp("2026-01-01", tz="UTC")
    ticks = []
    for offset, pair in enumerate(pairs):
        for i in range(hours):
            ticks.append({
                "pair": pair, "trade_id": offset * 10_000 + i,
                "price": 100.0 + offset * 50 + i, "quantity": 1.0,
                "traded_at": start + pd.Timedelta(hours=i), "buyer_is_maker": False,
            })
    write_bars(aggregate_ohlc(pd.DataFrame(ticks), freq="1h"), root, freq="1h")


@pytest.fixture
def seeded(tmp_path):
    seed_stocks(tmp_path)
    seed_crypto(tmp_path)
    write_snapshot(build_snapshot(tmp_path, generated_at=GENERATED_AT), tmp_path)
    return tmp_path


@pytest.fixture
def client(seeded):
    return TestClient(create_app(root=seeded))


def test_snapshot_covers_both_contours(seeded):
    snapshot = read_snapshot(seeded)
    kinds = {i["type"] for i in snapshot["instruments"]}

    assert kinds == {"stock", "crypto"}
    assert snapshot["generated_at"].startswith("2026-08-13")


def test_snapshot_has_no_nan_in_json(seeded):
    raw = snapshot_path(seeded).read_text(encoding="utf-8")
    assert "NaN" not in raw
    json.loads(raw)


def test_snapshot_write_is_atomic(seeded):
    assert not list(snapshot_path(seeded).parent.glob("*.tmp"))


def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_list_instruments(client):
    body = client.get("/instruments").json()

    assert body["instruments"]
    assert {"id", "type", "market", "close"} <= set(body["instruments"][0])


def test_filter_by_type(client):
    crypto = client.get("/instruments", params={"type": "crypto"}).json()["instruments"]
    assert crypto
    assert {i["type"] for i in crypto} == {"crypto"}


def test_unknown_type_is_rejected(client):
    assert client.get("/instruments", params={"type": "bonds"}).status_code == 422


def test_metrics_for_an_instrument(client):
    body = client.get("/instruments/AAPL/metrics").json()

    assert body["instrument"]["id"] == "AAPL"
    assert body["series"]
    assert {"date", "close", "daily_return", "drawdown"} == set(body["series"][0])


def test_metrics_for_an_unknown_instrument(client):
    response = client.get("/instruments/NOPE/metrics")
    assert response.status_code == 404
    assert "unknown instrument" in response.json()["detail"]


def test_benchmark_returns_both_series(client):
    body = client.get("/instruments/ETHUSDT/benchmark").json()

    assert body["benchmark_id"] == "BTCUSDT"
    assert body["series"]
    assert body["benchmark_series"]


def test_missing_snapshot_reports_unavailable(tmp_path):
    response = TestClient(create_app(root=tmp_path)).get("/instruments")
    assert response.status_code == 503


def test_snapshot_is_reloaded_when_the_file_changes(seeded):
    client = TestClient(create_app(root=seeded))
    assert len(client.get("/instruments").json()["instruments"]) > 1

    trimmed = read_snapshot(seeded)
    trimmed["instruments"] = trimmed["instruments"][:1]
    write_snapshot(trimmed, seeded)

    assert len(client.get("/instruments").json()["instruments"]) == 1
