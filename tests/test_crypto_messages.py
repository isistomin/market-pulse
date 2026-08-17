import json

import pytest

from ingestion.crypto.messages import (
    MalformedMessage,
    decode_tick,
    encode_tick,
    parse_trade,
)
from ingestion.crypto.universe import PAIRS, stream_url

TRADE = {
    "e": "trade",
    "E": 1786000000000,
    "s": "BTCUSDT",
    "t": 4212345,
    "p": "61234.50000000",
    "q": "0.01230000",
    "T": 1786000000123,
    "m": True,
    "M": True,
}


def test_parse_combined_stream_envelope():
    frame = json.dumps({"stream": "btcusdt@trade", "data": TRADE})
    tick = parse_trade(frame)

    assert tick == {
        "pair": "BTCUSDT",
        "trade_id": 4212345,
        "price": 61234.5,
        "quantity": 0.0123,
        "traded_at": 1786000000123,
        "buyer_is_maker": True,
    }


def test_parse_bare_payload():
    assert parse_trade(TRADE)["pair"] == "BTCUSDT"


def test_prices_and_quantities_become_numbers():
    tick = parse_trade(TRADE)
    assert isinstance(tick["price"], float)
    assert isinstance(tick["quantity"], float)


def test_non_trade_event_is_rejected():
    with pytest.raises(MalformedMessage, match="not a trade event"):
        parse_trade({"e": "kline", "s": "BTCUSDT"})


def test_invalid_json_is_rejected():
    with pytest.raises(MalformedMessage, match="not valid JSON"):
        parse_trade("{oops")


def test_missing_field_is_rejected():
    incomplete = {k: v for k, v in TRADE.items() if k != "p"}
    with pytest.raises(MalformedMessage, match="unusable trade payload"):
        parse_trade(incomplete)


def test_encode_decode_roundtrip():
    tick = parse_trade(TRADE)
    assert decode_tick(encode_tick(tick)) == tick


def test_stream_url_lists_every_pair():
    url = stream_url(["BTCUSDT", "ETHUSDT"])
    assert url.endswith("streams=btcusdt@trade/ethusdt@trade")


def test_stream_url_defaults_to_the_universe():
    assert stream_url().count("@trade") == len(PAIRS)


def test_stream_url_rejects_an_empty_selection():
    with pytest.raises(ValueError, match="at least one pair"):
        stream_url([])
