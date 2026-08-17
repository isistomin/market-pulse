"""Tracked crypto pairs and the Binance stream URL built from them."""

PAIRS = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT",
    "MATICUSDT", "LTCUSDT",
]

# BTC is the sector benchmark, the way an index is for the equity side.
BENCHMARK_PAIR = "BTCUSDT"

STREAM_HOST = "wss://stream.binance.com:9443"


def stream_url(pairs: list[str] | None = None) -> str:
    """Combined stream endpoint for the trade feed of every pair."""
    selected = pairs if pairs is not None else PAIRS
    if not selected:
        raise ValueError("at least one pair is required")
    streams = "/".join(f"{pair.lower()}@trade" for pair in selected)
    return f"{STREAM_HOST}/stream?streams={streams}"
