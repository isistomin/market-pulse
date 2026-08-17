"""Tracked instruments and market lookup by ticker."""

IBEX35 = [
    "ACS.MC", "ACX.MC", "AENA.MC", "AMS.MC", "ANA.MC", "ANE.MC", "BBVA.MC",
    "BKT.MC", "CABK.MC", "CLNX.MC", "COL.MC", "ELE.MC", "ENG.MC", "FDR.MC",
    "FER.MC", "GRF.MC", "IAG.MC", "IBE.MC", "IDR.MC", "ITX.MC", "LOG.MC",
    "MAP.MC", "MRL.MC", "MTS.MC", "NTGY.MC", "PUIG.MC", "RED.MC", "REP.MC",
    "ROVI.MC", "SAB.MC", "SAN.MC", "SCYR.MC", "SLR.MC", "TEF.MC", "UNI.MC",
]

US_LARGE_CAP = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AVGO", "LLY",
    "JPM", "V", "XOM", "UNH", "MA", "COST", "HD", "PG", "JNJ", "ABBV", "WMT",
    "MRK", "KO", "PEP", "BAC", "CVX",
]

BENCHMARKS = {
    "IBEX": "^IBEX",
    "US": "^GSPC",
}

ALL_TICKERS = IBEX35 + US_LARGE_CAP


def market_of(ticker: str) -> str:
    """Market for a ticker. Yahoo suffixes Madrid-listed symbols with .MC."""
    return "IBEX" if ticker.endswith(".MC") else "US"


def tickers_for(market: str | None = None) -> list[str]:
    if market is None:
        return list(ALL_TICKERS)
    if market not in ("IBEX", "US"):
        raise ValueError(f"unknown market: {market}")
    return [t for t in ALL_TICKERS if market_of(t) == market]
