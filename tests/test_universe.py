import pytest

from ingestion.stocks.universe import ALL_TICKERS, IBEX35, market_of, tickers_for


def test_ibex_has_35_members():
    assert len(IBEX35) == 35


def test_universe_has_no_duplicates():
    assert len(ALL_TICKERS) == len(set(ALL_TICKERS))


@pytest.mark.parametrize(
    ("ticker", "expected"),
    [("SAN.MC", "IBEX"), ("ITX.MC", "IBEX"), ("AAPL", "US"), ("BRK-B", "US")],
)
def test_market_of(ticker, expected):
    assert market_of(ticker) == expected


def test_tickers_for_splits_the_universe():
    ibex = tickers_for("IBEX")
    us = tickers_for("US")
    assert set(ibex) | set(us) == set(ALL_TICKERS)
    assert not set(ibex) & set(us)


def test_tickers_for_rejects_unknown_market():
    with pytest.raises(ValueError, match="unknown market"):
        tickers_for("MOEX")
