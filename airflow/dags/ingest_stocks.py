"""Daily ingestion of stock bars into the partitioned raw layer.

The DAG stays a thin wrapper: scheduling, retries and task order live here, while
fetching, validation and writing live in `ingestion.stocks` and are tested without
an Airflow process.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pandas as pd
from airflow.sdk import dag, task

from ingestion.stocks.fetch import add_market, fetch_daily_bars
from ingestion.stocks.storage import data_root, write_daily_partitions
from ingestion.stocks.universe import ALL_TICKERS
from ingestion.stocks.validate import validate_bars

log = logging.getLogger(__name__)

# Both exchanges are closed by this hour in UTC: Madrid shuts at 17:30 CET and the
# US session ends at 21:00 UTC at the latest, so one late run covers both markets.
SCHEDULE = "0 23 * * 1-5"

START_DATE = datetime(2023, 8, 14)

DEFAULT_ARGS = {
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


@dag(
    dag_id="ingest_stocks",
    schedule=SCHEDULE,
    start_date=START_DATE,
    catchup=True,
    # yfinance is a shared free endpoint: backfilling years of daily runs in
    # parallel gets throttled, so runs are serialised.
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["stocks", "batch", "raw"],
    doc_md=__doc__,
)
def ingest_stocks():
    @task
    def fetch_prices(data_interval_start=None, data_interval_end=None) -> list[dict]:
        """Fetch one interval of daily bars for the whole universe."""
        start = data_interval_start.strftime("%Y-%m-%d")
        end = data_interval_end.strftime("%Y-%m-%d")

        bars = fetch_daily_bars(ALL_TICKERS, start=start, end=end)
        log.info("fetched %d rows for [%s, %s)", len(bars), start, end)

        # XCom is JSON-backed, so timestamps have to cross as ISO strings.
        bars = bars.copy()
        bars["date"] = bars["date"].dt.strftime("%Y-%m-%d")
        return bars.to_dict(orient="records")

    @task
    def validate_raw(records: list[dict], data_interval_start=None, data_interval_end=None):
        bars = _to_frame(records)
        report = validate_bars(
            bars,
            expected_tickers=ALL_TICKERS,
            start=data_interval_start.strftime("%Y-%m-%d"),
            end=data_interval_end.strftime("%Y-%m-%d"),
        )
        log.info("validation: %s", report)
        for warning in report.warnings:
            log.warning("validation: %s", warning)
        return records

    @task
    def write_parquet(records: list[dict]) -> list[str]:
        bars = add_market(_to_frame(records))
        written = write_daily_partitions(bars, data_root())
        log.info("wrote %d partitions under %s", len(written), data_root())
        return [str(path) for path in written]

    write_parquet(validate_raw(fetch_prices()))


def _to_frame(records: list[dict]) -> pd.DataFrame:
    bars = pd.DataFrame(records)
    bars["date"] = pd.to_datetime(bars["date"])
    return bars


ingest_stocks_dag = ingest_stocks()
