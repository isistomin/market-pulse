"""Daily aggregation of raw crypto ticks into OHLC bars.

Runs after the streaming pair has landed a day of ticks. Deliberately batch rather
than streaming: bars are only read once a day by the marts, so a nightly pass is
enough, and a batch job is far easier to reason about than a streaming one with
checkpoints and watermarks.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.sdk import dag, task

from ingestion.crypto.bars import FREQUENCIES, aggregate_ohlc
from ingestion.crypto.storage import data_root, read_ticks, write_bars

log = logging.getLogger(__name__)

SCHEDULE = "30 0 * * *"
START_DATE = datetime(2026, 8, 13)

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


@dag(
    dag_id="process_crypto",
    schedule=SCHEDULE,
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["crypto", "batch", "bars"],
    doc_md=__doc__,
)
def process_crypto():
    @task
    def aggregate(freq: str, data_interval_start=None) -> int:
        """Rebuild one day of bars at a single frequency."""
        day = data_interval_start.date()
        ticks = read_ticks(data_root(), date=day)

        if ticks.empty:
            log.warning("no ticks landed for %s, nothing to aggregate", day)
            return 0

        bars = aggregate_ohlc(ticks, freq=freq)
        write_bars(bars, data_root(), freq=freq)
        log.info("built %d %s bars for %s from %d ticks", len(bars), freq, day, len(ticks))
        return len(bars)

    aggregate.expand(freq=sorted(FREQUENCIES))


process_crypto_dag = process_crypto()
