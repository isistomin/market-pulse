"""Unified marts for both contours, exported as the snapshot the API serves.

Waits for the two upstream DAGs instead of running on a clock of its own: the
marts are only correct once the day's stock bars and crypto bars both exist.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pendulum
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.sdk import dag, task

from analytics.metrics import build_metrics
from analytics.snapshot import build_snapshot, write_snapshot
from analytics.unify import build_daily_series
from ingestion.stocks.storage import data_root

log = logging.getLogger(__name__)

SCHEDULE = "0 2 * * *"
START_DATE = datetime(2026, 8, 13)

DEFAULT_ARGS = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


@dag(
    dag_id="build_marts",
    schedule=SCHEDULE,
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["marts", "serving"],
    doc_md=__doc__,
)
def build_marts():
    # Both upstream DAGs run earlier in the day, so the sensors look back to their
    # schedules rather than at this DAG's own interval.
    wait_for_stocks = ExternalTaskSensor(
        task_id="wait_for_ingest_stocks",
        external_dag_id="ingest_stocks",
        allowed_states=["success"],
        failed_states=["failed"],
        execution_delta=timedelta(hours=3),
        mode="reschedule",
        poke_interval=300,
        timeout=60 * 60 * 2,
    )

    wait_for_crypto = ExternalTaskSensor(
        task_id="wait_for_process_crypto",
        external_dag_id="process_crypto",
        allowed_states=["success"],
        failed_states=["failed"],
        execution_delta=timedelta(hours=1, minutes=30),
        mode="reschedule",
        poke_interval=300,
        timeout=60 * 60 * 2,
    )

    @task
    def check_coverage() -> dict:
        """Fail early if a contour is missing rather than publish a half-empty page."""
        series = build_daily_series(data_root())
        if series.empty:
            raise ValueError("no daily series available for either contour")

        counts = series.groupby("instrument_type")["instrument_id"].nunique().to_dict()
        log.info("instruments per contour: %s", counts)

        metrics = build_metrics(series)
        log.info("metric rows: %d", len(metrics))
        return {key: int(value) for key, value in counts.items()}

    @task
    def export_snapshot(coverage: dict, logical_date=None) -> str:
        generated_at = pendulum.instance(logical_date) if logical_date else pendulum.now("UTC")
        snapshot = build_snapshot(data_root(), generated_at=generated_at)
        target = write_snapshot(snapshot, data_root())
        log.info("snapshot covers %s", coverage)
        return str(target)

    coverage = check_coverage()
    [wait_for_stocks, wait_for_crypto] >> coverage
    export_snapshot(coverage)


build_marts_dag = build_marts()
