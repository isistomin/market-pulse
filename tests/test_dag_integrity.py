"""Import-time checks for the DAG files.

A DAG that fails to import is invisible: the scheduler stays healthy and nothing
appears in the UI. Catching that here keeps the failure in CI instead of in Airflow.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

pytest.importorskip("airflow.sdk", reason="Airflow task SDK not installed")

DAGS_DIR = Path(__file__).resolve().parents[1] / "airflow" / "dags"


def _load(path: Path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _dag_files() -> list[Path]:
    return sorted(p for p in DAGS_DIR.glob("*.py") if not p.name.startswith("_"))


@pytest.fixture(scope="module")
def ingest_stocks_dag():
    from airflow.sdk.definitions.dag import DAG

    module = _load(DAGS_DIR / "ingest_stocks.py")
    # The DAG has to be reachable from module globals, otherwise the dag-processor
    # parses the file without registering anything.
    dags = [value for value in vars(module).values() if isinstance(value, DAG)]
    assert len(dags) == 1, f"expected exactly one DAG in the module, found {len(dags)}"
    return dags[0]


def test_every_dag_file_imports():
    files = _dag_files()
    assert files, "no DAG files found"
    for path in files:
        _load(path)


def test_dag_id_and_schedule(ingest_stocks_dag):
    assert ingest_stocks_dag.dag_id == "ingest_stocks"
    assert ingest_stocks_dag.schedule == "0 23 * * 1-5"


def test_catchup_is_enabled_for_backfill(ingest_stocks_dag):
    assert ingest_stocks_dag.catchup is True


def test_runs_are_serialised_against_the_shared_endpoint(ingest_stocks_dag):
    assert ingest_stocks_dag.max_active_runs == 1


def test_tasks_and_order(ingest_stocks_dag):
    assert set(ingest_stocks_dag.task_ids) == {"fetch_prices", "validate_raw", "write_parquet"}

    fetch = ingest_stocks_dag.get_task("fetch_prices")
    validate = ingest_stocks_dag.get_task("validate_raw")
    write = ingest_stocks_dag.get_task("write_parquet")

    assert validate.task_id in fetch.downstream_task_ids
    assert write.task_id in validate.downstream_task_ids
    assert not write.downstream_task_ids


def test_retries_are_configured(ingest_stocks_dag):
    for task_id in ingest_stocks_dag.task_ids:
        task = ingest_stocks_dag.get_task(task_id)
        assert task.retries == 3
        assert task.retry_exponential_backoff is True
