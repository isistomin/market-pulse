"""JSON snapshot consumed by the dashboard API.

The API reads a file rather than recomputing from Parquet on every request: the
data only changes once a day, so serving is a file read and the API keeps no
connection to the storage layer.
"""

from __future__ import annotations

import json
import logging
import math
from datetime import datetime
from pathlib import Path

import pandas as pd

from analytics.metrics import build_metrics, latest_per_instrument
from analytics.unify import build_daily_series

log = logging.getLogger(__name__)

SNAPSHOT_NAME = "snapshot.json"
HISTORY_DAYS = 400

SUMMARY_FIELDS = [
    "daily_return", "volatility_20d", "volatility_60d",
    "drawdown", "benchmark_return", "excess_return",
]
SERIES_FIELDS = ["close", "daily_return", "drawdown"]


def snapshot_path(root: Path | str) -> Path:
    return Path(root) / "marts" / SNAPSHOT_NAME


def _clean(value):
    """JSON has no NaN, so missing numbers become null."""
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.strftime("%Y-%m-%d")
    if hasattr(value, "item"):
        return _clean(value.item())
    return value


def build_snapshot(root: Path | str, generated_at: datetime) -> dict:
    series = build_daily_series(root)
    metrics = build_metrics(series)

    if metrics.empty:
        return {"generated_at": generated_at.isoformat(), "instruments": [], "series": {}}

    instruments = []
    for row in latest_per_instrument(metrics).to_dict(orient="records"):
        instrument = {
            "id": row["instrument_id"],
            "type": row["instrument_type"],
            "market": row["market"],
            "last_date": _clean(row["date"]),
            "close": _clean(row["close"]),
            "benchmark_id": row.get("benchmark_id"),
        }
        instrument.update({field: _clean(row.get(field)) for field in SUMMARY_FIELDS})
        instruments.append(instrument)

    cutoff = metrics["date"].max() - pd.Timedelta(days=HISTORY_DAYS)
    recent = metrics[metrics["date"] >= cutoff]

    history: dict[str, list[dict]] = {}
    for instrument_id, group in recent.groupby("instrument_id"):
        history[instrument_id] = [
            {"date": _clean(row["date"]), **{f: _clean(row.get(f)) for f in SERIES_FIELDS}}
            for row in group.sort_values("date").to_dict(orient="records")
        ]

    return {
        "generated_at": generated_at.isoformat(),
        "instruments": sorted(instruments, key=lambda i: i["id"]),
        "series": history,
    }


def write_snapshot(snapshot: dict, root: Path | str) -> Path:
    target = snapshot_path(root)
    target.parent.mkdir(parents=True, exist_ok=True)

    # Written beside the target and moved into place so a reader never sees a
    # half-written file.
    staging = target.with_suffix(".json.tmp")
    staging.write_text(json.dumps(snapshot, separators=(",", ":")), encoding="utf-8")
    staging.replace(target)

    log.info("wrote snapshot with %d instruments to %s", len(snapshot["instruments"]), target)
    return target


def read_snapshot(root: Path | str) -> dict:
    target = snapshot_path(root)
    if not target.exists():
        raise FileNotFoundError(f"no snapshot at {target}; run the build_marts DAG first")
    return json.loads(target.read_text(encoding="utf-8"))
