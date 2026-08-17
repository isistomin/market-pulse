"""Partitioned Parquet writes for the raw tick layer.

Unlike the daily stock partitions, ticks arrive continuously, so a partition is
appended to many times a day and cannot be replaced wholesale. File names are
derived from the trade id range instead, which makes replaying the same batch
overwrite the same file rather than add another copy.

Kafka gives at-least-once delivery, so duplicates across differently sized batches
are still possible. Deduplication by (pair, trade_id) belongs downstream, in the
dbt staging model, where the whole day can be seen at once.
"""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

SCHEMA = ["pair", "trade_id", "price", "quantity", "traded_at", "buyer_is_maker"]


def data_root() -> Path:
    return Path(os.environ.get("MARKET_PULSE_DATA_DIR", "data"))


def partition_path(root: Path | str, date: pd.Timestamp | str, pair: str) -> Path:
    day = pd.Timestamp(date).strftime("%Y-%m-%d")
    return Path(root) / "crypto_ticks" / f"date={day}" / f"pair={pair}"


def to_frame(ticks: list[dict]) -> pd.DataFrame:
    frame = pd.DataFrame(ticks, columns=SCHEMA)
    frame["traded_at"] = pd.to_datetime(frame["traded_at"], unit="ms", utc=True)
    return frame


def write_ticks(ticks: list[dict], root: Path | str) -> list[Path]:
    """Write a batch of ticks into date/pair partitions. Returns the files written."""
    if not ticks:
        return []

    frame = to_frame(ticks)
    written: list[Path] = []

    for (day, pair), group in frame.groupby([frame["traded_at"].dt.date, "pair"]):
        target = partition_path(root, pd.Timestamp(day), pair)
        target.mkdir(parents=True, exist_ok=True)

        payload = group.sort_values("trade_id").reset_index(drop=True)
        first, last = int(payload["trade_id"].iloc[0]), int(payload["trade_id"].iloc[-1])
        file = target / f"part-{first}-{last}.parquet"

        payload.to_parquet(file, index=False, engine="pyarrow")
        written.append(file)
        log.info("wrote %d ticks to %s", len(payload), file)

    return sorted(written)


def read_ticks(root: Path | str, date: pd.Timestamp | str | None = None) -> pd.DataFrame:
    """Read raw ticks, optionally limited to a single day partition."""
    day = "*" if date is None else pd.Timestamp(date).strftime("%Y-%m-%d")
    files = sorted(Path(root).glob(f"crypto_ticks/date={day}/pair=*/part-*.parquet"))
    if not files:
        return pd.DataFrame(columns=SCHEMA)
    return pd.concat(
        [pd.read_parquet(file, engine="pyarrow") for file in files], ignore_index=True
    )


def bars_partition_path(
    root: Path | str, freq: str, date: pd.Timestamp | str, pair: str
) -> Path:
    day = pd.Timestamp(date).strftime("%Y-%m-%d")
    return Path(root) / "crypto_bars" / f"freq={freq}" / f"date={day}" / f"pair={pair}"


def write_bars(bars: pd.DataFrame, root: Path | str, freq: str) -> list[Path]:
    """Write OHLC bars into freq/date/pair partitions, replacing what is there.

    Bars are derived data: a rerun recomputes a partition from the ticks, so the
    partition is replaced rather than appended to, the same way the stock DAG works.
    """
    if bars.empty:
        return []

    written: list[Path] = []
    for (day, pair), group in bars.groupby([bars["bar_start"].dt.date, "pair"]):
        target = bars_partition_path(root, freq, pd.Timestamp(day), pair)
        if target.exists():
            shutil.rmtree(target)
        target.mkdir(parents=True, exist_ok=True)

        payload = group.sort_values("bar_start").reset_index(drop=True)
        payload.to_parquet(target / "bars.parquet", index=False, engine="pyarrow")
        written.append(target)
        log.info("wrote %d %s bars to %s", len(payload), freq, target)

    return sorted(written)


def read_bars(root: Path | str, freq: str) -> pd.DataFrame:
    files = sorted(Path(root).glob(f"crypto_bars/freq={freq}/date=*/pair=*/bars.parquet"))
    if not files:
        return pd.DataFrame()
    return pd.concat(
        [pd.read_parquet(file, engine="pyarrow") for file in files], ignore_index=True
    )
