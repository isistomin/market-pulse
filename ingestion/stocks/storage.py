"""Partitioned Parquet writes for the raw layer.

Idempotency comes from the layout rather than from a merge: a partition path is a
pure function of (date, market), and a rerun overwrites that path. Rerunning the
same logical date therefore replaces the partition instead of appending to it,
which is what makes catchup and manual reruns safe.
"""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

SCHEMA = ["ticker", "date", "open", "high", "low", "close", "adj_close", "volume"]


def data_root() -> Path:
    """Raw layer root. Containers set MARKET_PULSE_DATA_DIR to the shared volume."""
    return Path(os.environ.get("MARKET_PULSE_DATA_DIR", "data"))


def partition_path(root: Path | str, date: pd.Timestamp | str, market: str) -> Path:
    day = pd.Timestamp(date).strftime("%Y-%m-%d")
    return Path(root) / "stocks" / f"date={day}" / f"market={market}"


def write_daily_partitions(bars: pd.DataFrame, root: Path | str) -> list[Path]:
    """Write bars into date/market partitions, replacing any existing ones.

    Expects a `market` column alongside SCHEMA. Returns the partition directories
    written, sorted.
    """
    if bars.empty:
        return []
    if "market" not in bars.columns:
        raise ValueError("bars must carry a market column; see fetch.add_market")

    written: list[Path] = []
    for (date, market), group in bars.groupby([bars["date"].dt.normalize(), "market"]):
        target = partition_path(root, date, market)
        if target.exists():
            shutil.rmtree(target)
        target.mkdir(parents=True, exist_ok=True)

        payload = group[SCHEMA].sort_values("ticker").reset_index(drop=True)
        payload.to_parquet(target / "bars.parquet", index=False, engine="pyarrow")
        written.append(target)
        log.info("wrote %d rows to %s", len(payload), target)

    return sorted(written)


def read_partitions(root: Path | str) -> pd.DataFrame:
    """Read the whole raw layer back. Used by tests and by ad-hoc checks."""
    files = sorted(Path(root).glob("stocks/date=*/market=*/bars.parquet"))
    if not files:
        return pd.DataFrame(columns=[*SCHEMA, "market"])

    frames = []
    for file in files:
        frame = pd.read_parquet(file, engine="pyarrow")
        frame["market"] = file.parent.name.split("=", 1)[1]
        frames.append(frame)
    return pd.concat(frames, ignore_index=True)
