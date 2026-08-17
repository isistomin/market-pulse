"""Read-only API over the marts snapshot.

The snapshot is a file produced once a day by the build_marts DAG, so a request is
a dictionary lookup. Nothing here touches Parquet or a database, which keeps the
public surface small: no query is ever built from user input.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from analytics.snapshot import snapshot_path

log = logging.getLogger("api")

INSTRUMENT_TYPES = ("stock", "crypto")


def data_root() -> Path:
    return Path(os.environ.get("MARKET_PULSE_DATA_DIR", "data"))


def allowed_origins() -> list[str]:
    raw = os.environ.get("API_CORS_ORIGINS", "http://localhost:5173")
    return [origin.strip() for origin in raw.split(",") if origin.strip()]


class SnapshotCache:
    """Keeps the parsed snapshot in memory and reloads it when the file changes."""

    def __init__(self, root: Path):
        self._root = root
        self._loaded_from: float | None = None
        self._payload: dict | None = None

    def get(self) -> dict:
        path = snapshot_path(self._root)
        if not path.exists():
            raise HTTPException(
                status_code=503,
                detail="snapshot is not available yet; the build_marts DAG has not run",
            )

        stamp = path.stat().st_mtime
        if self._payload is None or stamp != self._loaded_from:
            self._payload = json.loads(path.read_text(encoding="utf-8"))
            self._loaded_from = stamp
            log.info("loaded snapshot with %d instruments", len(self._payload["instruments"]))

        return self._payload


def create_app(root: Path | None = None) -> FastAPI:
    app = FastAPI(
        title="Market Pulse",
        description="Metrics over a daily batch contour and a streaming one.",
        version="0.1.0",
    )
    cache = SnapshotCache(root or data_root())

    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins(),
        allow_credentials=False,
        allow_methods=["GET"],
        allow_headers=["*"],
    )

    def _instrument(instrument_id: str) -> dict:
        snapshot = cache.get()
        for instrument in snapshot["instruments"]:
            if instrument["id"] == instrument_id:
                return instrument
        raise HTTPException(status_code=404, detail=f"unknown instrument {instrument_id}")

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    @app.get("/instruments")
    def list_instruments(
        type: str | None = Query(default=None, pattern="^(stock|crypto)$"),
    ) -> dict:
        snapshot = cache.get()
        instruments = snapshot["instruments"]
        if type is not None:
            instruments = [i for i in instruments if i["type"] == type]
        return {"generated_at": snapshot["generated_at"], "instruments": instruments}

    @app.get("/instruments/{instrument_id}/metrics")
    def instrument_metrics(instrument_id: str) -> dict:
        instrument = _instrument(instrument_id)
        snapshot = cache.get()
        return {
            "instrument": instrument,
            "series": snapshot["series"].get(instrument_id, []),
        }

    @app.get("/instruments/{instrument_id}/benchmark")
    def instrument_benchmark(instrument_id: str) -> dict:
        instrument = _instrument(instrument_id)
        snapshot = cache.get()

        benchmark_id = instrument.get("benchmark_id")
        if not benchmark_id:
            raise HTTPException(
                status_code=404, detail=f"no benchmark defined for {instrument_id}"
            )

        return {
            "instrument_id": instrument_id,
            "benchmark_id": benchmark_id,
            "excess_return": instrument.get("excess_return"),
            "series": snapshot["series"].get(instrument_id, []),
            "benchmark_series": snapshot["series"].get(benchmark_id, []),
        }

    return app


app = create_app()
