"""Parsing of Binance trade messages into the tick shape stored downstream.

Binance sends prices and quantities as strings and timestamps as epoch
milliseconds, so every message needs converting before it is worth keeping.
"""

from __future__ import annotations

import json
from typing import Any

TICK_FIELDS = ["pair", "trade_id", "price", "quantity", "traded_at", "buyer_is_maker"]


class MalformedMessage(Exception):
    """Raised when a frame does not carry a usable trade event."""


def parse_trade(frame: str | bytes | dict[str, Any]) -> dict[str, Any]:
    """Turn one websocket frame into a tick.

    Accepts both the combined-stream envelope ({"stream": ..., "data": {...}}) and a
    bare single-stream payload.
    """
    if isinstance(frame, (str, bytes)):
        try:
            frame = json.loads(frame)
        except json.JSONDecodeError as exc:
            raise MalformedMessage(f"not valid JSON: {exc}") from exc

    if not isinstance(frame, dict):
        raise MalformedMessage(f"expected an object, got {type(frame).__name__}")

    payload = frame.get("data", frame)

    if payload.get("e") != "trade":
        raise MalformedMessage(f"not a trade event: {payload.get('e')!r}")

    try:
        return {
            "pair": payload["s"],
            "trade_id": int(payload["t"]),
            "price": float(payload["p"]),
            "quantity": float(payload["q"]),
            "traded_at": int(payload["T"]),
            "buyer_is_maker": bool(payload["m"]),
        }
    except (KeyError, TypeError, ValueError) as exc:
        raise MalformedMessage(f"unusable trade payload: {exc}") from exc


def encode_tick(tick: dict[str, Any]) -> bytes:
    return json.dumps(tick, separators=(",", ":")).encode()


def decode_tick(payload: str | bytes) -> dict[str, Any]:
    return json.loads(payload)
