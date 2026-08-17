"""Consumer loop behaviour, driven by fakes instead of a broker."""

from __future__ import annotations

import json

import pytest

pytest.importorskip("confluent_kafka", reason="confluent-kafka not installed")

from ingestion.crypto.buffer import TickBuffer  # noqa: E402
from ingestion.crypto.consumer import run  # noqa: E402
from ingestion.crypto.storage import read_ticks  # noqa: E402
from tests.test_crypto_storage import ticks  # noqa: E402


class FakeMessage:
    def __init__(self, tick: dict):
        self._value = json.dumps(tick).encode()

    def error(self):
        return None

    def value(self) -> bytes:
        return self._value


class FakeConsumer:
    """Serves a fixed script of messages, then None forever."""

    def __init__(self, messages: list, events: list):
        self._messages = list(messages)
        self.events = events
        self.subscribed: list[str] = []
        self.closed = False

    def subscribe(self, topics):
        self.subscribed = list(topics)

    def poll(self, _timeout):
        return self._messages.pop(0) if self._messages else None

    def commit(self, asynchronous=False):
        self.events.append("commit")

    def close(self):
        self.closed = True


class StopAfter:
    """Stops the loop once poll has been drained a few times."""

    def __init__(self, consumer: FakeConsumer, extra_polls: int = 3):
        self._consumer = consumer
        self._extra = extra_polls

    def is_set(self) -> bool:
        if self._consumer._messages:
            return False
        self._extra -= 1
        return self._extra < 0


def test_ticks_reach_parquet(tmp_path):
    batch = ticks("BTCUSDT", 4)
    consumer = FakeConsumer([FakeMessage(t) for t in batch], events=[])
    buffer = TickBuffer(max_ticks=4, max_seconds=999, clock=lambda: 0.0)

    written = run(consumer, "crypto_ticks", buffer, tmp_path, StopAfter(consumer))

    assert written == 4
    assert len(read_ticks(tmp_path)) == 4
    assert consumer.subscribed == ["crypto_ticks"]
    assert consumer.closed


# At-least-once: a crash between the write and the commit replays the batch, which
# duplicates ticks. Committing first would lose them instead.
def test_offsets_are_committed_only_after_the_write(tmp_path):
    events: list[str] = []

    class WatchingBuffer(TickBuffer):
        def drain(self):
            events.append("drain")
            return super().drain()

    batch = ticks("BTCUSDT", 2)
    consumer = FakeConsumer([FakeMessage(t) for t in batch], events=events)
    buffer = WatchingBuffer(max_ticks=2, max_seconds=999, clock=lambda: 0.0)

    run(consumer, "crypto_ticks", buffer, tmp_path, StopAfter(consumer))

    assert events == ["drain", "commit"]
    assert len(read_ticks(tmp_path)) == 2


def test_remaining_ticks_are_flushed_on_shutdown(tmp_path):
    batch = ticks("BTCUSDT", 3)
    consumer = FakeConsumer([FakeMessage(t) for t in batch], events=[])
    # Bounds are far from reached, so only the shutdown flush can persist these.
    buffer = TickBuffer(max_ticks=10_000, max_seconds=10_000, clock=lambda: 0.0)

    written = run(consumer, "crypto_ticks", buffer, tmp_path, StopAfter(consumer))

    assert written == 3
    assert len(read_ticks(tmp_path)) == 3


def test_nothing_is_committed_when_no_ticks_arrive(tmp_path):
    events: list[str] = []
    consumer = FakeConsumer([], events=events)
    buffer = TickBuffer(max_ticks=10, max_seconds=10, clock=lambda: 0.0)

    written = run(consumer, "crypto_ticks", buffer, tmp_path, StopAfter(consumer))

    assert written == 0
    assert events == []
    assert read_ticks(tmp_path).empty
