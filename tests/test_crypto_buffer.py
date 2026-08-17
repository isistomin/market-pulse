import pytest

from ingestion.crypto.buffer import TickBuffer


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def test_empty_buffer_never_flushes():
    assert not TickBuffer(max_ticks=2, max_seconds=1, clock=FakeClock()).should_flush()


def test_flushes_once_the_size_bound_is_reached():
    buffer = TickBuffer(max_ticks=3, max_seconds=999, clock=FakeClock())
    for i in range(2):
        buffer.add({"trade_id": i})
    assert not buffer.should_flush()

    buffer.add({"trade_id": 2})
    assert buffer.should_flush()


# A quiet pair would otherwise sit in memory until the size bound is hit.
def test_flushes_on_the_time_bound_for_a_thin_stream():
    clock = FakeClock()
    buffer = TickBuffer(max_ticks=10_000, max_seconds=300, clock=clock)
    buffer.add({"trade_id": 1})

    clock.advance(299)
    assert not buffer.should_flush()

    clock.advance(2)
    assert buffer.should_flush()


def test_drain_returns_and_clears():
    buffer = TickBuffer(max_ticks=2, max_seconds=999, clock=FakeClock())
    buffer.add({"trade_id": 1})
    buffer.add({"trade_id": 2})

    assert buffer.drain() == [{"trade_id": 1}, {"trade_id": 2}]
    assert len(buffer) == 0
    assert not buffer.should_flush()


def test_drain_restarts_the_time_window():
    clock = FakeClock()
    buffer = TickBuffer(max_ticks=10_000, max_seconds=100, clock=clock)
    buffer.add({"trade_id": 1})

    clock.advance(150)
    buffer.drain()

    buffer.add({"trade_id": 2})
    assert not buffer.should_flush()


@pytest.mark.parametrize(("ticks", "seconds"), [(0, 10), (10, 0), (-1, 10)])
def test_bounds_must_be_positive(ticks, seconds):
    with pytest.raises(ValueError):
        TickBuffer(max_ticks=ticks, max_seconds=seconds)
