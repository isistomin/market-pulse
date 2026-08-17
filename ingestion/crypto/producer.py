"""Binance trade stream to Kafka.

Runs as its own long-lived process rather than an Airflow task: Airflow schedules
bounded work with a start and an end, while this holds a socket open indefinitely.

Each tick is keyed by pair, which pins a pair to a partition and keeps its trades
ordered. Without a key Kafka round-robins and the ordering guarantee is lost.
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal

import websockets
from confluent_kafka import Producer

from ingestion.crypto.messages import MalformedMessage, encode_tick, parse_trade
from ingestion.crypto.universe import PAIRS, stream_url

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("crypto.producer")

RECONNECT_DELAY_SECONDS = 5
STATS_EVERY = 1_000


def build_producer(bootstrap_servers: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "client.id": "crypto-producer",
            # Retries plus idempotence keep a broker hiccup from dropping or
            # reordering ticks inside a partition.
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 50,
            "compression.type": "lz4",
        }
    )


def _on_delivery(error, message) -> None:
    if error is not None:
        log.error("delivery failed for %s: %s", message.key(), error)


async def stream_to_kafka(producer: Producer, topic: str, pairs: list[str], stop: asyncio.Event):
    url = stream_url(pairs)
    delivered = 0

    while not stop.is_set():
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as socket:
                log.info("connected to %d streams", len(pairs))
                while not stop.is_set():
                    frame = await socket.recv()
                    try:
                        tick = parse_trade(frame)
                    except MalformedMessage as exc:
                        log.warning("skipped a frame: %s", exc)
                        continue

                    producer.produce(
                        topic,
                        key=tick["pair"].encode(),
                        value=encode_tick(tick),
                        on_delivery=_on_delivery,
                    )
                    producer.poll(0)

                    delivered += 1
                    if delivered % STATS_EVERY == 0:
                        log.info("produced %d ticks", delivered)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("stream dropped (%s), reconnecting in %ds", exc, RECONNECT_DELAY_SECONDS)
            await asyncio.sleep(RECONNECT_DELAY_SECONDS)

    producer.flush(10)
    log.info("stopped after %d ticks", delivered)


async def main() -> None:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.environ.get("CRYPTO_TOPIC", "crypto_ticks")

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for received in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(received, stop.set)

    await stream_to_kafka(build_producer(bootstrap), topic, PAIRS, stop)


if __name__ == "__main__":
    asyncio.run(main())
