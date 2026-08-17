"""Kafka to the raw Parquet tick layer.

Offsets are committed only after a batch has been written. The order matters: on a
crash between the write and the commit the batch is replayed, which duplicates
ticks but never loses them. Committing first would trade duplicates for gaps, and
duplicates are removable downstream while missing trades are not.
"""

from __future__ import annotations

import logging
import os
import signal

from confluent_kafka import Consumer, KafkaError

from ingestion.crypto.buffer import DEFAULT_MAX_SECONDS, DEFAULT_MAX_TICKS, TickBuffer
from ingestion.crypto.messages import decode_tick
from ingestion.crypto.storage import data_root, write_ticks

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("crypto.consumer")

POLL_TIMEOUT_SECONDS = 1.0


def build_consumer(bootstrap_servers: str, group_id: str) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            # Offsets are committed by hand once the batch is on disk.
            "enable.auto.commit": False,
        }
    )


def run(consumer: Consumer, topic: str, buffer: TickBuffer, root, stop) -> int:
    consumer.subscribe([topic])
    written = 0

    try:
        while not stop.is_set():
            message = consumer.poll(POLL_TIMEOUT_SECONDS)

            if message is not None:
                if message.error():
                    if message.error().code() != KafkaError._PARTITION_EOF:
                        log.error("consume error: %s", message.error())
                else:
                    buffer.add(decode_tick(message.value()))

            if buffer.should_flush():
                batch = buffer.drain()
                write_ticks(batch, root)
                consumer.commit(asynchronous=False)
                written += len(batch)
                log.info("flushed %d ticks, %d total", len(batch), written)

        if len(buffer):
            batch = buffer.drain()
            write_ticks(batch, root)
            consumer.commit(asynchronous=False)
            written += len(batch)
            log.info("flushed %d ticks on shutdown, %d total", len(batch), written)
    finally:
        consumer.close()

    return written


def main() -> None:
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.environ.get("CRYPTO_TOPIC", "crypto_ticks")
    group = os.environ.get("CRYPTO_CONSUMER_GROUP", "crypto_parquet_writer")

    max_ticks = int(os.environ.get("CRYPTO_BATCH_TICKS", DEFAULT_MAX_TICKS))
    max_seconds = float(os.environ.get("CRYPTO_BATCH_SECONDS", DEFAULT_MAX_SECONDS))

    stop = _StopFlag()
    for received in (signal.SIGINT, signal.SIGTERM):
        signal.signal(received, lambda *_: stop.set())

    run(
        build_consumer(bootstrap, group),
        topic,
        TickBuffer(max_ticks=max_ticks, max_seconds=max_seconds),
        data_root(),
        stop,
    )


class _StopFlag:
    def __init__(self):
        self._set = False

    def set(self) -> None:
        self._set = True

    def is_set(self) -> bool:
        return self._set


if __name__ == "__main__":
    main()
