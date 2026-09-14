"""
Single-Broker Kafka Producer for Edge-IIoT

Replays the per-device CSV files in data/processed as live IoT streams into a
single Kafka broker (kafka-broker-1:9092). All devices stream concurrently:
round-robin, one reading per device per turn, at a configurable total rate.

Only the first `stream_rows_per_device` rows of each device are streamed. The
remaining rows are held out so Spark can evaluate the federated model on
readings no device trained on (see config_loader.get_stream_config).

Device files are read lazily, one row at a time, so memory use does not grow
with the size of the dataset.

Every send is confirmed through a delivery callback. If Kafka confirms nothing
for DELIVERY_STALL_SECONDS while readings keep being queued, the Kafka client
is closed and recreated (see SingleBrokerProducer.delivery_stalled).

Usage:
    python scripts/02_kafka_producer.py --source data/processed --rate 150
"""

import argparse
import csv
import itertools
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Sequence, Tuple

from kafka import KafkaProducer
from kafka.errors import KafkaError

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config_loader import get_kafka_config, get_stream_config  # noqa: E402

# -----------------------------------------------------
# LOGGING
# -----------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Stream metric carried in the "data"/"value" fields (first one present wins).
# flow_duration/Rate exist in older demo files; the Edge-IIoTset device files
# use tcp.ack.
METRIC_CANDIDATES = ("flow_duration", "Rate", "tcp.ack", "tcp.seq")

# Recreate the Kafka client when no send has been confirmed for this long while
# readings are still being queued. When the broker loses its session (seen
# after the host resumed from sleep), kafka-python kept expiring every batch
# and never recovered: the producer reported progress while the topic stood
# still.
DELIVERY_STALL_SECONDS = 60.0


def parse_row(header: Sequence[str], values: Sequence[str]) -> Dict[str, Any]:
    """Convert one CSV row to a record: numbers as floats, blanks as None."""
    record: Dict[str, Any] = {}
    for key, raw in zip(header, values):
        if raw == "":
            record[key] = None
        elif key == "timestamp":
            record[key] = raw
        else:
            try:
                record[key] = float(raw)
            except ValueError:
                record[key] = raw
    return record


def select_metric(record: Dict[str, Any]) -> float:
    """
    The single numeric reading used for per-device anomaly scoring.

    Uses the first candidate column that is present with a numeric value, so a
    legitimate 0.0 is kept (a chained `or` skipped zero values).
    """
    for key in METRIC_CANDIDATES:
        value = record.get(key)
        if isinstance(value, (int, float)):
            return float(value)
    return 0.0


def build_message(device_id: str, record: Dict[str, Any]) -> Dict[str, Any]:
    """Kafka message for one reading: the CSV record plus stream metadata."""
    message = dict(record)
    message["device_id"] = device_id

    # Flink reads data["data"]; the TimescaleDB collector reads msg["value"].
    metric = select_metric(record)
    message["data"] = metric
    message["value"] = metric

    # Stamp the event with the send time. The CSV timestamps are synthetic and
    # repeat on every replay pass, which froze Spark's watermark and put every
    # reading days in the past for Grafana. The original is kept for reference.
    message["source_timestamp"] = record.get("timestamp")
    message["timestamp"] = datetime.now().astimezone().isoformat()
    return message


class SingleBrokerProducer:
    """Kafka producer streaming every device's readings to a single broker."""

    def __init__(
        self,
        topic: str = "edge-iiot-stream",
        rows_per_second: int = 150,
        rows_per_device: Optional[int] = None,
        bootstrap_servers: Optional[str] = None,
        repeat: bool = True,
        stall_seconds: float = DELIVERY_STALL_SECONDS,
    ):
        self.topic = topic
        self.rows_per_second = rows_per_second
        self.interval = 1.0 / rows_per_second
        self.rows_per_device = rows_per_device or get_stream_config()["stream_rows_per_device"]
        self.bootstrap_servers = bootstrap_servers or get_kafka_config()["bootstrap_servers"]
        self.repeat = repeat
        self.stall_seconds = stall_seconds

        self.producer: Optional[KafkaProducer] = None
        self.start_time: Optional[float] = None
        # Queued = handed to the Kafka client; delivered/failed = confirmed by
        # the delivery callbacks (which run on the client's I/O thread)
        self.messages_sent = 0
        self.messages_delivered = 0
        self.delivery_failures = 0
        self.reconnects = 0
        self.last_delivery_at = time.time()
        self._sent_at_connect = 0

        logger.info("Single-Broker Producer initialized:")
        logger.info(f"  Bootstrap servers: {self.bootstrap_servers}")
        logger.info(f"  Topic: {self.topic}")
        logger.info(f"  Rate: {self.rows_per_second} msgs/s (shared by all devices)")
        logger.info(f"  Rows streamed per device: {self.rows_per_device} (the rest is held out)")

    # -------------------------------------------------
    # KAFKA CONNECTION
    # -------------------------------------------------
    def connect(self, max_retries: int = 30, retry_delay: int = 5) -> None:
        """Connect to Kafka with retry logic (more patient for startup)."""
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"Connecting to Kafka at {self.bootstrap_servers} (attempt {attempt})...")
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers.split(","),
                    key_serializer=lambda k: k.encode("utf-8"),
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    acks="all",
                    retries=5,
                    max_in_flight_requests_per_connection=1,
                    request_timeout_ms=60000,
                    connections_max_idle_ms=30000,
                    reconnect_backoff_ms=5000,
                )
                self.mark_connected()
                logger.info("Successfully connected to Kafka")
                return
            except Exception as e:
                if attempt == max_retries:
                    logger.error("Producer failed to connect after %d attempts: %s", max_retries, e)
                    raise
                logger.warning(
                    "Producer connection attempt %d/%d failed: %s. Retrying in %ds...",
                    attempt, max_retries, e, retry_delay,
                )
                time.sleep(retry_delay)

    def mark_connected(self) -> None:
        """Start the delivery watchdog afresh for a new Kafka client."""
        self.last_delivery_at = time.time()
        self._sent_at_connect = self.messages_sent

    # -------------------------------------------------
    # DELIVERY TRACKING
    # -------------------------------------------------
    def _on_delivered(self, _metadata: Any) -> None:
        self.messages_delivered += 1
        self.last_delivery_at = time.time()

    def _on_failed(self, error: Exception) -> None:
        self.delivery_failures += 1
        if self.delivery_failures % 1000 == 1:
            logger.warning("Kafka did not accept a reading (%d failed so far): %s", self.delivery_failures, error)

    def delivery_stalled(self, now: float) -> bool:
        """True when readings were queued on this client but none was confirmed for `stall_seconds`."""
        return self.messages_sent > self._sent_at_connect and now - self.last_delivery_at > self.stall_seconds

    def reconnect(self) -> None:
        """Replace a Kafka client that stopped delivering (its unsent readings are dropped)."""
        self.reconnects += 1
        logger.warning(
            "Kafka confirmed no delivery for %.0fs (queued %d, delivered %d, failed %d); "
            "recreating the Kafka client (reconnect #%d)",
            time.time() - self.last_delivery_at, self.messages_sent, self.messages_delivered,
            self.delivery_failures, self.reconnects,
        )
        if self.producer is not None:
            try:
                self.producer.close(timeout=5)
            except Exception as e:
                logger.warning("Closing the stalled Kafka client failed: %s", e)
        self.connect()

    # -------------------------------------------------
    # DEVICE FILES
    # -------------------------------------------------
    @staticmethod
    def discover_device_files(directory: str) -> List[Tuple[str, Path]]:
        """(device_id, path) for every device_N.csv, in numeric order."""
        device_files = sorted(
            Path(directory).glob("device_*.csv"),
            key=lambda x: int(x.stem.split("_")[-1]),
        )
        if device_files:
            logger.info(
                f"Discovered {len(device_files)} device files "
                f"({device_files[0].stem} … {device_files[-1].stem}), all streamed to broker 1"
            )
        else:
            logger.warning(f"No device_*.csv files found in {directory}")
        return [(f.stem, f) for f in device_files]

    def device_streams(
        self, devices: List[Tuple[str, Path]]
    ) -> Generator[Tuple[str, Dict[str, Any]], None, None]:
        """
        One pass over all devices: round-robin, one reading per device per turn,
        stopping each device after `rows_per_device` rows.
        """
        handles = []
        try:
            cursors = []
            for device_id, path in devices:
                fh = open(path, newline="", encoding="utf-8")
                handles.append(fh)
                reader = csv.reader(fh)
                header = next(reader, None)
                if header:
                    cursors.append((device_id, header, itertools.islice(reader, self.rows_per_device)))

            while cursors:
                remaining = []
                for device_id, header, rows in cursors:
                    values = next(rows, None)
                    if values is None:
                        continue
                    remaining.append((device_id, header, rows))
                    yield device_id, parse_row(header, values)
                cursors = remaining
        finally:
            for fh in handles:
                fh.close()

    # -------------------------------------------------
    # MAIN RUN LOOP
    # -------------------------------------------------
    def run(self, directory: str) -> None:
        """Main loop: discover devices, connect, and stream."""
        devices = self.discover_device_files(directory)
        if not devices:
            logger.error("No devices found. Exiting.")
            return

        self.connect()
        logger.info(f"Starting stream at {self.rows_per_second} rows/sec...")
        self.start_time = time.time()
        next_send_at = self.start_time

        try:
            while True:
                for device_id, record in self.device_streams(devices):
                    # Key by device_id so all readings of a device land on one
                    # partition and reach Flink in order (per-device state).
                    try:
                        future = self.producer.send(self.topic, key=device_id, value=build_message(device_id, record))
                        future.add_callback(self._on_delivered)
                        future.add_errback(self._on_failed)
                    except KafkaError as e:
                        # e.g. metadata or buffer timeout; the watchdog below
                        # replaces the client if this persists
                        self._on_failed(e)
                    self.messages_sent += 1

                    if self.messages_sent % 5000 == 0:
                        elapsed = time.time() - self.start_time
                        logger.info(
                            f"Queued {self.messages_sent} | delivered {self.messages_delivered} | "
                            f"failed {self.delivery_failures} | reconnects {self.reconnects} | "
                            f"Last: {device_id} | Delivered rate: {self.messages_delivered / elapsed:.2f} msg/s"
                        )

                    now = time.time()
                    if self.delivery_stalled(now):
                        self.reconnect()
                        now = next_send_at = time.time()

                    # Pace against a schedule (a fixed sleep on top of send time
                    # undershoots the rate). If more than 1s behind, resync
                    # instead of bursting.
                    next_send_at = max(next_send_at + self.interval, now - 1.0)
                    time.sleep(max(0.0, next_send_at - now))

                if not self.repeat:
                    break
                logger.info("Finished one pass through all devices. Repeating...")

        except KeyboardInterrupt:
            logger.info("Stopped by user.")
        except Exception as e:
            logger.error(f"Error in producer loop: {e}")
        finally:
            if self.producer:
                try:
                    self.producer.flush(timeout=10)
                    self.producer.close(timeout=5)
                except Exception as e:
                    logger.warning("Kafka client did not shut down cleanly: %s", e)
            logger.info(
                "Producer closed (queued %d, delivered %d, failed %d, reconnects %d).",
                self.messages_sent, self.messages_delivered, self.delivery_failures, self.reconnects,
            )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default="data/processed", help="Directory with device_*.csv files")
    parser.add_argument("--rate", type=int, default=150, help="Total messages per second (all devices)")
    parser.add_argument("--topic", default="edge-iiot-stream", help="Kafka topic")
    parser.add_argument("--rows-per-device", type=int, default=None,
                        help="Rows streamed per device (default: STREAM_ROWS_PER_DEVICE / 660)")
    args = parser.parse_args()

    SingleBrokerProducer(
        topic=args.topic,
        rows_per_second=args.rate,
        rows_per_device=args.rows_per_device,
    ).run(args.source)


if __name__ == "__main__":
    main()
