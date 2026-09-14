"""
Kafka to TimescaleDB collector.

Consumes three topics and writes them in batches (every 1,000 messages or
10 seconds):

    edge-iiot-stream     -> iot_data(ts, device_id, value, label)
    anomalies            -> anomalies(ts, device_id, value, anomaly_score, severity, detection_method, label)
    local-model-updates  -> local_model_updates(ts, device_id, model_version, accuracy, loss, mean, std)

Fleet KPIs (row totals, device counts) are computed by
dashboard_metrics_updater.py, so no batch insert triggers a table scan.
"""

import json
import logging
import time
from datetime import datetime
import sys

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Configuration (inside Docker: hosts are service names)
# ---------------------------------------------------------------------

import os  # noqa: E402

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config_loader import get_db_config, get_kafka_config  # noqa: E402

# Kafka and database settings come from the shared config (environment
# variables set by docker-compose, with defaults for local runs).
KAFKA_BROKERS = [
    s.strip() for s in get_kafka_config()["bootstrap_servers"].split(",") if s.strip()
]
KAFKA_TOPICS = ["edge-iiot-stream", "anomalies", "local-model-updates"]
GROUP_ID = "timescaledb-collector"

_db = get_db_config()
DB_HOST = _db["host"]
DB_PORT = _db["port"]
DB_NAME = _db["database"]
DB_USER = _db["user"]
DB_PASSWORD = _db["password"]

# Batch behaviour
BATCH_SIZE = 1000
INSERT_INTERVAL_SECONDS = 10


class KafkaToTimescaleDB:
    """Collect Kafka data and write to TimescaleDB."""

    def __init__(self):
        self.kafka_consumer = None
        self.db_conn = None
        
        # Separate batches for each table
        self.iot_batch = []
        self.anomaly_batch = []
        self.model_batch = []
        
        self.last_insert_time = time.time()
        self.total_messages = 0
        self.total_inserts = 0

    # --------------------------------------------------------------
    # Connections
    # --------------------------------------------------------------
    def connect_kafka(self) -> bool:
        """Connect to Kafka with retries."""
        max_retries = 30
        delay = 5

        last_err: Exception | None = None

        for attempt in range(1, max_retries + 1):
            try:
                self.kafka_consumer = KafkaConsumer(
                    *KAFKA_TOPICS,  # Subscribe to all topics
                    bootstrap_servers=KAFKA_BROKERS,
                    group_id=GROUP_ID,
                    auto_offset_reset="earliest",
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    session_timeout_ms=30000,
                    heartbeat_interval_ms=10000,
                    max_poll_records=500,
                    connections_max_idle_ms=540000,
                )
                logger.info(
                    "✓ Connected to Kafka brokers: %s",
                    ", ".join(KAFKA_BROKERS),
                )
                return True
            except NoBrokersAvailable as e:
                last_err = e
                logger.warning(
                    "Kafka not ready yet for timescaledb-collector "
                    "(attempt %d/%d): %s",
                    attempt,
                    max_retries,
                    e,
                )
                time.sleep(delay)
            except Exception as e:
                last_err = e
                logger.warning(
                    "Error connecting to Kafka (attempt %d/%d): %s",
                    attempt,
                    max_retries,
                    e,
                )
                time.sleep(delay)

        logger.error(
            "✗ Failed to connect to Kafka after %d attempts: %s",
            max_retries,
            last_err,
        )
        return False

    def connect_db(self) -> bool:
        """Connect to TimescaleDB with retries."""
        max_retries = 5
        delay = 5

        for attempt in range(max_retries):
            try:
                self.db_conn = psycopg2.connect(
                    host=DB_HOST,
                    port=DB_PORT,
                    database=DB_NAME,
                    user=DB_USER,
                    password=DB_PASSWORD,
                    connect_timeout=10,
                )
                logger.info(
                    "✓ Connected to TimescaleDB: %s:%s/%s",
                    DB_HOST,
                    DB_PORT,
                    DB_NAME,
                )
                return True
            except Exception as e:
                logger.warning(
                    "DB connection attempt %d/%d failed: %s",
                    attempt + 1,
                    max_retries,
                    e,
                )
                if attempt < max_retries - 1:
                    logger.info("Retrying in %ds…", delay)
                    time.sleep(delay)
                else:
                    logger.error(
                        "✗ Failed to connect to database after %d attempts",
                        max_retries,
                    )
                    return False

    # --------------------------------------------------------------
    # Schema
    # --------------------------------------------------------------
    def create_tables(self) -> bool:
        """
        Ensure iot_data, anomalies, local_model_updates, and dashboard_metrics tables exist.
        """
        try:
            cur = self.db_conn.cursor()

            # 1. iot_data for raw sensor events
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS iot_data (
                    ts        TIMESTAMPTZ NOT NULL,
                    device_id TEXT        NOT NULL,
                    value     DOUBLE PRECISION,
                    label     INT
                );
                SELECT create_hypertable('iot_data', 'ts', if_not_exists => TRUE);
                CREATE INDEX IF NOT EXISTS idx_iot_data_device_ts ON iot_data (device_id, ts);
                """
            )

            # 2. anomalies table (RCF-based detection)
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS anomalies (
                    ts              TIMESTAMPTZ NOT NULL,
                    device_id       TEXT        NOT NULL,
                    value           DOUBLE PRECISION,
                    anomaly_score   DOUBLE PRECISION,
                    severity        TEXT,
                    detection_method TEXT DEFAULT 'random_cut_forest',
                    label           INT
                );
                SELECT create_hypertable('anomalies', 'ts', if_not_exists => TRUE);
                CREATE INDEX IF NOT EXISTS idx_anomalies_device_ts ON anomalies (device_id, ts);
                """
            )

            # 3. local_model_updates table
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS local_model_updates (
                    ts            TIMESTAMPTZ NOT NULL,
                    device_id     TEXT        NOT NULL,
                    model_version INT,
                    accuracy      DOUBLE PRECISION,
                    loss          DOUBLE PRECISION,
                    mean          DOUBLE PRECISION,
                    std           DOUBLE PRECISION
                );
                SELECT create_hypertable('local_model_updates', 'ts', if_not_exists => TRUE);
                CREATE INDEX IF NOT EXISTS idx_models_device_ts ON local_model_updates (device_id, ts);
                """
            )

            # 4. dashboard_metrics snapshot table
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS dashboard_metrics (
                    id           BIGSERIAL,
                    metric_name  TEXT            NOT NULL,
                    metric_value DOUBLE PRECISION NOT NULL,
                    metric_unit  TEXT            NOT NULL,
                    device_id    TEXT,
                    timestamp    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
                    updated_at   TIMESTAMPTZ     NOT NULL DEFAULT NOW()
                );
                SELECT create_hypertable('dashboard_metrics', 'timestamp', if_not_exists => TRUE);
                """
            )

            self.db_conn.commit()
            logger.info("✓ All tables (iot_data, anomalies, models, metrics) ready")
            cur.close()
            return True
        except Exception as e:
            logger.error("✗ Failed to create tables: %s", e)
            self.db_conn.rollback()
            return False

    # --------------------------------------------------------------
    # Insertion
    # --------------------------------------------------------------
    def insert_batches(self) -> None:
        """Insert all pending batches into their respective tables."""
        if not (self.iot_batch or self.anomaly_batch or self.model_batch):
            return

        try:
            cur = self.db_conn.cursor()

            # 1. Insert IoT Data
            if self.iot_batch:
                data_tuples = []
                for msg in self.iot_batch:
                    ts = self._parse_ts(msg.get("timestamp"))
                    device_id = msg.get("device_id", "device_0")
                    value = float(msg.get("value", 0.0))
                    label = int(msg.get("label", 0))
                    data_tuples.append((ts, device_id, value, label))

                execute_values(
                    cur,
                    "INSERT INTO iot_data (ts, device_id, value, label) VALUES %s",
                    data_tuples,
                )
                self.total_inserts += len(self.iot_batch)
                self.iot_batch = []

            # 2. Insert Anomalies (RCF-based detection)
            if self.anomaly_batch:
                anom_tuples = []
                for msg in self.anomaly_batch:
                    ts = self._parse_ts(msg.get("timestamp"))
                    device_id = msg.get("device_id", "unknown")
                    value = float(msg.get("value", 0.0))
                    # Support both old z_score and new anomaly_score field
                    anomaly_score = float(msg.get("anomaly_score", msg.get("z_score", 0.0)))
                    severity = msg.get("severity", "info")
                    detection_method = msg.get("detection_method", "random_cut_forest")
                    label = msg.get("label")
                    label = None if label is None else int(float(label))
                    anom_tuples.append((ts, device_id, value, anomaly_score, severity, detection_method, label))

                execute_values(
                    cur,
                    "INSERT INTO anomalies (ts, device_id, value, anomaly_score, severity, detection_method, label) VALUES %s",
                    anom_tuples,
                )
                self.anomaly_batch = []

            # 3. Insert Model Updates
            if self.model_batch:
                model_tuples = []
                for msg in self.model_batch:
                    ts = self._parse_ts(msg.get("timestamp"))
                    device_id = msg.get("device_id", "unknown")
                    version = int(msg.get("model_version", 0))
                    acc = float(msg.get("accuracy", 0.0))
                    loss = float(msg.get("loss", 0.0))
                    mean = float(msg.get("mean", 0.0))
                    std = float(msg.get("std", 0.0))
                    model_tuples.append((ts, device_id, version, acc, loss, mean, std))

                execute_values(
                    cur,
                    "INSERT INTO local_model_updates (ts, device_id, model_version, accuracy, loss, mean, std) VALUES %s",
                    model_tuples,
                )
                self.model_batch = []

            self.db_conn.commit()
            logger.info(
                "✓ Flushed batches. Total inserts: %d",
                self.total_inserts,
            )

            self.last_insert_time = time.time()
            cur.close()

        except Exception as e:
            logger.error("✗ Failed to insert batches: %s", e)
            self.db_conn.rollback()

    def _parse_ts(self, ts_raw) -> datetime:
        if isinstance(ts_raw, str):
            try:
                return datetime.fromisoformat(ts_raw)
            except Exception:
                pass
        return datetime.utcnow()

    # --------------------------------------------------------------
    # Main loop
    # --------------------------------------------------------------
    def run(self) -> None:
        """Main collector loop."""
        if not self.connect_kafka():
            sys.exit(1)
        if not self.connect_db():
            sys.exit(1)
        if not self.create_tables():
            sys.exit(1)

        logger.info(
            "Starting collection from topics %s (batch=%d, interval=%ds)",
            KAFKA_TOPICS,
            BATCH_SIZE,
            INSERT_INTERVAL_SECONDS,
        )

        try:
            for message in self.kafka_consumer:
                try:
                    data = message.value
                    topic = message.topic

                    # Route to appropriate batch
                    if topic == "edge-iiot-stream":
                        if "device_id" not in data:
                            # Never invent a device id: the reading cannot be attributed
                            logger.warning("Skipping a reading without device_id")
                            continue
                        self.iot_batch.append(data)
                    
                    elif topic == "anomalies":
                        self.anomaly_batch.append(data)
                    
                    elif topic == "local-model-updates":
                        self.model_batch.append(data)

                    self.total_messages += 1

                    # Check flush conditions
                    total_pending = len(self.iot_batch) + len(self.anomaly_batch) + len(self.model_batch)
                    time_since_last = time.time() - self.last_insert_time
                    
                    if (
                        total_pending >= BATCH_SIZE
                        or time_since_last >= INSERT_INTERVAL_SECONDS
                    ):
                        self.insert_batches()

                    if self.total_messages % 100 == 0:
                        logger.info(
                            "Progress: %d messages processed",
                            self.total_messages,
                        )

                except Exception as e:
                    logger.error("Error processing message: %s", e)

        except KeyboardInterrupt:
            logger.info("Shutting down collector…")
        except Exception as e:
            logger.error("Collector error: %s", e)
        finally:
            self.insert_batches()

            logger.info(
                "Final stats: %d messages seen, %d rows inserted",
                self.total_messages,
                self.total_inserts,
            )
            if self.kafka_consumer:
                self.kafka_consumer.close()
            if self.db_conn:
                self.db_conn.close()


if __name__ == "__main__":
    collector = KafkaToTimescaleDB()
    collector.run()
