#!/usr/bin/env python3
"""
Dashboard metrics updater for FLEAD

Every 15 s, computes the fleet-wide KPIs that scan the large tables (row
totals, device counts, anomaly rate) and writes one dashboard_metrics row per
KPI:

- Grafana derives fleet totals and rates from these snapshots.
- The monitoring dashboard and its Prometheus exporter read the latest
  snapshot instead of re-running the scans on every refresh or scrape.

Reads: iot_data, local_models, federated_models, anomalies
Writes: dashboard_metrics(metric_name, metric_value, metric_unit, timestamp)
"""

import logging
import os
import sys
import time

import psycopg2

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config_loader import get_db_config  # noqa: E402

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# DB config – shared config (environment variables from docker-compose)
# ---------------------------------------------------------------------
_db = get_db_config()
DB_HOST = _db["host"]
DB_PORT = _db["port"]
DB_NAME = _db["database"]
DB_USER = _db["user"]
DB_PASSWORD = _db["password"]

INTERVAL_SECONDS = 15  # how often to write a new metrics row (optimized from 30s)


class DashboardMetricsUpdater:
    def __init__(self):
        self.conn = None

    def connect_db(self) -> bool:
        max_retries = 10
        delay = 5

        for attempt in range(1, max_retries + 1):
            try:
                self.conn = psycopg2.connect(
                    host=DB_HOST,
                    port=DB_PORT,
                    dbname=DB_NAME,
                    user=DB_USER,
                    password=DB_PASSWORD,
                    connect_timeout=10,
                )
                self.conn.autocommit = False
                logger.info("✓ Connected to TimescaleDB for dashboard_metrics")
                return True
            except Exception as e:
                logger.warning(
                    "DB connect attempt %d/%d failed: %s",
                    attempt,
                    max_retries,
                    e,
                )
                if attempt < max_retries:
                    time.sleep(delay)
        logger.error("✗ Could not connect to DB after %d attempts", max_retries)
        return False

    def ensure_table(self) -> bool:
        """
        Create dashboard_metrics table if it doesn't exist.
        Matches schema from 00_init_database.py
        """
        try:
            cur = self.conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS dashboard_metrics (
                    id           BIGSERIAL,
                    metric_name  TEXT            NOT NULL,
                    metric_value DOUBLE PRECISION NOT NULL,
                    metric_unit  TEXT            NOT NULL,
                    device_id    TEXT,
                    timestamp    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
                    updated_at   TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
                    CONSTRAINT dashboard_metrics_unique_name_ts
                        UNIQUE (metric_name, timestamp)
                );

                SELECT create_hypertable('dashboard_metrics', 'timestamp', if_not_exists => TRUE);

                CREATE INDEX IF NOT EXISTS idx_dashboard_metrics_name
                    ON dashboard_metrics (metric_name, timestamp);
                """
            )
            self.conn.commit()
            cur.close()
            logger.info("✓ dashboard_metrics table ready")
            return True
        except Exception as e:
            # It might fail if hypertable already exists or other race conditions, 
            # but usually safe to ignore if table exists.
            logger.warning("⚠ ensure_table warning (might be safe if table exists): %s", e)
            self.conn.rollback()
            return True  # Assume it exists

    # (metric_name, unit, SQL returning one number)
    METRICS = [
        ("total_iot_count", "count", "SELECT COUNT(*) FROM iot_data"),
        ("total_local_models_count", "count", "SELECT COUNT(*) FROM local_models"),
        ("total_federated_models_count", "count", "SELECT COUNT(*) FROM federated_models"),
        ("total_anomalies_count", "count", "SELECT COUNT(*) FROM anomalies"),
        ("devices_with_models", "count", "SELECT COUNT(DISTINCT device_id) FROM local_models"),
        ("active_devices_5m", "count",
         "SELECT COUNT(DISTINCT device_id) FROM iot_data WHERE ts > NOW() - INTERVAL '5 minutes'"),
        # Devices with local models but no readings in the last hour: one
        # (device_id, ts) index probe per device
        ("stale_devices_count", "count",
         "SELECT COUNT(*) FROM (SELECT DISTINCT device_id FROM local_models) m "
         "WHERE NOT EXISTS (SELECT 1 FROM iot_data i "
         "WHERE i.device_id = m.device_id AND i.ts > NOW() - INTERVAL '1 hour')"),
        ("anomaly_rate_1h", "ratio",
         "SELECT (SELECT COUNT(*) FROM anomalies WHERE ts > NOW() - INTERVAL '1 hour')::float "
         "/ NULLIF((SELECT COUNT(*) FROM iot_data WHERE ts > NOW() - INTERVAL '1 hour'), 0)"),
        ("anomaly_attack_share_1h", "ratio",
         "SELECT AVG(label)::float FROM anomalies "
         "WHERE ts > NOW() - INTERVAL '1 hour' AND label IS NOT NULL"),
    ]

    def compute_kpis(self):
        """Return {metric_name: (value, unit)}; a KPI whose query fails or has no value is left out."""
        values = {}
        for name, unit, sql in self.METRICS:
            try:
                with self.conn.cursor() as cur:
                    cur.execute(sql)
                    row = cur.fetchone()
                if row and row[0] is not None:
                    values[name] = (float(row[0]), unit)
            except Exception as e:
                logger.warning("KPI %s failed: %s", name, e)
                self.conn.rollback()
        return values

    def insert_metrics_row(self, values):
        """Insert one dashboard_metrics row per KPI, stamped with the database clock."""
        try:
            with self.conn.cursor() as cur:
                for name, (value, unit) in values.items():
                    cur.execute(
                        """
                        INSERT INTO dashboard_metrics
                            (metric_name, metric_value, metric_unit, timestamp, updated_at)
                        VALUES (%s, %s, %s, NOW(), NOW())
                        ON CONFLICT (metric_name, timestamp) DO NOTHING;
                        """,
                        (name, value, unit),
                    )
            self.conn.commit()
            logger.info(
                "✓ Inserted %d dashboard_metrics rows: %s",
                len(values),
                ", ".join(f"{name}={value:g}" for name, (value, _) in values.items()),
            )
        except Exception as e:
            logger.error("✗ Failed to insert dashboard_metrics rows: %s", e)
            self.conn.rollback()

    def run(self):
        if not self.connect_db():
            return
        if not self.ensure_table():
            return

        logger.info(
            "Starting dashboard_metrics updater (interval=%ds)",
            INTERVAL_SECONDS,
        )

        try:
            while True:
                self.insert_metrics_row(self.compute_kpis())
                time.sleep(INTERVAL_SECONDS)
        except KeyboardInterrupt:
            logger.info("Stopping dashboard_metrics updater (Ctrl+C)")
        finally:
            if self.conn:
                self.conn.close()


if __name__ == "__main__":
    DashboardMetricsUpdater().run()
