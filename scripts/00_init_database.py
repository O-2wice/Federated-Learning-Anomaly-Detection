#!/usr/bin/env python3
"""
Database Initialization Script
Creates all required tables in TimescaleDB for the FLEAD pipeline.

Run this BEFORE starting the pipeline to ensure database schema exists.

It uses config_loader.get_db_config(), so it works:
- inside Docker (DB host = 'timescaledb')
- on the host (DB host = 'localhost', mapped port 5432)

Tables created:

  Raw data:
    - iot_data

  Federated learning:
    - local_models
    - federated_models

  Analytics & monitoring:
    - dashboard_metrics
    - batch_analysis_results
    - stream_analysis_results
    - model_evaluations

All time-series tables are converted to TimescaleDB hypertables.
"""

import psycopg2
import logging
import time
import os
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# config_loader integration
# ---------------------------------------------------------------------
import sys
sys.path.insert(0, os.path.dirname(__file__))
from config_loader import get_db_config

db_config = get_db_config()
DB_HOST = db_config["host"]
DB_PORT = db_config["port"]
DB_NAME = db_config["database"]
DB_USER = db_config["user"]
DB_PASSWORD = db_config["password"]

logger.info(
    "Database Config: host=%s, port=%s, database=%s, user=%s",
    DB_HOST, DB_PORT, DB_NAME, DB_USER
)

# ---------------------------------------------------------------------
# SCHEMA – this MUST match the Python code (Spark, Grafana, aggregator)
# ---------------------------------------------------------------------
CREATE_TABLES_SQL = """
-- Clean start (optional – you can comment these out if you don't want drops)
DROP TABLE IF EXISTS iot_data CASCADE;
DROP TABLE IF EXISTS anomalies CASCADE;
DROP TABLE IF EXISTS local_models CASCADE;
DROP TABLE IF EXISTS federated_models CASCADE;
DROP TABLE IF EXISTS dashboard_metrics CASCADE;
DROP TABLE IF EXISTS batch_analysis_results CASCADE;
DROP TABLE IF EXISTS stream_analysis_results CASCADE;
DROP TABLE IF EXISTS model_evaluations CASCADE;
DROP TABLE IF EXISTS local_model_updates CASCADE;

-- ------------------------------------------------------------------
-- RAW IOT DATA
-- ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iot_data (
    ts          TIMESTAMPTZ NOT NULL,
    device_id   TEXT        NOT NULL,
    value       DOUBLE PRECISION,
    label       INT
);

SELECT create_hypertable('iot_data', 'ts', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_iot_data_device_ts
    ON iot_data (device_id, ts DESC);

-- ------------------------------------------------------------------
-- ANOMALIES (RCF-based detection from Flink)
-- ------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS anomalies (
    ts               TIMESTAMPTZ NOT NULL,
    device_id        TEXT        NOT NULL,
    value            DOUBLE PRECISION,
    anomaly_score    DOUBLE PRECISION,      -- RCF score (0-1 scale)
    severity         TEXT,                   -- info, warning, critical
    detection_method TEXT DEFAULT 'random_cut_forest',
    label            INT                     -- ground-truth attack label of the flagged reading
);

SELECT create_hypertable('anomalies', 'ts', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_anomalies_device_ts
    ON anomalies (device_id, ts DESC);

-- ------------------------------------------------------------------
-- LOCAL MODEL UPDATES (raw Kafka events from Flink training)
-- ------------------------------------------------------------------
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
CREATE INDEX IF NOT EXISTS idx_local_model_updates_device_ts
    ON local_model_updates (device_id, ts DESC);

-- ------------------------------------------------------------------
-- FEDERATED LEARNING TABLES
-- ------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS local_models (
    id                BIGSERIAL,              -- no PRIMARY KEY here
    device_id         TEXT            NOT NULL,
    model_version     INT             NOT NULL,
    global_version    INT             NOT NULL,
    accuracy          DOUBLE PRECISION NOT NULL,
    samples_processed INT             NOT NULL,
    created_at        TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS federated_models (
    id                 BIGSERIAL,              -- no PRIMARY KEY here
    global_version     INT             NOT NULL,
    aggregation_round  INT             NOT NULL,
    num_devices        INT             NOT NULL,
    accuracy           DOUBLE PRECISION NOT NULL,  -- mean local TRAINING accuracy (held-out metrics: model_evaluations)
    total_samples      BIGINT,                     -- training samples behind this round
    update_norm        DOUBLE PRECISION,           -- L2 norm of the aggregated parameter update
    mean_update_cosine DOUBLE PRECISION,           -- mean cosine similarity of device updates (1 = full agreement)
    num_clusters       INT,                        -- groups of devices with similar update directions
    dp_noise_std       DOUBLE PRECISION,           -- Gaussian noise std added to the averaged update (0 = DP off)
    dp_clipped_updates INT,                        -- device updates scaled down to the clip norm
    dp_epsilon         DOUBLE PRECISION,           -- cumulative (epsilon, delta=1e-5) privacy budget so far
    created_at         TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

SELECT create_hypertable('local_models', 'created_at',     if_not_exists => TRUE);
SELECT create_hypertable('federated_models', 'created_at', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_local_models_device_created_at
    ON local_models (device_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_federated_models_version_created_at
    ON federated_models (global_version, created_at DESC);

-- ------------------------------------------------------------------
-- DASHBOARD METRICS
-- ------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dashboard_metrics (
    id           BIGSERIAL,                   -- no PK
    metric_name  TEXT            NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    metric_unit  TEXT            NOT NULL,
    device_id    TEXT,
    timestamp    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    -- If you want uniqueness, include the partition column:
    CONSTRAINT dashboard_metrics_unique_name_ts
        UNIQUE (metric_name, timestamp)
);

SELECT create_hypertable('dashboard_metrics', 'timestamp', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_dashboard_metrics_name
    ON dashboard_metrics (metric_name, timestamp);

-- ------------------------------------------------------------------
-- BATCH ANALYSIS RESULTS (Spark batch)
-- ------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS batch_analysis_results (
    id                BIGSERIAL,              -- no PK
    device_id         TEXT            NOT NULL,
    metric_name       TEXT            NOT NULL,
    avg_value         DOUBLE PRECISION,
    min_value         DOUBLE PRECISION,
    max_value         DOUBLE PRECISION,
    stddev_value      DOUBLE PRECISION,
    sample_count      BIGINT,
    analysis_date     DATE            NOT NULL,
    analysis_timestamp TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

SELECT create_hypertable('batch_analysis_results', 'analysis_timestamp', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_batch_results_device_metric_ts
    ON batch_analysis_results (device_id, metric_name, analysis_timestamp DESC);

-- ------------------------------------------------------------------
-- STREAM ANALYSIS RESULTS (Spark streaming)
-- ------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS stream_analysis_results (
    id                 BIGSERIAL,             -- no PK
    device_id          TEXT            NOT NULL,
    metric_name        TEXT            NOT NULL,
    raw_value          DOUBLE PRECISION,
    moving_avg_30s     DOUBLE PRECISION,
    anomaly_score      DOUBLE PRECISION,       -- fleet z-score scaled to 0-1
    is_anomaly         BOOLEAN         NOT NULL DEFAULT FALSE,
    anomaly_confidence DOUBLE PRECISION,
    detection_method   TEXT            DEFAULT 'fleet_zscore',
    timestamp          TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

SELECT create_hypertable('stream_analysis_results', 'timestamp', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_stream_results_device_metric_ts
    ON stream_analysis_results (device_id, metric_name, timestamp DESC);

-- ------------------------------------------------------------------
-- MODEL EVALUATIONS (global model performance)
-- ------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS model_evaluations (
    id                    BIGSERIAL,          -- no PK
    model_version         TEXT            NOT NULL,
    device_id             TEXT,
    model_accuracy        DOUBLE PRECISION,
    prediction_result     TEXT,
    actual_result         TEXT,
    is_correct            BOOLEAN,
    confidence            DOUBLE PRECISION,
    precision             DOUBLE PRECISION,
    recall                DOUBLE PRECISION,
    f1_score              DOUBLE PRECISION,
    sample_count          BIGINT,
    true_positives        BIGINT,
    false_positives       BIGINT,
    false_negatives       BIGINT,
    true_negatives        BIGINT,
    evaluation_timestamp  TIMESTAMPTZ    NOT NULL DEFAULT NOW()
);

SELECT create_hypertable('model_evaluations', 'evaluation_timestamp', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_model_evaluations_version_ts
    ON model_evaluations (model_version, evaluation_timestamp DESC);

-- Every evaluation writes one row per device plus 'ALL'; dashboards, the
-- monitor and the device viewer look rows up by device
CREATE INDEX IF NOT EXISTS idx_model_evaluations_device_ts
    ON model_evaluations (device_id, evaluation_timestamp DESC);
"""



# ---------------------------------------------------------------------
# Helper: wait for DB
# ---------------------------------------------------------------------
def wait_for_database(max_retries: int = 60, retry_interval: int = 2) -> bool:
    """Wait for TimescaleDB to be ready with retries."""
    logger.info("Waiting for TimescaleDB to be ready...")
    logger.info(
        "Connection: host=%s:%s, database=%s, user=%s",
        DB_HOST, DB_PORT, DB_NAME, DB_USER
    )
    logger.info(
        "Max retries: %d (total wait ~%ds)",
        max_retries, max_retries * retry_interval
    )

    for i in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD,
                connect_timeout=10,
            )
            conn.close()
            logger.info("✓ TimescaleDB is accepting connections")
            time.sleep(2)
            return True
        except psycopg2.OperationalError as e:
            attempt = i + 1
            logger.info(
                "  DB not ready yet (%d/%d): %s",
                attempt, max_retries, str(e).splitlines()[0]
            )
            if attempt < max_retries:
                time.sleep(retry_interval)
            else:
                logger.error("✗ Database did not become ready in time")
                return False

    return False


def init_database() -> bool:
    """Initialize database schema."""
    logger.info("=" * 70)
    logger.info("DATABASE INITIALIZATION")
    logger.info("=" * 70)

    if not wait_for_database():
        return False

    try:
        logger.info("Connecting to TimescaleDB...")
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        conn.autocommit = True
        cur = conn.cursor()

        logger.info("✓ Connected. Creating tables & hypertables...")
        cur.execute(CREATE_TABLES_SQL)
        logger.info("✓ Schema created / updated")

        # Show resulting tables
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        tables = [r[0] for r in cur.fetchall()]
        logger.info("Existing tables: %s", ", ".join(tables))

        cur.execute("""
            SELECT hypertable_name
            FROM timescaledb_information.hypertables
            ORDER BY hypertable_name;
        """)
        hts = [r[0] for r in cur.fetchall()]
        logger.info("Hypertables: %s", ", ".join(hts))

        cur.close()
        conn.close()

        logger.info("=" * 70)
        logger.info("✓ DATABASE INITIALIZATION COMPLETE")
        logger.info("=" * 70)
        return True

    except Exception as e:
        logger.error("✗ Database initialization failed: %s", e)
        return False


def main() -> int:
    try:
        ok = init_database()
        if ok:
            logger.info("Database is ready for the FLEAD pipeline.")
            return 0
        logger.error("Database initialization failed.")
        return 1
    except KeyboardInterrupt:
        logger.info("Initialization cancelled by user.")
        return 1
    except Exception as e:
        logger.error("Unexpected error: %s", e)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())