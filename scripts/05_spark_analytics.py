"""
FLEAD Spark Analytics - Batch & Streaming Analysis with Global Model Evaluation

- Batch analysis: Daily aggregations, trends, anomalies (from CSV)
- Stream analysis: Real-time processing from Kafka
- Global model evaluation: Uses federated global model snapshots
- Database storage: All results in TimescaleDB (JSON-friendly schema)
- Visualization: Grafana-ready metrics via dashboard_metrics
"""

import logging
import json
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    avg,
    stddev,
    count,
    min as spark_min,
    max as spark_max,
    lit,
    to_timestamp,
    to_date,
    window as spark_window,
    from_json,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)

import sys
import os

# ---------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# CONFIG LOADER (shared helpers)
# ---------------------------------------------------------------------
sys.path.insert(0, os.path.dirname(__file__))
from config_loader import get_db_config, get_kafka_config, get_stream_config  # noqa: E402
from evaluation_metrics import classification_metrics, score_fleet_windows  # noqa: E402

# ---------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------

# Database config (auto-detect Docker vs host)
DB_CONFIG = get_db_config()

# Kafka (for streaming) – single-broker bootstrap configuration (uses get_kafka_config())
_kafka_conf = get_kafka_config()
_raw_bootstrap = _kafka_conf["bootstrap_servers"]
if isinstance(_raw_bootstrap, str):
    KAFKA_BOOTSTRAP_SERVERS_LIST = [
        s.strip() for s in _raw_bootstrap.split(",") if s.strip()
    ]
else:
    KAFKA_BOOTSTRAP_SERVERS_LIST = list(_raw_bootstrap)

KAFKA_BOOTSTRAP_SERVERS_STR = ",".join(KAFKA_BOOTSTRAP_SERVERS_LIST)
KAFKA_TOPIC = "edge-iiot-stream"

# Spark master (env override; auto-detect host vs container)
def _default_spark_master() -> str:
    if os.path.exists("/.dockerenv"):
        return "spark://spark-master:7077"
    return "spark://localhost:7077"


SPARK_MASTER = os.getenv("SPARK_MASTER", _default_spark_master())
SPARK_PARALLELISM = 4

# Analysis
BATCH_WINDOW_HOURS = 24
# Stream analysis flags a device whose 30-second mean reading deviates from the
# fleet mean in the same window by more than this many fleet standard
# deviations (fleet z-score, see evaluation_metrics.score_fleet_windows).
# Complementary to Flink's RRCF, which compares a device with its own past.
ANOMALY_THRESHOLD_STD = 3.0

# Global model evaluation
EVAL_SAMPLE_FRACTION = 0.05          # Sample of held-out device rows used for evaluation
EVALUATION_INTERVAL_SECONDS = 120    # How often to check for a new global model version
# The producer streams only the first rows of each device file; evaluation uses
# the remaining rows, so the global model is scored on readings no device trained on.
STREAM_CONFIG = get_stream_config()

# Global model location – shared with federated aggregator
MODEL_DIR = Path("/app/models/global")

# ---------------------------------------------------------------------
# Global Model Evaluator
# ---------------------------------------------------------------------
class GlobalModelEvaluator:
    """Loads the latest FedAvg global model published by the aggregator."""

    LATEST_JSON = "global_model_latest.json"

    def __init__(self, base_dir: Path = MODEL_DIR):
        self.base_dir = base_dir
        self.model: Optional[Dict[str, Any]] = None
        self.model_version = None
        self.accuracy = 0.0
        self._load_latest_model()

    def _load_latest_model(self) -> bool:
        """
        Read global_model_latest.json (parameters + metadata). The aggregator
        rewrites it atomically every round. JSON is used instead of the pickle
        snapshots, which depend on the aggregator's class and numpy versions.
        """
        path = self.base_dir / self.LATEST_JSON
        try:
            with open(path, encoding="utf-8") as f:
                model = json.load(f)
        except FileNotFoundError:
            logger.warning("⚠ No global model published yet at %s", path)
            return False
        except (OSError, ValueError) as e:
            logger.error("✗ Error loading global model: %s", e)
            return False

        if not model.get("weights"):
            logger.warning("⚠ Global model at %s has no parameters", path)
            return False

        self.model = model
        self.model_version = model.get("version")
        self.accuracy = float(model.get("accuracy", 0.0))
        logger.info(
            "✓ Loaded global model v%s (%d features, mean local training accuracy %.2f%%)",
            self.model_version,
            len(model["weights"]),
            self.accuracy * 100.0,
        )
        return True


# ---------------------------------------------------------------------
# TimescaleDB Manager (aligned with 00_init_database.py schema)
# ---------------------------------------------------------------------
class TimescaleDBManager:
    """
    Writes Spark results into the tables created by 00_init_database.py:
      - batch_analysis_results  (per device/day statistics of the stream metric)
      - stream_analysis_results (30-second window means with fleet z-scores)
      - model_evaluations       (held-out metrics of each global model version)
      - dashboard_metrics       (KPIs for Grafana)
    """

    def __init__(self):
        self.conn: psycopg2.extensions.connection | None = None
        self._connect()

    def _connect(self) -> None:
        try:
            self.conn = psycopg2.connect(**DB_CONFIG)
            logger.info("✓ Connected to TimescaleDB from Spark Analytics")
        except Exception as e:
            logger.error(f"✗ Connection to TimescaleDB failed: {e}", exc_info=True)
            raise

    # --------------------- Batch Analysis ---------------------
    def insert_batch_results(self, results: List[Dict[str, Any]]) -> None:
        """
        Insert batch analysis results into batch_analysis_results using the
        schema from 00_init_database.py.
        """
        if not results or not self.conn:
            return

        try:
            payload = []
            now_ts = datetime.utcnow()
            for r in results:
                payload.append(
                    (
                        r.get("device_id"),
                        r.get("metric_name", "unknown_metric"),
                        r.get("avg_value"),
                        r.get("min_value"),
                        r.get("max_value"),
                        r.get("stddev_value"),
                        r.get("sample_count"),
                        r.get("analysis_date"),
                        now_ts,
                    )
                )

            with self.conn.cursor() as cur:
                query = """
                    INSERT INTO batch_analysis_results (
                        device_id,
                        metric_name,
                        avg_value,
                        min_value,
                        max_value,
                        stddev_value,
                        sample_count,
                        analysis_date,
                        analysis_timestamp
                    ) VALUES %s
                """
                execute_values(cur, query, payload)
                self.conn.commit()
                logger.info("✓ Inserted %d batch analysis rows", len(payload))
        except Exception as e:
            logger.error(f"✗ Error inserting batch results: {e}", exc_info=True)
            if self.conn:
                self.conn.rollback()

    # --------------------- Stream Analysis ---------------------
    def insert_stream_results(self, results: List[Dict[str, Any]]) -> None:
        """
        Insert stream analysis results into stream_analysis_results table
        (device_id, metric_name, raw_value, moving_avg_30s, anomaly_score,
        is_anomaly, anomaly_confidence, detection_method, timestamp).
        """
        if not results or not self.conn:
            return

        try:
            payload = []
            now_ts = datetime.utcnow()
            for r in results:
                payload.append(
                    (
                        r.get("device_id"),
                        r.get("metric_name", "data_metric"),
                        r.get("raw_value"),
                        r.get("moving_avg_30s"),
                        r.get("anomaly_score"),
                        bool(r.get("is_anomaly")) if r.get("is_anomaly") is not None else False,
                        r.get("anomaly_confidence"),
                        r.get("detection_method", "fleet_zscore"),
                        r.get("window_end") or r.get("window_start") or now_ts,
                    )
                )

            with self.conn.cursor() as cur:
                query = """
                    INSERT INTO stream_analysis_results (
                        device_id,
                        metric_name,
                        raw_value,
                        moving_avg_30s,
                        anomaly_score,
                        is_anomaly,
                        anomaly_confidence,
                        detection_method,
                        timestamp
                    ) VALUES %s
                """
                execute_values(cur, query, payload)
                self.conn.commit()
                logger.info("✓ Inserted %d stream analysis rows", len(payload))
        except Exception as e:
            logger.error(f"✗ Error inserting stream results: {e}", exc_info=True)
            if self.conn:
                self.conn.rollback()

    # --------------------- Model Evaluations ---------------------
    def insert_model_evaluations(self, evaluations: List[Dict[str, Any]]) -> None:
        """
        Insert global-model evaluation metrics into model_evaluations
        (one row per device plus an overall row with device_id 'ALL').
        """
        if not evaluations or not self.conn:
            return

        try:
            payload = [
                (
                    str(e["model_version"]),
                    e["device_id"],
                    e["accuracy"],
                    e["precision"],
                    e["recall"],
                    e["f1_score"],
                    e["sample_count"],
                    e["true_positives"],
                    e["false_positives"],
                    e["false_negatives"],
                    e["true_negatives"],
                )
                for e in evaluations
            ]

            with self.conn.cursor() as cur:
                query = """
                    INSERT INTO model_evaluations (
                        model_version, device_id, model_accuracy,
                        precision, recall, f1_score, sample_count,
                        true_positives, false_positives, false_negatives, true_negatives
                    ) VALUES %s
                """
                execute_values(cur, query, payload)
                self.conn.commit()
                logger.info("✓ Inserted %d model evaluation rows", len(payload))
        except Exception as e:
            logger.error(f"✗ Error inserting model evaluations: {e}", exc_info=True)
            if self.conn:
                self.conn.rollback()

    # --------------------- Dashboard Metrics ---------------------
    def update_dashboard_metric(
        self,
        metric_name: str,
        value: float,
        unit: str = "count",
    ) -> None:
        """
        Insert a dashboard metric row.
        Compatible with existing schema (metric_name, metric_value, metric_unit).
        """
        if not self.conn:
            return

        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO dashboard_metrics (metric_name, metric_value, metric_unit)
                    VALUES (%s, %s, %s)
                    """,
                    (metric_name, value, unit),
                )
                self.conn.commit()
        except Exception as e:
            logger.error(f"Error updating metric {metric_name}: {e}", exc_info=True)
            if self.conn:
                self.conn.rollback()

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None


# ---------------------------------------------------------------------
# Spark Analytics Engine
# ---------------------------------------------------------------------
class SparkAnalyticsEngine:
    """Main analytics engine for batch & streaming."""

    def __init__(self) -> None:
        self.spark = self._create_spark_session()
        self.db = TimescaleDBManager()
        self.model_eval = GlobalModelEvaluator()
        # Overall held-out metrics of the last evaluated global model
        self.last_heldout: Optional[Dict[str, Any]] = None
        # Cached held-out sample used to evaluate every new global model
        self.eval_df: Optional[DataFrame] = None

    def _create_spark_session(self) -> SparkSession:
        """Create Spark session configured for our cluster."""
        session = (
            SparkSession.builder.appName("FLEAD-Analytics")
            .master(SPARK_MASTER)
            .config("spark.sql.shuffle.partitions", str(SPARK_PARALLELISM))
            .config("spark.streaming.kafka.maxRetries", "3")
            .getOrCreate()
        )
        session.sparkContext.setLogLevel("INFO")
        logger.info("✓ Spark session created (master=%s)", SPARK_MASTER)
        return session

    # ===================== BATCH ANALYSIS =====================
    def run_batch_analysis(self, window_hours: int = BATCH_WINDOW_HOURS) -> List[Dict[str, Any]]:
        """
        Batch analysis over the per-device CSVs in /opt/spark/data/processed.

        The device CSVs have no device_id column (the device is the file name)
        and no "temperature" column — the monitored metric is the same one the
        Kafka producer streams (flow_duration, falling back to Rate). The old
        version required device_id + temperature and therefore always skipped.

        Returns the aggregated rows so they can be evaluated with the global model.
        """
        import pyspark.sql.functions as F

        logger.info("🔄 Starting batch analysis on CSV files...")
        csv_path = "/opt/spark/data/processed/device_*.csv"

        try:
            df = (
                self.spark.read.option("header", "true")
                .option("inferSchema", "true")
                # 2,400 files / ~800 MB: infer column types from a sample
                # instead of an extra full pass over the data.
                .option("samplingRatio", "0.1")
                .csv(csv_path)
            )

            if df.rdd.isEmpty():
                logger.warning("⚠ No CSV data found for batch analysis at %s", csv_path)
                return []

            # Same metric precedence as 02_kafka_producer.py
            metric_col = next(
                (c for c in ("flow_duration", "Rate", "tcp.ack", "tcp.seq") if c in df.columns),
                None,
            )
            if metric_col is None or "timestamp" not in df.columns:
                logger.warning(
                    "⚠ CSV data has no known metric column or timestamp column. Skipping batch analysis."
                )
                return []

            df = (
                df.withColumn(
                    "device_id",
                    F.regexp_extract(F.input_file_name(), r"(device_\d+)\.csv$", 1),
                )
                .withColumn("timestamp", to_timestamp(col("timestamp")))
                # Backticks: Edge-IIoTset names contain dots ("tcp.ack"), which
                # Spark would otherwise parse as struct field access.
                .withColumn("metric_value", col(f"`{metric_col}`").cast("double"))
            )

            if "label" in df.columns:
                first_unseen_row = F.lit(STREAM_CONFIG["device_csv_start"]).cast("timestamp") + F.expr(
                    f"INTERVAL {STREAM_CONFIG['stream_rows_per_device']} SECONDS"
                )
                if self.eval_df is not None:
                    self.eval_df.unpersist()
                self.eval_df = (
                    df.filter(col("timestamp") >= first_unseen_row)
                    .sample(fraction=EVAL_SAMPLE_FRACTION, seed=42)
                    .cache()
                )

            daily_agg = (
                df.groupBy(
                    col("device_id"),
                    to_date(col("timestamp")).alias("analysis_date"),
                )
                .agg(
                    avg(col("metric_value")).alias("avg_value"),
                    stddev(col("metric_value")).alias("stddev_value"),
                    spark_min(col("metric_value")).alias("min_value"),
                    spark_max(col("metric_value")).alias("max_value"),
                    count("*").alias("sample_count"),
                )
                .withColumn("metric_name", lit(metric_col))
            )

            rows = daily_agg.collect()
            batch_dicts = [r.asDict() for r in rows]
            logger.info("✓ Batch aggregation completed (%d device-day rows)", len(batch_dicts))
            self.db.insert_batch_results(batch_dicts)
            return batch_dicts

        except Exception as e:
            logger.error(f"✗ Batch analysis error: {e}", exc_info=True)
            return []

    # ===================== STREAM ANALYSIS =====================
    def _write_stream_batch(self, df: DataFrame, epoch_id: int) -> None:
        """
        foreachBatch sink: score each 30-second window against the fleet and
        write the results to TimescaleDB.

        The previous score divided a window's mean by its own standard
        deviation (a signal-to-noise ratio, not a deviation from normal), with
        a fixed "confidence" of 0.95 for every flag.
        """
        try:
            rows = [r.asDict() for r in df.collect()]
            if rows:
                scored = score_fleet_windows(rows, ANOMALY_THRESHOLD_STD)
                if not hasattr(self, "stream_db"):
                    # Separate connection: foreachBatch runs on Spark's streaming
                    # thread while batch analysis uses self.db on the main thread.
                    self.stream_db = TimescaleDBManager()
                self.stream_db.insert_stream_results(scored)
                flagged = sum(1 for s in scored if s["is_anomaly"])
                logger.info("✓ Stream batch %s: wrote %d windows (%d fleet outliers)",
                            epoch_id, len(scored), flagged)
        except Exception as e:
            logger.error(f"Error writing stream batch {epoch_id}: {e}", exc_info=True)

    def run_stream_analysis(self, run_seconds: Optional[int] = None, wait: bool = True):
        """
        Real-time stream analysis from Kafka topic edge-iiot-stream.

        - reads JSON messages with: device_id, timestamp, data
        - computes each device's mean reading per 30-second event-time window
        - scores every window against the fleet (see _write_stream_batch)
        """
        logger.info("🔄 Starting stream analysis from Kafka...")

        try:
            kafka_stream = (
                self.spark.readStream.format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_STR)
                .option("subscribe", KAFKA_TOPIC)
                .option("startingOffsets", "latest")
                .load()
            )

            schema = StructType(
                [
                    StructField("device_id", StringType()),
                    StructField("timestamp", StringType()),
                    StructField("data", DoubleType()),
                ]
            )

            parsed = kafka_stream.select(
                from_json(col("value").cast(StringType()), schema).alias("data")
            ).select("data.*")

            stream_data = parsed.withColumn(
                "event_time", to_timestamp(col("timestamp"))
            ).withWatermark("event_time", "1 minute")

            result = (
                stream_data.groupBy(
                    col("device_id"),
                    spark_window(col("event_time"), "30 seconds").alias("time_window"),
                )
                .agg(
                    avg(col("data")).alias("moving_avg_30s"),
                    count("*").alias("readings"),
                )
                .select(
                    col("device_id"),
                    col("moving_avg_30s"),
                    col("readings"),
                    col("time_window.start").alias("window_start"),
                    col("time_window.end").alias("window_end"),
                )
            )

            query = (
                result.writeStream.outputMode("append")
                .foreachBatch(self._write_stream_batch)
                .option("checkpointLocation", "/tmp/stream_checkpoint")
                # Windows are 30 s long, so a micro-batch every 30 s loses
                # nothing; without a trigger Spark ran micro-batches back to
                # back and used about 1.5 CPU cores
                .trigger(processingTime="30 seconds")
                .start()
            )

            logger.info("✓ Stream analysis query started (foreachBatch sink)")

            if not wait:
                return query

            # If run_seconds is None, keep running until externally stopped.
            if run_seconds is None:
                query.awaitTermination()
            else:
                query.awaitTermination(run_seconds)
                query.stop()
                logger.info("✓ Stream analysis stopped after %ss", run_seconds)

        except Exception as e:
            logger.error(f"✗ Stream analysis error: {e}", exc_info=True)

    # ===================== MODEL EVALUATION =====================
    def evaluate_global_model(self) -> None:
        """
        Score the latest FedAvg global model (logistic regression) on held-out
        device rows and store accuracy, precision, recall and F1 per device and
        overall.

        The previous version never applied the model: it always predicted
        "normal" and stored the model's own accuracy figure as "confidence".
        """
        import pyspark.sql.functions as F

        model = self.model_eval.model
        if not model or self.eval_df is None:
            logger.info("Global model evaluation skipped (model or evaluation data not ready)")
            return

        weights = model["weights"]
        feature_names = [n for n in weights if n in self.eval_df.columns]
        if not feature_names:
            logger.warning("⚠ No global model features found in the evaluation data")
            return

        logger.info(
            "🔍 Evaluating global model v%s on held-out rows (%d features)...",
            model.get("version"),
            len(feature_names),
        )

        logit = F.lit(float(model.get("bias", 0.0)))
        for name in feature_names:
            logit = logit + F.lit(float(weights[name])) * F.coalesce(
                F.col(f"`{name}`").cast("double"), F.lit(0.0)
            )

        scored = self.eval_df.select(
            "device_id",
            F.col("label").cast("int").alias("actual"),
            F.when(logit > 0, 1).otherwise(0).alias("predicted"),
        )

        def _count(condition):
            return F.sum(F.when(condition, 1).otherwise(0))

        per_device = scored.groupBy("device_id").agg(
            F.count("*").alias("n"),
            _count((col("predicted") == 1) & (col("actual") == 1)).alias("tp"),
            _count((col("predicted") == 1) & (col("actual") == 0)).alias("fp"),
            _count((col("predicted") == 0) & (col("actual") == 1)).alias("fn"),
        ).collect()

        def _row(device_id, n, tp, fp, fn):
            metrics = classification_metrics(tp, fp, fn, n)
            metrics.update({"model_version": model.get("version"), "device_id": device_id})
            return metrics

        evaluations = [_row(r["device_id"], r["n"], r["tp"], r["fp"], r["fn"]) for r in per_device]
        totals = {k: sum(r[k] for r in per_device) for k in ("n", "tp", "fp", "fn")}
        overall = _row("ALL", totals["n"], totals["tp"], totals["fp"], totals["fn"])
        evaluations.append(overall)

        self.db.insert_model_evaluations(evaluations)
        self.last_heldout = overall
        logger.info(
            "✓ Global model v%s on %d held-out rows: accuracy=%.2f%% precision=%.2f%% recall=%.2f%% F1=%.3f",
            model.get("version"),
            overall["sample_count"],
            overall["accuracy"] * 100.0,
            overall["precision"] * 100.0,
            overall["recall"] * 100.0,
            overall["f1_score"],
        )

    # ===================== DASHBOARD METRICS =====================
    def update_dashboard_metrics(self, batch_rows: Optional[int] = None) -> None:
        """Push Spark's own KPIs into dashboard_metrics for Grafana.

        Only measured values are written: the number of device-day rows from
        the batch pass (when given) and the held-out accuracy and F1 of the
        last evaluated global model. The mean local training accuracy is not
        written here because it does not measure the global model.
        """
        try:
            if batch_rows is not None:
                self.db.update_dashboard_metric("spark_batch_device_day_rows", float(batch_rows))
            if self.last_heldout is not None:
                self.db.update_dashboard_metric(
                    "global_model_heldout_accuracy", float(self.last_heldout["accuracy"]), "ratio")
                self.db.update_dashboard_metric(
                    "global_model_heldout_f1", float(self.last_heldout["f1_score"]), "ratio")
            logger.info("✓ Dashboard metrics updated")
        except Exception as e:
            logger.error(f"Error updating dashboard metrics: {e}", exc_info=True)

    # ===================== FULL PIPELINE =====================
    def run_full_pipeline(self) -> None:
        logger.info("=" * 70)
        logger.info("FLEAD SPARK ANALYTICS - FULL PIPELINE")
        logger.info("=" * 70)

        try:
            # 1) Start stream analysis first so live results appear right away;
            #    the batch pass over ~800 MB of device CSVs takes several minutes.
            stream_query = self.run_stream_analysis(wait=False)

            # 2) Batch analysis (from CSV); also prepares the evaluation sample
            batch_results = self.run_batch_analysis()
            self.update_dashboard_metrics(batch_rows=len(batch_results))

            # 3) While streaming runs, evaluate each new global model version
            evaluated_version = None
            while stream_query is not None and stream_query.isActive:
                if (
                    self.model_eval._load_latest_model()
                    and self.model_eval.model_version != evaluated_version
                ):
                    self.evaluate_global_model()
                    evaluated_version = self.model_eval.model_version
                    self.update_dashboard_metrics()
                stream_query.awaitTermination(EVALUATION_INTERVAL_SECONDS)

            logger.info("=" * 70)
            logger.info("✓ Spark analytics pipeline completed successfully")
            logger.info("=" * 70)
        except Exception as e:
            logger.error(f"Pipeline error: {e}", exc_info=True)
        finally:
            self.db.close()


# ---------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------
def main() -> None:
    logger.info("Starting FLEAD Spark Analytics Engine...")
    try:
        engine = SparkAnalyticsEngine()
        engine.run_full_pipeline()
    except Exception as e:
        logger.error(f"Fatal error in Spark Analytics Engine: {e}", exc_info=True)
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
