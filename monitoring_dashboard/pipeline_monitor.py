#!/usr/bin/env python3
"""
FLEAD Pipeline Live Monitor
Real-time visualization of the entire federated learning pipeline
Kafka → Flink → Federated Aggregation → TimescaleDB → Grafana
"""

from flask import Flask, render_template, jsonify
import psycopg2
import subprocess
import shutil
from datetime import datetime, timezone
import json
import math
import os
import socket
import logging
import threading
import time

# --------------------------------------------------------------------
# Logging
# --------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logging.getLogger("kafka").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# --------------------------------------------------------------------
# Flask app – in Docker we expect /app as WORKDIR and templates in /app/templates
# --------------------------------------------------------------------
app = Flask(
    __name__,
    template_folder="/app/templates",
    static_folder="/app/static",
)

# --------------------------------------------------------------------
# Database configuration – uses env vars from docker-compose
# --------------------------------------------------------------------
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "timescaledb"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "flead"),
    "user": os.getenv("DB_USER", "flead"),
    "password": os.getenv("DB_PASSWORD", "password"),
}

# Kafka bootstrap – same env as other services
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka-broker-1:9092",
)

# --------------------------------------------------------------------
# Service definitions
# --------------------------------------------------------------------
TCP_SERVICES = {
    # Core Kafka broker (single)
    "kafka-broker-1": ("kafka-broker-1", 9092),

    # Storage
    "timescaledb": ("timescaledb", 5432),

    # Stream / batch engines
    "flink-jobmanager": ("flink-jobmanager", 8081),
    "spark-master": ("spark-master", 7077),

    # Visualization / UIs
    "grafana": ("grafana", 3000),
    "kafka-ui": ("kafka-ui", 8080),
    "device-viewer": ("device-viewer", 5000),
    "monitoring-dashboard": ("monitoring-dashboard", 5000),  # this app
}

# Logical Python services (conceptual; we infer state from DB)
LOGICAL_COMPONENTS = [
    "timescaledb-collector",
    "federated-aggregator",
    "spark-analytics",
]

# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------
# Failed TimescaleDB connection attempts since start (Prometheus counter)
DB_CONNECTION_ERRORS = 0


def get_db_connection():
    """Return TimescaleDB connection or None."""
    global DB_CONNECTION_ERRORS
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        DB_CONNECTION_ERRORS += 1
        logger.error(f"DB connection failed: {e}")
        return None


def _tcp_check(host: str, port: int, timeout: float = 2.0) -> bool:
    """Simple TCP connection check – works inside Docker without docker CLI."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def check_service_tcp(service_name: str) -> str:
    """
    Check if a Docker service is reachable over TCP.
    Returns: 'running', 'stopped', or 'unknown'
    """
    endpoint = TCP_SERVICES.get(service_name)
    if endpoint is None:
        return "unknown"

    host, port = endpoint
    return "running" if _tcp_check(host, port) else "stopped"


def get_kafka_message_count(topic: str) -> int:
    """
    Get Kafka topic message count.

    Priority:
      1. kafka-python directly (inside container)
      2. docker exec + GetOffsetShell (host mode)

    On error: returns 0.
    """
    # Try kafka-python first
    try:
        from kafka import KafkaConsumer, TopicPartition  # type: ignore
    except ImportError:
        KafkaConsumer = None  # type: ignore
        TopicPartition = None  # type: ignore

    if KafkaConsumer is not None and TopicPartition is not None:
        try:
            bootstrap = [
                b.strip()
                for b in KAFKA_BOOTSTRAP_SERVERS.split(",")
                if b.strip()
            ]
            consumer = KafkaConsumer(
                bootstrap_servers=bootstrap,
                enable_auto_commit=False,
                group_id=None,
                consumer_timeout_ms=1000,
            )
            partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                consumer.close()
                return 0

            total = 0
            for p in partitions:
                tp = TopicPartition(topic, p)
                consumer.assign([tp])
                consumer.seek_to_end(tp)
                end = consumer.position(tp)
                total += end

            consumer.close()
            return total
        except Exception as e:
            logger.warning(
                f"Kafka (kafka-python) message count failed for {topic}: {e}"
            )

    # Fallback: docker exec (host only)
    try:
        result = subprocess.run(
            [
                "docker",
                "exec",
                "kafka-broker-1",
                "kafka-run-class",
                "kafka.tools.GetOffsetShell",
                "--broker-list",
                KAFKA_BOOTSTRAP_SERVERS,
                "--topic",
                topic,
                "--time",
                "-1",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr)

        total = 0
        for line in result.stdout.strip().split("\n"):
            if ":" in line:
                total += int(line.split(":")[-1])
        return total
    except Exception as e:
        logger.warning(
            f"Kafka message count (docker exec) failed for {topic}: {e}"
        )
        return 0


def get_flink_jobs():
    """
    Get Flink job status.

    Priority:
      1. Flink REST API (works from inside Docker network)
      2. docker exec + `flink list` (host mode fallback)
    """
    # --- Try REST API first (preferred in Docker) ---
    try:
        import requests  # type: ignore

        resp = requests.get(
            "http://flink-jobmanager:8081/jobs/overview",
            timeout=5,
        )
        resp.raise_for_status()
        data = resp.json()
        jobs = []
        for job in data.get("jobs", []):
            jobs.append(
                {
                    "status": job.get("state", "UNKNOWN"),
                    "name": job.get("name", "Unknown"),
                }
            )
        return jobs
    except Exception as e:
        logger.info(
            f"Flink REST query failed or unavailable, "
            f"falling back to docker exec: {e}"
        )

    # --- Fallback: docker exec (host) ---
    try:
        result = subprocess.run(
            ["docker", "exec", "flink-jobmanager", "flink", "list"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr)

        jobs = []
        for line in result.stdout.split("\n"):
            if "RUNNING" in line or "FINISHED" in line:
                jobs.append(
                    {
                        "status": "RUNNING" if "RUNNING" in line else "FINISHED",
                        "name": line.split(":", 1)[-1].strip()
                        if ":" in line
                        else "Unknown",
                    }
                )
        return jobs
    except Exception as e:
        logger.info(f"Flink job query (docker exec) skipped/failed: {e}")
        return []


def _empty_db_stats():
    """Default DB stats structure so UI never sees None."""
    return {
        "local_models": {
            "total": 0,
            "last_minute": 0,
            "last_5min": 0,
            "latest": None,
        },
        "federated_models": {
            "total": 0,
            "last_minute": 0,
            "last_5min": 0,
            "latest": None,
        },
    }


def get_database_stats():
    """Get database statistics from TimescaleDB (or safe zeros on error)."""
    conn = get_db_connection()
    if not conn:
        return _empty_db_stats()

    try:
        with conn.cursor() as cur:
            # The page refreshes every 2 s and local_models grows by thousands
            # of rows per minute: its total comes from the dashboard-metrics-updater
            # snapshot (a full count only when no snapshot is under 2 minutes old),
            # and the windowed counts use the hypertable's time index.
            cur.execute(
                """
                SELECT
                    'local_models' AS table_name,
                    COALESCE(
                        (SELECT metric_value::bigint FROM dashboard_metrics
                          WHERE metric_name = 'total_local_models_count'
                            AND timestamp > NOW() - INTERVAL '2 minutes'
                          ORDER BY timestamp DESC LIMIT 1),
                        (SELECT COUNT(*) FROM local_models)
                    ) AS total_records,
                    (SELECT COUNT(*) FROM local_models
                      WHERE created_at > NOW() - INTERVAL '1 minute') AS last_minute,
                    (SELECT COUNT(*) FROM local_models
                      WHERE created_at > NOW() - INTERVAL '5 minutes') AS last_5min,
                    (SELECT MAX(created_at) FROM local_models) AS latest_record
                UNION ALL
                SELECT
                    'federated_models',
                    COUNT(*),
                    COUNT(*) FILTER (
                        WHERE created_at > NOW() - INTERVAL '1 minute'
                    ),
                    COUNT(*) FILTER (
                        WHERE created_at > NOW() - INTERVAL '5 minutes'
                    ),
                    MAX(created_at)
                FROM federated_models;
                """
            )

            rows = cur.fetchall()
            stats = _empty_db_stats()
            for row in rows:
                name = row[0]
                stats[name] = {
                    "total": row[1],
                    "last_minute": row[2],
                    "last_5min": row[3],
                    "latest": row[4].isoformat() if row[4] else None,
                }
            return stats
    except Exception as e:
        logger.error(f"Database stats query failed: {e}")
        return _empty_db_stats()
    finally:
        conn.close()

def get_recent_models():
    """Get recent local model training activity."""
    conn = get_db_connection()
    if not conn:
        return []

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 
                    device_id,
                    model_version,
                    global_version,
                    accuracy,
                    samples_processed,
                    created_at
                FROM local_models
                ORDER BY created_at DESC
                LIMIT 10;
                """
            )

            models = []
            for row in cur.fetchall():
                models.append(
                    {
                        "device_id": row[0],
                        "model_version": row[1],
                        "global_version": row[2],
                        "accuracy": float(row[3]),
                        "samples": row[4],
                        "timestamp": row[5].isoformat(),
                    }
                )
            return models
    except Exception as e:
        logger.error(f"Recent models query failed: {e}")
        return []
    finally:
        conn.close()


def get_python_processes_host():
    """
    (Host-only) Get running Python pipeline processes on the Windows host.

    Inside the Docker container this will just return [] because
    PowerShell won't be available.
    """
    try:
        # Check if powershell is available before trying to run it
        if shutil.which("powershell") is None:
            return []

        result = subprocess.run(
            [
                "powershell",
                "-Command",
                "Get-Process python | "
                "Select-Object Id, @{Name='CommandLine';Expression={"
                "(Get-WmiObject Win32_Process -Filter \"ProcessId=$($_.Id)\").CommandLine"
                "}} | Where-Object { $_.CommandLine -like '*scripts*' } | ConvertTo-Json",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if not result.stdout.strip():
            return []

        processes_raw = json.loads(result.stdout)
        if isinstance(processes_raw, dict):
            processes_raw = [processes_raw]

        processes = []
        for proc in processes_raw:
            cmd = proc.get("CommandLine", "") or ""
            if "kafka_producer" in cmd:
                processes.append(
                    {"name": "Kafka Producer", "pid": proc["Id"], "status": "running"}
                )
            elif "federated_aggregation" in cmd:
                processes.append(
                    {
                        "name": "Federated Aggregation",
                        "pid": proc["Id"],
                        "status": "running",
                    }
                )
            elif "spark_analytics" in cmd:
                processes.append(
                    {"name": "Spark Analytics", "pid": proc["Id"], "status": "running"}
                )
        return processes
    except Exception as e:
        logger.info(
            f"Process check skipped/failed (likely inside Docker): {e}"
        )
        return []


# --------------------------------------------------------------------
# Flask routes
# --------------------------------------------------------------------
@app.route("/")
def index():
    """Main dashboard page."""
    return render_template("pipeline_monitor.html")


@app.route("/api/status")
def get_status():
    """Get complete pipeline status (JSON for the dashboard)."""

    # 1) Container-level service health via TCP
    docker_services = {
        name: check_service_tcp(name) for name in TCP_SERVICES.keys()
    }

    # 2) Database-driven logical health (collector / aggregator / analytics)
    db_stats = get_database_stats()

    # Timescaledb collector: running if it wrote readings in the last 2 minutes
    # (an EXISTS on the time index rather than a count of the whole table)
    collector_writing = False
    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT EXISTS (SELECT 1 FROM iot_data WHERE ts > NOW() - INTERVAL '2 minutes')")
                collector_writing = bool(cur.fetchone()[0])
        except Exception as e:
            logger.info(f"iot_data freshness check failed: {e}")
        finally:
            conn.close()

    docker_services["timescaledb-collector"] = "running" if collector_writing else "unknown"

    # Federated aggregator: look at federated_models table
    if db_stats["federated_models"]["total"] > 0:
        docker_services["federated-aggregator"] = "running"
    else:
        docker_services["federated-aggregator"] = "unknown"
        
    # Spark analytics: treat as running whenever spark-master is up
    spark_status = "running" if docker_services.get("spark-master") == "running" else "unknown"
    docker_services["spark-analytics"] = spark_status

    # 3) Kafka topic stats
    kafka_stats = {
        "edge-iiot-stream": get_kafka_message_count("edge-iiot-stream"),
        "local-model-updates": get_kafka_message_count("local-model-updates"),
        "global-model-updates": get_kafka_message_count("global-model-updates"),
    }

    # 4) Flink jobs
    flink_jobs = get_flink_jobs()

    # 5) Recent model activity
    recent_models = get_recent_models()

    # 6) Python “services”
    host_python = get_python_processes_host()
    if host_python:
        python_processes = host_python
    else:
        python_processes = []
        for comp_name, display_name in [
            ("timescaledb-collector", "TimescaleDB Collector"),
            ("federated-aggregator", "Federated Aggregator"),
            ("spark-analytics", "Spark Analytics"),
        ]:
            if docker_services.get(comp_name) == "running":
                python_processes.append(
                    {"name": display_name, "pid": 0, "status": "running"}
                )

    # ----------------- HEALTH LOGIC -----------------
    has_kafka_flow = kafka_stats["edge-iiot-stream"] > 0
    has_local_models = db_stats["local_models"]["total"] > 0
    has_global_models = db_stats["federated_models"]["total"] > 0
    collector_ok = docker_services.get("timescaledb-collector") == "running"
    aggregator_ok = docker_services.get("federated-aggregator") == "running"

    # Spark analytics is nice-to-have, not required for "healthy"
    analytics_ok = spark_status == "running"

    core_signals = [
        has_kafka_flow,
        has_local_models,
        has_global_models,
        collector_ok,
        aggregator_ok,
    ]

    if all(core_signals):
        pipeline_health = "healthy"
    elif any(core_signals):
        pipeline_health = "degraded"
    else:
        pipeline_health = "unknown"
    # ---------------------------------------------------

    return jsonify(
        {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "docker_services": docker_services,
            "kafka": kafka_stats,
            "flink": flink_jobs,
            "database": db_stats,
            "recent_models": recent_models,
            "python_processes": python_processes,
            "pipeline_health": pipeline_health,
        }
    )


@app.route("/api/health")
def health_check():
    """Quick health check endpoint."""
    return jsonify({"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()})


# --------------------------------------------------------------------
# Prometheus Metrics Endpoint
# --------------------------------------------------------------------
# Fleet-wide aggregates over the large tables come from the latest snapshot
# that dashboard_metrics_updater.py writes every 15 s, so a scrape (every 10 s)
# never re-scans iot_data or local_models.
# (name, type, help, dashboard_metrics.metric_name)
SNAPSHOT_METRICS = [
    ("flead_iot_records_total", "counter",
     "IoT readings stored in TimescaleDB", "total_iot_count"),
    ("flead_local_models_total", "counter",
     "Local model updates received by the federated aggregator", "total_local_models_count"),
    ("flead_anomalies_total", "counter",
     "Readings flagged by the RRCF anomaly detector", "total_anomalies_count"),
    ("flead_anomaly_rate", "gauge",
     "Share of readings in the last hour that were flagged as anomalies", "anomaly_rate_1h"),
    ("flead_anomaly_attack_share", "gauge",
     "Share of anomalies flagged in the last hour whose reading is labelled as an attack",
     "anomaly_attack_share_1h"),
    ("flead_active_devices", "gauge",
     "Devices that sent readings in the last 5 minutes", "active_devices_5m"),
    ("flead_stale_devices_count", "gauge",
     "Devices with local models but no readings in the last hour", "stale_devices_count"),
    ("flead_devices_with_models", "gauge",
     "Devices that have produced at least one local model", "devices_with_models"),
]
SNAPSHOT_SQL = (
    "SELECT DISTINCT ON (metric_name) metric_name, metric_value, "
    "EXTRACT(EPOCH FROM NOW() - timestamp) "
    "FROM dashboard_metrics WHERE timestamp > NOW() - INTERVAL '10 minutes' "
    "ORDER BY metric_name, timestamp DESC"
)

# Direct queries: small tables, or LIMIT 1 / short time ranges on a hypertable index
# (name, type, help, SQL returning a single number)
PROMETHEUS_METRICS = [
    ("flead_federated_models_total", "counter",
     "Global model versions produced by federated averaging",
     "SELECT COUNT(*) FROM federated_models"),
    ("flead_global_model_train_accuracy", "gauge",
     "Mean local training accuracy of the devices in the latest aggregation round",
     "SELECT accuracy FROM federated_models ORDER BY created_at DESC LIMIT 1"),
    ("flead_global_model_heldout_accuracy", "gauge",
     "Latest global model accuracy on held-out readings no device trained on",
     "SELECT model_accuracy FROM model_evaluations WHERE device_id = 'ALL' "
     "ORDER BY evaluation_timestamp DESC LIMIT 1"),
    ("flead_global_model_heldout_f1", "gauge",
     "Latest global model F1 score on held-out readings",
     "SELECT f1_score FROM model_evaluations WHERE device_id = 'ALL' "
     "ORDER BY evaluation_timestamp DESC LIMIT 1"),
    ("flead_global_model_heldout_baseline_accuracy", "gauge",
     "Accuracy of always predicting benign on the same held-out readings",
     "SELECT (true_negatives + false_positives)::float / NULLIF(sample_count, 0) "
     "FROM model_evaluations WHERE device_id = 'ALL' ORDER BY evaluation_timestamp DESC LIMIT 1"),
    ("flead_models_per_minute", "gauge",
     "Local model updates per minute over the last 5 minutes",
     "SELECT COUNT(*) / 5.0 FROM local_models WHERE created_at > NOW() - INTERVAL '5 minutes'"),
    ("flead_dp_epsilon", "gauge",
     "Cumulative differential-privacy budget epsilon (delta=1e-5) of the latest global model",
     "SELECT dp_epsilon FROM federated_models ORDER BY created_at DESC LIMIT 1"),
]


@app.route("/metrics")
def prometheus_metrics():
    """
    Expose FLEAD pipeline metrics in Prometheus text format.

    Every query runs independently, so one failure does not drop the rest,
    and each metric carries its own HELP/TYPE lines. Metrics with no value
    yet (no evaluation has run, or no recent snapshot) are omitted.
    """
    lines = []

    def emit(name, metric_type, help_text, value):
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} {metric_type}")
        if value is not None:
            lines.append(f"{name} {float(value)}")

    def query(sql, fetch_all=False):
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                return cur.fetchall() if fetch_all else cur.fetchone()
        except Exception as e:
            conn.rollback()
            logger.warning(f"Metrics query failed ({sql[:60]}...): {e}")
            return [] if fetch_all else None

    conn = get_db_connection()
    emit("flead_db_up", "gauge", "1 if the monitor can connect to TimescaleDB", 1 if conn else 0)
    if conn:
        try:
            snapshot = {name: (value, age) for name, value, age in query(SNAPSHOT_SQL, fetch_all=True)}
            for name, metric_type, help_text, key in SNAPSHOT_METRICS:
                emit(name, metric_type, help_text, snapshot.get(key, (None, None))[0])
            ages = [age for _, age in snapshot.values()]
            emit("flead_metrics_snapshot_age_seconds", "gauge",
                 "Age of the newest dashboard_metrics snapshot written by dashboard-metrics-updater",
                 min(ages) if ages else None)

            for name, metric_type, help_text, sql in PROMETHEUS_METRICS:
                row = query(sql)
                emit(name, metric_type, help_text, row[0] if row else None)
        finally:
            conn.close()

    emit("flead_db_connection_errors_total", "counter",
         "Failed TimescaleDB connection attempts since the monitor started", DB_CONNECTION_ERRORS)
    lines.append("")
    return "\n".join(lines), 200, {"Content-Type": "text/plain; version=0.0.4; charset=utf-8"}


# --------------------------------------------------------------------
# Overview API (used by the monitor page)
# --------------------------------------------------------------------
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://prometheus:9090")
EPSILON_LIMIT = 50.0            # PrivacyBudgetHigh threshold in prometheus/alerts.yml
LAG_LIMIT = 9000                # readings: one minute of the 150/s stream (FlinkFallingBehind)
OVERVIEW_CACHE_SECONDS = 3.0    # open pages share one computation

# Web interfaces as published on the host by docker-compose.yml
PORTALS = [
    ("Grafana", "http://localhost:3001", "Dashboards"),
    ("Prometheus", "http://localhost:9090/alerts", "Metrics and alert rules"),
    ("Alertmanager", "http://localhost:9093", "Routed alerts"),
    ("Flink", "http://localhost:8161", "Streaming job"),
    ("Spark", "http://localhost:8086", "Analytics cluster"),
    ("Spark job", "http://localhost:4040", "Running analytics job"),
    ("Kafka UI", "http://localhost:8081", "Topics and consumer lag"),
    ("Device Viewer", "http://localhost:8082", "Per-device data"),
    ("Jupyter", "http://localhost:8888", "Notebooks"),
]

# (stage, SQL returning the age in seconds of the newest row, age limit in seconds).
# Every query is bounded to one day so TimescaleDB only reads recent chunks.
FRESHNESS_CHECKS = [
    ("Readings stored",
     "SELECT EXTRACT(EPOCH FROM NOW() - MAX(ts)) FROM iot_data WHERE ts > NOW() - INTERVAL '1 day'", 60),
    # Anomalies carry the reading's timestamp, so this age includes Flink's lag
    ("Anomalies scored",
     "SELECT EXTRACT(EPOCH FROM NOW() - MAX(ts)) FROM anomalies WHERE ts > NOW() - INTERVAL '1 day'", 300),
    ("Local models trained",
     "SELECT EXTRACT(EPOCH FROM NOW() - MAX(created_at)) FROM local_models "
     "WHERE created_at > NOW() - INTERVAL '1 day'", 600),
    ("Federated rounds",
     "SELECT EXTRACT(EPOCH FROM NOW() - MAX(created_at)) FROM federated_models", 300),
    ("Held-out evaluations",
     "SELECT EXTRACT(EPOCH FROM NOW() - MAX(evaluation_timestamp)) FROM model_evaluations "
     "WHERE device_id = 'ALL' AND evaluation_timestamp > NOW() - INTERVAL '1 day'", 600),
    ("Fleet z-score windows",
     "SELECT EXTRACT(EPOCH FROM NOW() - MAX(timestamp)) FROM stream_analysis_results "
     "WHERE timestamp > NOW() - INTERVAL '1 day'", 300),
]

_overview_cache = {"at": 0.0, "data": None}
_overview_lock = threading.Lock()


def _num(value):
    """JSON-safe float: Decimal/numpy become float, NaN and infinities become None."""
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _prometheus_value(expr):
    """First sample of an instant Prometheus query, or None."""
    try:
        import requests  # type: ignore

        resp = requests.get(f"{PROMETHEUS_URL}/api/v1/query", params={"query": expr}, timeout=3)
        result = resp.json()["data"]["result"]
        return _num(result[0]["value"][1]) if result else None
    except Exception as e:
        logger.info(f"Prometheus query failed ({expr}): {e}")
        return None


def _prometheus_alerts():
    """(pending and firing alerts, whether Prometheus answered)."""
    try:
        import requests  # type: ignore

        resp = requests.get(f"{PROMETHEUS_URL}/api/v1/alerts", timeout=3)
        alerts = resp.json()["data"]["alerts"]
    except Exception as e:
        logger.info(f"Prometheus alerts unavailable: {e}")
        return [], False
    return [
        {
            "name": a.get("labels", {}).get("alertname"),
            "severity": a.get("labels", {}).get("severity", "info"),
            "state": a.get("state"),
            "summary": a.get("annotations", {}).get("summary", ""),
            "active_at": a.get("activeAt"),
        }
        for a in alerts
    ], True


def _rates(rows):
    """Per-interval rates from the metrics updater's cumulative snapshots."""
    series = {}
    for name, ts, value in rows:
        series.setdefault(name, []).append((ts, float(value)))

    def per(name, scale):
        # Snapshots are ~15 s apart but the collector inserts in batches, so
        # rates are taken over at least 60 s to avoid a saw-tooth
        points = series.get(name, [])
        out = []
        start = 0
        for i in range(1, len(points)):
            (t0, v0), (t1, v1) = points[start], points[i]
            if v1 < v0:  # a restart resets the totals
                start = i
            elif (t1 - t0).total_seconds() >= 60:
                out.append([t1.isoformat(), round((v1 - v0) / (t1 - t0).total_seconds() * scale, 2)])
                start = i
        return out

    return {
        "readings_per_s": per("total_iot_count", 1),
        "anomalies_per_min": per("total_anomalies_count", 60),
        "local_models_per_min": per("total_local_models_count", 60),
    }


def _overall_status(overview):
    critical, degraded = [], []
    if not overview.get("db_up"):
        critical.append("TimescaleDB is unreachable")

    flink = overview.get("flink") or {}
    if not any((job.get("status") or "").upper() == "RUNNING" for job in flink.get("jobs") or []):
        critical.append("The Flink training job is not running")

    for item in overview.get("freshness") or []:
        age, limit = item["age_s"], item["limit_s"]
        if age is None or age <= limit:
            continue  # no rows yet (starting up) or fresh
        message = f"{item['stage']}: newest is {age / 60:.0f} min old"
        (critical if item["stage"] == "Readings stored" else degraded).append(message)

    lag = flink.get("lag")
    if lag is not None and lag > LAG_LIMIT:
        degraded.append(f"Flink is {lag:,.0f} readings behind the stream")

    alerts = overview.get("alerts") or {}
    firing = [a for a in alerts.get("active") or [] if a.get("state") == "firing"]
    if firing:
        target = critical if any(a.get("severity") == "critical" for a in firing) else degraded
        target.append(f"{len(firing)} alert(s) firing: " + ", ".join(a["name"] for a in firing[:3]))
    if not alerts.get("prometheus_up"):
        degraded.append("Prometheus is unreachable")

    level = "critical" if critical else "degraded" if degraded else "healthy"
    return {"level": level, "reasons": critical + degraded}


def build_overview():
    """Everything the monitor page shows, from bounded queries only."""
    overview = {"generated_at": datetime.now(timezone.utc).isoformat(), "db_up": False, "freshness": [],
                "heldout_history": [], "rounds": [], "snapshot": {}, "rates": _rates([])}

    conn = get_db_connection()
    if conn:
        try:
            with conn.cursor() as cur:
                overview["db_up"] = True
                for stage, sql, limit in FRESHNESS_CHECKS:
                    try:
                        cur.execute(sql)
                        age = _num(cur.fetchone()[0])
                    except Exception as e:
                        conn.rollback()
                        logger.info(f"Freshness query failed for {stage}: {e}")
                        age = None
                    overview["freshness"].append({"stage": stage, "age_s": age, "limit_s": limit})

                cur.execute(
                    "SELECT model_version, model_accuracy, f1_score, precision, recall, "
                    "(true_negatives + false_positives)::float / NULLIF(sample_count, 0), "
                    "sample_count, evaluation_timestamp FROM model_evaluations "
                    "WHERE device_id = 'ALL' ORDER BY evaluation_timestamp DESC LIMIT 60"
                )
                overview["heldout_history"] = [
                    {"version": str(version), "accuracy": _num(acc), "f1": _num(f1),
                     "precision": _num(prec), "recall": _num(rec), "baseline": _num(base),
                     "samples": samples, "at": at.isoformat()}
                    for version, acc, f1, prec, rec, base, samples, at in reversed(cur.fetchall())
                ]

                cur.execute(
                    "SELECT global_version, num_devices, total_samples, accuracy, mean_update_cosine, "
                    "num_clusters, dp_noise_std, dp_clipped_updates, dp_epsilon, created_at "
                    "FROM federated_models ORDER BY created_at DESC LIMIT 12"
                )
                overview["rounds"] = [
                    {"version": version, "devices": devices, "samples": samples,
                     "train_accuracy": _num(train_acc), "agreement": _num(cosine), "clusters": clusters,
                     "dp_noise_std": _num(noise), "dp_clipped": clipped, "epsilon": _num(epsilon),
                     "rollback": devices == 0, "at": at.isoformat()}
                    for version, devices, samples, train_acc, cosine, clusters, noise, clipped, epsilon, at
                    in cur.fetchall()
                ]

                cur.execute(SNAPSHOT_SQL)
                overview["snapshot"] = {name: _num(value) for name, value, _age in cur.fetchall()}

                cur.execute(
                    "SELECT metric_name, timestamp, metric_value FROM dashboard_metrics "
                    "WHERE metric_name IN ('total_iot_count', 'total_anomalies_count', 'total_local_models_count') "
                    "AND timestamp > NOW() - INTERVAL '30 minutes' ORDER BY metric_name, timestamp"
                )
                overview["rates"] = _rates(cur.fetchall())
        except Exception as e:
            conn.rollback()
            logger.error(f"Overview query failed: {e}")
        finally:
            conn.close()

    overview["flink"] = {
        "jobs": get_flink_jobs(),
        # Readings pulled from Kafka per second; under backpressure this is the processing rate
        "consumed_per_s": _prometheus_value(
            "sum(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_consumed_rate)"),
        "lag": _prometheus_value(
            "max(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lag_max)"),
        "backpressured": _prometheus_value("max(flink_taskmanager_job_task_isBackPressured)"),
    }

    active, prometheus_up = _prometheus_alerts()
    overview["alerts"] = {"prometheus_up": prometheus_up, "active": active,
                          "received": list(reversed(RECEIVED_ALERTS[-10:]))}
    overview["services"] = {
        name: "running" if _tcp_check(host, port, timeout=0.5) else "stopped"
        for name, (host, port) in TCP_SERVICES.items()
    }
    overview["portals"] = [{"name": n, "url": u, "purpose": p} for n, u, p in PORTALS]
    overview["limits"] = {"epsilon": EPSILON_LIMIT, "lag": LAG_LIMIT}
    overview["status"] = _overall_status(overview)
    return overview


@app.route("/api/overview")
def api_overview():
    with _overview_lock:
        if _overview_cache["data"] is None or time.time() - _overview_cache["at"] > OVERVIEW_CACHE_SECONDS:
            _overview_cache["data"] = build_overview()
            _overview_cache["at"] = time.time()
        return jsonify(_overview_cache["data"])


# --------------------------------------------------------------------
# Alert Receiver Endpoint (for Alertmanager webhooks)
# --------------------------------------------------------------------
RECEIVED_ALERTS = []

@app.route("/api/alerts", methods=["POST"])
def receive_alerts():
    """
    Receive alerts from Alertmanager.
    Stores them in memory for display on dashboard.
    """
    from flask import request
    try:
        data = request.get_json()
        if data:
            for alert in data.get("alerts", []):
                alert_entry = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "status": alert.get("status", "unknown"),
                    "alertname": alert.get("labels", {}).get("alertname", "unknown"),
                    "severity": alert.get("labels", {}).get("severity", "info"),
                    "component": alert.get("labels", {}).get("component", "unknown"),
                    "summary": alert.get("annotations", {}).get("summary", ""),
                    "description": alert.get("annotations", {}).get("description", ""),
                }
                RECEIVED_ALERTS.append(alert_entry)
                # Keep only last 100 alerts
                if len(RECEIVED_ALERTS) > 100:
                    RECEIVED_ALERTS.pop(0)
                logger.info(f"Alert received: {alert_entry['alertname']} ({alert_entry['severity']})")
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        logger.error(f"Error receiving alert: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/alerts", methods=["GET"])
def get_alerts():
    """Get recent alerts received from Alertmanager."""
    return jsonify({"alerts": RECEIVED_ALERTS[-50:]})  # Return last 50


# --------------------------------------------------------------------
# Main entry point
# --------------------------------------------------------------------
if __name__ == "__main__":
    print("=" * 70)
    print(" FLEAD Pipeline Monitor Starting...")
    print("=" * 70)
    print("[DASHBOARD] http://localhost:5001")
    print("[UPDATE] Real-time updates every 2 seconds")
    print("[MONITORING] Kafka → Flink → Database → Grafana")
    print("=" * 70)
    # In the container we listen on 0.0.0.0:5000 and docker-compose maps 5001:5000
    app.run(host="0.0.0.0", port=5000, debug=False)
