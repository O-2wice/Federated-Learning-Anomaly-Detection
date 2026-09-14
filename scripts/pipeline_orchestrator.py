#!/usr/bin/env python3
"""
FLEAD Pipeline Orchestrator (Docker-first)

- Assumes `docker compose up -d` has already brought up the stack.
- Waits for core Docker services to be ready (Kafka, TimescaleDB, Flink, Spark) by default.
    You can pass optional flags to shorten or skip waiting:
        --fast     Use shorter wait timeouts for faster startup
        --no-wait   Skip Docker health waits entirely (start immediately)
        --timeout N Override default wait timeout in seconds
        --interval N Override default polling interval in seconds
- Runs:
    1) Kafka topic setup (01_setup_kafka_topics.py) on the host
    2) Submits the Flink local training job to flink-jobmanager via `docker exec`
    3) Starts Spark analytics (host script -> Spark in Docker), if script exists
    4) Runs Grafana setup script (grafana-init container or host script), if present
- Prints access URLs and opens dashboards in the browser.

NO Flask dependency.
"""

import os
import sys
import time
import logging
import subprocess
import argparse
from datetime import datetime
from pathlib import Path
import webbrowser

# ---------------------------------------------------------------------
# Paths & logging setup
# ---------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
LOGS_DIR = PROJECT_ROOT / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

PIPELINE_LOG = LOGS_DIR / "pipeline_orchestrator.log"

# Consoles and redirected output on Windows use cp1252, which cannot encode
# the ✓ marks in the log messages; replace such characters instead of
# printing a logging traceback for every line
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(errors="replace")
    except (AttributeError, ValueError):
        pass

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(PIPELINE_LOG, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("pipeline_orchestrator")

# Core scripts (some optional)
KAFKA_TOPICS_SCRIPT = SCRIPTS_DIR / "01_setup_kafka_topics.py"

# We’ll auto-detect a spark analytics script
POSSIBLE_SPARK_SCRIPTS = [
    SCRIPTS_DIR / "05_spark_analytics.py",
    SCRIPTS_DIR / "05_spark_analytics_full.py",
    SCRIPTS_DIR / "05_spark_batch_stream_analytics.py",
]
SPARK_ANALYTICS_SCRIPT = next(
    (p for p in POSSIBLE_SPARK_SCRIPTS if p.exists()), None
)

# Optional Grafana setup script (in case you also keep one on host)
GRAFANA_SETUP_SCRIPT = SCRIPTS_DIR / "06_setup_grafana_dashboards.py"

# Default wait settings - can be overridden via CLI
STARTUP_TIMEOUT_DEFAULT = 120  # seconds
STARTUP_INTERVAL_DEFAULT = 2   # polling interval in seconds
OPEN_DASHBOARD_DELAY_DEFAULT = 0.8  # seconds between opening dashboards

# Docker container names we care about
REQUIRED_CONTAINERS = [
    "kafka-broker-1",
    "timescaledb",
    "flink-jobmanager",
    "spark-master",
]


# ---------------------------------------------------------------------
# Small Docker helpers
# ---------------------------------------------------------------------
def _docker_inspect(name: str, template: str) -> str | None:
    try:
        result = subprocess.run(
            ["docker", "inspect", "-f", template, name],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            return None
        return result.stdout.strip()
    except Exception:
        return None


def is_container_running(name: str) -> bool:
    val = _docker_inspect(name, "{{.State.Running}}")
    return val == "true"


def is_container_healthy(name: str) -> bool:
    val = _docker_inspect(name, "{{.State.Health.Status}}")
    return val == "healthy"


def wait_for_docker_services(timeout: int = STARTUP_TIMEOUT_DEFAULT, interval: int = STARTUP_INTERVAL_DEFAULT) -> None:
    """
    Wait until key containers are running, and TimescaleDB is healthy.
    """
    logger.info("=" * 67)
    logger.info("Waiting for Docker services to become healthy (max %d seconds)...", timeout)

    deadline = time.time() + timeout
    while time.time() < deadline:
        all_up = True
        for name in REQUIRED_CONTAINERS:
            # Check container is running
            if not is_container_running(name):
                all_up = False
                break

            # If container has a health status, require it to be healthy.
            health = _docker_inspect(name, "{{.State.Health.Status}}")
            if health and health.lower() != "healthy":
                all_up = False
                break

        # require TimescaleDB health as well (explicit check)
        if not is_container_healthy("timescaledb"):
            all_up = False

        if all_up:
            logger.info("[OK] Docker services are ready (Kafka brokers up, TimescaleDB healthy)")
            logger.info("Done")
            logger.info("")
            return

        logger.info("...still waiting for containers to be ready...")
        time.sleep(interval)

    logger.warning(
        "Timeout while waiting for Docker services. Some containers may not be ready."
    )


def show_docker_ps() -> None:
    logger.info("Current Docker container status:")
    try:
        subprocess.run(["docker", "compose", "ps"], check=False)
    except Exception as e:
        logger.warning("Failed to run `docker compose ps`: %s", e)


def show_log_snippet() -> None:
    """
    Show a short snippet of interesting logs (best effort).
    This is just for quick CLI feedback; failures are non-fatal.
    """
    logger.info("\nShowing Docker logs summary:")
    interesting = [
        "flink-taskmanager",
        "kafka-broker-1",
        "timescaledb",
        "federated-aggregator",
        "kafka-producer",
        "timescaledb-collector",
        "monitoring-dashboard",
    ]
    for name in interesting:
        try:
            subprocess.run(
                ["docker", "logs", "--tail=5", name],
                check=False,
            )
        except Exception:
            # silently ignore if container not present
            pass


# ---------------------------------------------------------------------
# Step helpers
# ---------------------------------------------------------------------
def run_python_step_blocking(description: str, script_path: Path) -> None:
    """
    Run a Python script on the host and wait for it to finish.
    """
    logger.info("=" * 70)
    logger.info("Starting: %s", description)
    logger.info("=" * 70)

    if not script_path.exists():
        logger.warning("Script not found: %s (skipping)", script_path)
        return

    logger.info("  Waiting for service to complete...")
    cmd = [sys.executable, str(script_path)]
    result = subprocess.run(cmd)
    if result.returncode == 0:
        logger.info("Service completed successfully")
    else:
        logger.error(
            "Service %r failed with exit code %s", description, result.returncode
        )
        sys.exit(result.returncode)


def start_spark_analytics_nonblocking(
    description: str, script_path: Path, max_retries: int = 3, retry_delay: int = 15
) -> None:
    """
    Start Spark analytics script as a long-running host process (non-blocking).
    Submit the job from inside the spark-master container to avoid host PySpark deps.
    
    Includes retry logic to handle transient Maven download failures.
    
    Args:
        description: Human-readable description for logging
        script_path: Path to the Spark analytics script
        max_retries: Maximum number of submission attempts (default: 3)
        retry_delay: Seconds to wait between retries (default: 15)
    """
    logger.info("=" * 70)
    logger.info("Starting: %s", description)
    logger.info("=" * 70)

    if not script_path or not script_path.exists():
        logger.warning("No Spark analytics script found (skipping)")
        return

    # Submit from spark-master container so PySpark and Spark binaries are available.
    target_path = f"/opt/spark/scripts/{script_path.name}"
    cmd = [
        "docker",
        "compose",
        "exec",
        "-u",
        "0",
        "-T",
        "spark-master",
        "/opt/spark/bin/spark-submit",
        "--packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "--master",
        "spark://spark-master:7077",
        target_path,
    ]

    submit_log = LOGS_DIR / "spark_analytics_submit.log"
    # The analytics job streams from Kafka forever (awaitTermination), so
    # spark-submit never exits on success. Launch it detached and treat
    # "still running after the grace period" as a successful submission;
    # only an early exit is a failure. A blocking subprocess.run() here
    # previously hung the orchestrator before Grafana setup / dashboards.
    startup_grace_seconds = 45

    for attempt in range(1, max_retries + 1):
        try:
            logger.info("  Spark submit attempt %d/%d...", attempt, max_retries)
            log_fh = open(submit_log, "a", encoding="utf-8")
            log_fh.write(f"=== Attempt {attempt}/{max_retries} ===\nCMD: {' '.join(cmd)}\n")
            log_fh.flush()
            proc = subprocess.Popen(
                cmd,
                cwd=str(PROJECT_ROOT),
                stdout=log_fh,
                stderr=subprocess.STDOUT,
                text=True,
            )

            try:
                returncode = proc.wait(timeout=startup_grace_seconds)
            except subprocess.TimeoutExpired:
                logger.info("  ✓ Spark analytics running on spark-master (script: %s)", target_path)
                logger.info("  Streaming job continues in the background; output in %s", submit_log)
                return  # Success - job is up and streaming

            log_fh.close()
            with open(submit_log, "r", encoding="utf-8", errors="replace") as f:
                output = f.read()

            if returncode == 0:
                logger.info("  ✓ Spark analytics job completed (script: %s)", target_path)
                return

            class _Result:  # keep the retry classification below unchanged
                stderr = output
            result = _Result()

            # Check if it's a transient Maven/download error (worth retrying)
            stderr_lower = result.stderr.lower()
            is_transient = any(
                err in stderr_lower
                for err in ["download failed", "content length", "please retry", "connection", "timeout"]
            )
            
            if is_transient and attempt < max_retries:
                logger.warning(
                    "  Spark submit failed (transient error), retrying in %ds... (attempt %d/%d)",
                    retry_delay, attempt, max_retries
                )
                # Clear corrupted Ivy cache before retry
                subprocess.run(
                    ["docker", "exec", "spark-master", "rm", "-rf", "/root/.ivy2/cache/org.apache.hadoop"],
                    capture_output=True,
                )
                time.sleep(retry_delay)
            elif attempt < max_retries:
                logger.warning(
                    "  Spark submit failed, retrying in %ds... (attempt %d/%d)",
                    retry_delay, attempt, max_retries
                )
                time.sleep(retry_delay)
            else:
                logger.error("Spark analytics submission failed after %d attempts (see %s)", max_retries, submit_log)
                
        except Exception as e:
            logger.error("Failed to submit Spark analytics job (attempt %d): %s", attempt, e)
            if attempt < max_retries:
                time.sleep(retry_delay)


def _wait_for_flink_rest_api(timeout: int = 120, interval: int = 5) -> bool:
    """
    Wait for Flink REST API to be ready (not just container running).
    This ensures the actor system is fully initialized before job submission.
    """
    deadline = time.time() + timeout
    logger.info("Waiting for Flink REST API to be ready (max %ds)...", timeout)
    
    while time.time() < deadline:
        # First check container is running
        if not is_container_running("flink-jobmanager"):
            logger.info("  flink-jobmanager container not running yet...")
            time.sleep(interval)
            continue
        
        # Try to reach Flink REST API inside container
        try:
            result = subprocess.run(
                ["docker", "exec", "flink-jobmanager", "curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", "http://localhost:8081/overview"],
                capture_output=True, text=True, timeout=10
            )
            if result.returncode == 0 and result.stdout.strip() == "200":
                logger.info("  Flink REST API is ready!")
                return True
            else:
                logger.info("  Flink REST API not ready yet (http: %s)...", result.stdout.strip() if result.stdout else "N/A")
        except subprocess.TimeoutExpired:
            logger.info("  Flink REST API check timed out...")
        except Exception as e:
            logger.info("  Flink REST API check failed: %s", e)
        
        time.sleep(interval)
    
    return False


def submit_flink_local_training_job(max_retries: int = 3, retry_delay: int = 10) -> None:
    """
    Submit the Flink local training job to the JobManager via docker exec.

    This mirrors the manual command you've been using:
      docker exec -it flink-jobmanager \
        flink run -d -py /opt/flink/scripts/03_flink_local_training.py
    
    Includes retry logic to handle transient failures during Flink startup.
    
    Args:
        max_retries: Maximum number of submission attempts (default: 3)
        retry_delay: Seconds to wait between retries (default: 10)
    """
    logger.info("=" * 70)
    logger.info("Starting: Flink Local Model Training (Real-time Streaming)")
    logger.info("=" * 70)
    logger.info("  Waiting for Flink REST API to be ready...")

    cmd = [
        "docker",
        "exec",
        "flink-jobmanager",
        "flink",
        "run",
        "-d",
        "-py",
        "/opt/flink/scripts/03_flink_local_training.py",
    ]
    # Wait for Flink REST API to be ready (actor system fully initialized)
    if not _wait_for_flink_rest_api(timeout=180, interval=5):
        logger.warning("Flink REST API not ready after 180s, attempting job submission anyway...")
    
    flink_log = LOGS_DIR / "flink_job_submit.log"
    
    for attempt in range(1, max_retries + 1):
        logger.info("  Flink submit attempt %d/%d...", attempt, max_retries)
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        # Log this attempt
        log_entry = (
            f"=== Attempt {attempt}/{max_retries} ===\n"
            f"CMD: {' '.join(cmd)}\n"
            f"RET: {result.returncode}\n"
            f"STDOUT:\n{result.stdout}\n"
            f"STDERR:\n{result.stderr}\n\n"
        )
        with open(flink_log, "a") as f:
            f.write(log_entry)

        if result.returncode == 0:
            logger.info("  ✓ Flink job submitted successfully")
            if result.stdout.strip():
                logger.info("Flink submission output:\n%s", result.stdout.strip())
            break  # Success
        else:
            # Check if worth retrying
            stderr_lower = result.stderr.lower()
            is_transient = any(
                err in stderr_lower
                for err in ["connection refused", "not ready", "timeout", "actor system", "unreachable"]
            )
            
            if attempt < max_retries:
                if is_transient:
                    logger.warning(
                        "  Flink submit failed (transient error), retrying in %ds...",
                        retry_delay
                    )
                else:
                    logger.warning(
                        "  Flink submit failed, retrying in %ds...",
                        retry_delay
                    )
                time.sleep(retry_delay)
            else:
                logger.error("Flink job submission failed after %d attempts!", max_retries)
                logger.error("STDOUT:\n%s", result.stdout)
                logger.error("STDERR:\n%s", result.stderr)
                logger.error("See %s for full details", flink_log)
                sys.exit(result.returncode)

    # Optional: show current jobs
    try:
        jobs = subprocess.run(
            ["docker", "exec", "flink-jobmanager", "flink", "list"],
            capture_output=True,
            text=True,
        )
        if jobs.returncode == 0 and jobs.stdout.strip():
            logger.info("Flink job list:\n%s", jobs.stdout.strip())
        else:
            logger.info("Flink job list unavailable or empty.")
    except Exception as e:
        logger.info("Could not query Flink jobs: %s", e)


def run_grafana_setup() -> None:
    """
    Either rely on grafana-init container OR (optionally) run a host script.
    This keeps backwards compatibility with your older setup.
    """
    logger.info("=" * 70)
    logger.info("Starting: Grafana Dashboard Configuration (Automated Setup)")
    logger.info("=" * 70)

    # If you have a dedicated host script, run it. Otherwise we assume the
    # grafana-init container handles provisioning.
    if GRAFANA_SETUP_SCRIPT.exists():
        logger.info("  Waiting for service to complete...")
        cmd = [sys.executable, str(GRAFANA_SETUP_SCRIPT)]
        result = subprocess.run(cmd)
        if result.returncode == 0:
            logger.info("Service completed successfully")
        else:
            logger.error(
                "Grafana setup script failed with exit code %s",
                result.returncode,
            )
            sys.exit(result.returncode)
    else:
        logger.info("No host Grafana setup script found; using grafana-init container.")
        logger.info("Service completed successfully")


# ---------------------------------------------------------------------
# Pretty printing helpers
# ---------------------------------------------------------------------
def print_pipeline_status_summary() -> None:
    logger.info("")
    logger.info("=" * 70)
    logger.info("PIPELINE STATUS")
    logger.info("=" * 70)
    if SPARK_ANALYTICS_SCRIPT:
        logger.info("Spark Analytics (Batch + Stream + Model Evaluation)")
    else:
        logger.info("Spark Analytics script not configured (optional).")
    logger.info("")


def print_access_points() -> None:
    logger.info("=" * 70)
    logger.info("ACCESS POINTS")
    logger.info("=" * 70)
    logger.info("Grafana Dashboard:        http://localhost:3001 (admin/admin)")
    logger.info("Kafka UI:                 http://localhost:8081")
    logger.info("Device Viewer Website:    http://localhost:8082")
    logger.info("Flink Dashboard:          http://localhost:8161")
    logger.info("Spark Master:             http://localhost:8086")
    logger.info("Spark Worker:             http://localhost:8087")
    logger.info("TimescaleDB:              localhost:5432")
    logger.info("Monitoring Dashboard:     http://localhost:5001")
    logger.info("Jupyter Dev UI:           http://localhost:8888")
    logger.info("Prometheus:               http://localhost:9090")
    logger.info("Alertmanager:             http://localhost:9093")
    logger.info("")


def print_pipeline_flow() -> None:
    logger.info("=" * 70)
    logger.info("PIPELINE FLOW")
    logger.info("=" * 70)
    logger.info("IoT Devices (CSV files)")
    logger.info("    ↓")
    logger.info("Kafka Producer (kafka-producer container)")
    logger.info("    ↓")
    logger.info("edge-iiot-stream (Kafka Topic)")
    logger.info("    ├→ Flink (Real-time Local Training)")
    logger.info("    │  ├→ anomalies topic")
    logger.info("    │  └→ local-model-updates topic")
    logger.info("    │")
    logger.info("    └→ TimescaleDB (via timescaledb-collector / aggregation)")
    logger.info("        ↓")
    logger.info("Federated Aggregation Service (federated-aggregator)")
    logger.info("    ↓")
    logger.info("global-model-updates topic")
    logger.info("    ↓")
    logger.info("Global Model (stored under /app/models/global in container)")
    logger.info("    ↓")
    logger.info("Spark Batch & Stream Analytics")
    logger.info("    ↓")
    logger.info("Grafana Dashboards + Analytics Tables")
    logger.info("")


def print_monitoring_help() -> None:
    logger.info("=" * 70)
    logger.info("MONITORING")
    logger.info("=" * 70)
    logger.info("Log files location: %s", LOGS_DIR)
    logger.info("View logs:")
    logger.info("  tail -f %s", LOGS_DIR / "database_init.log")
    logger.info("  tail -f %s", LOGS_DIR / "kafka_topics.log")
    logger.info("  tail -f %s", LOGS_DIR / "kafka_producer.log")
    logger.info("  tail -f %s", LOGS_DIR / "flink_training.log")
    logger.info("  tail -f %s", LOGS_DIR / "federated_aggregation.log")
    logger.info("  tail -f %s", LOGS_DIR / "monitoring_dashboard.log")
    logger.info("  tail -f %s", LOGS_DIR / "spark_analytics.log")
    logger.info("  tail -f %s", LOGS_DIR / "device_viewer.log")
    logger.info("  tail -f %s", LOGS_DIR / "grafana_setup.log")
    logger.info("")


def open_dashboards_in_browser() -> None:
    logger.info("=" * 70)
    logger.info("LAUNCHING WEB INTERFACES")
    logger.info("=" * 70)
    urls = [
        "http://localhost:8082",  # Device viewer
        "http://localhost:8081",  # Kafka UI
        "http://localhost:3001",  # Grafana
        "http://localhost:8161",  # Flink
        "http://localhost:8086",  # Spark master
        "http://localhost:8087",  # Spark worker (if mapped)
        "http://localhost:5001",  # Live monitoring dashboard
        "http://localhost:8888",  # Jupyter Dev UI
        "http://localhost:9090",  # Prometheus
        "http://localhost:9093",  # Alertmanager
    ]
    # Use a configurable delay between opening dashboards
    delay = float(os.getenv("OPEN_DASHBOARD_DELAY", OPEN_DASHBOARD_DELAY_DEFAULT))
    for url in urls:
        try:
            logger.info("Opening %s", url)
            webbrowser.open(url)
            time.sleep(delay)
        except Exception:
            pass

    logger.info("=" * 70)
    logger.info("All dashboards opened ")
    logger.info("Press any key to close this window.")
    logger.info("=" * 70)


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def _wait_for_specific_containers(names: list[str], timeout: int = STARTUP_TIMEOUT_DEFAULT, interval: int = STARTUP_INTERVAL_DEFAULT) -> bool:
    """Wait for a given list of containers (returns True if all up)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        all_up = True
        for name in names:
            if not is_container_running(name):
                all_up = False
                break
            health = _docker_inspect(name, "{{.State.Health.Status}}")
            if health and health.lower() != "healthy":
                all_up = False
                break
        if all_up:
            return True
        time.sleep(interval)
    return False


def main() -> None:
    logger.info("=" * 68)
    logger.info("STARTING PIPELINE ORCHESTRATOR")
    logger.info("=" * 68)
    logger.info("")
    logger.info("This will:")
    logger.info("  - Submit Flink job for local training")
    logger.info("  - Submit Spark job for analytics")
    logger.info("  - Run optional Grafana setup")
    logger.info("  (Kafka Producer, Federated Aggregator, Monitoring, Device Viewer")
    logger.info("   are managed directly by Docker Compose in this version.)")
    logger.info("")
    logger.info("Pipeline logs will be saved to: %s", LOGS_DIR)
    logger.info("")

    # 1) Show Docker status
    show_docker_ps()
    show_log_snippet()
    # Parse any CLI overrides for wait behaviour
    parser = argparse.ArgumentParser(description="FLEAD pipeline orchestrator")
    parser.add_argument("--no-wait", action="store_true", help="skip waiting for container health checks")
    parser.add_argument("--fast", action="store_true", help="use short wait timeouts for faster startup")
    parser.add_argument("--timeout", type=int, default=os.getenv("STARTUP_TIMEOUT", STARTUP_TIMEOUT_DEFAULT), help="startup timeout in seconds")
    parser.add_argument("--interval", type=int, default=os.getenv("STARTUP_INTERVAL", STARTUP_INTERVAL_DEFAULT), help="polling interval in seconds")
    parser.add_argument("--no-browser", action="store_true",
                        default=os.getenv("FLEAD_NO_BROWSER", "").lower() in ("1", "true", "yes"),
                        help="do not open the web interfaces in a browser (also FLEAD_NO_BROWSER=1)")
    args = parser.parse_args()

    # Adjust values for fast start
    if args.fast:
        args.timeout = min(args.timeout, 60)
        args.interval = max(1, args.interval)

    if not args.no_wait:
        wait_for_docker_services(timeout=args.timeout, interval=args.interval)

    logger.info("")
    logger.info("====================================================================")
    logger.info("FLEAD COMPLETE PIPELINE STARTUP")
    logger.info("====================================================================")
    logger.info("Timestamp: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("====================================================================")

    # 2) Steps in order
    # 2a) Kafka Topics
    run_python_step_blocking(
        "Setup Kafka Topics (single-broker)",
        KAFKA_TOPICS_SCRIPT,
    )

    # 2b) Flink local training job
    # Ensure Flink components are ready
    if not _wait_for_specific_containers(["flink-jobmanager"], timeout=(args.timeout if 'args' in locals() else STARTUP_TIMEOUT_DEFAULT), interval=(args.interval if 'args' in locals() else STARTUP_INTERVAL_DEFAULT)):
        logger.warning("Timed out waiting for Flink JobManager; trying to submit job anyway")
    submit_flink_local_training_job()

    # 2c) Spark analytics (non-blocking)
    # Ensure Spark master up before submitting the analytics job
    if SPARK_ANALYTICS_SCRIPT:
        if not _wait_for_specific_containers(["spark-master"], timeout=(args.timeout if 'args' in locals() else STARTUP_TIMEOUT_DEFAULT), interval=(args.interval if 'args' in locals() else STARTUP_INTERVAL_DEFAULT)):
            logger.warning("Spark master not ready yet; submit attempt will proceed")
    start_spark_analytics_nonblocking(
        "Spark Analytics (Batch + Stream + Model Evaluation)",
        SPARK_ANALYTICS_SCRIPT,
    )

    # 2d) Grafana setup
    run_grafana_setup()

    # 3) Summary & helper info
    print_pipeline_status_summary()
    print_access_points()
    print_pipeline_flow()
    print_monitoring_help()

    logger.info("Pipeline started successfully!")
    logger.info("")
    if args.no_browser:
        logger.info("Not opening the web interfaces (--no-browser); see ACCESS POINTS above.")
    else:
        open_dashboards_in_browser()


if __name__ == "__main__":
    main()
