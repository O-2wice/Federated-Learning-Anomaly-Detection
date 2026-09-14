#!/usr/bin/env python3
"""
01_setup_kafka_topics.py

Create all Kafka topics needed by the FLEAD pipeline
for the Kafka cluster (single-broker mode).

This script runs on the *HOST* but uses `docker exec` to run the
`kafka-topics` CLI *inside* a Kafka container (default: kafka-broker-1),
so it can use Docker's internal DNS names (kafka-broker-1:9092, etc.).

You can override the defaults with environment variables:
  - KAFKA_CONTAINER
  - KAFKA_BOOTSTRAP_INTERNAL
"""

from __future__ import annotations

import logging
import os
import re
import subprocess
import sys
import time
from typing import Dict, Any, List

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# Container & internal bootstrap settings
# ---------------------------------------------------------------------
# Which container to exec into to run kafka-topics
KAFKA_CONTAINER = os.getenv("KAFKA_CONTAINER", "kafka-broker-1")

# Which bootstrap string to use *inside* the Docker network
# You can set this to a single broker or a comma-separated list.
KAFKA_BOOTSTRAP_INTERNAL = os.getenv(
    "KAFKA_BOOTSTRAP_INTERNAL",
    "kafka-broker-1:9092",
)

# Topics used in your pipeline
TOPICS: Dict[str, Dict[str, Any]] = {
    "edge-iiot-stream": {
        "partitions": 4,
        "replication_factor": 1,
    },
    "local-model-updates": {
        "partitions": 4,
        "replication_factor": 1,
    },
    "global-model-updates": {
        "partitions": 4,
        "replication_factor": 1,
    },
    "anomalies": {
        "partitions": 4,
        "replication_factor": 1,
    },
}

MAX_ATTEMPTS = 120
SLEEP_SECONDS = 5


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------
def run_in_container(cmd: str) -> subprocess.CompletedProcess:
    """
    Run a shell command *inside* the Kafka container using bash -lc.
    """
    docker_cmd: List[str] = [
        "docker",
        "exec",
        "-i",
        KAFKA_CONTAINER,
        "bash",
        "-lc",
        cmd,
    ]
    logger.debug("Running in container: %s", " ".join(docker_cmd))
    return subprocess.run(
        docker_cmd,
        capture_output=True,
        text=True,
    )


def wait_for_kafka_cli() -> None:
    """
    Poll `kafka-topics --list` inside the broker container until it works.
    """
    logger.info("======================================================================")
    logger.info("Kafka Topics Setup for FLEAD Pipeline (CLI via docker exec)")
    logger.info("======================================================================")
    logger.info(
        "Container: %s | Internal bootstrap: %s",
        KAFKA_CONTAINER,
        KAFKA_BOOTSTRAP_INTERNAL,
    )
    logger.info(
        "Waiting for Kafka to become reachable via CLI "
        "(max_attempts=%d, sleep=%ds)...",
        MAX_ATTEMPTS,
        SLEEP_SECONDS,
    )

    for attempt in range(1, MAX_ATTEMPTS + 1):
        logger.info(
            "Kafka CLI connectivity check (attempt %d/%d)...",
            attempt,
            MAX_ATTEMPTS,
        )

        cmd = f"kafka-topics --bootstrap-server {KAFKA_BOOTSTRAP_INTERNAL} --list"
        result = run_in_container(cmd)

        if result.returncode == 0:
            logger.info("✓ Kafka is reachable via CLI.")
            existing = (result.stdout or "").strip()
            if existing:
                logger.info("Existing topics:\n%s", existing)
            else:
                logger.info("No topics yet (fresh cluster).")
            return

        stderr = (result.stderr or "").strip()
        logger.warning(
            "Kafka CLI not ready yet (rc=%s): %s. Retrying in %ds...",
            result.returncode,
            stderr if stderr else "<no stderr>",
            SLEEP_SECONDS,
        )
        time.sleep(SLEEP_SECONDS)

    msg = (
        f"Kafka not reachable via CLI in container '{KAFKA_CONTAINER}' "
        f"after {MAX_ATTEMPTS} attempts."
    )
    logger.error(msg)
    raise RuntimeError(msg)


def ensure_partition_count(name: str, partitions: int) -> None:
    """
    Raise the partition count of a topic created earlier with fewer partitions.

    `--if-not-exists` leaves existing topics untouched, so a Kafka volume from
    an older run keeps its single partition. Kafka can only add partitions;
    a failure here is logged and does not stop the pipeline.
    """
    describe = run_in_container(
        f"kafka-topics --bootstrap-server {KAFKA_BOOTSTRAP_INTERNAL} --describe --topic {name}"
    )
    match = re.search(r"PartitionCount:\s*(\d+)", describe.stdout or "")
    if describe.returncode != 0 or not match:
        logger.warning("Could not read the partition count of '%s'; leaving it unchanged.", name)
        return
    current = int(match.group(1))
    if current >= partitions:
        return
    alter = run_in_container(
        f"kafka-topics --bootstrap-server {KAFKA_BOOTSTRAP_INTERNAL} "
        f"--alter --topic {name} --partitions {partitions}"
    )
    if alter.returncode == 0:
        logger.info("✓ Topic '%s': partitions raised from %d to %d.", name, current, partitions)
    else:
        logger.warning(
            "Could not raise the partitions of '%s' (rc=%d): %s",
            name,
            alter.returncode,
            (alter.stderr or "").strip() or "<no stderr>",
        )


def ensure_topics() -> None:
    """
    Ensure all topics in TOPICS exist using kafka-topics --if-not-exists.
    """
    logger.info(
        "Ensuring Kafka topics exist using CLI in container '%s' "
        "(bootstrap=%s)...",
        KAFKA_CONTAINER,
        KAFKA_BOOTSTRAP_INTERNAL,
    )

    # Wait until Kafka is actually ready
    wait_for_kafka_cli()

    for name, cfg in TOPICS.items():
        partitions = int(cfg["partitions"])
        replication_factor = int(cfg["replication_factor"])

        logger.info(
            "Ensuring topic '%s' (partitions=%d, replication_factor=%d)...",
            name,
            partitions,
            replication_factor,
        )

        cmd = (
            "kafka-topics "
            f"--bootstrap-server {KAFKA_BOOTSTRAP_INTERNAL} "
            "--create "
            "--if-not-exists "
            f"--topic {name} "
            f"--partitions {partitions} "
            f"--replication-factor {replication_factor}"
        )

        result = run_in_container(cmd)

        stdout = (result.stdout or "").strip()
        stderr = (result.stderr or "").strip()
        combined = f"{stdout}\n{stderr}"

        if result.returncode == 0:
            if stdout:
                logger.info("kafka-topics output for '%s': %s", name, stdout)
            logger.info("✓ Topic '%s' is ensured.", name)
            ensure_partition_count(name, partitions)
        elif "already exists" in combined:
            logger.info("✓ Topic '%s' already exists.", name)
            ensure_partition_count(name, partitions)
        else:
            logger.error(
                "Failed to create/ensure topic '%s' (rc=%d).\nSTDOUT: %s\nSTDERR: %s",
                name,
                result.returncode,
                stdout or "<empty>",
                stderr or "<empty>",
            )
            raise RuntimeError(
                f"Failed to create topic '{name}' (rc={result.returncode}). "
                f"See logs above."
            )


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main() -> int:
    try:
        ensure_topics()
        logger.info("======================================================================")
        logger.info("✓ Kafka topic setup completed successfully.")
        logger.info("======================================================================")
        return 0
    except Exception as e:
        logger.error("Kafka topic setup failed: %s", e, exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
