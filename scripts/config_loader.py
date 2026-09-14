#!/usr/bin/env python3
"""
Database & Service Configuration Utility
Automatically detects if running inside Docker or on the host.

Used by:
  - Federated Aggregation Service
  - Kafka → TimescaleDB collector
  - Grafana setup scripts
  - Any other pipeline components needing DB/Kafka/Grafana config
"""

import os
from typing import Dict, Any


def _in_docker() -> bool:
    """Return True if running inside a Docker container."""
    return os.path.exists("/.dockerenv")


def get_db_config() -> Dict[str, Any]:
    """
    Get database configuration based on environment.

    Inside Docker:
      - host defaults to 'timescaledb' (docker-compose service name)

    Outside Docker (host):
      - host defaults to 'localhost' (mapped port 5432 via docker-compose)

    Environment variables (optional overrides):
      DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
    """
    is_docker = _in_docker()
    default_host = "timescaledb" if is_docker else "localhost"

    return {
        "host": os.getenv("DB_HOST", default_host),
        "port": int(os.getenv("DB_PORT", 5432)),
        "database": os.getenv("DB_NAME", "flead"),
        "user": os.getenv("DB_USER", "flead"),
        "password": os.getenv("DB_PASSWORD", "password"),
    }


def get_kafka_config() -> Dict[str, str]:
    """
    Get Kafka configuration based on environment.

    Inside Docker:
      - use internal service names on PLAINTEXT port 9092 for broker 1:
            kafka-broker-1:9092

    Outside Docker (host):
      - use localhost:9092 (mapped in docker-compose):
            localhost:9092

    Environment override:
      KAFKA_BOOTSTRAP_SERVERS="host1:port1,host2:port2,..."
    """
    is_docker = _in_docker()

    if is_docker:
        default_bootstrap = "kafka-broker-1:9092"
    else:
        default_bootstrap = "localhost:9092"

    return {
        "bootstrap_servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", default_bootstrap)
    }


# Device CSV layout, shared by the data converter (writes it), the Kafka
# producer (streams the first rows) and Spark (evaluates on the rest).
# Rows in every device_*.csv are timestamped one second apart from this start.
DEVICE_CSV_START = "2025-01-01 00:00:00"


def get_stream_config() -> Dict[str, Any]:
    """
    Train / held-out split of each device file.

    The producer streams only the first `stream_rows_per_device` rows (the
    readings local models train on); Spark evaluates the global model on the
    remaining rows, which no device has trained on. Devices have ~827 rows, so
    the default 660 is an 80/20 split.

    Environment override: STREAM_ROWS_PER_DEVICE
    """
    return {
        "stream_rows_per_device": int(os.getenv("STREAM_ROWS_PER_DEVICE", "660")),
        "device_csv_start": DEVICE_CSV_START,
    }


def get_grafana_config() -> Dict[str, str]:
    """
    Get Grafana configuration based on environment.

    Inside Docker:
      - use 'http://grafana:3000' (service name + internal port)

    Outside Docker (host):
      - use 'http://localhost:3001' (host-mapped port from 3000)

    Environment overrides:
      GRAFANA_URL, GRAFANA_USER, GRAFANA_PASSWORD
    """
    is_docker = _in_docker()

    if is_docker:
        default_url = "http://grafana:3000"
    else:
        default_url = "http://localhost:3001"

    return {
        "url": os.getenv("GRAFANA_URL", default_url),
        "user": os.getenv("GRAFANA_USER", "admin"),
        "password": os.getenv("GRAFANA_PASSWORD", "admin"),
    }


def print_config_info() -> None:
    """Print current configuration (for quick debugging from CLI)."""
    is_docker = _in_docker()
    print(f"Running in Docker: {is_docker}")
    print(f"Database Config:  {get_db_config()}")
    print(f"Kafka Config:     {get_kafka_config()}")
    print(f"Grafana Config:   {get_grafana_config()}")


if __name__ == "__main__":
    print_config_info()