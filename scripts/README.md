# Scripts

Everything here runs inside Docker. `START.bat` (Windows) or `./start`
(Linux/macOS/WSL) builds and starts the containers with `docker compose up -d
--build`, then runs `pipeline_orchestrator.py`, which submits the Flink and
Spark jobs and opens the dashboards. `STOP.bat` / `./stop` stop the project's
containers and keep the data volumes.

## Pipeline

```text
data/processed/device_N.csv (2,400 devices)
        │  02_kafka_producer.py
        ▼
Kafka: edge-iiot-stream ──► kafka_to_timescaledb_collector.py ──► TimescaleDB
        │                                                          ▲
        ├─► 03_flink_local_training.py                              │
        │     • RRCF anomaly detection ──► anomalies ───────────────┤
        │     • local logistic regression ──► local-model-updates ──┤
        │                                        │                  │
        │                04_federated_aggregation.py (FedAvg + DP) ─┤
        │                    └─► models/global/global_model_latest.json
        │                                        │                  │
        └─► 05_spark_analytics.py ◄──────────────┘                  │
              • 30-second windows + fleet z-score ──────────────────┤
              • held-out evaluation of each global model ───────────┤
              • batch statistics per device ────────────────────────┘
                                                        Grafana / Prometheus
```

## Scripts and where they run

| Script | Runs in | Started by | Purpose |
| --- | --- | --- | --- |
| `data_preprocessor.py` | `data-preprocessor` container | `START.bat` / `./start` when `data/processed` is empty | Downloads Edge-IIoTset from Kaggle, drops identifier columns, standardizes features, writes compressed chunks |
| `convert_chunks_to_device_csvs.py` | `data-preprocessor` container | same | Assigns rows randomly to 2,400 devices and shuffles each device's reading order |
| `00_init_database.py` | `database-init` container | `docker compose up` | Recreates all TimescaleDB tables and hypertables (fresh run each start) |
| `01_setup_kafka_topics.py` | host (`docker exec` into the broker) | orchestrator | Creates the Kafka topics |
| `02_kafka_producer.py` | `kafka-producer` container | `docker compose up` | Streams every device concurrently, keyed by device, at 150 msg/s; only the first `STREAM_ROWS_PER_DEVICE` rows per device. Confirms each delivery and recreates its Kafka client if nothing is confirmed for 60 s |
| `03_flink_local_training.py` | Flink cluster | orchestrator (`flink run`) | Per-reading RRCF anomaly scoring with adaptive thresholds; per-device logistic regression trained on the true labels, starting from the latest global model |
| `04_federated_aggregation.py` | `federated-aggregator` container | `docker compose up` | FedAvg rounds every 60 s, DP-FedAvg with privacy accounting, model registry with automatic rollback, update clustering |
| `05_spark_analytics.py` | Spark cluster | orchestrator (`spark-submit`) | Stream windows with fleet z-scores, batch statistics, held-out accuracy/precision/recall/F1 of each global model |
| `evaluation_metrics.py` | imported by `05_spark_analytics.py` | – | Classification metrics and fleet z-score (no Spark dependency, unit-tested) |
| `kafka_to_timescaledb_collector.py` | `timescaledb-collector` container | `docker compose up` | Writes readings, anomalies and local model updates to TimescaleDB |
| `dashboard_metrics_updater.py` | `dashboard-metrics-updater` container | `docker compose up` | Writes KPI snapshots to `dashboard_metrics` every 15 s |
| `06_setup_grafana.py` | `grafana-init` container | `docker compose up` | Checks the Grafana data source; dashboards are provisioned from `grafana/dashboards` |
| `pipeline_orchestrator.py` | host | `START.bat` / `./start` | Waits for services, creates topics, submits the Flink and Spark jobs, opens the web UIs (skip with `--no-browser`, e.g. `START.bat --no-browser`) |
| `config_loader.py` | imported everywhere | – | Database, Kafka, Grafana and train/held-out settings from environment variables |

## Configuration

Settings come from environment variables set in `docker-compose.yml`, with
local-demo defaults. Copy `.env.example` to `.env` to override them.

| Variable | Default | Used by |
| --- | --- | --- |
| `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD` | `flead`, `flead`, `password` | TimescaleDB and every service that connects to it |
| `GRAFANA_ADMIN_USER`, `GRAFANA_ADMIN_PASSWORD` | `admin`, `admin` | Grafana |
| `STREAM_ROWS_PER_DEVICE` | `660` | Producer (rows streamed) and Spark (rows held out) |
| `FLEAD_ROUND_INTERVAL_SECONDS`, `FLEAD_MIN_DEVICES_PER_ROUND` | `60`, `200` | Federated aggregator |
| `FLEAD_DP_ENABLED`, `FLEAD_DP_NOISE_MULTIPLIER`, `FLEAD_DP_CLIP_NORM` | `true`, `5.0`, `1.0` | Federated aggregator |

## Tests

Unit tests cover the producer, data generation, federated aggregation,
differential-privacy accounting, the model registry and evaluation metrics.
They run on the host without Docker:

```bash
pip install -r requirements/tests.txt
pytest tests
```

## Logs and troubleshooting

| Check | Command |
| --- | --- |
| Container status | `docker compose ps` |
| Service logs | `docker compose logs -f kafka-producer` (any service name) |
| Orchestrator log | `logs/pipeline_orchestrator.log` |
| Spark job output | `logs/spark_analytics_submit.log` |
| Flink job | <http://localhost:8161> |
| Table row counts | `docker exec timescaledb psql -U flead -d flead -c "SELECT COUNT(*) FROM iot_data;"` |
| Topic contents | `docker exec kafka-broker-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic edge-iiot-stream --max-messages 5` |

Held-out model metrics appear a few minutes after start: Spark first runs its
batch pass over the device files, and the aggregator needs its first rounds.
