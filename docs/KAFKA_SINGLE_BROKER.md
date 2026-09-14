# Kafka: Single-Broker Setup

FLEAD runs one Kafka broker (`kafka-broker-1`, Confluent Platform 7.6.1) in
KRaft mode: the same process acts as broker and controller, so no ZooKeeper is
needed. A multi-broker cluster with replication would tolerate a broker
failure; a single broker gives up that fault tolerance for a simpler, lighter
setup.

## Broker configuration (`docker-compose.yml`)

| Setting | Value | Why |
|---|---|---|
| `KAFKA_PROCESS_ROLES` | `broker,controller` | One-node KRaft cluster |
| `KAFKA_LISTENERS` | `PLAINTEXT://:9092`, `CONTROLLER://:29093` | Data listener and internal controller listener |
| `KAFKA_ADVERTISED_LISTENERS` | `PLAINTEXT://kafka-broker-1:9092` | Clients inside the Docker network connect by service name |
| Replication factor | 1 (topics and internal offsets) | Only one broker to replicate to |
| `KAFKA_NUM_PARTITIONS` | 4 | Topics auto-created on first use get the same partitions as the ones the orchestrator creates |
| Heap | 600 MB | Fits alongside Flink, Spark and TimescaleDB |

Port `9092` is published to the host. Inside the Docker network every service
uses `kafka-broker-1:9092`, provided through `KAFKA_BOOTSTRAP_SERVERS`.

## Topics

`scripts/01_setup_kafka_topics.py` (run by the orchestrator) creates missing
topics with 4 partitions. The producer and aggregator start before that step
and may publish first; the broker then auto-creates the topic, also with 4
partitions (`KAFKA_NUM_PARTITIONS`).

| Topic | Producer | Consumers |
|---|---|---|
| `edge-iiot-stream` | `02_kafka_producer.py` | Flink job (`flink-training`), TimescaleDB collector, Spark streaming |
| `anomalies` | Flink job | TimescaleDB collector |
| `local-model-updates` | Flink job | Federated aggregator, TimescaleDB collector |
| `global-model-updates` | Federated aggregator | (published for downstream consumers) |
| `system-alerts` | Federated aggregator | (published for downstream consumers) |

Partitions provide parallelism, not redundancy. Flink's Kafka source spreads
the partitions over its 2 parallel readers, and `key_by(device_id)` spreads
the processing over both task slots.

A topic that already exists in the Kafka volume keeps its partition count
(volumes from before `KAFKA_NUM_PARTITIONS` was set may hold 1-partition
topics). Check with
`docker exec kafka-broker-1 kafka-topics --bootstrap-server localhost:9092 --describe`.
Raise the count on a stopped pipeline with `kafka-topics --alter --topic <name> --partitions 4`.
Readings are keyed by device, so a device's readings move to a new partition
once, at that point.

## How devices are streamed

`scripts/02_kafka_producer.py` replays the 2,400 `data/processed/device_N.csv`
files as live devices:

- **Round-robin:** one reading per device per turn, so all devices stream
  concurrently at a shared rate (`--rate 150` messages/s in compose, about one
  reading per device every 16 s).
- **Keyed by `device_id`:** every reading of a device lands on the same
  partition, so Flink sees each device's readings in order.
- **Train / held-out split:** only the first `STREAM_ROWS_PER_DEVICE` rows
  (default 660 of ~827) are streamed; Spark evaluates the federated model on
  the rest.
- **Event time:** each message is stamped with its send time; the CSV
  timestamp is kept as `source_timestamp`.

## Inspecting Kafka

- Kafka UI: http://localhost:8081
- List topics: `docker exec kafka-broker-1 kafka-topics --bootstrap-server localhost:9092 --list`
- Sample messages: `docker exec kafka-broker-1 kafka-console-consumer --bootstrap-server localhost:9092 --topic edge-iiot-stream --max-messages 5`

## Scaling out

To run more brokers, add `kafka-broker-N` services with unique `KAFKA_NODE_ID`s
and a shared `CLUSTER_ID`, list the controllers in
`KAFKA_CONTROLLER_QUORUM_VOTERS`, raise the replication factors (topics and
`KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR`) and pass a comma-separated
`KAFKA_BOOTSTRAP_SERVERS` to every client. Producer and consumers need no code
changes.
