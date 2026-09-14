# FLEAD System Design

FLEAD detects attacks in streams of IoT network traffic without collecting the
devices' raw data in one place. It combines two detectors:

- **RRCF anomaly scoring** in Flink: unsupervised, per reading, with no labels needed.
- **A federated classifier:** each device trains a logistic regression on its
  own recent labelled readings, and an aggregator combines the parameter
  updates with differentially private federated averaging.

Spark scores every new global model on readings no device trained on.
TimescaleDB, Grafana and Prometheus make the whole pipeline observable.

Container layout: [DOCKER_ARCHITECTURE.md](DOCKER_ARCHITECTURE.md). Scripts
and settings: [scripts/README.md](scripts/README.md). Anomaly detector:
[docs/RCF_EXPLAINED.md](docs/RCF_EXPLAINED.md).

## 1. Data

| Step | What happens | Code |
| --- | --- | --- |
| Source | Edge-IIoTset `DNN-EdgeIIoT-dataset.csv`: 1,985,453 labelled network flows after removing duplicates, about 28% attacks (`Attack_label`) | `data_preprocessor.py` |
| Cleaning | Drops 15 columns that are unique per packet (timestamps, IP and ARP addresses, ports, TCP options, HTTP/TCP/MQTT payloads) and keeps every label-derived column out of the features (`Attack_type` would leak the answer) | `data_preprocessor.py` |
| Features | 46 features, standardized (zero mean, unit variance); the CSV is processed in 100,000-row chunks to bound memory | `data_preprocessor.py` |
| Devices | Rows are assigned at random to 2,400 simulated devices (~827 readings each, every device mixing benign and attack traffic), and each device's order is shuffled. Readings get synthetic timestamps one second apart from 2025-01-01. | `convert_chunks_to_device_csvs.py` |
| Train / held-out | The producer streams each device's first 660 readings (`STREAM_ROWS_PER_DEVICE`). The remaining ~167 per device are never streamed; Spark evaluates on them. | `02_kafka_producer.py`, `05_spark_analytics.py` |

The stream is a replay of a static dataset. Shuffling removes any temporal
pattern within a device, so the detectors score each reading on its features,
not on sequences.

## 2. Pipeline

```text
device CSVs ─► producer (150 readings/s, keyed by device) ─► Kafka: edge-iiot-stream
                                                                 │
        ┌────────────────────────────┬────────────────────────────┼──────────────────────────┐
        ▼                            ▼                            ▼                          ▼
  Flink job (2 slots)          TimescaleDB collector        Spark Structured Streaming   (Kafka UI)
  • RRCF score per reading     • iot_data                   • 30 s window mean per device
    ─► anomalies               • anomalies                  • fleet z-score
  • local logistic regression  • local_model_updates          ─► stream_analysis_results
    ─► local-model-updates
        │
        ▼
  Federated aggregator ─► global-model-updates, federated_models,
  (buffered FedAvg + DP)   models_global/global_model_latest.json
        │                          │
        │                          ├─► Flink: next local round starts from it
        │                          └─► Spark: held-out evaluation ─► model_evaluations
        ▼
  Model registry + automatic rollback (uses Spark's held-out F1)
```

## 3. Components

### 3.1 Producer

`02_kafka_producer.py` streams all devices at once, round-robin, one reading
per device per turn, at 150 readings/s in total. Messages are keyed by
`device_id`, so each device's readings stay in order, and stamped with the
send time. Every send is confirmed through a delivery callback. If Kafka
confirms nothing for 60 s while readings keep being queued, the producer
recreates its Kafka client. After the broker lost its session, the old client
kept expiring every batch without recovering.

### 3.2 Anomaly detection (Flink)

Each Flink worker keeps one shared Robust Random Cut Forest (4 trees, 256
points each) over all 46 features. Each reading's collusive displacement is
ranked against the last 500 scores, which gives a 0–1 score. Per-device
thresholds start at 0.4 and adapt toward flagging 5% of readings. Flagged
readings go to the `anomalies` topic with their score, threshold, severity and
ground-truth label, so detection precision can be measured.
Details and measurements: [docs/RCF_EXPLAINED.md](docs/RCF_EXPLAINED.md).

### 3.3 Local training (Flink)

For each device the job keeps its last 200 labelled readings. Once at least
20 are buffered, it trains after every 30 new readings (about every 8 minutes
per device; a 15-minute fallback covers slow devices):

- **Model:** logistic regression (attack vs benign) with class-weighted
  mini-batch SGD. Settings: learning rate 0.2, attack weight 2.5, L2 1e-4,
  3 epochs, batch size 32.
- **Start:** each round begins from the latest global model (the file is
  re-read every 30 s).
- **Output:** the new parameters, the global version they started from, and
  the sample count go to `local-model-updates`.

The learning rate and class weight were chosen offline (300 devices, 60
rounds). They raised held-out F1 from 0.67 to 0.75 by lifting attack recall
from 52% to 70%.

### 3.4 Federated aggregation

`04_federated_aggregation.py` implements buffered asynchronous FedAvg (as in
FedBuff). It keeps the latest update of each device and runs a round every
60 s once at least 200 devices have reported:

```text
delta_i  = w_i − w_base(i)          update since the global version device i trained from
w_global = w_global + A(delta_1..n)
```

`A` is the sample-weighted mean. With differential privacy on (the default),
it becomes DP-FedAvg (McMahan et al., 2018):

- clip each `delta_i` to L2 norm C = 1;
- average the clipped updates;
- add Gaussian noise with standard deviation σ·C / n, where σ = 5.

The cumulative (ε, δ = 1e-5) budget comes from a Rényi-DP accountant for the
Gaussian mechanism, composed over rounds. It ignores subsampling
amplification, so it is conservative. Noise is added by the aggregator, which
the design therefore trusts.

The 200-device minimum matters. With 40 devices per round (noise std 0.125),
an offline run left the model at the always-benign baseline (F1 0.58, against
0.73 without DP). Live, a round runs every minute with about 300 devices
(each device retrains every 30 readings, staggered), so the noise std is
about 0.017.

Every round also records:

- **Update clustering:** devices are grouped by cosine similarity of their
  updates (≥ 0.5); the number of groups of 3 or more devices and the mean
  agreement with the consensus direction show whether devices learn the same
  thing.
- **Model registry:** the last 10 global versions (JSON, no pickles). Spark's
  held-out F1 decides the best version. If a newer evaluated version's F1 is
  more than 0.10 below the best, the aggregator republishes the best
  parameters as a new version (once per degraded version).
- **TimescaleDB `federated_models`:** devices, samples, update norm,
  agreement, clusters, DP noise, clipped updates, ε.

### 3.5 Analytics and evaluation (Spark)

`05_spark_analytics.py` runs three parts on the Spark cluster:

1. **Held-out evaluation.** A fixed 5% sample of the unseen rows (about 20,000
   readings) is cached. Every 120 s the job checks for a new global version and
   scores it with a confusion matrix, per device and overall (`device_id =
   'ALL'`): accuracy, precision, recall, F1 and the four counts. The
   always-benign baseline is `(TN + FP) / n`.
2. **Stream analysis.** Each device's mean reading over 30-second event-time
   windows (1-minute watermark) is compared with the fleet in the same window:
   `z = (device_mean − fleet_mean) / fleet_std`, flagged when |z| > 3 and at
   least 10 devices report. It complements RRCF: a device out of line with the
   fleet at one moment.
3. **Batch statistics.** Daily mean, standard deviation, min and max of the
   stream metric per device, from the CSVs.

### 3.6 Storage (TimescaleDB)

| Table | Written by | Contents |
| --- | --- | --- |
| `iot_data` | collector | Every streamed reading |
| `anomalies` | collector (from Flink) | Flagged readings with score, threshold, severity, label |
| `local_model_updates` | collector | Raw local model messages |
| `local_models` | aggregator | One row per received local update |
| `federated_models` | aggregator | One row per global version, with DP and clustering statistics |
| `model_evaluations` | Spark | Held-out metrics per version (overall and per device) |
| `stream_analysis_results` | Spark | Fleet z-score per device window |
| `batch_analysis_results` | Spark | Daily statistics per device |
| `dashboard_metrics` | metrics updater, Spark | Fleet KPI snapshots every 15 s; held-out accuracy and F1 after each evaluation |

All tables are hypertables. `00_init_database.py` recreates them on every start.

### 3.7 Observability

- **Grafana** (5 dashboards, generated by `grafana/build_dashboards.py`):
  Overview, Federated Learning & Privacy, Anomalies, Devices and Operations.
  Model panels show held-out metrics next to the always-benign baseline;
  Operations combines TimescaleDB freshness with Prometheus throughput, lag,
  JVM memory and alert states. The build rules (one data source uid each, no
  unbounded queries on the large tables) are unit tested.
- **Device viewer** (port 8082): each device's file, streamed / held-out split
  and attack share, next to its stored readings, anomalies, local models and
  held-out evaluation.
- **Metrics updater:** computes the expensive fleet aggregates once every 15 s
  (row totals, active and stale devices, anomaly rate, attack share). The
  monitoring page and the Prometheus exporter read that snapshot instead of
  scanning the large tables on every refresh.
- **Monitor** (port 5001): one page that answers "is it working and how
  well": an overall verdict with its reasons, the freshness of each stage,
  held-out accuracy and F1 against the baseline, the share of flagged
  readings that are attacks, the privacy budget, Flink's lag, recent rounds,
  firing alerts and links to every interface. It reads one cached
  `/api/overview` call built from bounded queries, Prometheus and the Flink
  REST API; it also serves `/metrics` for Prometheus and the Alertmanager
  webhook.
- **Prometheus + Alertmanager:** scrape the pipeline, Flink and Spark, and
  evaluate 16 rules. The rules cover the model below baseline or F1 under 0.5,
  no new global models, low training rate, ε above 50, high anomaly rate,
  stale devices, database errors, a stale KPI snapshot, targets down, Flink
  restarts, heap and falling behind the stream, and lost Spark workers.

## 4. Measured results

**Offline** (the same code, without Docker):

| Experiment | Result |
| --- | --- |
| RRCF, 4 × 256, two samples of 60 devices | ROC AUC 0.65 / 0.66; 81–86% of flagged readings are attacks (28% base rate) |
| Full per-reading path, 60 devices × 660 readings | 5.8% flagged, 64.5% of them attacks; global model accuracy 0.856, F1 0.73 without DP (baseline 0.723) |
| DP with 40 devices per round | accuracy 0.733, F1 0.58: noise dominates, hence the 200-device minimum |

**Live run** (14 Sep 2026, 77 minutes):

| Metric | Value |
| --- | --- |
| Devices per round | Median 307 (200–502), 69 rounds |
| Held-out accuracy / F1 | 0.770 / 0.553 at v1, 0.858 / 0.739 at v68 (precision 0.777, recall 0.704 on 19,981 readings; always-benign baseline 0.714) |
| Anomalies flagged | 5.8% of readings; 74% of them attacks |
| Privacy budget | ε = 9.35 after 69 rounds (δ = 1e-5); median noise std 0.016 |
| Flink throughput | Median 150 readings/s, the full stream rate. The earlier ~3-reading training trigger held Flink at ~105/s |
| Producer delivery | 694,999 of 695,000 confirmed, 0 failed |

## 5. Limitations

- **One broker, replication factor 1:** no fault tolerance. The design targets
  a single machine; [docs/KAFKA_SINGLE_BROKER.md](docs/KAFKA_SINGLE_BROKER.md)
  describes scaling out.
- **Linear model:** logistic regression limits accuracy. The federated
  mechanics (buffered deltas, DP, registry) do not depend on the model.
- **RRCF on its own:** it ranks readings usefully (AUC 0.66) but does not
  separate attacks; it is the label-free signal next to the classifier.
- **Privacy guarantee:** DP protects individual updates from anyone who sees
  the global models, not from the aggregator; the accountant is conservative.
- **Simulated fleet:** devices are random partitions of one dataset with
  shuffled order, so there is little real heterogeneity or temporal structure.
- **Lost readings:** when the producer replaces a stalled Kafka client, the
  readings that client had not delivered are dropped.
