"""
Flink Local Model Training Job

Real-time processing of the IoT stream, one reading at a time:

1. Anomaly detection with a Robust Random Cut Forest (RRCF) over all
   standardized features. Each Flink worker keeps one shared forest over the
   recent readings of the devices it serves, so every reading is compared with
   current fleet traffic. Per-device adaptive thresholds turn scores into
   anomalies, published to the `anomalies` topic with the reading's label.
2. Federated local training: each device trains a logistic regression on its
   recent labelled readings, starting from the latest global FedAvg model, and
   publishes its parameters to `local-model-updates` for the aggregator.

The models themselves live in flink_models.py (no PyFlink dependency, unit
tested); this file wires them into the Flink job.

NOTE: runs INSIDE the Flink cluster. Submitted by pipeline_orchestrator.py:
  docker exec flink-jobmanager flink run -d -py /opt/flink/scripts/03_flink_local_training.py
"""

import json
import logging
import os
import sys
from collections import defaultdict, deque
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

try:
    from pyflink.common import WatermarkStrategy
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.common.typeinfo import Types
    from pyflink.datastream import StreamExecutionEnvironment
    from pyflink.datastream.connectors.kafka import (
        KafkaOffsetsInitializer,
        KafkaRecordSerializationSchema,
        KafkaSink,
        KafkaSource,
    )
    from pyflink.datastream.functions import MapFunction, RuntimeContext
except ImportError:
    print("ERROR: PyFlink is not available. This job runs inside the Flink Docker cluster:")
    print("  docker exec flink-jobmanager flink run -d -py /opt/flink/scripts/03_flink_local_training.py")
    sys.exit(1)

import flink_models as fm  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker-1:9092")
INPUT_TOPIC = "edge-iiot-stream"
ANOMALY_OUTPUT_TOPIC = "anomalies"
MODEL_UPDATE_TOPIC = "local-model-updates"

# Shared RRCF per Flink worker (see docs/RCF_EXPLAINED.md for the evaluation)
RCF_NUM_TREES = int(os.getenv("FLEAD_RCF_TREES", "4"))
RCF_TREE_SIZE = int(os.getenv("FLEAD_RCF_TREE_SIZE", "256"))

# Local training trigger: every 30 new readings, once 20 are buffered. A device
# receives a reading about every 16 s, so it retrains about every 8 minutes:
# roughly 5 trainings/s fleet-wide and 300 devices per 60 s federated round.
# The former 45 s time trigger retrained after ~3 readings (~42 trainings/s at
# ~9 ms each) and held Flink at ~105 readings/s while 150/s were produced.
# The time trigger remains as a fallback for devices that stream slowly.
MODEL_TRAINING_INTERVAL_ROWS = 30
MODEL_TRAINING_INTERVAL_SECONDS = 900
LOCAL_WINDOW_SIZE = 200
MIN_TRAIN_SAMPLES = 20

# Per-device parameter files are written only on request: nothing in the
# pipeline reads them (parameters travel on local-model-updates)
SAVE_LOCAL_MODELS = os.getenv("FLEAD_SAVE_LOCAL_MODELS", "").lower() in ("1", "true", "yes")
MODEL_DIR = Path(os.getenv("LOCAL_MODEL_DIR", "/opt/flink/models/local"))


class AnomalyDetectionFunction(MapFunction):
    """
    Per-reading RRCF scoring and per-device federated local training.

    State is created in open() on each TaskManager worker. Readings of one
    device always reach the same worker (the stream is keyed by device_id).
    """

    def open(self, runtime_context: RuntimeContext):
        self.forest = fm.RandomCutForest(num_trees=RCF_NUM_TREES, tree_size=RCF_TREE_SIZE)
        self.thresholds = fm.AdaptiveThresholdManager() if fm.ADAPTIVE_THRESHOLD_ENABLED else None
        self.global_model_cache = fm.GlobalModelCache()
        self.feature_names: Optional[List[str]] = None  # fixed column order, set by the first reading
        self.local_models: Dict[str, fm.LocalLogisticModel] = {}
        # Recent labelled readings per device, stored as float32 vectors: 200
        # readings x 2,400 devices as dicts would take ~1.6 GB
        self.windows = defaultdict(lambda: deque(maxlen=LOCAL_WINDOW_SIZE))
        self.metric_windows = defaultdict(lambda: deque(maxlen=100))
        self.readings_since_training = defaultdict(int)
        self.last_training_time: Dict[str, float] = {}
        self.model_versions = defaultdict(int)

    # ---------------------------------------------------------------
    def _training_due(self, device_id: str, now: float) -> bool:
        if len(self.windows[device_id]) < MIN_TRAIN_SAMPLES:
            return False
        last = self.last_training_time.setdefault(device_id, now)
        if device_id in self.model_versions:
            needed = MODEL_TRAINING_INTERVAL_ROWS
        else:
            # Staggered first round, so devices do not all train at once
            needed = fm.first_training_readings(device_id, MIN_TRAIN_SAMPLES, MODEL_TRAINING_INTERVAL_ROWS)
        return (self.readings_since_training[device_id] >= needed
                or now - last >= MODEL_TRAINING_INTERVAL_SECONDS)

    def _severity(self, score: float, threshold: float) -> str:
        margin = score - threshold
        if margin > 0.3 or score > 0.8:
            return "critical"
        if margin > 0.15 or score > 0.6:
            return "warning"
        return "info"

    # ---------------------------------------------------------------
    def map(self, element: str) -> str:
        results: Dict[str, List[str]] = {"anomalies": [], "models": []}
        try:
            data = json.loads(element)
            device_id = data.get("device_id", "unknown")
            features = fm.extract_features(data)
            if not features:
                return json.dumps(results)
            if self.feature_names is None:
                self.feature_names = sorted(features)

            vector = np.array([features.get(n, 0.0) for n in self.feature_names], dtype=np.float32)
            raw_label = data.get("label")
            label = None if raw_label is None else int(float(raw_label))
            metric = float(data.get("data", 0.0))
            self.metric_windows[device_id].append(metric)

            # ---------------- RRCF anomaly detection ----------------
            score = self.forest.update(vector)
            threshold = self.thresholds.get_threshold(device_id) if self.thresholds else fm.BASE_ANOMALY_THRESHOLD
            is_anomaly = score > threshold
            if self.thresholds:
                self.thresholds.update(device_id, score, is_anomaly)

            if is_anomaly:
                results["anomalies"].append(json.dumps({
                    "device_id": device_id,
                    "value": metric,
                    "anomaly_score": score,
                    "raw_codisp": float(self.forest.last_raw_score),
                    "threshold": float(threshold),
                    "severity": self._severity(score, threshold),
                    "detection_method": "random_cut_forest",
                    # Ground-truth label of the flagged reading, so detection
                    # precision can be measured against the dataset labels
                    "label": label,
                    "timestamp": data.get("timestamp") or datetime.now().isoformat(),
                }))

            # ---------------- federated local training ----------------
            if label is not None:
                self.windows[device_id].append((vector, label))
                self.readings_since_training[device_id] += 1

            now = datetime.now().timestamp()
            if self._training_due(device_id, now):
                results["models"].append(self._train_local_model(device_id, now))

        except Exception as e:
            logger.error(f"Error in AnomalyDetectionFunction: {e}")
        return json.dumps(results)

    def _train_local_model(self, device_id: str, now: float) -> str:
        self.readings_since_training[device_id] = 0
        self.last_training_time[device_id] = now
        self.model_versions[device_id] += 1

        local = self.local_models.get(device_id)
        if local is None:
            local = fm.LocalLogisticModel(device_id)
            self.local_models[device_id] = local
        local.sync_from_global(self.global_model_cache.get(), self.feature_names)

        window = self.windows[device_id]
        X = np.stack([v for v, _ in window]).astype(float)
        if local.feature_names != self.feature_names:
            # Align columns with the model's feature order
            index = {name: i for i, name in enumerate(self.feature_names)}
            X = np.stack([X[:, index[n]] if n in index else np.zeros(len(X)) for n in local.feature_names], axis=1)
        labels = [y for _, y in window]

        loss, accuracy = local.train_arrays(X, labels)
        if SAVE_LOCAL_MODELS:
            self._save_local_model(local, self.model_versions[device_id])

        metrics = self.metric_windows[device_id]
        return json.dumps({
            "device_id": device_id,
            "model_version": self.model_versions[device_id],
            "base_global_version": local.base_global_version,
            "accuracy": float(accuracy),
            "loss": float(loss),
            "samples_processed": len(labels),
            "weights": local.weights_dict(),
            "bias": float(local.bias),
            "mean": float(np.mean(metrics)) if metrics else 0.0,
            "std": float(np.std(metrics)) if metrics else 0.0,
            "timestamp": datetime.now().isoformat(),
        })

    @staticmethod
    def _save_local_model(local: fm.LocalLogisticModel, version: int) -> None:
        """Latest parameters, one file per device (overwritten each round)."""
        try:
            MODEL_DIR.mkdir(parents=True, exist_ok=True)
            with open(MODEL_DIR / f"{local.device_id}.json", "w", encoding="utf-8") as f:
                json.dump(local.to_dict(version), f)
        except Exception as e:
            logger.error(f"Error saving model for {local.device_id}: {e}")


def main():
    logger.info("Starting Flink Local Training Job (Kafka: %s)", KAFKA_BROKER)
    logger.info("RRCF: shared forest per worker, %d trees x %d points", RCF_NUM_TREES, RCF_TREE_SIZE)

    env = StreamExecutionEnvironment.get_execution_environment()
    parallelism = int(os.getenv("FLINK_PARALLELISM", "2"))
    env.set_parallelism(parallelism)
    env.enable_checkpointing(60000)
    env.set_buffer_timeout(100)

    kafka_jar = "file:///opt/flink/usrlib/flink-sql-connector-kafka-3.1.0-1.18.jar"
    env.add_jars(kafka_jar)
    env.add_classpaths(kafka_jar)
    # Ship the model module to the TaskManagers' Python workers
    env.add_python_file(os.path.join(SCRIPT_DIR, "flink_models.py"))

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BROKER)
        .set_topics(INPUT_TOPIC)
        .set_group_id("flink-training")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-source")

    def device_key(element: str) -> str:
        try:
            return json.loads(element).get("device_id", "unknown")
        except Exception:
            return "unknown"

    processed = stream.key_by(device_key).map(AnomalyDetectionFunction(), output_type=Types.STRING())

    def extract(kind):
        return lambda element: "\n".join(json.loads(element).get(kind, []))

    def kafka_sink(topic: str) -> KafkaSink:
        return (
            KafkaSink.builder()
            .set_bootstrap_servers(KAFKA_BROKER)
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(topic)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            )
            .build()
        )

    # Each output line is one JSON message
    anomalies = processed.map(extract("anomalies"), output_type=Types.STRING()).filter(lambda x: len(x) > 0)
    models = processed.map(extract("models"), output_type=Types.STRING()).filter(lambda x: len(x) > 0)
    anomalies.flat_map(lambda x: x.split("\n"), output_type=Types.STRING()).sink_to(kafka_sink(ANOMALY_OUTPUT_TOPIC))
    models.flat_map(lambda x: x.split("\n"), output_type=Types.STRING()).sink_to(kafka_sink(MODEL_UPDATE_TOPIC))

    env.execute("Local Training Job")


if __name__ == "__main__":
    main()
