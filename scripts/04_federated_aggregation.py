"""
Federated Learning Aggregation Service

Aggregates per-device logistic-regression updates trained in Flink into a
global model with Federated Averaging (FedAvg), optionally with differential
privacy.

Subscribes to: local-model-updates   (device parameters after a local round)
Publishes to:  global-model-updates  (new global parameters + round metadata)
Writes:        TimescaleDB federated_models / local_models, and
               models/global/global_model_latest.json (read by Flink and Spark)

Round policy
    Updates are buffered (latest update per device). A round runs every
    ROUND_INTERVAL_SECONDS once at least MIN_DEVICES_FOR_AGGREGATION devices
    have reported. Each device contributes its update relative to the global
    version it trained from, so devices that report at different times are
    combined correctly (buffered asynchronous FedAvg, as in FedBuff,
    Nguyen et al. 2022).

Differential privacy (DP-FedAvg, McMahan et al. 2018)
    Each device update is clipped to L2 norm DP_CLIP_NORM, the clipped updates
    are averaged and Gaussian noise with std DP_NOISE_MULTIPLIER * DP_CLIP_NORM
    / n is added. The cumulative (epsilon, delta) budget is tracked with Renyi
    differential privacy (Mironov 2017) and stored with every round.

Model registry
    The last MAX_MODEL_VERSIONS_KEPT global versions are kept (parameters in
    memory, JSON files on disk). Spark writes held-out metrics for global
    versions to model_evaluations; the registry treats the version with the
    best held-out F1 as "best" and rolls back automatically when a newer
    evaluated version is worse by more than ROLLBACK_F1_DROP.

Update clustering
    Devices are grouped by the direction of their parameter updates. The
    number of groups and the agreement with the consensus direction show
    whether devices are learning the same thing (IID) or diverging.
"""

import json
import logging
import math
import os
import shutil
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import psycopg2
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

# ---------------------------------------------------------------------
# LOGGING
# ---------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------
# CONFIG LOADER (shared helper)
# ---------------------------------------------------------------------
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config_loader import get_db_config, get_kafka_config  # noqa: E402

KAFKA_BOOTSTRAP_SERVERS: List[str] = [
    s.strip() for s in get_kafka_config()["bootstrap_servers"].split(",") if s.strip()
]
INPUT_TOPIC = "local-model-updates"
OUTPUT_TOPIC = "global-model-updates"
ALERT_TOPIC = "system-alerts"
CONSUMER_GROUP = "federated-aggregation"

db_config = get_db_config()
DB_HOST = db_config["host"]
DB_PORT = db_config["port"]
DB_NAME = db_config["database"]
DB_USER = db_config["user"]
DB_PASSWORD = db_config["password"]


def _env_float(name: str, default: float) -> float:
    return float(os.getenv(name, default))


def _env_bool(name: str, default: bool) -> bool:
    return os.getenv(name, str(default)).strip().lower() in ("1", "true", "yes", "on")


# ---------------------------------------------------------------------
# MODEL STORAGE
# ---------------------------------------------------------------------
GLOBAL_MODELS_DIR = Path("models") / "global"
LATEST_MODEL_FILE = "global_model_latest.json"

# ---------------------------------------------------------------------
# AGGREGATION SETTINGS
# ---------------------------------------------------------------------
ROUND_INTERVAL_SECONDS = _env_float("FLEAD_ROUND_INTERVAL_SECONDS", 60)
# DP noise on the averaged update has std noise_multiplier * clip_norm / devices,
# so small rounds are noise-dominated. An offline run with 40 devices per round
# (std 0.125) left the global model at the always-benign baseline (F1 0.58,
# 0.73 without DP). 200 devices keeps the std at 0.025; the fleet has 2,400.
MIN_DEVICES_FOR_AGGREGATION = int(_env_float("FLEAD_MIN_DEVICES_PER_ROUND", 200))

MAX_MODEL_VERSIONS_KEPT = 10      # Global versions kept for rollback
ROLLBACK_F1_DROP = 0.10           # Roll back if held-out F1 falls this far below the best
ACCURACY_DEGRADATION_THRESHOLD = 0.05
MODEL_STALENESS_HOURS = 24
PERFORMANCE_HISTORY_SIZE = 50

# ---------------------------------------------------------------------
# DIFFERENTIAL PRIVACY SETTINGS
# ---------------------------------------------------------------------
DIFFERENTIAL_PRIVACY_ENABLED = _env_bool("FLEAD_DP_ENABLED", True)
DP_CLIP_NORM = _env_float("FLEAD_DP_CLIP_NORM", 1.0)
DP_NOISE_MULTIPLIER = _env_float("FLEAD_DP_NOISE_MULTIPLIER", 5.0)
DP_DELTA = 1e-5

# ---------------------------------------------------------------------
# UPDATE CLUSTERING SETTINGS
# ---------------------------------------------------------------------
DEVICE_CLUSTERING_ENABLED = True
CLUSTER_COSINE_THRESHOLD = 0.5    # Updates at least this similar share a cluster
CLUSTER_MIN_DEVICES = 3           # Smaller groups are not counted as clusters


# ---------------------------------------------------------------------
# ALERTS
# ---------------------------------------------------------------------
class AlertSeverity(Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass
class Alert:
    """Represents a system alert"""
    timestamp: datetime
    severity: AlertSeverity
    category: str
    message: str
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "severity": self.severity.value,
            "category": self.category,
            "message": self.message,
            "metadata": self.metadata,
        }


# ---------------------------------------------------------------------
# GLOBAL MODEL
# ---------------------------------------------------------------------
class GlobalModel:
    """
    Global federated model: logistic regression parameters (one weight per
    feature plus a bias) and aggregation metadata.
    """

    def __init__(self, version: int = 0):
        self.version = version
        self.feature_names: List[str] = []
        self.weights: Dict[str, float] = {}
        self.bias = 0.0
        # Sample-weighted mean of the devices' local TRAINING accuracy
        # (monitoring only; held-out metrics come from Spark)
        self.accuracy = 0.0
        self.created_at = datetime.now()
        self.num_devices_aggregated = 0
        self.aggregation_round = 0
        self.total_samples_processed = 0
        self.parent_version: Optional[int] = None
        self.rolled_back_from: Optional[int] = None

    def vector(self, feature_names: List[str]) -> np.ndarray:
        """Parameters laid out as [weights in feature_names order..., bias]."""
        return np.array([float(self.weights.get(n, 0.0)) for n in feature_names] + [float(self.bias)])

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "feature_names": self.feature_names,
            "weights": self.weights,
            "bias": self.bias,
            "accuracy": self.accuracy,
            "created_at": self.created_at.isoformat(),
            "num_devices_aggregated": self.num_devices_aggregated,
            "aggregation_round": self.aggregation_round,
            "total_samples_processed": self.total_samples_processed,
            "parent_version": self.parent_version,
            "rolled_back_from": self.rolled_back_from,
        }

    def save_json(self, path: Path) -> None:
        """Atomically write the model as JSON (read by Flink and Spark)."""
        tmp_path = path.with_name(path.name + ".tmp")
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f)
        os.replace(tmp_path, path)


# ---------------------------------------------------------------------
# MODEL REGISTRY
# ---------------------------------------------------------------------
@dataclass
class ModelVersion:
    """A global model version kept for evaluation tracking and rollback."""
    version: int
    train_accuracy: float
    num_devices: int
    aggregation_round: int
    created_at: datetime
    feature_names: List[str]
    weights: Dict[str, float]
    bias: float
    file_path: Optional[Path] = None
    heldout_accuracy: Optional[float] = None
    heldout_f1: Optional[float] = None
    rolled_back_from: Optional[int] = None

    def vector(self, feature_names: List[str]) -> np.ndarray:
        return np.array([float(self.weights.get(n, 0.0)) for n in feature_names] + [float(self.bias)])

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "train_accuracy": self.train_accuracy,
            "heldout_accuracy": self.heldout_accuracy,
            "heldout_f1": self.heldout_f1,
            "num_devices": self.num_devices,
            "aggregation_round": self.aggregation_round,
            "created_at": self.created_at.isoformat(),
            "rolled_back_from": self.rolled_back_from,
        }


class ModelRegistry:
    """
    Keeps the most recent global versions (parameters + held-out metrics).

    Older versions are evicted and their files deleted, so disk use stays
    bounded (previous versions were archived forever).
    """

    def __init__(self, models_dir: Path = GLOBAL_MODELS_DIR, max_versions: int = MAX_MODEL_VERSIONS_KEPT):
        self.models_dir = models_dir
        self.max_versions = max_versions
        self.versions: Dict[int, ModelVersion] = {}
        self._lock = threading.Lock()

    def register(self, model: GlobalModel) -> ModelVersion:
        path = self.models_dir / f"global_model_v{model.version}.json"
        model.save_json(path)
        entry = ModelVersion(
            version=model.version,
            train_accuracy=model.accuracy,
            num_devices=model.num_devices_aggregated,
            aggregation_round=model.aggregation_round,
            created_at=model.created_at,
            feature_names=list(model.feature_names),
            weights=dict(model.weights),
            bias=model.bias,
            file_path=path,
            rolled_back_from=model.rolled_back_from,
        )
        with self._lock:
            self.versions[model.version] = entry
            for old in sorted(self.versions)[:-self.max_versions]:
                evicted = self.versions.pop(old)
                if evicted.file_path and evicted.file_path.exists():
                    evicted.file_path.unlink()
        return entry

    def get(self, version: Optional[int]) -> Optional[ModelVersion]:
        return self.versions.get(version) if version is not None else None

    def record_evaluations(self, results: Dict[int, Tuple[float, float]]) -> None:
        """results: version -> (held-out accuracy, held-out F1)."""
        with self._lock:
            for version, (accuracy, f1) in results.items():
                if version in self.versions:
                    self.versions[version].heldout_accuracy = accuracy
                    self.versions[version].heldout_f1 = f1

    def evaluated(self) -> List[ModelVersion]:
        return sorted((v for v in self.versions.values() if v.heldout_f1 is not None), key=lambda v: v.version)

    def best(self) -> Optional[ModelVersion]:
        evaluated = self.evaluated()
        return max(evaluated, key=lambda v: v.heldout_f1) if evaluated else None

    def get_registry_status(self) -> Dict[str, Any]:
        best = self.best()
        return {
            "total_versions": len(self.versions),
            "best_version": best.version if best else None,
            "best_heldout_f1": best.heldout_f1 if best else None,
            "versions": [v.to_dict() for v in sorted(self.versions.values(), key=lambda v: -v.version)[:5]],
        }


# ---------------------------------------------------------------------
# PERFORMANCE MONITOR
# ---------------------------------------------------------------------
class PerformanceMonitor:
    """Tracks rounds and raises alerts (training-accuracy drops, stale devices, low participation)."""

    def __init__(self, history_size: int = PERFORMANCE_HISTORY_SIZE):
        self.accuracy_history: deque = deque(maxlen=history_size)
        self.device_last_seen: Dict[str, datetime] = {}
        self.alerts: List[Alert] = []
        self.alert_callback = None
        self._lock = threading.Lock()

    def record_aggregation(self, version: int, accuracy: float, num_devices: int, device_ids: List[str]) -> List[Alert]:
        new_alerts: List[Alert] = []
        with self._lock:
            now = datetime.now()
            for device_id in device_ids:
                self.device_last_seen[device_id] = now

            if len(self.accuracy_history) >= 3:
                recent_avg = float(np.mean(list(self.accuracy_history)[-3:]))
                if accuracy < recent_avg - ACCURACY_DEGRADATION_THRESHOLD:
                    new_alerts.append(Alert(
                        timestamp=now,
                        severity=AlertSeverity.WARNING,
                        category="train_accuracy_degradation",
                        message=f"Mean local training accuracy dropped from {recent_avg:.2%} to {accuracy:.2%}",
                        metadata={"version": version, "current": accuracy, "recent_average": recent_avg},
                    ))
            self.accuracy_history.append(accuracy)

            stale_threshold = now - timedelta(hours=MODEL_STALENESS_HOURS)
            stale = [d for d, seen in self.device_last_seen.items() if seen < stale_threshold]
            if stale and len(stale) > len(self.device_last_seen) * 0.3:
                new_alerts.append(Alert(
                    timestamp=now,
                    severity=AlertSeverity.WARNING,
                    category="stale_devices",
                    message=f"{len(stale)} devices have not sent updates in {MODEL_STALENESS_HOURS}h",
                    metadata={"stale_devices": stale[:10]},
                ))

            if num_devices < MIN_DEVICES_FOR_AGGREGATION * 2:
                new_alerts.append(Alert(
                    timestamp=now,
                    severity=AlertSeverity.INFO,
                    category="low_participation",
                    message=f"Only {num_devices} devices participated in round {version}",
                    metadata={"num_devices": num_devices},
                ))

            self.alerts = (self.alerts + new_alerts)[-100:]

        if self.alert_callback:
            for alert in new_alerts:
                self.alert_callback(alert)
        return new_alerts

    def get_performance_summary(self) -> Dict[str, Any]:
        history = list(self.accuracy_history)
        return {
            "total_rounds": len(history),
            "current_train_accuracy": history[-1] if history else 0.0,
            "average_train_accuracy": float(np.mean(history)) if history else 0.0,
            "active_devices": len(self.device_last_seen),
            "recent_alerts": [a.to_dict() for a in self.alerts[-5:]],
        }


# ---------------------------------------------------------------------
# DIFFERENTIAL PRIVACY
# ---------------------------------------------------------------------
RDP_ORDERS = [1.0 + x / 10.0 for x in range(1, 100)] + [float(a) for a in range(11, 1025)]


def gaussian_rdp_epsilon(noise_multiplier: float, rounds: int, delta: float = DP_DELTA) -> float:
    """
    (epsilon, delta) after `rounds` compositions of the Gaussian mechanism.

    Renyi DP of one Gaussian mechanism with noise multiplier sigma at order
    alpha is alpha / (2 sigma^2); RDP composes additively over rounds and
    converts to (epsilon, delta)-DP as

        epsilon = min_alpha  rounds * alpha / (2 sigma^2) + log(1/delta) / (alpha - 1)

    (Mironov 2017). No subsampling amplification is applied: every device is
    assumed to take part in every round, so this is a conservative
    device-level bound (neighbouring datasets differ by one device).
    """
    if rounds <= 0:
        return 0.0
    if noise_multiplier <= 0:
        return math.inf
    return min(
        rounds * alpha / (2.0 * noise_multiplier ** 2) + math.log(1.0 / delta) / (alpha - 1.0)
        for alpha in RDP_ORDERS
    )


class DifferentialPrivacy:
    """
    DP-FedAvg aggregation of device updates.

    Enabled:  clip each update to `clip_norm`, average with equal weight and
              add N(0, (noise_multiplier * clip_norm / n)^2) per coordinate.
    Disabled: plain FedAvg, the sample-weighted mean of the updates.

    Noise is scaled by the round size n, which is treated as public.
    """

    def __init__(
        self,
        clip_norm: float = DP_CLIP_NORM,
        noise_multiplier: float = DP_NOISE_MULTIPLIER,
        delta: float = DP_DELTA,
        enabled: bool = DIFFERENTIAL_PRIVACY_ENABLED,
        seed: Optional[int] = None,
    ):
        self.clip_norm = clip_norm
        self.noise_multiplier = noise_multiplier
        self.delta = delta
        self.enabled = enabled
        self.rounds_processed = 0
        self.clipped_updates = 0
        self.total_updates = 0
        self.rng = np.random.default_rng(seed)
        self._lock = threading.Lock()
        if enabled:
            logger.info(f"🔐 DP-FedAvg enabled: clip_norm={clip_norm}, noise_multiplier={noise_multiplier}, delta={delta}")
        else:
            logger.info("DP disabled: plain FedAvg (sample-weighted mean)")

    @property
    def epsilon(self) -> Optional[float]:
        return gaussian_rdp_epsilon(self.noise_multiplier, self.rounds_processed, self.delta) if self.enabled else None

    def aggregate_updates(self, deltas: List[np.ndarray], sample_counts: List[int]) -> Tuple[np.ndarray, Dict[str, Any]]:
        deltas = [np.asarray(d, dtype=float) for d in deltas]
        n = len(deltas)

        if not self.enabled:
            counts = np.asarray(sample_counts, dtype=float)
            weights = counts / counts.sum() if counts.sum() > 0 else np.full(n, 1.0 / n)
            update = np.sum([w * d for w, d in zip(weights, deltas)], axis=0)
            return update, {"dp_applied": False, "noise_std": 0.0, "clipped_updates": 0, "epsilon": None}

        with self._lock:
            clipped, n_clipped = [], 0
            for d in deltas:
                norm = float(np.linalg.norm(d))
                if norm > self.clip_norm:
                    d = d * (self.clip_norm / norm)
                    n_clipped += 1
                clipped.append(d)

            noise_std = self.noise_multiplier * self.clip_norm / n
            update = np.mean(clipped, axis=0) + self.rng.normal(0.0, noise_std, size=clipped[0].shape)

            self.rounds_processed += 1
            self.clipped_updates += n_clipped
            self.total_updates += n

            metadata = {
                "dp_applied": True,
                "clip_norm": self.clip_norm,
                "noise_multiplier": self.noise_multiplier,
                "noise_std": noise_std,
                "clipped_updates": n_clipped,
                "num_updates": n,
                "epsilon": self.epsilon,
                "delta": self.delta,
            }
            logger.info(f"🔐 DP-FedAvg: clipped {n_clipped}/{n} updates, noise std={noise_std:.5f}, "
                        f"epsilon={metadata['epsilon']:.2f} after {self.rounds_processed} rounds")
            return update, metadata

    def get_privacy_status(self) -> Dict[str, Any]:
        return {
            "enabled": self.enabled,
            "clip_norm": self.clip_norm,
            "noise_multiplier": self.noise_multiplier,
            "delta": self.delta,
            "rounds_processed": self.rounds_processed,
            "epsilon": self.epsilon,
            "clipped_update_fraction": self.clipped_updates / self.total_updates if self.total_updates else 0.0,
        }


# ---------------------------------------------------------------------
# UPDATE CLUSTERING
# ---------------------------------------------------------------------
class UpdateClusterer:
    """
    Groups devices by the direction of their parameter updates.

    Greedy leader clustering on cosine similarity: a device joins the first
    cluster whose leader update has cosine >= `threshold`, otherwise it starts
    a new cluster. Reports the number of clusters with at least `min_devices`
    members, each device's agreement with the consensus (mean) direction and
    the devices pointing away from it (cosine < 0). One dominant cluster and
    high agreement mean devices learn the same pattern; many clusters or many
    divergent devices point to non-IID data or misbehaving devices.

    Informational: the global model is still a single FedAvg model. (The
    previous version clustered devices by their reported accuracy numbers.)
    """

    def __init__(self, threshold: float = CLUSTER_COSINE_THRESHOLD, min_devices: int = CLUSTER_MIN_DEVICES,
                 enabled: bool = DEVICE_CLUSTERING_ENABLED):
        self.threshold = threshold
        self.min_devices = min_devices
        self.enabled = enabled
        self.last_result: Dict[str, Any] = {}

    def analyse(self, device_ids: List[str], deltas: List[np.ndarray]) -> Dict[str, Any]:
        if not self.enabled or not deltas:
            return {"enabled": self.enabled, "num_clusters": None, "mean_update_cosine": None}

        D = np.asarray(deltas, dtype=float)
        norms = np.linalg.norm(D, axis=1, keepdims=True)
        U = D / np.maximum(norms, 1e-12)

        consensus = U.mean(axis=0)
        consensus_norm = float(np.linalg.norm(consensus))
        agreement = U @ (consensus / consensus_norm) if consensus_norm > 0 else np.zeros(len(U))

        leaders: List[np.ndarray] = []
        sizes: List[int] = []
        for u in U:
            if leaders:
                sims = np.asarray(leaders) @ u
                best = int(np.argmax(sims))
                if sims[best] >= self.threshold:
                    sizes[best] += 1
                    continue
            leaders.append(u)
            sizes.append(1)

        divergent = [device_ids[i] for i in np.where(agreement < 0)[0]]
        self.last_result = {
            "enabled": True,
            "num_clusters": sum(1 for s in sizes if s >= self.min_devices),
            "cluster_sizes": sorted(sizes, reverse=True)[:10],
            "mean_update_cosine": float(np.mean(agreement)),
            "num_divergent_devices": len(divergent),
            "divergent_devices": divergent[:20],
        }
        return self.last_result

    def get_cluster_status(self) -> Dict[str, Any]:
        return self.last_result or {"enabled": self.enabled}


# ---------------------------------------------------------------------
# AGGREGATOR
# ---------------------------------------------------------------------
class FederatedAggregator:
    """Buffered FedAvg aggregator with DP, model registry/rollback, clustering and alerts."""

    def __init__(self, producer: Optional[KafkaProducer], connect_db: bool = True,
                 models_dir: Path = GLOBAL_MODELS_DIR):
        self.producer = producer
        self.models_dir = models_dir
        self.global_model = GlobalModel(version=0)
        self.pending_updates: Dict[str, Dict[str, Any]] = {}
        self.aggregation_round = 0
        self.last_round_at = time.time()
        self.db_connection = None

        self.model_registry = ModelRegistry(models_dir=models_dir)
        self.performance_monitor = PerformanceMonitor()
        self.performance_monitor.alert_callback = self._handle_alert
        self.differential_privacy = DifferentialPrivacy()
        self.clusterer = UpdateClusterer()

        self._reset_model_dir()
        if connect_db:
            self._connect_database()

    # ----------------------------------------------------------- storage
    def _reset_model_dir(self) -> None:
        """
        Start from a clean model directory. database-init resets the tables on
        every START, and the aggregator restarts its version numbers at 1, so
        snapshots left in the models_global volume from a previous run would be
        mistaken for current versions.
        """
        self.models_dir.mkdir(parents=True, exist_ok=True)
        removed = 0
        for path in self.models_dir.iterdir():
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
            removed += 1
        if removed:
            logger.info("Cleared %d stale entries from %s", removed, self.models_dir)

    # ----------------------------------------------------------- alerts
    def _handle_alert(self, alert: Alert) -> None:
        level = logging.INFO if alert.severity == AlertSeverity.INFO else logging.WARNING
        logger.log(level, f"ALERT [{alert.category}]: {alert.message}")
        if self.producer is not None:
            try:
                self.producer.send(ALERT_TOPIC, value={**alert.to_dict(), "type": "system_alert"})
            except Exception as e:
                logger.debug(f"Could not publish alert to Kafka: {e}")

    def get_system_status(self) -> Dict[str, Any]:
        return {
            "global_model": self.global_model.to_dict(),
            "aggregation_round": self.aggregation_round,
            "pending_devices": len(self.pending_updates),
            "model_registry": self.model_registry.get_registry_status(),
            "performance": self.performance_monitor.get_performance_summary(),
            "differential_privacy": self.differential_privacy.get_privacy_status(),
            "update_clustering": self.clusterer.get_cluster_status(),
            "timestamp": datetime.now().isoformat(),
        }

    # ----------------------------------------------------------- database
    def _connect_database(self) -> None:
        """Tables are created by the database-init service (docker-compose waits for it)."""
        self.db_connection = psycopg2.connect(
            host=DB_HOST, port=DB_PORT, database=DB_NAME, user=DB_USER, password=DB_PASSWORD,
        )
        self.db_connection.autocommit = False
        logger.info("✓ Connected to TimescaleDB")

    def _execute(self, sql: str, params: Tuple, fetch: bool = False):
        if self.db_connection is None:
            return None
        try:
            if self.db_connection.closed:
                self._connect_database()
            with self.db_connection.cursor() as cursor:
                cursor.execute(sql, params)
                rows = cursor.fetchall() if fetch else None
            self.db_connection.commit()
            return rows
        except Exception as e:
            try:
                self.db_connection.rollback()
            except Exception:
                pass
            logger.warning(f"Database error: {e}")
            return None

    # ----------------------------------------------------------- updates
    def process_local_model_update(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Buffer a device update; run a round when one is due. Returns the round result, if any."""
        device_id = record.get("device_id")
        weights = record.get("weights") or {}
        if not device_id or not weights:
            logger.warning("Ignoring local model update without device_id or weights")
            return None

        update = {
            "device_id": device_id,
            "model_version": int(record.get("model_version", 0)),
            "base_global_version": int(record.get("base_global_version") or 0),
            "accuracy": float(record.get("accuracy", 0.0)),
            "samples_processed": int(record.get("samples_processed", 0)),
            "weights": {k: float(v) for k, v in weights.items()},
            "bias": float(record.get("bias", 0.0)),
        }
        # Latest update per device: a device that trained twice since the last
        # round contributes once, with its most recent parameters
        self.pending_updates[device_id] = update

        self._execute(
            "INSERT INTO local_models (device_id, model_version, global_version, accuracy, samples_processed) "
            "VALUES (%s, %s, %s, %s, %s)",
            (device_id, update["model_version"], int(self.global_model.version),
             update["accuracy"], update["samples_processed"]),
        )
        return self.maybe_aggregate()

    def round_due(self, now: Optional[float] = None) -> bool:
        now = time.time() if now is None else now
        return (len(self.pending_updates) >= MIN_DEVICES_FOR_AGGREGATION
                and now - self.last_round_at >= ROUND_INTERVAL_SECONDS)

    def maybe_aggregate(self) -> Optional[Dict[str, Any]]:
        if not self.round_due():
            return None
        result = self.aggregate()
        if result is not None:
            self._publish_global_update(result)
            self._refresh_evaluations_and_rollback()
        return result

    # ----------------------------------------------------------- FedAvg
    def _base_vector(self, base_version: int, feature_names: List[str]) -> np.ndarray:
        """Parameters the device started from (zeros for the initial model)."""
        if base_version == self.global_model.version:
            return self.global_model.vector(feature_names)
        entry = self.model_registry.get(base_version)
        if entry is not None:
            return entry.vector(feature_names)
        if base_version == 0:
            return np.zeros(len(feature_names) + 1)
        # Older than the registry keeps: measure against the current model
        return self.global_model.vector(feature_names)

    def aggregate(self) -> Optional[Dict[str, Any]]:
        """
        One FedAvg round over the buffered device updates:

            delta_i  = w_i - w_base(i)                       (update since the version device i trained from)
            w_global = w_global + aggregate_i(delta_i)       (weighted mean, or DP clip + mean + noise)
        """
        updates = list(self.pending_updates.values())
        num_devices = len(updates)
        if num_devices < MIN_DEVICES_FOR_AGGREGATION:
            return None

        feature_names = sorted(set(self.global_model.feature_names).union(
            name for u in updates for name in u["weights"]))
        current = self.global_model.vector(feature_names)

        deltas, counts, device_ids = [], [], []
        for u in updates:
            local = np.array([u["weights"].get(n, 0.0) for n in feature_names] + [u["bias"]])
            deltas.append(local - self._base_vector(u["base_global_version"], feature_names))
            counts.append(u["samples_processed"])
            device_ids.append(u["device_id"])

        clustering = self.clusterer.analyse(device_ids, deltas)
        aggregated, dp = self.differential_privacy.aggregate_updates(deltas, counts)
        new_vector = current + aggregated

        total_samples = int(sum(counts))
        train_accuracy = (sum(u["accuracy"] * u["samples_processed"] for u in updates) / total_samples
                          if total_samples else 0.0)

        model = self.global_model
        model.parent_version = model.version
        model.version += 1
        model.feature_names = feature_names
        model.weights = {n: float(w) for n, w in zip(feature_names, new_vector[:-1])}
        model.bias = float(new_vector[-1])
        model.accuracy = float(train_accuracy)
        model.num_devices_aggregated = num_devices
        self.aggregation_round += 1
        model.aggregation_round = self.aggregation_round
        model.total_samples_processed = total_samples
        model.rolled_back_from = None
        model.created_at = datetime.now()

        update_norm = float(np.linalg.norm(aggregated))
        self._publish_model_files(model)
        self.model_registry.register(model)
        alerts = self.performance_monitor.record_aggregation(model.version, train_accuracy, num_devices, device_ids)
        self._save_round_to_db(model, total_samples, update_norm, clustering, dp)

        self.pending_updates.clear()
        self.last_round_at = time.time()

        logger.info(
            "Round %d → global v%d: %d devices, %d samples, mean local train acc %.2f%%, update norm %.4f, "
            "update agreement %s, clusters %s",
            self.aggregation_round, model.version, num_devices, total_samples, train_accuracy * 100,
            update_norm, _fmt(clustering.get("mean_update_cosine")), clustering.get("num_clusters"),
        )

        return {
            "version": model.version,
            "aggregation_round": self.aggregation_round,
            "global_train_accuracy": train_accuracy,
            "num_devices": num_devices,
            "total_samples": total_samples,
            "feature_names": feature_names,
            "weights": model.weights,
            "bias": model.bias,
            "update_norm": update_norm,
            "differential_privacy": dp,
            "update_clustering": clustering,
            "alerts_triggered": len(alerts),
            "timestamp": datetime.now().isoformat(),
        }

    def _publish_model_files(self, model: GlobalModel) -> None:
        try:
            model.save_json(self.models_dir / LATEST_MODEL_FILE)
        except Exception as e:
            logger.error(f"✗ Error publishing latest global model: {e}")

    def _save_round_to_db(self, model: GlobalModel, total_samples: int, update_norm: float,
                          clustering: Dict[str, Any], dp: Dict[str, Any]) -> None:
        self._execute(
            """
            INSERT INTO federated_models
                (global_version, aggregation_round, num_devices, accuracy, total_samples, update_norm,
                 mean_update_cosine, num_clusters, dp_noise_std, dp_clipped_updates, dp_epsilon)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                int(model.version), int(model.aggregation_round), int(model.num_devices_aggregated),
                float(model.accuracy), int(total_samples), float(update_norm),
                _float_or_none(clustering.get("mean_update_cosine")),
                clustering.get("num_clusters"),
                float(dp.get("noise_std") or 0.0),
                int(dp.get("clipped_updates") or 0),
                _float_or_none(dp.get("epsilon")),
            ),
        )

    # ----------------------------------------------------------- registry / rollback
    def _refresh_evaluations_and_rollback(self) -> None:
        """Pull Spark's held-out metrics for registry versions; roll back on a large F1 drop."""
        versions = [str(v) for v in self.model_registry.versions]
        rows = self._execute(
            "SELECT DISTINCT ON (model_version) model_version, model_accuracy, f1_score FROM model_evaluations "
            "WHERE device_id = 'ALL' AND model_version = ANY(%s) ORDER BY model_version, evaluation_timestamp DESC",
            (versions,), fetch=True,
        )
        if rows:
            self.model_registry.record_evaluations({int(v): (float(a), float(f)) for v, a, f in rows})
        self.check_rollback()

    def check_rollback(self) -> bool:
        evaluated = self.model_registry.evaluated()
        best = self.model_registry.best()
        if not evaluated or best is None:
            return False
        latest = evaluated[-1]
        if latest.version == best.version or latest.heldout_f1 >= best.heldout_f1 - ROLLBACK_F1_DROP:
            return False
        # Only roll back once per degraded version
        if self.global_model.rolled_back_from is not None or any(
                v.rolled_back_from == best.version and v.version > latest.version
                for v in self.model_registry.versions.values()):
            return False
        return self.rollback_to(best.version, reason=f"held-out F1 of v{latest.version} is "
                                                     f"{latest.heldout_f1:.3f} vs {best.heldout_f1:.3f} for v{best.version}")

    def rollback_to(self, target_version: int, reason: str = "manual") -> bool:
        """Publish the parameters of a registry version as a new global version."""
        source = self.model_registry.get(target_version)
        if source is None:
            logger.error("Cannot roll back: version %s is not in the registry", target_version)
            return False

        model = self.global_model
        model.parent_version = model.version
        model.version += 1
        model.feature_names = list(source.feature_names)
        model.weights = dict(source.weights)
        model.bias = source.bias
        model.accuracy = source.train_accuracy
        model.num_devices_aggregated = 0
        model.total_samples_processed = 0
        model.rolled_back_from = target_version
        model.created_at = datetime.now()

        self._publish_model_files(model)
        self.model_registry.register(model)
        self._save_round_to_db(model, 0, 0.0, {}, {"noise_std": 0.0, "clipped_updates": 0,
                                                   "epsilon": self.differential_privacy.epsilon})
        self._handle_alert(Alert(
            timestamp=datetime.now(), severity=AlertSeverity.WARNING, category="model_rollback",
            message=f"Rolled back to v{target_version} as v{model.version}: {reason}",
            metadata={"new_version": model.version, "source_version": target_version},
        ))
        self._publish_global_update({
            "type": "model_rollback",
            "version": model.version,
            "source_version": target_version,
            "feature_names": model.feature_names,
            "weights": model.weights,
            "bias": model.bias,
            "reason": reason,
            "timestamp": datetime.now().isoformat(),
        })
        return True

    # ----------------------------------------------------------- Kafka
    def _publish_global_update(self, update: Dict[str, Any]) -> None:
        if self.producer is None:
            return
        try:
            self.producer.send(OUTPUT_TOPIC, value=update)
            self.producer.flush()
            logger.info("✓ Published global model v%s to '%s'", update.get("version"), OUTPUT_TOPIC)
        except Exception as e:
            logger.error(f"Error publishing global model update: {e}", exc_info=True)


def _float_or_none(value: Any) -> Optional[float]:
    if value is None:
        return None
    value = float(value)
    return value if math.isfinite(value) else None


def _fmt(value: Optional[float]) -> str:
    return "n/a" if value is None else f"{value:.3f}"


# ---------------------------------------------------------------------
# KAFKA HELPERS
# ---------------------------------------------------------------------
def _json_default(obj: Any) -> Any:
    """json.dumps fallback for numpy scalars/arrays and datetimes."""
    if isinstance(obj, np.generic):
        return obj.item()
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")


def _with_retries(factory, what: str, max_retries: int = 30, delay: float = 5.0):
    last_err: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            client = factory()
            logger.info("✓ Aggregator %s connected (attempt %d)", what, attempt)
            return client
        except NoBrokersAvailable as e:
            last_err = e
            logger.warning("Kafka not ready yet for aggregator %s (attempt %d/%d)", what, attempt, max_retries)
        except Exception as e:
            last_err = e
            logger.warning("Error creating aggregator %s (attempt %d/%d): %s", what, attempt, max_retries, e)
        time.sleep(delay)
    raise last_err or RuntimeError(f"Unable to create Kafka {what}")


def create_kafka_producer() -> KafkaProducer:
    return _with_retries(lambda: KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=_json_default).encode("utf-8"),
        acks="all",
        retries=5,
    ), "producer")


def create_kafka_consumer() -> KafkaConsumer:
    return _with_retries(lambda: KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
    ), "consumer")


# ---------------------------------------------------------------------
# MAIN SERVICE LOOP
# ---------------------------------------------------------------------
def main() -> None:
    logger.info("=" * 70)
    logger.info("Federated Learning Aggregation Service")
    logger.info("=" * 70)
    logger.info("Input topic: %s | Output topic: %s", INPUT_TOPIC, OUTPUT_TOPIC)
    logger.info("Kafka: %s | TimescaleDB: %s:%s/%s", ", ".join(KAFKA_BOOTSTRAP_SERVERS), DB_HOST, DB_PORT, DB_NAME)
    logger.info("Rounds: every %.0fs with >= %d devices", ROUND_INTERVAL_SECONDS, MIN_DEVICES_FOR_AGGREGATION)
    logger.info("=" * 70)

    consumer: Optional[KafkaConsumer] = None
    producer: Optional[KafkaProducer] = None
    aggregator: Optional[FederatedAggregator] = None
    try:
        producer = create_kafka_producer()
        aggregator = FederatedAggregator(producer=producer)
        consumer = create_kafka_consumer()
        logger.info("Waiting for local model updates... (Ctrl+C to stop)")

        while True:
            # poll() instead of iterating, so a due round also runs when
            # updates pause
            batches = consumer.poll(timeout_ms=1000)
            for messages in batches.values():
                for message in messages:
                    aggregator.process_local_model_update(message.value)
            aggregator.maybe_aggregate()

    except KeyboardInterrupt:
        logger.info("Service interrupted by user")
    except Exception as e:
        logger.error(f"Error in aggregation service: {e}", exc_info=True)
    finally:
        logger.info("Shutting down federated aggregation service...")
        for closer in (
            lambda: consumer and consumer.close(),
            lambda: producer and (producer.flush(), producer.close()),
            lambda: aggregator and aggregator.db_connection and aggregator.db_connection.close(),
        ):
            try:
                closer()
            except Exception:
                pass
        logger.info("✓ Service stopped cleanly")


if __name__ == "__main__":
    main()
