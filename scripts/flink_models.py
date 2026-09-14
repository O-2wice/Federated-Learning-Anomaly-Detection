"""
Streaming models used by the Flink job (03_flink_local_training.py).

Kept free of PyFlink so they can be unit-tested and reused. The Flink job ships
this file to the TaskManagers with `env.add_python_file`.

- RandomCutForest       Robust Random Cut Forest anomaly scoring (rrcf)
- AdaptiveThresholdManager  per-device anomaly thresholds tuned to a target rate
- LocalLogisticModel    per-device logistic regression trained on true labels
- GlobalModelCache      latest FedAvg global model published by the aggregator
- extract_features      numeric model features of a stream message
"""

import json
import logging
import os
import time
import zlib
from collections import defaultdict, deque
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import rrcf

logger = logging.getLogger(__name__)

# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------
# Message keys that are metadata rather than model features
RESERVED_MESSAGE_KEYS = {"device_id", "label", "timestamp", "source_timestamp", "data", "value"}

# Local training. Chosen with an offline experiment on the device files
# (300 devices, 60 rounds): learning rate 0.2 and positive-class weight 2.5
# (roughly the benign/attack ratio of 72/28) raised held-out F1 from 0.67 to
# 0.75 by lifting attack recall from 52% to 70%.
LEARNING_RATE = 0.2
POSITIVE_CLASS_WEIGHT = 2.5
BATCH_SIZE = 32
L2_REGULARIZATION = 1e-4
LOCAL_EPOCHS = 3

# Anomaly thresholds (scores are on a 0-1 scale)
BASE_ANOMALY_THRESHOLD = 0.4
ADAPTIVE_THRESHOLD_ENABLED = True
THRESHOLD_ADAPTATION_WINDOW = 100   # Recent scores considered when adapting
MIN_THRESHOLD = 0.2
MAX_THRESHOLD = 0.8
TARGET_ANOMALY_RATE = 0.05          # Aim to flag about 5% of readings
THRESHOLD_ADJUSTMENT_FACTOR = 0.02

GLOBAL_MODEL_PATH = Path(
    os.getenv("GLOBAL_MODEL_PATH", "/opt/flink/models/global/global_model_latest.json")
)


def extract_features(record: Dict[str, Any]) -> Dict[str, float]:
    """Numeric model features of a stream message (metadata keys excluded)."""
    features: Dict[str, float] = {}
    for key, raw in record.items():
        if key in RESERVED_MESSAGE_KEYS or raw is None:
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        if np.isfinite(value):
            features[key] = value
    return features


def first_training_readings(device_id: str, min_samples: int, interval: int) -> int:
    """
    Readings a device buffers before its first local training round.

    Devices are streamed round-robin, so with a common threshold every device
    reaches it at the same moment and all of them train in one burst every
    `interval` readings: rounds of 200 and 2,200 devices, then nothing for
    minutes. A stable per-device offset in [0, interval) spreads the first
    rounds, and therefore every later one, evenly over the interval.
    """
    return min_samples + zlib.crc32(device_id.encode("utf-8")) % interval


# -------------------------------------------------------------------
# Robust Random Cut Forest
# -------------------------------------------------------------------
class RandomCutForest:
    """
    Robust Random Cut Forest (Guha et al., 2016) on feature-vector points,
    built on `rrcf`, the reference open-source implementation.

    Each tree is a random cut tree: points are separated by random cuts whose
    dimension is chosen in proportion to the bounding-box range, so isolated
    points sit close to the root. A point's anomaly score is its collusive
    displacement (CoDisp) averaged over the trees. Every tree keeps a sliding
    window of the most recent `tree_size` points, so the model follows drift
    without retraining.

    The raw CoDisp is converted to 0-1 by its percentile rank among recent
    scores: 0 for the bottom 90%, rising linearly to 1 at the very top. The
    0.4 base threshold therefore flags roughly the top 6% of readings.

    Readings are scored as points over all features rather than as shingles
    of one value: within a device the readings have no meaningful order, and
    an offline test showed a one-feature shingle scored at chance (AUC 0.51).
    """

    def __init__(self, num_trees: int, tree_size: int, score_history: int = 500, seed: Optional[int] = None):
        if seed is not None:
            np.random.seed(seed)  # rrcf draws its random cuts from numpy's global generator
        self.num_trees = num_trees
        self.tree_size = tree_size
        self.trees = [rrcf.RCTree() for _ in range(num_trees)]
        self.points_seen = 0
        self.score_history: deque = deque(maxlen=score_history)
        self.last_raw_score = 0.0

    def update(self, point: Sequence[float]) -> float:
        """Insert a point and return its anomaly score in [0, 1]."""
        x = np.asarray(point, dtype=float)
        index = self.points_seen
        self.points_seen += 1

        total = 0.0
        for tree in self.trees:
            if len(tree.leaves) > self.tree_size:
                tree.forget_point(index - self.tree_size)
            tree.insert_point(x, index=index)
            total += tree.codisp(index)
        raw = total / self.num_trees
        self.last_raw_score = raw
        self.score_history.append(raw)

        if len(self.score_history) <= 10:
            return 0.0  # not enough history for a baseline yet
        history = np.fromiter(self.score_history, dtype=float)
        percentile = np.mean(history < raw) + 0.5 * np.mean(history == raw)
        return float(np.clip((percentile - 0.9) / 0.1, 0.0, 1.0))

    def get_stats(self) -> Dict[str, Any]:
        return {
            "num_trees": self.num_trees,
            "tree_size": self.tree_size,
            "points_seen": self.points_seen,
            "avg_tree_size": float(np.mean([len(t.leaves) for t in self.trees])),
        }


# -------------------------------------------------------------------
# Adaptive thresholds
# -------------------------------------------------------------------
class AdaptiveThresholdManager:
    """
    Per-device anomaly thresholds that drift towards a target anomaly rate.

    Every half window, if more than 1.5x the target share of the device's
    recent scores exceed its threshold, the threshold rises by a small step;
    below 0.5x the target it falls. Thresholds stay within [MIN, MAX].
    """

    def __init__(self, base_threshold: float = BASE_ANOMALY_THRESHOLD, target_rate: float = TARGET_ANOMALY_RATE,
                 window_size: int = THRESHOLD_ADAPTATION_WINDOW):
        self.base_threshold = base_threshold
        self.target_rate = target_rate
        self.window_size = window_size
        self.device_thresholds: Dict[str, float] = {}
        self.device_scores: Dict[str, deque] = defaultdict(lambda: deque(maxlen=window_size))
        self.device_anomaly_counts: Dict[str, int] = defaultdict(int)
        self.device_total_counts: Dict[str, int] = defaultdict(int)
        self.adaptations: Dict[str, int] = defaultdict(int)

    def get_threshold(self, device_id: str) -> float:
        return self.device_thresholds.setdefault(device_id, self.base_threshold)

    def update(self, device_id: str, score: float, is_anomaly: bool) -> float:
        self.get_threshold(device_id)
        self.device_scores[device_id].append(score)
        self.device_total_counts[device_id] += 1
        if is_anomaly:
            self.device_anomaly_counts[device_id] += 1
        if self.device_total_counts[device_id] % max(1, self.window_size // 2) == 0:
            self._adapt(device_id)
        return self.device_thresholds[device_id]

    def _adapt(self, device_id: str) -> None:
        scores = self.device_scores[device_id]
        if len(scores) < 20:
            return
        current = self.device_thresholds[device_id]
        rate = sum(1 for s in scores if s > current) / len(scores)
        new = current
        if rate > self.target_rate * 1.5:
            new += THRESHOLD_ADJUSTMENT_FACTOR
        elif rate < self.target_rate * 0.5:
            new -= THRESHOLD_ADJUSTMENT_FACTOR
        new = max(MIN_THRESHOLD, min(MAX_THRESHOLD, new))
        if abs(new - current) > 0.005:
            self.device_thresholds[device_id] = new
            self.adaptations[device_id] += 1

    def get_stats(self, device_id: str) -> Dict[str, Any]:
        total = self.device_total_counts.get(device_id, 0)
        return {
            "device_id": device_id,
            "current_threshold": self.device_thresholds.get(device_id, self.base_threshold),
            "total_samples": total,
            "total_anomalies": self.device_anomaly_counts.get(device_id, 0),
            "anomaly_rate": self.device_anomaly_counts.get(device_id, 0) / max(1, total),
            "adaptations": self.adaptations.get(device_id, 0),
        }


# -------------------------------------------------------------------
# Federated local model
# -------------------------------------------------------------------
class GlobalModelCache:
    """Latest FedAvg global model, read from the aggregator's shared volume."""

    def __init__(self, path: Path = GLOBAL_MODEL_PATH, refresh_seconds: float = 30.0):
        self.path = Path(path)
        self.refresh_seconds = refresh_seconds
        self.model: Optional[Dict[str, Any]] = None
        self._checked_at = float("-inf")
        self._mtime: Optional[float] = None

    def get(self) -> Optional[Dict[str, Any]]:
        now = time.time()
        if now - self._checked_at < self.refresh_seconds:
            return self.model
        self._checked_at = now
        try:
            mtime = self.path.stat().st_mtime
            if mtime != self._mtime:
                with open(self.path, encoding="utf-8") as f:
                    self.model = json.load(f)
                self._mtime = mtime
        except FileNotFoundError:
            pass  # no global model published yet
        except (OSError, ValueError) as e:
            logger.warning(f"Could not read global model at {self.path}: {e}")
        return self.model


class LocalLogisticModel:
    """
    Per-device logistic regression (attack vs. benign) trained with mini-batch
    SGD on the device's recent readings and their ground-truth labels.

    Each round starts from the latest federated global model when a newer one
    is available (FedAvg: broadcast global -> local training -> aggregate).
    The loss weights attack readings by `pos_weight` to counter the class
    imbalance.
    """

    def __init__(self, device_id: str, learning_rate: float = LEARNING_RATE, l2: float = L2_REGULARIZATION,
                 pos_weight: float = POSITIVE_CLASS_WEIGHT, epochs: int = LOCAL_EPOCHS, batch_size: int = BATCH_SIZE,
                 seed: Optional[int] = None):
        self.device_id = device_id
        self.learning_rate = learning_rate
        self.l2 = l2
        self.pos_weight = pos_weight
        self.epochs = epochs
        self.batch_size = batch_size
        self.rng = np.random.default_rng(seed)
        self.feature_names: List[str] = []
        self.weights = np.zeros(0)
        self.bias = 0.0
        self.base_global_version = 0
        self.n_updates = 0

    def sync_from_global(self, global_model: Optional[Dict[str, Any]], fallback_feature_names) -> None:
        """Adopt the global parameters when a newer global version exists."""
        if global_model and global_model.get("weights"):
            version = int(global_model.get("version", 0))
            if version > self.base_global_version or not self.feature_names:
                weights = global_model["weights"]
                self.feature_names = list(global_model.get("feature_names") or sorted(weights))
                self.weights = np.array([float(weights.get(n, 0.0)) for n in self.feature_names])
                self.bias = float(global_model.get("bias", 0.0))
                self.base_global_version = version
        if not self.feature_names:
            self.feature_names = sorted(fallback_feature_names)
            self.weights = np.zeros(len(self.feature_names))

    def vectorize(self, rows: List[Dict[str, float]]) -> np.ndarray:
        return np.array([[row.get(n, 0.0) for n in self.feature_names] for row in rows], dtype=float)

    @staticmethod
    def sigmoid(z: np.ndarray) -> np.ndarray:
        return 1.0 / (1.0 + np.exp(-np.clip(z, -30, 30)))

    def predict_proba(self, rows: List[Dict[str, float]]) -> np.ndarray:
        return self.sigmoid(self.vectorize(rows) @ self.weights + self.bias)

    def train(self, rows: List[Dict[str, float]], labels: List[int]) -> Tuple[float, float]:
        """Train on feature dicts (see train_arrays)."""
        return self.train_arrays(self.vectorize(rows), labels)

    def train_arrays(self, X: np.ndarray, labels: Sequence[int]) -> Tuple[float, float]:
        """
        Run local epochs of class-weighted mini-batch SGD on a feature matrix
        whose columns follow `feature_names`; return (loss, training accuracy).
        """
        X = np.asarray(X, dtype=float)
        y = np.asarray(labels, dtype=float)
        sample_weight = np.where(y == 1, self.pos_weight, 1.0)

        for _ in range(self.epochs):
            order = self.rng.permutation(len(y))
            for start in range(0, len(y), self.batch_size):
                idx = order[start:start + self.batch_size]
                sw = sample_weight[idx]
                error = (self.sigmoid(X[idx] @ self.weights + self.bias) - y[idx]) * sw
                self.weights -= self.learning_rate * (X[idx].T @ error / sw.sum() + self.l2 * self.weights)
                self.bias -= self.learning_rate * float(error.sum() / sw.sum())
                self.n_updates += 1

        p = np.clip(self.sigmoid(X @ self.weights + self.bias), 1e-7, 1 - 1e-7)
        loss = float(-np.mean(sample_weight * (y * np.log(p) + (1 - y) * np.log(1 - p))))
        accuracy = float(np.mean((p >= 0.5) == (y >= 0.5)))
        return loss, accuracy

    def weights_dict(self) -> Dict[str, float]:
        return {n: float(w) for n, w in zip(self.feature_names, self.weights)}

    def to_dict(self, version: int) -> Dict[str, Any]:
        return {
            "device_id": self.device_id,
            "version": version,
            "base_global_version": self.base_global_version,
            "feature_names": self.feature_names,
            "weights": self.weights_dict(),
            "bias": float(self.bias),
            "n_updates": self.n_updates,
            "saved_at": datetime.now().isoformat(),
        }
