"""Streaming models used by the Flink job: RRCF, adaptive thresholds, local training."""
import json

import numpy as np
import pytest

import flink_models as fm


def test_extract_features_skips_metadata_and_non_numeric():
    record = {"tcp.ack": "0.5", "mqtt.len": 2.0, "label": 1.0, "device_id": "device_1",
              "timestamp": "2026-01-01T00:00:00", "data": 0.5, "value": 0.5, "mqtt.topic": "abc", "tcp.seq": None}
    assert fm.extract_features(record) == {"tcp.ack": 0.5, "mqtt.len": 2.0}


def test_rrcf_scores_an_outlier_above_normal_points():
    rng = np.random.default_rng(0)
    forest = fm.RandomCutForest(num_trees=8, tree_size=64, seed=0)
    normal = [forest.update(p) for p in rng.normal(size=(150, 5))]
    outlier = forest.update(np.full(5, 12.0))
    assert outlier > 0.9
    assert np.mean(np.array(normal[20:]) > 0.4) < 0.15
    assert max(len(t.leaves) for t in forest.trees) <= 65  # sliding window stays bounded


def test_rrcf_warmup_returns_zero():
    forest = fm.RandomCutForest(num_trees=2, tree_size=16, seed=0)
    assert all(forest.update([float(i), 0.0]) == 0.0 for i in range(10))


def test_first_training_is_staggered_across_the_interval():
    # Round-robin streaming gives every device the same reading count; without
    # an offset all devices would train in the same burst
    first = [fm.first_training_readings(f"device_{d}", min_samples=20, interval=30) for d in range(2400)]
    assert first == [fm.first_training_readings(f"device_{d}", 20, 30) for d in range(2400)]  # stable
    assert min(first) >= 20 and max(first) < 50
    counts = np.bincount(np.array(first) - 20, minlength=30)
    assert counts.min() > 40 and counts.max() < 120  # ~80 devices per reading step, no burst


def test_adaptive_threshold_rises_when_too_many_anomalies():
    manager = fm.AdaptiveThresholdManager(base_threshold=0.4, target_rate=0.05, window_size=40)
    for _ in range(200):
        manager.update("d1", score=0.9, is_anomaly=True)
    assert manager.get_threshold("d1") > 0.4
    assert manager.get_threshold("d1") <= fm.MAX_THRESHOLD


def test_local_model_learns_a_separable_problem():
    rng = np.random.default_rng(1)
    X = rng.normal(size=(400, 2))
    y = (X[:, 0] + 0.5 * X[:, 1] > 0).astype(int)
    rows = [{"f1": a, "f2": b} for a, b in X]
    model = fm.LocalLogisticModel("d1", seed=0, epochs=10)
    model.sync_from_global(None, rows[0].keys())
    _, accuracy = model.train(rows, list(y))
    assert accuracy > 0.9
    assert model.weights_dict()["f1"] > 0


def test_class_weight_raises_attack_recall():
    rng = np.random.default_rng(2)
    X = np.concatenate([rng.normal(0.0, 1.0, size=(700, 1)), rng.normal(1.0, 1.0, size=(300, 1))])
    y = np.array([0] * 700 + [1] * 300)
    rows = [{"f": float(v)} for v in X[:, 0]]

    def recall(pos_weight):
        model = fm.LocalLogisticModel("d", pos_weight=pos_weight, seed=0, epochs=20)
        model.sync_from_global(None, ["f"])
        model.train(rows, list(y))
        return float(np.mean(model.predict_proba(rows)[y == 1] >= 0.5))

    assert recall(2.5) > recall(1.0)


def test_local_model_starts_from_newer_global_model():
    model = fm.LocalLogisticModel("d1")
    model.sync_from_global(None, ["a", "b"])
    model.sync_from_global({"version": 3, "feature_names": ["a", "b"], "weights": {"a": 0.5, "b": -1.0}, "bias": 0.2}, [])
    assert model.base_global_version == 3
    assert model.weights_dict() == {"a": 0.5, "b": -1.0} and model.bias == pytest.approx(0.2)
    model.sync_from_global({"version": 2, "feature_names": ["a", "b"], "weights": {"a": 9.0, "b": 9.0}, "bias": 9.0}, [])
    assert model.base_global_version == 3  # older version ignored


def test_global_model_cache_reads_latest_file(tmp_path):
    path = tmp_path / "global_model_latest.json"
    cache = fm.GlobalModelCache(path=path, refresh_seconds=0)
    assert cache.get() is None
    path.write_text(json.dumps({"version": 7, "weights": {"a": 1.0}}))
    assert cache.get()["version"] == 7
