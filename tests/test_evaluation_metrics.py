"""Held-out classification metrics and the Spark fleet z-score."""
import pytest

from evaluation_metrics import classification_metrics, score_fleet_windows


def test_classification_metrics_from_confusion_counts():
    m = classification_metrics(tp=30, fp=10, fn=20, n=200)
    assert m["true_negatives"] == 140
    assert m["accuracy"] == pytest.approx(170 / 200)
    assert m["precision"] == pytest.approx(30 / 40)
    assert m["recall"] == pytest.approx(30 / 50)
    assert m["f1_score"] == pytest.approx(2 * 0.75 * 0.6 / (0.75 + 0.6))


def test_metrics_are_zero_when_undefined():
    m = classification_metrics(tp=0, fp=0, fn=0, n=50)
    assert (m["precision"], m["recall"], m["f1_score"], m["accuracy"]) == (0.0, 0.0, 0.0, 1.0)


def rows(values, window="w1"):
    return [{"device_id": f"d{i}", "moving_avg_30s": v, "window_end": window} for i, v in enumerate(values)]


def test_fleet_zscore_flags_the_outlier_device():
    scored = score_fleet_windows(rows([0.0] * 19 + [10.0]), threshold=3.0)
    flagged = [s["device_id"] for s in scored if s["is_anomaly"]]
    assert flagged == ["d19"]
    assert all(0.0 <= s["anomaly_score"] <= 1.0 for s in scored)


def test_fleet_zscore_needs_enough_devices():
    scored = score_fleet_windows(rows([0.0, 0.0, 10.0]), threshold=1.0, min_devices=10)
    assert not any(s["is_anomaly"] for s in scored)


def test_windows_are_scored_independently():
    data = rows([0.0] * 19 + [10.0], window="w1") + rows([10.0] * 20, window="w2")
    scored = score_fleet_windows(data, threshold=3.0)
    assert sum(s["is_anomaly"] for s in scored if s["window_end"] == "w2") == 0
