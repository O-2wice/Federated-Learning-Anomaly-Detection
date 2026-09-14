"""
Evaluation helpers used by the Spark analytics job (05_spark_analytics.py).

Kept free of PySpark so the maths can be unit-tested without a Spark install.
"""

from collections import defaultdict
from typing import Any, Dict, Iterable, List


def classification_metrics(tp: int, fp: int, fn: int, n: int) -> Dict[str, float]:
    """
    Binary classification metrics from confusion-matrix counts.

    `n` is the total number of scored rows; true negatives are derived.
    Precision/recall/F1 are 0.0 when undefined (no predicted or no actual
    positives), matching scikit-learn's zero_division=0 behaviour.
    """
    tn = n - tp - fp - fn
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    return {
        "accuracy": (tp + tn) / n if n else 0.0,
        "precision": precision,
        "recall": recall,
        "f1_score": f1,
        "sample_count": int(n),
        "true_positives": int(tp),
        "false_positives": int(fp),
        "false_negatives": int(fn),
        "true_negatives": int(tn),
    }


def score_fleet_windows(
    rows: Iterable[Dict[str, Any]],
    threshold: float,
    min_devices: int = 10,
) -> List[Dict[str, Any]]:
    """
    Fleet z-score for each device's 30-second window mean.

    For every window, compare each device's mean reading with the mean and
    standard deviation across all devices reporting in that same window:

        z = (device_mean - fleet_mean) / fleet_std

    A device is flagged when |z| > threshold. This catches a device whose
    30-second average stream metric departs from the rest of the fleet at the
    same moment. It complements the RRCF detector in Flink, which scores each
    individual reading on all features against the recent traffic of the
    devices served by the same Flink worker. Windows with fewer than
    `min_devices` devices are scored 0 because the fleet statistics are not
    meaningful.

    Input rows need: device_id, moving_avg_30s, window_end.
    """
    by_window: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row.get("moving_avg_30s") is not None:
            by_window[row["window_end"]].append(row)

    results: List[Dict[str, Any]] = []
    for window_end, group in by_window.items():
        values = [float(g["moving_avg_30s"]) for g in group]
        n = len(values)
        mean = sum(values) / n
        std = (sum((v - mean) ** 2 for v in values) / n) ** 0.5
        enough = n >= min_devices and std > 0

        for g, value in zip(group, values):
            z = (value - mean) / std if enough else 0.0
            score = min(1.0, abs(z) / (2.0 * threshold))
            results.append(
                {
                    "device_id": g["device_id"],
                    "metric_name": "stream_metric_mean_30s",
                    "raw_value": value,
                    "moving_avg_30s": value,
                    "anomaly_score": score,
                    "is_anomaly": enough and abs(z) > threshold,
                    "anomaly_confidence": score,
                    "detection_method": "fleet_zscore",
                    "window_end": window_end,
                }
            )
    return results
