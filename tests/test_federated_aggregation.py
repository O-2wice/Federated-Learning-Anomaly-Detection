"""FedAvg, DP-FedAvg, privacy accounting, model registry/rollback and update clustering."""
import math

import numpy as np
import pytest


def make_update(device, weights, bias=0.0, base=0, samples=100, accuracy=0.9):
    return {
        "device_id": device,
        "model_version": 1,
        "base_global_version": base,
        "accuracy": accuracy,
        "samples_processed": samples,
        "weights": weights,
        "bias": bias,
    }


@pytest.fixture
def aggregator(aggregation, tmp_path, monkeypatch):
    monkeypatch.setattr(aggregation, "ROUND_INTERVAL_SECONDS", 0)
    monkeypatch.setattr(aggregation, "MIN_DEVICES_FOR_AGGREGATION", 3)
    agg = aggregation.FederatedAggregator(producer=None, connect_db=False, models_dir=tmp_path)
    agg.differential_privacy = aggregation.DifferentialPrivacy(enabled=False)
    return agg


def run_round(agg, updates):
    result = None
    for u in updates:
        result = agg.process_local_model_update(u) or result
    assert result is not None, "round did not run"
    return result


def test_startup_clears_stale_model_files(aggregation, tmp_path):
    (tmp_path / "global_model_v99.pkl").write_text("old run")
    aggregation.FederatedAggregator(producer=None, connect_db=False, models_dir=tmp_path)
    assert list(tmp_path.iterdir()) == []


def test_fedavg_is_sample_weighted_mean(aggregator):
    result = run_round(aggregator, [
        make_update("d0", {"a": 0.0, "b": 1.0}, samples=100),
        make_update("d1", {"a": 1.0, "b": 1.0}, samples=200),
        make_update("d2", {"a": 2.0, "b": 1.0}, samples=700),
    ])
    assert result["version"] == 1
    assert result["weights"]["a"] == pytest.approx((0 * 100 + 1 * 200 + 2 * 700) / 1000)
    assert result["weights"]["b"] == pytest.approx(1.0)


def test_updates_are_measured_from_the_version_each_device_trained_on(aggregator):
    v1 = run_round(aggregator, [make_update(f"d{i}", {"a": 1.0}) for i in range(3)])
    # Two devices trained from v1 and moved +1; one trained from v0 (zeros) and reached 2.0
    v2 = run_round(aggregator, [
        make_update("d0", {"a": 2.0}, base=v1["version"]),
        make_update("d1", {"a": 2.0}, base=v1["version"]),
        make_update("d2", {"a": 2.0}, base=0),
    ])
    # deltas: +1, +1, +2 (equal samples) -> mean +4/3 applied to v1's a=1.0
    assert v2["weights"]["a"] == pytest.approx(1.0 + 4.0 / 3.0)


def test_latest_update_per_device_counts_once(aggregator):
    aggregator.process_local_model_update(make_update("d0", {"a": 100.0}))
    result = run_round(aggregator, [
        make_update("d0", {"a": 1.0}),
        make_update("d1", {"a": 1.0}),
        make_update("d2", {"a": 1.0}),
    ])
    assert result["num_devices"] == 3
    assert result["weights"]["a"] == pytest.approx(1.0)


def test_dp_clips_updates_and_bounds_the_aggregate(aggregation):
    dp = aggregation.DifferentialPrivacy(clip_norm=1.0, noise_multiplier=0.0001, enabled=True, seed=0)
    update, meta = dp.aggregate_updates([np.array([30.0, 40.0])] * 10, [100] * 10)
    assert meta["clipped_updates"] == 10
    assert np.linalg.norm(update) == pytest.approx(1.0, abs=1e-3)
    assert meta["noise_std"] == pytest.approx(0.0001 * 1.0 / 10)


def test_rdp_epsilon_grows_with_rounds_and_shrinks_with_noise(aggregation):
    eps = aggregation.gaussian_rdp_epsilon
    assert eps(5.0, 0) == 0.0
    assert math.isinf(eps(0.0, 10))
    assert eps(5.0, 10) < eps(5.0, 60) < eps(5.0, 280)
    assert eps(5.0, 60) < eps(1.1, 60)
    # Value checked independently: sigma=5, 60 rounds, delta=1e-5 -> ~8.6
    assert eps(5.0, 60) == pytest.approx(8.6, abs=0.1)


def test_registry_keeps_a_bounded_number_of_versions(aggregation, aggregator, tmp_path):
    for _ in range(aggregation.MAX_MODEL_VERSIONS_KEPT + 5):
        base = aggregator.global_model.version
        run_round(aggregator, [make_update(f"d{i}", {"a": 1.0}, base=base) for i in range(3)])
    assert len(aggregator.model_registry.versions) == aggregation.MAX_MODEL_VERSIONS_KEPT
    assert len(list(tmp_path.glob("global_model_v*.json"))) == aggregation.MAX_MODEL_VERSIONS_KEPT
    assert (tmp_path / "global_model_latest.json").exists()


def test_rollback_to_best_heldout_version_on_f1_drop(aggregator):
    for k in range(4):
        base = aggregator.global_model.version
        run_round(aggregator, [make_update(f"d{i}", {"a": float(k)}, base=base) for i in range(3)])
    registry = aggregator.model_registry
    registry.record_evaluations({2: (0.90, 0.80), 4: (0.70, 0.55)})

    assert aggregator.check_rollback() is True
    model = aggregator.global_model
    assert model.version == 5 and model.rolled_back_from == 2
    assert model.weights == registry.get(2).weights
    assert aggregator.check_rollback() is False  # not repeated


def test_no_rollback_for_small_f1_change(aggregator):
    for k in range(3):
        base = aggregator.global_model.version
        run_round(aggregator, [make_update(f"d{i}", {"a": float(k)}, base=base) for i in range(3)])
    aggregator.model_registry.record_evaluations({1: (0.90, 0.80), 3: (0.88, 0.75)})
    assert aggregator.check_rollback() is False


def test_update_clusterer_finds_opposing_groups(aggregation):
    clusterer = aggregation.UpdateClusterer(threshold=0.5, min_devices=3)
    ids = [f"d{i}" for i in range(8)]
    result = clusterer.analyse(ids, [np.array([1.0, 0.1])] * 5 + [np.array([-1.0, 0.0])] * 3)
    assert result["num_clusters"] == 2
    assert result["cluster_sizes"][:2] == [5, 3]
    assert result["num_divergent_devices"] == 3
