"""Kafka producer record handling, train/held-out split config, and device CSV generation."""
import numpy as np
import pandas as pd
import pytest

import config_loader


def test_parse_row_converts_numbers_and_blanks(producer_module):
    record = producer_module.parse_row(["tcp.ack", "label", "timestamp", "mqtt.topic"],
                                       ["0.25", "1", "2025-01-01 00:00:00", ""])
    assert record == {"tcp.ack": 0.25, "label": 1.0, "timestamp": "2025-01-01 00:00:00", "mqtt.topic": None}


def test_select_metric_keeps_zero(producer_module):
    # A chained `or` used to skip a legitimate 0.0 and fall through to another column
    assert producer_module.select_metric({"tcp.ack": 0.0, "tcp.seq": 5.0}) == 0.0
    assert producer_module.select_metric({"tcp.seq": 5.0}) == 5.0
    assert producer_module.select_metric({}) == 0.0


def test_build_message_adds_stream_fields(producer_module):
    msg = producer_module.build_message("device_3", {"tcp.ack": 1.5, "label": 0.0, "timestamp": "2025-01-01 00:00:07"})
    assert msg["device_id"] == "device_3"
    assert msg["data"] == msg["value"] == 1.5
    assert msg["source_timestamp"] == "2025-01-01 00:00:07"
    assert msg["timestamp"] != msg["source_timestamp"]


def test_device_streams_round_robin_and_row_limit(producer_module, tmp_path):
    for dev, n in (("device_0", 5), ("device_1", 2)):
        pd.DataFrame({"tcp.ack": range(n), "label": [0] * n}).to_csv(tmp_path / f"{dev}.csv", index=False)
    producer = producer_module.SingleBrokerProducer(rows_per_device=3, bootstrap_servers="unused:9092")
    devices = producer.discover_device_files(str(tmp_path))
    order = [(d, r["tcp.ack"]) for d, r in producer.device_streams(devices)]
    assert order == [("device_0", 0.0), ("device_1", 0.0), ("device_0", 1.0), ("device_1", 1.0), ("device_0", 2.0)]


class _FakeFuture:
    def __init__(self, deliver):
        self.deliver = deliver

    def add_callback(self, fn):
        if self.deliver:
            fn(None)
        return self

    def add_errback(self, fn):
        return self


class _FakeKafkaClient:
    """Stands in for KafkaProducer: confirms every send, or never confirms any."""

    def __init__(self, deliver):
        self.deliver = deliver
        self.closed = False

    def send(self, topic, key=None, value=None):
        return _FakeFuture(self.deliver)

    def flush(self, timeout=None):
        pass

    def close(self, timeout=None):
        self.closed = True


def _stream_with_fake_kafka(producer_module, tmp_path, monkeypatch, deliver):
    for dev in ("device_0", "device_1"):
        pd.DataFrame({"tcp.ack": range(20), "label": [0] * 20}).to_csv(tmp_path / f"{dev}.csv", index=False)
    clients = []

    def fake_connect(self, max_retries=30, retry_delay=5):
        clients.append(_FakeKafkaClient(deliver))
        self.producer = clients[-1]
        self.mark_connected()

    monkeypatch.setattr(producer_module.SingleBrokerProducer, "connect", fake_connect)
    # 40 readings at 200/s take ~0.2 s; a stall is declared after 0.05 s
    producer = producer_module.SingleBrokerProducer(rows_per_second=200, rows_per_device=20,
                                                    bootstrap_servers="unused:9092", repeat=False,
                                                    stall_seconds=0.05)
    producer.run(str(tmp_path))
    return producer, clients


def test_producer_counts_confirmed_deliveries(producer_module, tmp_path, monkeypatch):
    producer, clients = _stream_with_fake_kafka(producer_module, tmp_path, monkeypatch, deliver=True)
    assert producer.messages_sent == producer.messages_delivered == 40
    assert producer.reconnects == 0 and len(clients) == 1


def test_producer_recreates_client_when_deliveries_stall(producer_module, tmp_path, monkeypatch):
    # A client that stops confirming sends (the broker lost its session) used to
    # drop every reading silently while the producer reported progress
    producer, clients = _stream_with_fake_kafka(producer_module, tmp_path, monkeypatch, deliver=False)
    assert producer.messages_delivered == 0
    assert producer.reconnects >= 2
    assert len(clients) == producer.reconnects + 1
    assert all(client.closed for client in clients[:-1])


def test_stream_rows_per_device_env_override(monkeypatch):
    assert config_loader.get_stream_config()["stream_rows_per_device"] == 660
    monkeypatch.setenv("STREAM_ROWS_PER_DEVICE", "500")
    assert config_loader.get_stream_config()["stream_rows_per_device"] == 500


def test_device_csvs_are_balanced_mixed_and_shuffled(tmp_path):
    from scipy import sparse

    import convert_chunks_to_device_csvs as convert

    chunks = tmp_path / "chunks"
    chunks.mkdir()
    rng = np.random.default_rng(0)
    # Source ordered by class (all benign first), like the raw dataset
    labels = np.array([0] * 300 + [1] * 100)
    for i, part in enumerate(np.array_split(np.arange(400), 4)):
        sparse.save_npz(chunks / f"X_chunk_{i}.npz", sparse.csr_matrix(rng.normal(size=(len(part), 3))))
        np.save(chunks / f"y_chunk_{i}.npy", labels[part])

    created = convert.create_device_csvs(convert.find_chunk_pairs(chunks), tmp_path, num_devices=4)
    assert created == 4
    frames = [pd.read_csv(tmp_path / f"device_{d}.csv") for d in range(4)]
    assert sorted(len(f) for f in frames) == [100, 100, 100, 100]
    for f in frames:
        assert 0.1 < f["label"].mean() < 0.4          # every device mixes benign and attack traffic
        assert f["label"].head(40).mean() > 0          # attacks appear early, not only at the end
        assert f["timestamp"].is_monotonic_increasing
