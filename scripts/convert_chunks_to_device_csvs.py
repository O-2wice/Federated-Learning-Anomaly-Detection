#!/usr/bin/env python3
"""
Convert chunked NPZ/NPY files to device_*.csv format for the Kafka Producer.

Input (by default):
  data/processed/chunks/
    - X_chunk_0.npz, X_chunk_1.npz, ...
    - y_chunk_0.npy, y_chunk_1.npy, ...

  data_preprocessor.py writes X chunks either as scipy sparse matrices
  (scipy.sparse.save_npz — the normal case, because of one-hot encoding)
  or as dense arrays under key 'X' (np.savez). Both formats are supported.

Output:
  data/processed/
    - device_0.csv, device_1.csv, ..., device_N.csv

These CSVs are then consumed by:
    scripts/02_kafka_producer.py --source data/processed

Chunks are processed one at a time so the full dataset is never held in
memory at once.
"""

import json
import re
import sys
import logging
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from scipy import sparse

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config_loader import DEVICE_CSV_START  # noqa: E402

# -----------------------------------------------------
# LOGGING
# -----------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

NUM_DEVICES = 2400


def _chunk_index(path: Path) -> int:
    """Numeric index from X_chunk_12.npz / y_chunk_12.npy (lexical sort puts 10 before 2)."""
    m = re.search(r"_(\d+)\.np[yz]$", path.name)
    return int(m.group(1)) if m else -1


def find_chunk_pairs(chunks_dir: Path) -> List[Tuple[Path, Path]]:
    """Return (X_chunk, y_chunk) path pairs in numeric order."""
    if not chunks_dir.exists():
        logger.error(f"Chunks directory not found: {chunks_dir}")
        return []

    x_chunks = sorted(chunks_dir.glob("X_chunk_*.npz"), key=_chunk_index)
    y_by_index = {_chunk_index(p): p for p in chunks_dir.glob("y_chunk_*.npy")}

    pairs = []
    for x_path in x_chunks:
        y_path = y_by_index.get(_chunk_index(x_path))
        if y_path is None:
            logger.warning(f"No matching y chunk for {x_path.name}; skipping")
            continue
        pairs.append((x_path, y_path))

    if not pairs:
        logger.error(f"No chunk files found in {chunks_dir}")
    else:
        logger.info(f"Found {len(pairs)} X/y chunk pairs")
    return pairs


def load_x_chunk(path: Path) -> np.ndarray:
    """Load one X chunk as a dense float32 array (sparse or dense NPZ)."""
    with np.load(path, allow_pickle=False) as npz:
        is_dense = "X" in npz.files
        if is_dense:
            return np.asarray(npz["X"], dtype=np.float32)
    return sparse.load_npz(path).astype(np.float32).toarray()


def count_rows(pairs: List[Tuple[Path, Path]]) -> int:
    """Total samples across all chunks (y chunks are small to load)."""
    return sum(len(np.load(y_path, mmap_mode="r")) for _, y_path in pairs)


def load_feature_names(processed_dir: Path, num_features: int) -> List[str]:
    """
    Real transformed feature names from processing_summary.json (written by
    data_preprocessor.py). Falls back to approximate names only for old chunk
    sets that predate the summary field.
    """
    summary_path = processed_dir / "processing_summary.json"
    try:
        with open(summary_path, encoding="utf-8") as f:
            names = json.load(f).get("feature_names") or []
        if len(names) == num_features:
            logger.info(f"Using {len(names)} feature names from {summary_path.name}")
            return [str(n) for n in names]
        logger.warning(
            f"{summary_path.name} has {len(names)} feature names but chunks have "
            f"{num_features} columns; using approximate names"
        )
    except FileNotFoundError:
        logger.warning(f"{summary_path.name} not found; using approximate feature names")
    return infer_feature_names(num_features)


def infer_feature_names(num_features: int):
    """Generate feature names based on Edge-IIoT dataset structure (approx)."""
    base_features = [
        "flow_duration", "Header_length", "Protocol Type", "Duration",
        "Rate", "Srate", "Drate", "fin_flag_number", "syn_flag_number",
        "rst_flag_number", "psh_flag_number", "ack_flag_number",
        "urg_flag_number", "cwr_flag_number", "ece_flag_number",
        "Src_Port", "Dst_Port", "Protocol", "Timestamp", "TCP_Length",
        "TCP_Flags", "Sequence_Num", "Ack_Num", "TCP_Win_Size",
        "TCP_Chksum", "TCP_Urgent_Ptr", "TCP_Options", "ICMP_Type",
        "ICMP_Code", "ICMP_Checksum", "ICMP_ID", "ICMP_Sequence",
        "UDP_Length", "UDP_Checksum", "ARP_Hard_Type", "ARP_Proto_Type",
        "ARP_Hard_Size", "ARP_Proto_Size", "ARP_Opcode", "ARP_Src_IP",
        "ARP_Src_MAC", "ARP_Dst_IP", "ARP_Dst_MAC", "IPv6_Version",
        "IPv6_Traffic_Class", "IPv6_Flow_Label", "IPv6_Payload_Length",
        "IPv6_Next_Header", "IPv6_Hop_Limit", "IGMP_Type", "IGMP_Max_Resp_Time",
        "DNS_ID", "DNS_QR", "DNS_OPCODE", "DNS_AA", "DNS_TC", "DNS_RD",
        "DHCP_Opcode", "DHCP_Hardware_Type", "DHCP_Hardware_Length",
        "DHCP_Hops", "DHCP_Transaction_ID", "DHCP_Seconds", "DHCP_Flags",
        "NTP_Timestamp", "NTP_Version", "MDNS_ID", "SSH_Version",
        "TLS_Version", "Packet_Loss", "Latency", "Throughput",
    ]

    features = list(base_features)

    # If we have more features than names, generate additional generic ones
    while len(features) < num_features:
        features.append(f"feature_{len(features)}")

    return features[:num_features]


def create_device_csvs(
    pairs: List[Tuple[Path, Path]],
    output_dir: Path,
    num_devices: int = NUM_DEVICES,
    seed: int = 42,
    flush_every_chunks: int = 4,
) -> int:
    """
    Stream chunks into device_*.csv files with a random, balanced row
    assignment.

    The source CSV is ordered by traffic type, so giving each device a
    contiguous slice produced devices that were 100% normal or 100% attack
    (1,727 and 672 of 2,400). Shuffling the assignment gives every device the
    dataset's overall mix of benign and attack traffic.

    Rows are buffered per device and appended every few chunks to bound memory
    and the number of file appends.

    Returns:
        int: number of device files actually created
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    total_samples = count_rows(pairs)
    if total_samples == 0:
        logger.error("Chunks contain no samples")
        return 0

    rng = np.random.default_rng(seed)
    assignment = np.arange(total_samples) % num_devices
    rng.shuffle(assignment)

    logger.info(f"Total samples available: {total_samples}")
    logger.info(
        f"Assigning rows randomly to {num_devices} devices "
        f"(~{total_samples // num_devices} rows each)"
    )

    # Remove stale device files (e.g. from an earlier, smaller dataset) so the
    # producer never mixes datasets.
    for old in output_dir.glob("device_*.csv"):
        old.unlink()

    feature_names = None
    written = np.zeros(num_devices, dtype=np.int64)
    buffers: Dict[int, List[pd.DataFrame]] = defaultdict(list)
    base_time = pd.Timestamp(DEVICE_CSV_START)

    def flush() -> None:
        for dev, frames in buffers.items():
            df = pd.concat(frames, ignore_index=True)
            start = int(written[dev])
            df["timestamp"] = base_time + pd.to_timedelta(
                np.arange(start, start + len(df)), unit="s"
            )
            df.to_csv(
                output_dir / f"device_{dev}.csv",
                mode="a",
                header=start == 0,
                index=False,
            )
            written[dev] += len(df)
        buffers.clear()

    offset = 0
    for i, (x_path, y_path) in enumerate(pairs, start=1):
        logger.info(f"Loading {x_path.name} / {y_path.name}...")
        X = load_x_chunk(x_path)
        y = np.load(y_path)
        n = min(len(X), len(y))
        if len(X) != len(y):
            logger.warning(f"{x_path.name}: X rows ({len(X)}) != y rows ({len(y)}); using {n}")

        if feature_names is None:
            feature_names = load_feature_names(output_dir, X.shape[1])

        df = pd.DataFrame(X[:n], columns=feature_names)
        df["label"] = y[:n]
        devices = assignment[offset:offset + n]
        offset += n

        for dev, part in df.groupby(devices, sort=False):
            buffers[int(dev)].append(part)

        del X, y, df
        if i % flush_every_chunks == 0:
            flush()
            logger.info(f"Written {int(written.sum()):,} / {total_samples:,} rows")

    flush()
    created = int((written > 0).sum())
    logger.info(f"Successfully created {created} device CSV files in {output_dir}")

    shuffle_device_rows(output_dir, num_devices, seed, base_time)
    return created


def shuffle_device_rows(
    output_dir: Path,
    num_devices: int,
    seed: int,
    base_time: pd.Timestamp,
) -> None:
    """
    Shuffle the row order inside each device file and renumber its timestamps.

    Rows reach a device in source-file order, and the source CSV lists all
    benign traffic before the attacks. Without this pass every device stream
    started with ~160 benign readings, so the producer (which replays the first
    rows of each device) never emitted an attack. Files are rewritten one at a
    time, so memory use stays at one device (~0.3 MB).
    """
    logger.info("Shuffling reading order within each device file...")
    for dev in range(num_devices):
        path = output_dir / f"device_{dev}.csv"
        if not path.exists():
            continue
        df = pd.read_csv(path)
        df = df.sample(frac=1.0, random_state=seed + dev).reset_index(drop=True)
        df["timestamp"] = base_time + pd.to_timedelta(np.arange(len(df)), unit="s")
        df.to_csv(path, index=False)
        if (dev + 1) % 400 == 0:
            logger.info(f"Shuffled {dev + 1} device files")


def main() -> int:
    """Main conversion process."""
    # Repo root / scripts / (this file)
    repo_root = Path(__file__).resolve().parent.parent

    chunks_dir = repo_root / "data" / "processed" / "chunks"
    output_dir = repo_root / "data" / "processed"

    logger.info("=" * 70)
    logger.info("CHUNK → DEVICE CSV CONVERSION")
    logger.info("=" * 70)
    logger.info(f"Chunks directory: {chunks_dir}")
    logger.info(f"Output directory: {output_dir}")

    pairs = find_chunk_pairs(chunks_dir)
    if not pairs:
        logger.error("Failed to load chunk files")
        return 1

    num_devices = create_device_csvs(pairs, output_dir)
    if num_devices == 0:
        return 1

    logger.info("=" * 70)
    logger.info(f"CONVERSION COMPLETE - Created {num_devices} device CSV files")
    logger.info("=" * 70)
    logger.info("")
    logger.info("Next step: start the Kafka producer (inside Docker):")
    logger.info("  docker compose up -d kafka-producer")
    logger.info("or from host (if you run it locally):")
    logger.info("  python scripts/02_kafka_producer.py --source data/processed")
    logger.info("")

    return 0


if __name__ == "__main__":
    sys.exit(main())
