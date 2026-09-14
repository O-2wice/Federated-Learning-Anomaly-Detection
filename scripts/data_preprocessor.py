#!/usr/bin/env python3
"""
FLEAD Data Preprocessing Script

Headless version of `notebooks/Data PreProcessing.ipynb`.

Pipeline:
  1. Ensure Kaggle credentials (~/.kaggle/kaggle.json)
  2. Download Edge-IIoTSet dataset from Kaggle into data/raw/
  3. Stream DNN-EdgeIIoT-dataset.csv in chunks (the file is ~1.2 GB, so it is
     never loaded into memory at once)
  4. Fit the sklearn preprocessing pipeline on a random sample of rows
  5. Transform every chunk and write:
       - feature chunks: data/processed/chunks/X_chunk_*.npz
       - label chunks:   data/processed/chunks/y_chunk_*.npy
       - preprocessor.pkl, processing_summary.json (incl. real feature names)

This script is called automatically by ./start (Linux/WSL/macOS)
and START.bat (Windows) if `data/processed/chunks/` does not exist.
"""

import os
import json
from pathlib import Path
from dataclasses import dataclass
from typing import List, Optional, Tuple, Dict, Any

import numpy as np
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi

from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, FunctionTransformer, StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from scipy.sparse import issparse, save_npz

# Optional but nice to have
try:
    import joblib
except ImportError:  # pragma: no cover
    joblib = None


# ---------------------------------------------------------------------
# Paths & Kaggle config
# ---------------------------------------------------------------------
ROOT = Path(__file__).resolve().parent.parent
DATA_RAW = ROOT / "data" / "raw"
DATA_PROCESSED = ROOT / "data" / "processed"
CHUNKS_DIR = DATA_PROCESSED / "chunks"

SOURCE_CSV = "DNN-EdgeIIoT-dataset.csv"
CHUNK_SIZE = 100_000
FIT_SAMPLE_FRACTION = 0.10
RANDOM_STATE = 42

# Identifier / timestamp / free-text columns. They are unique per packet (no
# generalisable signal) and explode one-hot encoding; the Edge-IIoTset authors
# drop the same columns before training.
DROP_COLUMNS = [
    "frame.time",
    "ip.src_host",
    "ip.dst_host",
    "arp.src.proto_ipv4",
    "arp.dst.proto_ipv4",
    "http.file_data",
    "http.request.full_uri",
    "icmp.transmit_timestamp",
    "http.request.uri.query",
    "tcp.options",
    "tcp.payload",
    "tcp.srcport",
    "tcp.dstport",
    "udp.port",
    "mqtt.msg",
]

# Make sure Kaggle API looks in ~/.kaggle
os.environ["KAGGLE_CONFIG_DIR"] = os.path.expanduser("~/.kaggle")


# ---------------------------------------------------------------------
# STEP 1 – Download from Kaggle
# ---------------------------------------------------------------------
def ensure_kaggle_credentials() -> None:
    kaggle_dir = Path(os.environ["KAGGLE_CONFIG_DIR"])
    cfg = kaggle_dir / "kaggle.json"
    if not cfg.exists():
        raise FileNotFoundError(
            f"kaggle.json not found at {cfg}. "
            "Your start script should copy it from ./kaggle/kaggle.json."
        )


def download_edge_iiot() -> None:
    DATA_RAW.mkdir(parents=True, exist_ok=True)

    if (DATA_RAW / SOURCE_CSV).exists():
        print(f"[INFO] {SOURCE_CSV} already present in data/raw, skipping Kaggle download.")
        return

    print("[INFO] Downloading Edge-IIoTSet dataset from Kaggle...")
    api = KaggleApi()
    api.authenticate()

    api.dataset_download_files(
        "sibasispradhan/edge-iiotset-dataset",
        path=str(DATA_RAW),
        unzip=True,
    )

    print("[INFO] Download complete. Files in data/raw:")
    for f in sorted(DATA_RAW.glob("*.csv")):
        size_mb = f.stat().st_size / (1024 * 1024)
        print(f"    {f.name} ({size_mb:.2f} MB)")


# ---------------------------------------------------------------------
# STEP 2 – Chunk preparation
# ---------------------------------------------------------------------
LABEL_GUESS_CANDIDATES = [
    "Attack_label",
    "attack",
    "Attack",
    "Label",
    "label",
    "class",
    "Class",
    "Attack_type",
    "AttackType",
    "Category",
]

# Every label-like column is removed from the features, not just the chosen
# one — Edge-IIoTset ships both Attack_label (0/1) and Attack_type (name), and
# leaving the other one in leaks the answer into the model inputs.
LABEL_COLUMNS = {"Attack_label", "Attack_type", "attack", "Attack", "Label",
                 "label", "class", "Class", "AttackType", "Category"}


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [
        str(c).strip().replace(" ", "_").replace("-", "_") for c in df.columns
    ]
    return df


def prepare_chunk(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise column names, drop identifier columns and exact duplicates."""
    df = clean_column_names(df)
    df = df.drop(columns=[c for c in DROP_COLUMNS if c in df.columns])
    return df.drop_duplicates()


def guess_label_column(columns: List[str], override: Optional[str] = None) -> str:
    if override and override in columns:
        return override
    for c in LABEL_GUESS_CANDIDATES:
        if c in columns:
            return c
    # fallback: last column
    return columns[-1]


def to_binary_labels(y: pd.Series) -> np.ndarray:
    y_norm = y.astype(str).str.lower().str.strip()
    attack = ~(y_norm.isin(["benign", "normal", "0", "0.0"]))
    return attack.astype(int).to_numpy()


@dataclass
class FittedPreprocessor:
    pipeline: Pipeline
    features: List[str]
    numeric_cols: List[str]
    categorical_cols: List[str]
    label_name: str
    feature_names_out: List[str]


def split_feature_types(sample: pd.DataFrame) -> Tuple[List[str], List[str]]:
    """
    Numeric = at least 95% of non-null values parse as numbers. Deciding this
    once on the fit sample keeps every chunk's column types consistent (a
    per-chunk guess can flip a column between numeric and text).
    """
    numeric_cols, cat_cols = [], []
    for c in sample.columns:
        non_null = sample[c].dropna()
        if non_null.empty:
            continue
        parsed = pd.to_numeric(non_null, errors="coerce")
        if parsed.notna().mean() >= 0.95:
            numeric_cols.append(c)
        else:
            cat_cols.append(c)
    return numeric_cols, cat_cols


def coerce_types(X: pd.DataFrame, numeric_cols: List[str], cat_cols: List[str]) -> pd.DataFrame:
    X = X.copy()
    for c in numeric_cols:
        X[c] = pd.to_numeric(X[c], errors="coerce")
    for c in cat_cols:
        X[c] = X[c].astype(str)
    return X


def _cast_to_str(X):
    return X.astype(str)


def build_pipeline(numeric_cols: List[str], cat_cols: List[str]) -> Pipeline:
    numeric_pipeline = Pipeline(
        steps=[
            ("impute", SimpleImputer(strategy="mean")),
            # One shared scale for every device: local models are averaged
            # with FedAvg, which is only meaningful if feature i means the
            # same thing (and has the same units) on every device.
            ("scale", StandardScaler()),
        ]
    )

    ohe = OneHotEncoder(
        handle_unknown="infrequent_if_exist",
        sparse_output=True,
        dtype=np.float32,
        min_frequency=0.01,
    )

    categorical_pipeline = Pipeline(
        steps=[
            ("impute", SimpleImputer(strategy="constant", fill_value="missing")),
            ("cast_str", FunctionTransformer(_cast_to_str, validate=False,
                                             feature_names_out="one-to-one")),
            ("ohe", ohe),
        ]
    )

    pre = ColumnTransformer(
        transformers=[
            ("num", numeric_pipeline, numeric_cols),
            ("cat", categorical_pipeline, cat_cols),
        ],
        remainder="drop",
        # Keep raw names ("tcp.ack", not "num__tcp.ack") so downstream
        # consumers can look features up by their dataset name.
        verbose_feature_names_out=False,
    )

    return Pipeline([("pre", pre)])


def fit_on_sample(csv_path: Path) -> FittedPreprocessor:
    print(f"[PRE] Pass 1: sampling {FIT_SAMPLE_FRACTION:.0%} of rows from {csv_path.name} ...")
    samples = []
    total = 0
    for i, chunk in enumerate(pd.read_csv(csv_path, chunksize=CHUNK_SIZE, low_memory=False)):
        chunk = prepare_chunk(chunk)
        total += len(chunk)
        samples.append(chunk.sample(frac=FIT_SAMPLE_FRACTION, random_state=RANDOM_STATE + i))
    sample = pd.concat(samples, ignore_index=True)
    print(f"[PRE] Read {total:,} rows; fit sample has {len(sample):,} rows.")

    label_col = guess_label_column(list(sample.columns))
    feature_cols = [c for c in sample.columns if c not in LABEL_COLUMNS]
    X_sample = sample[feature_cols].dropna(axis=1, how="all")
    feature_cols = list(X_sample.columns)

    numeric_cols, cat_cols = split_feature_types(X_sample)
    print(f"[PRE] Label column: {label_col}")
    print(f"[PRE] {len(numeric_cols)} numeric and {len(cat_cols)} categorical feature columns")

    pipe = build_pipeline(numeric_cols, cat_cols)
    pipe.fit(coerce_types(X_sample[numeric_cols + cat_cols], numeric_cols, cat_cols))

    feature_names_out = [str(n) for n in pipe.named_steps["pre"].get_feature_names_out()]
    return FittedPreprocessor(
        pipeline=pipe,
        features=feature_cols,
        numeric_cols=numeric_cols,
        categorical_cols=cat_cols,
        label_name=label_col,
        feature_names_out=feature_names_out,
    )


def transform_in_chunks(csv_path: Path, fitted: FittedPreprocessor, out_dir: Path) -> Dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    for stale in list(out_dir.glob("X_chunk_*.npz")) + list(out_dir.glob("y_chunk_*.npy")):
        stale.unlink()

    print(f"[PRE] Pass 2: transforming {csv_path.name} in chunks of {CHUNK_SIZE:,} rows ...")
    cols = fitted.numeric_cols + fitted.categorical_cols
    chunk_files: List[Dict[str, Any]] = []
    total_y = 0

    for xi, chunk in enumerate(pd.read_csv(csv_path, chunksize=CHUNK_SIZE, low_memory=False)):
        chunk = prepare_chunk(chunk)
        for c in cols:
            if c not in chunk.columns:
                chunk[c] = np.nan
        X_chunk = fitted.pipeline.transform(
            coerce_types(chunk[cols], fitted.numeric_cols, fitted.categorical_cols)
        )
        y_chunk = to_binary_labels(chunk[fitted.label_name])

        X_path = out_dir / f"X_chunk_{xi}.npz"
        y_path = out_dir / f"y_chunk_{xi}.npy"
        if issparse(X_chunk):
            save_npz(X_path, X_chunk.tocsr())
        else:
            np.savez(X_path, X=np.asarray(X_chunk, dtype=np.float32))
        np.save(y_path, y_chunk.astype(np.int64))

        chunk_files.append({"X": str(X_path), "y": str(y_path), "rows": int(len(y_chunk))})
        total_y += len(y_chunk)
        if xi % 5 == 0:
            print(f"  Saved chunk {xi} ({total_y:,} rows so far)")

    return {
        "source_csv": csv_path.name,
        "total_samples": int(total_y),
        "n_features": len(fitted.feature_names_out),
        "standardized": True,
        "feature_names": fitted.feature_names_out,
        "label_name": fitted.label_name,
        "dropped_columns": [c for c in DROP_COLUMNS],
        "out_dir": str(out_dir),
        "chunks": chunk_files,
    }


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main() -> None:
    print("==========================================================")
    print("FLEAD DATA PREPROCESSING (Edge-IIoTSet)")
    print("==========================================================")

    ensure_kaggle_credentials()
    download_edge_iiot()

    csv_path = DATA_RAW / SOURCE_CSV
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Expected {SOURCE_CSV} in {DATA_RAW}. "
            "Available: " + ", ".join(p.name for p in DATA_RAW.glob("*.csv"))
        )

    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)

    fitted = fit_on_sample(csv_path)
    meta = transform_in_chunks(csv_path, fitted, CHUNKS_DIR)

    # Save preprocessor + summary
    if joblib is not None:
        preproc_path = DATA_PROCESSED / "preprocessor.pkl"
        print(f"[INFO] Saving fitted preprocessor to {preproc_path} ...")
        joblib.dump(fitted, preproc_path)
    else:
        print("[WARN] joblib not installed, skipping preprocessor.pkl save.")

    summary_path = DATA_PROCESSED / "processing_summary.json"
    print(f"[INFO] Saving preprocessing summary to {summary_path} ...")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)

    print("\n==========================================================")
    print("PREPROCESSING COMPLETE")
    print("----------------------------------------------------------")
    print(f"  Total rows processed:     {meta['total_samples']:,}")
    print(f"  Output feature dimension: {meta['n_features']}")
    print(f"  Label column detected:    {meta['label_name']}")
    print(f"  Chunks directory:         {meta['out_dir']}")
    print(f"  Number of chunks:         {len(meta['chunks'])}")
    print("  Preprocessor (if saved):  data/processed/preprocessor.pkl")
    print("  Summary JSON:             data/processed/processing_summary.json")
    print("==========================================================")


if __name__ == "__main__":
    main()
