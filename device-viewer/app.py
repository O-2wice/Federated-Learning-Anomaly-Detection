"""
FLEAD Device Viewer

Browse the 2,400 simulated devices: what each device file contains (and which
part is streamed or held out), and what the pipeline did with the device:
readings stored, anomalies flagged by RRCF and how many were real attacks,
local models trained and its latest held-out evaluation.

Device files come from data/processed; pipeline results from TimescaleDB. The
pages still work without the database, showing the file view only.
"""

import csv
import math
import os
import re
from datetime import datetime

import psycopg2
from flask import Flask, abort, redirect, render_template, request, url_for

STREAM_ROWS_PER_DEVICE = int(os.getenv("STREAM_ROWS_PER_DEVICE", "660"))
GRAFANA_URL = os.getenv("GRAFANA_PUBLIC_URL", "http://localhost:3001")
MONITOR_URL = os.getenv("MONITOR_PUBLIC_URL", "http://localhost:5001")
PREVIEW_ROWS = 25
PER_PAGE_DEFAULT = 12

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "timescaledb"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "dbname": os.getenv("DB_NAME", "flead"),
    "user": os.getenv("DB_USER", "flead"),
    "password": os.getenv("DB_PASSWORD", "password"),
    "connect_timeout": 3,
}

TEMPLATES = os.path.join(os.path.dirname(__file__), "templates")
STATIC = os.path.join(os.path.dirname(__file__), "static")
app = Flask(__name__, template_folder=TEMPLATES, static_folder=STATIC)

# (path, mtime) -> summary, so each device file is parsed once
_csv_cache = {}


def processed_dir():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "processed"))


def device_number(name):
    match = re.search(r"(\d+)", name)
    return int(match.group(1)) if match else math.inf


def device_files():
    folder = processed_dir()
    if not os.path.isdir(folder):
        return []
    names = [f for f in os.listdir(folder) if f.startswith("device_") and f.endswith(".csv")]
    return sorted(names, key=lambda n: (device_number(n), n))


def csv_summary(filename):
    """Rows, features, attack share and the streamed / held-out split of one device file."""
    path = os.path.join(processed_dir(), filename)
    mtime = os.path.getmtime(path)
    cached = _csv_cache.get(path)
    if cached and cached[0] == mtime:
        return cached[1]

    header, labels, first_ts, last_ts = [], [], None, None
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        header = next(reader, [])
        label_idx = header.index("label") if "label" in header else None
        ts_idx = header.index("timestamp") if "timestamp" in header else None
        for row in reader:
            if label_idx is not None and label_idx < len(row):
                try:
                    labels.append(int(float(row[label_idx])))
                except ValueError:
                    pass
            if ts_idx is not None and ts_idx < len(row):
                first_ts = first_ts or row[ts_idx]
                last_ts = row[ts_idx]

    rows = len(labels)
    streamed = labels[:STREAM_ROWS_PER_DEVICE]
    heldout = labels[STREAM_ROWS_PER_DEVICE:]
    summary = {
        "rows": rows,
        "features": len([h for h in header if h not in ("label", "timestamp")]),
        "attack_share": sum(labels) / rows if rows else None,
        "streamed_rows": len(streamed),
        "streamed_attack_share": sum(streamed) / len(streamed) if streamed else None,
        "heldout_rows": len(heldout),
        "heldout_attack_share": sum(heldout) / len(heldout) if heldout else None,
        "first_timestamp": first_ts,
        "last_timestamp": last_ts,
        "size_kb": round(os.path.getsize(path) / 1024, 1),
    }
    _csv_cache[path] = (mtime, summary)
    return summary


def db_connect():
    try:
        return psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        app.logger.warning("Database unavailable: %s", e)
        return None


def fleet_summary(device_ids):
    """Pipeline results for a page of devices (index lookups on device_id only)."""
    conn = db_connect()
    if conn is None:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT d.device_id,
                       (SELECT COUNT(*) FROM iot_data i WHERE i.device_id = d.device_id),
                       (SELECT COUNT(*) FROM anomalies a WHERE a.device_id = d.device_id),
                       (SELECT AVG(label)::float FROM anomalies a WHERE a.device_id = d.device_id),
                       (SELECT COUNT(*) FROM local_models m WHERE m.device_id = d.device_id)
                FROM unnest(%s::text[]) AS d(device_id)
                """,
                (list(device_ids),),
            )
            return {
                device: {"readings": readings, "anomalies": anomalies, "anomaly_attack_share": share,
                         "local_models": models}
                for device, readings, anomalies, share, models in cur.fetchall()
            }
    except Exception as e:
        app.logger.warning("Fleet summary query failed: %s", e)
        return None
    finally:
        conn.close()


def device_detail(device_id):
    conn = db_connect()
    if conn is None:
        return None
    detail = {}
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*), AVG(label)::float, MAX(ts) FROM iot_data WHERE device_id = %s", (device_id,))
            detail["readings"], detail["stored_attack_share"], detail["last_reading"] = cur.fetchone()

            cur.execute(
                "SELECT COUNT(*), AVG(label)::float, AVG(anomaly_score)::float, "
                "COUNT(*) FILTER (WHERE severity = 'critical') FROM anomalies WHERE device_id = %s",
                (device_id,),
            )
            (detail["anomalies"], detail["anomaly_attack_share"], detail["avg_score"],
             detail["critical"]) = cur.fetchone()

            cur.execute(
                "SELECT ts, anomaly_score, severity, label FROM anomalies WHERE device_id = %s "
                "ORDER BY ts DESC LIMIT 10",
                (device_id,),
            )
            detail["recent_anomalies"] = cur.fetchall()

            cur.execute(
                "SELECT COUNT(*) FROM local_models WHERE device_id = %s", (device_id,))
            detail["local_models"] = cur.fetchone()[0]
            cur.execute(
                "SELECT model_version, global_version, accuracy, samples_processed, created_at "
                "FROM local_models WHERE device_id = %s ORDER BY created_at DESC LIMIT 1",
                (device_id,),
            )
            detail["latest_model"] = cur.fetchone()

            cur.execute(
                "SELECT model_version, model_accuracy, sample_count, true_positives, false_positives, "
                "false_negatives, true_negatives, evaluation_timestamp FROM model_evaluations "
                "WHERE device_id = %s ORDER BY evaluation_timestamp DESC LIMIT 1",
                (device_id,),
            )
            detail["evaluation"] = cur.fetchone()

            cur.execute(
                "SELECT COUNT(*), COUNT(*) FILTER (WHERE is_anomaly) FROM stream_analysis_results "
                "WHERE device_id = %s",
                (device_id,),
            )
            detail["zscore_windows"], detail["zscore_flags"] = cur.fetchone()
        return detail
    except Exception as e:
        app.logger.warning("Device detail query failed for %s: %s", device_id, e)
        return None
    finally:
        conn.close()


def format_cell(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return value
    if number.is_integer() and abs(number) < 1e6:
        return str(int(number))
    return f"{number:.3f}"


@app.template_filter("pct")
def pct_filter(value, digits=1):
    return "–" if value is None else f"{value * 100:.{digits}f}%"


@app.template_filter("thousands")
def thousands_filter(value):
    return "–" if value is None else f"{value:,}"


@app.template_filter("when")
def when_filter(value):
    return "–" if value is None else value.strftime("%Y-%m-%d %H:%M:%S")


@app.context_processor
def links():
    return {"grafana_url": GRAFANA_URL, "monitor_url": MONITOR_URL, "stream_rows": STREAM_ROWS_PER_DEVICE}


@app.route("/")
def index():
    files = device_files()
    query = (request.args.get("q") or "").strip()
    if query:
        wanted = query if query.startswith("device_") else f"device_{query}"
        if f"{wanted}.csv" in files:
            return redirect(url_for("device", filename=f"{wanted}.csv"))

    if not files:
        return render_template("index.html", devices=[], total=0, query=query, not_found=bool(query))

    try:
        page = int(request.args.get("page", 1))
    except ValueError:
        page = 1
    per_page = PER_PAGE_DEFAULT
    total_pages = math.ceil(len(files) / per_page)
    page = max(1, min(page, total_pages))
    page_files = files[(page - 1) * per_page: page * per_page]

    ids = [f[:-4] for f in page_files]
    pipeline = fleet_summary(ids)
    devices = [
        {"id": device_id, "filename": filename, "csv": csv_summary(filename),
         "pipeline": (pipeline or {}).get(device_id)}
        for device_id, filename in zip(ids, page_files)
    ]
    sample = devices[0]["csv"] if devices else None
    return render_template(
        "index.html", devices=devices, total=len(files), page=page, total_pages=total_pages,
        sample=sample, db_available=pipeline is not None, query=query, not_found=bool(query),
    )


@app.route("/device/<path:filename>")
def device(filename):
    filename = os.path.basename(filename)
    if not filename.endswith(".csv"):
        filename += ".csv"
    if filename not in device_files():
        abort(404, f"Device file not found: {filename}")

    device_id = filename[:-4]
    summary = csv_summary(filename)
    path = os.path.join(processed_dir(), filename)
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        header = next(reader, [])
        rows = [row for _, row in zip(range(PREVIEW_ROWS), reader)]

    # Put timestamp and label first, then the features
    order = [header.index(c) for c in ("timestamp", "label") if c in header]
    order += [i for i, name in enumerate(header) if name not in ("timestamp", "label")]
    preview_header = [header[i] for i in order]
    preview_rows = [[format_cell(row[i]) if i < len(row) else "" for i in order] for row in rows]

    number = device_number(filename)
    files = device_files()
    position = files.index(filename)
    return render_template(
        "device.html", device_id=device_id, summary=summary, detail=device_detail(device_id),
        header=preview_header, rows=preview_rows,
        prev_file=files[position - 1] if position > 0 else None,
        next_file=files[position + 1] if position + 1 < len(files) else None,
        number=number, generated=datetime.now(),
    )


if __name__ == "__main__":
    # Inside Docker we listen on 5000 (mapped to 8082 on the host)
    port = int(os.environ.get("PORT", 5000))
    print(f"[device-viewer] Starting on 0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)
