#!/usr/bin/env python3
"""
Build the FLEAD Grafana dashboards (grafana/dashboards/*.json).

The dashboards are defined here as code so every panel follows the same rules:

- TimescaleDB panels use the fixed data source uid ``flead-timescaledb`` and
  only bounded queries: a ``$__timeFilter``, a device, ``LIMIT 1`` on a time
  index, or the 15-second KPI snapshots in ``dashboard_metrics``. Nothing
  counts the large tables from start to end.
- Throughput, lag, JVM memory and alert states come from Prometheus
  (uid ``flead-prometheus``).
- Units, colours and wording are shared, and every panel says what it measures.

Usage (from the repository root):

    python grafana/build_dashboards.py

Grafana loads the generated files through grafana/provisioning/dashboards.
"""

import json
from pathlib import Path

OUT_DIR = Path(__file__).resolve().parent / "dashboards"

TSDB = {"type": "grafana-postgresql-datasource", "uid": "flead-timescaledb"}
PROM = {"type": "prometheus", "uid": "flead-prometheus"}
MIXED = {"type": "datasource", "uid": "-- Mixed --"}

C = {
    "blue": "#3b82f6", "teal": "#14b8a6", "grey": "#94a3b8", "purple": "#a855f7", "amber": "#f59e0b",
    "orange": "#f97316", "red": "#ef4444", "green": "#22c55e",
}
SEVERITY_COLORS = {"critical": C["red"], "warning": C["orange"], "info": C["blue"]}

DEVICE_FILTER = "('$device' = 'All' OR device_id = '$device')"
BIG_TABLE_WINDOW = "INTERVAL '1 day'"


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------
def sql(raw, fmt="table", ref="A"):
    return {"refId": ref, "datasource": TSDB, "editorMode": "code", "rawQuery": True, "format": fmt, "rawSql": raw}


def promql(expr, legend="__auto", ref="A", instant=False):
    return {"refId": ref, "datasource": PROM, "editorMode": "code", "expr": expr, "legendFormat": legend,
            "range": not instant, "instant": instant}


def latest_eval(expr):
    return (f"SELECT {expr} AS value FROM model_evaluations WHERE device_id = 'ALL' "
            f"ORDER BY evaluation_timestamp DESC LIMIT 1")


def snapshot(metric):
    """Latest value the metrics updater wrote for a fleet KPI."""
    return (f"SELECT metric_value AS value FROM dashboard_metrics WHERE metric_name = '{metric}' "
            f"ORDER BY timestamp DESC LIMIT 1")


def snapshot_rate_series(metric, alias, scale=1):
    """
    Per-minute rate of a fleet total, from the last snapshot of each minute.
    The collector inserts in batches, so rates over single 15-second snapshots zig-zag.
    """
    return (f"WITH s AS (SELECT time_bucket('1 minute', timestamp) AS t, MAX(metric_value) AS v "
            f"FROM dashboard_metrics WHERE metric_name = '{metric}' AND $__timeFilter(timestamp) GROUP BY 1) "
            f"SELECT t AS time, GREATEST(0, (v - LAG(v) OVER (ORDER BY t)) * {scale} "
            f"/ NULLIF(EXTRACT(EPOCH FROM t - LAG(t) OVER (ORDER BY t)), 0)) AS \"{alias}\" FROM s ORDER BY 1")


def snapshot_rate_latest(metric, scale=1):
    """Rate over the last minute of snapshots."""
    return (f"SELECT GREATEST(0, (a.metric_value - b.metric_value) * {scale} "
            f"/ NULLIF(EXTRACT(EPOCH FROM a.timestamp - b.timestamp), 0)) AS value FROM "
            f"(SELECT metric_value, timestamp FROM dashboard_metrics WHERE metric_name = '{metric}' "
            f"ORDER BY timestamp DESC LIMIT 1) a CROSS JOIN LATERAL "
            f"(SELECT metric_value, timestamp FROM dashboard_metrics WHERE metric_name = '{metric}' "
            f"AND timestamp <= a.timestamp - INTERVAL '60 seconds' ORDER BY timestamp DESC LIMIT 1) b")


def newest_age(table, column, where=""):
    condition = f"{column} > NOW() - {BIG_TABLE_WINDOW}" + (f" AND {where}" if where else "")
    return f"SELECT EXTRACT(EPOCH FROM NOW() - MAX({column})) AS value FROM {table} WHERE {condition}"


FLINK_CONSUMED = "sum(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_consumed_rate)"
FLINK_LAG = "max(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lag_max)"
ALERTS_FIRING = 'count(ALERTS{alertstate="firing"}) or vector(0)'


# ---------------------------------------------------------------------------
# Dashboard and panel builders
# ---------------------------------------------------------------------------
class Dashboard:
    def __init__(self, uid, title, description, tags, time_from="now-1h", refresh="30s", variables=None,
                 annotations=None):
        self.uid, self.title, self.description, self.tags = uid, title, description, tags
        self.time_from, self.refresh = time_from, refresh
        self.variables = variables or []
        self.annotations = annotations or []
        self.panels = []
        self._next_id = 1
        self._x = self._y = self._row_height = 0

    # layout: panels fill 24-column rows left to right
    def _place(self, w, h):
        if self._x + w > 24:
            self.newline()
        pos = {"h": h, "w": w, "x": self._x, "y": self._y}
        self._x += w
        self._row_height = max(self._row_height, h)
        return pos

    def newline(self):
        if self._x:
            self._y += self._row_height
            self._x = self._row_height = 0

    def add(self, panel, w, h):
        panel["id"] = self._next_id
        self._next_id += 1
        panel["gridPos"] = self._place(w, h)
        self.panels.append(panel)
        return panel

    def row(self, title):
        self.newline()
        self.add({"type": "row", "title": title, "collapsed": False, "panels": []}, 24, 1)
        self.newline()

    def to_json(self):
        builtin = {"builtIn": 1, "datasource": {"type": "grafana", "uid": "-- Grafana --"}, "enable": True,
                   "hide": True, "iconColor": "rgba(0, 211, 255, 1)", "name": "Annotations & Alerts",
                   "type": "dashboard"}
        return {
            "annotations": {"list": [builtin] + self.annotations},
            "description": self.description,
            "editable": True,
            "fiscalYearStartMonth": 0,
            "graphTooltip": 1,
            "id": None,
            "links": [{"asDropdown": False, "icon": "external link", "includeVars": False, "keepTime": True,
                       "tags": ["flead"], "targetBlank": False, "title": "FLEAD dashboards", "tooltip": "",
                       "type": "dashboards", "url": ""}],
            "liveNow": False,
            "panels": self.panels,
            "refresh": self.refresh,
            "schemaVersion": 39,
            "tags": ["flead"] + self.tags,
            "templating": {"list": self.variables},
            "time": {"from": self.time_from, "to": "now"},
            "timepicker": {},
            "timezone": "browser",
            "title": self.title,
            "uid": self.uid,
            "version": 1,
            "weekStart": "",
        }


def _datasource(targets):
    uids = {t["datasource"]["uid"] for t in targets}
    return targets[0]["datasource"] if len(uids) == 1 else MIXED


def _color_overrides(colors, dashed=()):
    overrides = []
    for name, color in (colors or {}).items():
        props = [{"id": "color", "value": {"mode": "fixed", "fixedColor": color}}]
        if name in dashed:
            props.append({"id": "custom.lineStyle", "value": {"fill": "dash", "dash": [8, 6]}})
        overrides.append({"matcher": {"id": "byName", "options": name}, "properties": props})
    return overrides


def steps(*pairs):
    """Threshold steps from (value, color) pairs; the first value is None."""
    return {"mode": "absolute", "steps": [{"color": color, "value": value} for value, color in pairs]}


def stat(d, title, targets, description, w=4, h=4, unit="none", decimals=None, thresholds=None,
         mappings=None):
    defaults = {"unit": unit, "color": {"mode": "thresholds"},
                "thresholds": thresholds or steps((None, C["blue"])), "mappings": mappings or []}
    if decimals is not None:
        defaults["decimals"] = decimals
    return d.add({
        "type": "stat", "title": title, "description": description, "datasource": _datasource(targets),
        "targets": targets, "fieldConfig": {"defaults": defaults, "overrides": []},
        "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                    "colorMode": "value", "graphMode": "none", "justifyMode": "auto", "textMode": "value",
                    "orientation": "auto", "wideLayout": True, "showPercentChange": False},
    }, w, h)


def timeseries(d, title, targets, description, w=12, h=8, unit="none", decimals=None, colors=None, dashed=(),
               min_value=None, max_value=None, stack=False, style="line", threshold_line=None):
    custom = {"drawStyle": style, "lineWidth": 2, "fillOpacity": 70 if style == "bars" else 6,
              "gradientMode": "none", "showPoints": "never" if style != "points" else "always", "pointSize": 5,
              "spanNulls": True, "lineInterpolation": "linear", "axisBorderShow": False,
              "stacking": {"mode": "normal" if stack else "none", "group": "A"},
              "thresholdsStyle": {"mode": "line+area" if threshold_line is not None else "off"}}
    defaults = {"unit": unit, "custom": custom, "color": {"mode": "palette-classic"}}
    if threshold_line is not None:
        defaults["thresholds"] = steps((None, "transparent"), (threshold_line, "rgba(239, 68, 68, 0.12)"))
    if decimals is not None:
        defaults["decimals"] = decimals
    if min_value is not None:
        defaults["min"] = min_value
    if max_value is not None:
        defaults["max"] = max_value
    return d.add({
        "type": "timeseries", "title": title, "description": description, "datasource": _datasource(targets),
        "targets": targets, "fieldConfig": {"defaults": defaults, "overrides": _color_overrides(colors, dashed)},
        "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True,
                               "calcs": ["lastNotNull"]},
                    "tooltip": {"mode": "multi", "sort": "none"}},
    }, w, h)


def table(d, title, targets, description, w=12, h=8, overrides=None):
    return d.add({
        "type": "table", "title": title, "description": description, "datasource": _datasource(targets),
        "targets": targets,
        "fieldConfig": {"defaults": {"custom": {"align": "auto", "cellOptions": {"type": "auto"},
                                                "filterable": False, "minWidth": 80}},
                        "overrides": overrides or []},
        "options": {"showHeader": True, "cellHeight": "sm",
                    "footer": {"show": False, "reducer": ["sum"], "fields": ""}},
    }, w, h)


def donut(d, title, targets, description, w=8, h=8, colors=None):
    return d.add({
        "type": "piechart", "title": title, "description": description, "datasource": _datasource(targets),
        "targets": targets,
        "fieldConfig": {"defaults": {"color": {"mode": "palette-classic"}}, "overrides": _color_overrides(colors)},
        "options": {"pieType": "donut", "displayLabels": [],
                    "legend": {"displayMode": "table", "placement": "right", "showLegend": True,
                               "values": ["value", "percent"]},
                    "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": True},
                    "tooltip": {"mode": "single", "sort": "none"}},
    }, w, h)


def barchart(d, title, targets, description, x_field, w=12, h=8, unit="none", color=C["blue"]):
    return d.add({
        "type": "barchart", "title": title, "description": description, "datasource": _datasource(targets),
        "targets": targets,
        "fieldConfig": {"defaults": {"unit": unit, "color": {"mode": "fixed", "fixedColor": color},
                                     "custom": {"fillOpacity": 80, "lineWidth": 0, "gradientMode": "none"}},
                        "overrides": []},
        "options": {"xField": x_field, "orientation": "auto", "showValue": "never", "stacking": "none",
                    "barWidth": 0.85, "groupWidth": 0.7, "xTickLabelRotation": 0, "xTickLabelSpacing": 0,
                    "legend": {"displayMode": "list", "placement": "bottom", "showLegend": False},
                    "tooltip": {"mode": "single", "sort": "none"}},
    }, w, h)


def text(d, content, w=24, h=4):
    return d.add({"type": "text", "title": "", "transparent": True,
                  "options": {"mode": "markdown", "content": content}}, w, h)


def device_variable(include_all):
    query = "SELECT 'device_' || n FROM generate_series(0, 2399) AS n ORDER BY n"
    variable = {"name": "device", "label": "Device", "type": "query", "datasource": TSDB, "query": query,
                "definition": query, "refresh": 1, "sort": 0, "multi": False, "hide": 0, "regex": "",
                "skipUrlSync": False, "includeAll": include_all, "options": []}
    if include_all:
        variable["allValue"] = "All"
        variable["current"] = {"selected": True, "text": "All", "value": "$__all"}
    else:
        variable["current"] = {"selected": True, "text": "device_0", "value": "device_0"}
    return variable


SEVERITY_MAPPINGS = [{"type": "value", "options": {
    "critical": {"color": C["red"], "index": 0}, "warning": {"color": C["orange"], "index": 1},
    "info": {"color": C["blue"], "index": 2}, "attack": {"color": C["red"], "index": 3},
    "benign": {"color": C["green"], "index": 4}}}]
COLORED_TEXT = [{"id": "custom.cellOptions", "value": {"type": "color-text"}},
                {"id": "mappings", "value": SEVERITY_MAPPINGS}]
DEVICE_LINK = [{"id": "links", "value": [{"title": "Open in Devices dashboard",
                                          "url": "/d/flead-device?var-device=${__value.raw}&${__url_time_range}"}]}]

KPI_LABELS = {
    "total_iot_count": "Readings stored",
    "total_anomalies_count": "Anomalies stored",
    "total_local_models_count": "Local models stored",
    "total_federated_models_count": "Global versions",
    "devices_with_models": "Devices with models",
    "active_devices_5m": "Active devices (5 min)",
    "stale_devices_count": "Stale devices",
    "anomaly_rate_1h": "Readings flagged (1 h)",
    "anomaly_attack_share_1h": "RRCF precision (1 h)",
    "global_model_heldout_accuracy": "Held-out accuracy",
    "global_model_heldout_f1": "Held-out F1",
    "spark_batch_device_day_rows": "Daily statistics rows",
}


def clock_column(width):
    """Time of day only, so timestamps of the last minutes fit a narrow column."""
    return [{"id": "unit", "value": "time:HH:mm:ss"}, {"id": "custom.width", "value": width}]


NAV = ("**[Federated learning & privacy](/d/flead-fl-lab)** · **[Anomalies](/d/flead-anomaly)** · "
       "**[Devices](/d/flead-device)** · **[Operations](/d/flead-ops)** · "
       "[Live monitor ↗](http://localhost:5001) · [Device viewer ↗](http://localhost:8082)")


# ---------------------------------------------------------------------------
# Dashboards
# ---------------------------------------------------------------------------
def overview():
    d = Dashboard("flead-home", "FLEAD - Overview",
                  "What the pipeline achieves right now: model quality, anomaly detection, privacy and health.",
                  ["overview"])
    text(d, "### FLEAD overview\nStreaming anomaly detection (RRCF) and differentially private federated "
            "learning over 2,400 simulated IoT devices.  \n" + NAV)

    d.row("Global model on held-out readings")
    stat(d, "Held-out accuracy", [sql(latest_eval("model_accuracy"))],
         "Accuracy of the latest global model on readings no device trained on (Spark evaluation).",
         unit="percentunit", decimals=1, thresholds=steps((None, C["red"]), (0.75, C["amber"]), (0.8, C["green"])))
    stat(d, "Benign baseline", [sql(latest_eval("(true_negatives + false_positives)::float / NULLIF(sample_count, 0)"))],
         "Accuracy of predicting 'benign' for every held-out reading. The model has to beat this.",
         unit="percentunit", decimals=1, thresholds=steps((None, C["grey"])))
    stat(d, "Held-out F1", [sql(latest_eval("f1_score"))],
         "F1 score for the attack class on held-out readings.",
         decimals=3, thresholds=steps((None, C["red"]), (0.5, C["amber"]), (0.7, C["green"])))
    stat(d, "Precision", [sql(latest_eval("precision"))],
         "Share of readings the model calls attacks that really are attacks.", unit="percentunit", decimals=1)
    stat(d, "Recall", [sql(latest_eval("recall"))],
         "Share of attacks in the held-out readings that the model catches.", unit="percentunit", decimals=1)
    stat(d, "Global version", [sql("SELECT global_version AS value FROM federated_models ORDER BY created_at DESC LIMIT 1")],
         "Latest global model version (one per federated round).")

    timeseries(d, "Held-out accuracy, F1 and baseline", [
        sql("SELECT evaluation_timestamp AS time, model_accuracy AS \"Accuracy\" FROM model_evaluations "
            "WHERE device_id = 'ALL' AND $__timeFilter(evaluation_timestamp) ORDER BY 1", "time_series", "A"),
        sql("SELECT evaluation_timestamp AS time, f1_score AS \"F1\" FROM model_evaluations "
            "WHERE device_id = 'ALL' AND $__timeFilter(evaluation_timestamp) ORDER BY 1", "time_series", "B"),
        sql("SELECT evaluation_timestamp AS time, (true_negatives + false_positives)::float / NULLIF(sample_count, 0) "
            "AS \"Always-benign baseline\" FROM model_evaluations WHERE device_id = 'ALL' "
            "AND $__timeFilter(evaluation_timestamp) ORDER BY 1", "time_series", "C"),
    ], "Every new global version is scored on the same held-out sample.", w=16, h=8, unit="percentunit",
        colors={"Accuracy": C["blue"], "F1": C["teal"], "Always-benign baseline": C["grey"]},
        dashed=("Always-benign baseline",))
    table(d, "Confusion matrix (latest evaluation)", [sql(
        "WITH e AS (SELECT true_positives AS tp, false_positives AS fp, false_negatives AS fn, true_negatives AS tn "
        "FROM model_evaluations WHERE device_id = 'ALL' ORDER BY evaluation_timestamp DESC LIMIT 1) "
        "SELECT 'Attack' AS \"Actual\", tp AS \"Predicted attack\", fn AS \"Predicted benign\" FROM e "
        "UNION ALL SELECT 'Benign', fp, tn FROM e")],
        "Counts of held-out readings by true label and model prediction.", w=8, h=8)

    d.row("Anomaly detection (RRCF, no labels used)")
    stat(d, "Flagged (1 h)", [sql(snapshot("anomaly_rate_1h"))],
         "Share of readings stored in the last hour that RRCF flagged. The adaptive thresholds aim for about 5%.",
         unit="percentunit", decimals=1, thresholds=steps((None, C["green"]), (0.15, C["red"])))
    stat(d, "RRCF precision", [sql(snapshot("anomaly_attack_share_1h"))],
         "Last hour: share of flagged readings that are attacks (dataset labels). About 28% of all readings are attacks.",
         unit="percentunit", decimals=1, thresholds=steps((None, C["amber"]), (0.5, C["green"])))
    stat(d, "Anomalies stored", [sql(snapshot("total_anomalies_count"))],
         "All anomalies stored since the stack started (KPI snapshot).", decimals=0)
    stat(d, "Active devices", [sql(snapshot("active_devices_5m"))],
         "Devices with a reading stored in the last 5 minutes (KPI snapshot).", decimals=0,
         thresholds=steps((None, C["red"]), (2000, C["green"])))
    stat(d, "Readings / s", [sql(snapshot_rate_latest("total_iot_count"))],
         "Collector throughput over the last 15-second snapshot interval. The producer sends 150 per second.",
         unit="short", decimals=0)
    stat(d, "Alerts firing", [promql(ALERTS_FIRING, instant=True)],
         "Prometheus alert rules currently firing. Details on the Operations dashboard.",
         thresholds=steps((None, C["green"]), (1, C["red"])))

    timeseries(d, "Anomalies per minute by severity", [
        sql(f"SELECT time_bucket('1 minute', ts) AS time, COUNT(*) AS \"{sev}\" FROM anomalies "
            f"WHERE $__timeFilter(ts) AND severity = '{sev}' GROUP BY 1 ORDER BY 1", "time_series", ref)
        for sev, ref in (("critical", "A"), ("warning", "B"), ("info", "C"))
    ], "By reading time. A gap at the right edge means Flink is behind the stream.", w=16, h=8,
        style="bars", stack=True, colors=SEVERITY_COLORS)
    donut(d, "Severity of anomalies in range", [sql(
        "SELECT severity, COUNT(*) AS anomalies FROM anomalies WHERE $__timeFilter(ts) GROUP BY severity")],
        "Critical: score above 0.8 or 0.3 above the device's threshold.", w=8, h=8, colors=SEVERITY_COLORS)

    d.row("Privacy and participation")
    timeseries(d, "Privacy budget ε", [sql(
        "SELECT created_at AS time, dp_epsilon AS \"ε (δ = 1e-5)\" FROM federated_models "
        "WHERE $__timeFilter(created_at) ORDER BY 1", "time_series")],
        "Cumulative DP budget after each round (Rényi-DP accountant). The PrivacyBudgetHigh alert fires above 50.",
        w=8, h=7, decimals=2, colors={"ε (δ = 1e-5)": C["purple"]}, threshold_line=50)
    timeseries(d, "Devices per round", [sql(
        "SELECT created_at AS time, num_devices AS \"Devices\" FROM federated_models "
        "WHERE $__timeFilter(created_at) ORDER BY 1", "time_series")],
        "Devices whose update entered each round (minimum 200).", w=8, h=7, style="bars",
        colors={"Devices": C["teal"]}, min_value=0)
    timeseries(d, "Update agreement", [sql(
        "SELECT created_at AS time, mean_update_cosine AS \"Agreement\" FROM federated_models "
        "WHERE $__timeFilter(created_at) AND num_devices > 0 ORDER BY 1", "time_series")],
        "Mean cosine similarity of device updates with the round's consensus direction (1 = identical).",
        w=8, h=7, decimals=2, colors={"Agreement": C["blue"]}, min_value=0, max_value=1)
    return d


def federated_learning():
    rollbacks = {"datasource": TSDB, "enable": True, "hide": False, "iconColor": C["amber"], "name": "Rollbacks",
                 "target": sql("SELECT created_at AS time, 'Rollback: v' || global_version || "
                               "' republishes the best held-out version' AS text FROM federated_models "
                               "WHERE num_devices = 0 AND $__timeFilter(created_at)", ref="Anno")}
    d = Dashboard("flead-fl-lab", "FLEAD - Federated Learning & Privacy",
                  "Global model quality on held-out readings, differential privacy and the federated rounds behind it.",
                  ["federated-learning", "privacy"], annotations=[rollbacks])
    text(d, "### Federated learning and privacy\nDevices train logistic regressions locally; the aggregator "
            "combines their updates with buffered FedAvg, clipping and Gaussian noise (DP-FedAvg). Amber markers "
            "are automatic rollbacks.  \n" + NAV)

    d.row("Global model")
    stat(d, "Global version", [sql("SELECT global_version AS value FROM federated_models ORDER BY created_at DESC LIMIT 1")],
         "Latest global model version.")
    stat(d, "Held-out accuracy", [sql(latest_eval("model_accuracy"))], "Latest held-out accuracy.",
         unit="percentunit", decimals=1, thresholds=steps((None, C["red"]), (0.75, C["amber"]), (0.8, C["green"])))
    stat(d, "Held-out F1", [sql(latest_eval("f1_score"))], "Latest held-out F1 for the attack class.",
         decimals=3, thresholds=steps((None, C["red"]), (0.5, C["amber"]), (0.7, C["green"])))
    stat(d, "Round devices", [sql("SELECT num_devices AS value FROM federated_models ORDER BY created_at DESC LIMIT 1")],
         "Devices whose update entered the latest round.", thresholds=steps((None, C["amber"]), (200, C["teal"])))
    stat(d, "Privacy budget ε", [sql("SELECT dp_epsilon AS value FROM federated_models ORDER BY created_at DESC LIMIT 1")],
         "Cumulative ε after the latest round (δ = 1e-5).", decimals=2,
         thresholds=steps((None, C["green"]), (25, C["amber"]), (50, C["red"])))
    stat(d, "Rollbacks in range", [sql("SELECT COUNT(*) AS value FROM federated_models WHERE num_devices = 0 "
                                       "AND $__timeFilter(created_at)")],
         "Versions republished because a newer version's held-out F1 dropped by more than 0.10.",
         thresholds=steps((None, C["green"]), (1, C["amber"])))

    timeseries(d, "Held-out accuracy, F1 and baseline", [
        sql("SELECT evaluation_timestamp AS time, model_accuracy AS \"Accuracy\" FROM model_evaluations "
            "WHERE device_id = 'ALL' AND $__timeFilter(evaluation_timestamp) ORDER BY 1", "time_series", "A"),
        sql("SELECT evaluation_timestamp AS time, f1_score AS \"F1\" FROM model_evaluations "
            "WHERE device_id = 'ALL' AND $__timeFilter(evaluation_timestamp) ORDER BY 1", "time_series", "B"),
        sql("SELECT evaluation_timestamp AS time, (true_negatives + false_positives)::float / NULLIF(sample_count, 0) "
            "AS \"Always-benign baseline\" FROM model_evaluations WHERE device_id = 'ALL' "
            "AND $__timeFilter(evaluation_timestamp) ORDER BY 1", "time_series", "C"),
    ], "Scored on readings no device trained on.", unit="percentunit",
        colors={"Accuracy": C["blue"], "F1": C["teal"], "Always-benign baseline": C["grey"]},
        dashed=("Always-benign baseline",))
    timeseries(d, "Precision and recall", [
        sql("SELECT evaluation_timestamp AS time, precision AS \"Precision\" FROM model_evaluations "
            "WHERE device_id = 'ALL' AND $__timeFilter(evaluation_timestamp) ORDER BY 1", "time_series", "A"),
        sql("SELECT evaluation_timestamp AS time, recall AS \"Recall\" FROM model_evaluations "
            "WHERE device_id = 'ALL' AND $__timeFilter(evaluation_timestamp) ORDER BY 1", "time_series", "B"),
    ], "Precision: predicted attacks that are attacks. Recall: attacks that are caught.", unit="percentunit",
        colors={"Precision": C["purple"], "Recall": C["amber"]})

    d.row("Federated rounds")
    timeseries(d, "Devices per round", [sql(
        "SELECT created_at AS time, num_devices AS \"Devices\" FROM federated_models "
        "WHERE $__timeFilter(created_at) ORDER BY 1", "time_series")],
        "Devices whose latest update entered each round (minimum 200).", w=8, h=7, style="bars",
        colors={"Devices": C["teal"]}, min_value=0)
    timeseries(d, "Update agreement", [sql(
        "SELECT created_at AS time, mean_update_cosine AS \"Agreement\" FROM federated_models "
        "WHERE $__timeFilter(created_at) AND num_devices > 0 ORDER BY 1", "time_series")],
        "Mean cosine similarity of device updates with the consensus direction. Falling agreement means devices "
        "learn different things.", w=8, h=7, decimals=2, colors={"Agreement": C["blue"]}, min_value=0, max_value=1)
    timeseries(d, "Update clusters", [sql(
        "SELECT created_at AS time, num_clusters AS \"Clusters\" FROM federated_models "
        "WHERE $__timeFilter(created_at) AND num_devices > 0 ORDER BY 1", "time_series")],
        "Groups of at least 3 devices whose updates point in a similar direction (cosine ≥ 0.5).",
        w=8, h=7, colors={"Clusters": C["purple"]}, min_value=0)

    d.row("Differential privacy")
    timeseries(d, "Privacy budget ε", [sql(
        "SELECT created_at AS time, dp_epsilon AS \"ε (δ = 1e-5)\" FROM federated_models "
        "WHERE $__timeFilter(created_at) ORDER BY 1", "time_series")],
        "Cumulative budget; each round adds privacy loss. Alert threshold 50.", w=8, h=7, decimals=2,
        colors={"ε (δ = 1e-5)": C["purple"]}, threshold_line=50)
    timeseries(d, "Noise added per round", [sql(
        "SELECT created_at AS time, dp_noise_std AS \"Noise σ\" FROM federated_models "
        "WHERE $__timeFilter(created_at) AND num_devices > 0 ORDER BY 1", "time_series")],
        "Standard deviation of the Gaussian noise on the averaged update: noise multiplier × clip norm / devices.",
        w=8, h=7, decimals=4, colors={"Noise σ": C["amber"]}, min_value=0)
    timeseries(d, "Updates clipped per round", [sql(
        "SELECT created_at AS time, dp_clipped_updates AS \"Clipped updates\" FROM federated_models "
        "WHERE $__timeFilter(created_at) AND num_devices > 0 ORDER BY 1", "time_series")],
        "Device updates whose L2 norm exceeded the clip norm (1.0) and were scaled down.",
        w=8, h=7, style="bars", colors={"Clipped updates": C["orange"]}, min_value=0)

    d.row("Local training")
    timeseries(d, "Local training accuracy (training windows)", [
        sql(f"SELECT time_bucket('1 minute', created_at) AS time, {fn}(accuracy) AS \"{label}\" FROM local_models "
            f"WHERE $__timeFilter(created_at) GROUP BY 1 ORDER BY 1", "time_series", ref)
        for fn, label, ref in (("MAX", "Best device", "A"), ("AVG", "Average", "B"), ("MIN", "Worst device", "C"))
    ], "Accuracy on each device's own recent training window, not on held-out data.", unit="percentunit",
        colors={"Best device": C["green"], "Average": C["blue"], "Worst device": C["red"]})
    timeseries(d, "Local models per minute", [sql(
        snapshot_rate_series("total_local_models_count", "Local models", 60), "time_series")],
        "Local training rounds reported to the aggregator (from the 15-second KPI snapshots).",
        colors={"Local models": C["teal"]}, min_value=0)

    d.row("Round history")
    table(d, "Federated rounds", [sql(
        "SELECT created_at AS \"Time\", global_version AS \"Version\", num_devices AS \"Devices\", "
        "total_samples AS \"Samples\", ROUND(mean_update_cosine::numeric, 3) AS \"Agreement\", "
        "num_clusters AS \"Clusters\", ROUND(update_norm::numeric, 4) AS \"Update norm\", "
        "ROUND(dp_noise_std::numeric, 4) AS \"Noise σ\", dp_clipped_updates AS \"Clipped\", "
        "ROUND(dp_epsilon::numeric, 2) AS \"ε\", CASE WHEN num_devices = 0 THEN 'rollback' ELSE '' END AS \"Note\" "
        "FROM federated_models WHERE $__timeFilter(created_at) ORDER BY created_at DESC LIMIT 50")],
        "One row per global version.", w=24, h=9)
    return d


def anomalies():
    d = Dashboard("flead-anomaly", "FLEAD - Anomalies",
                  "RRCF anomaly detection in Flink, measured against the dataset labels, plus Spark's fleet z-score.",
                  ["anomaly", "rrcf"], variables=[device_variable(include_all=True)])
    text(d, "### Anomalies\nRRCF flags about 5% of readings without using labels; the labels here only measure "
            "how many flagged readings are real attacks. Times are reading times.  \n" + NAV)

    d.row("Detection in the selected range")
    stat(d, "Anomalies flagged", [sql(f"SELECT COUNT(*) AS value FROM anomalies WHERE $__timeFilter(ts) AND {DEVICE_FILTER}")],
         "Readings RRCF flagged in the selected time range.", decimals=0)
    stat(d, "Share flagged", [sql(
        f"SELECT (SELECT COUNT(*) FROM anomalies WHERE $__timeFilter(ts) AND {DEVICE_FILTER})::float "
        f"/ NULLIF((SELECT COUNT(*) FROM iot_data WHERE $__timeFilter(ts) AND {DEVICE_FILTER}), 0) AS value")],
        "Flagged readings divided by readings stored in the same range.", unit="percentunit", decimals=1,
        thresholds=steps((None, C["green"]), (0.15, C["red"])))
    stat(d, "Precision", [sql(
        f"SELECT AVG(label)::float AS value FROM anomalies WHERE $__timeFilter(ts) AND {DEVICE_FILTER}")],
        "Share of flagged readings labelled as attacks. About 28% of all readings are attacks.",
        unit="percentunit", decimals=1, thresholds=steps((None, C["amber"]), (0.5, C["green"])))
    stat(d, "Recall", [sql(
        f"SELECT (SELECT COUNT(*) FROM anomalies WHERE $__timeFilter(ts) AND {DEVICE_FILTER} AND label = 1)::float "
        f"/ NULLIF((SELECT COUNT(*) FROM iot_data WHERE $__timeFilter(ts) AND {DEVICE_FILTER} AND label = 1), 0) AS value")],
        "Attack readings flagged divided by attack readings stored. RRCF flags ~6% of readings, so recall stays low "
        "by design; the federated classifier is the high-recall detector.", unit="percentunit", decimals=1)
    stat(d, "Critical", [sql(f"SELECT COUNT(*) AS value FROM anomalies WHERE $__timeFilter(ts) AND severity = 'critical' AND {DEVICE_FILTER}")],
         "Score above 0.8 or more than 0.3 above the device's threshold.", decimals=0,
         thresholds=steps((None, C["red"])))
    stat(d, "Devices flagged", [sql(f"SELECT COUNT(DISTINCT device_id) AS value FROM anomalies WHERE $__timeFilter(ts) AND {DEVICE_FILTER}")],
         "Distinct devices with at least one flagged reading in range.", decimals=0)

    timeseries(d, "Anomalies per minute by severity", [
        sql(f"SELECT time_bucket('1 minute', ts) AS time, COUNT(*) AS \"{sev}\" FROM anomalies "
            f"WHERE $__timeFilter(ts) AND severity = '{sev}' AND {DEVICE_FILTER} GROUP BY 1 ORDER BY 1",
            "time_series", ref)
        for sev, ref in (("critical", "A"), ("warning", "B"), ("info", "C"))
    ], "Stacked counts by reading time.", w=16, h=8, style="bars", stack=True, colors=SEVERITY_COLORS)
    donut(d, "Severity", [sql(f"SELECT severity, COUNT(*) AS anomalies FROM anomalies WHERE $__timeFilter(ts) "
                              f"AND {DEVICE_FILTER} GROUP BY severity")],
          "Share of flagged readings per severity.", w=8, h=8, colors=SEVERITY_COLORS)

    timeseries(d, "Detection quality over time (5-minute buckets)", [sql(
        f"WITH a AS (SELECT time_bucket('5 minutes', ts) AS t, COUNT(*) AS flagged, "
        f"COUNT(*) FILTER (WHERE label = 1) AS flagged_attacks FROM anomalies "
        f"WHERE $__timeFilter(ts) AND {DEVICE_FILTER} GROUP BY 1), "
        f"r AS (SELECT time_bucket('5 minutes', ts) AS t, COUNT(*) AS readings, "
        f"COUNT(*) FILTER (WHERE label = 1) AS attacks FROM iot_data "
        f"WHERE $__timeFilter(ts) AND {DEVICE_FILTER} GROUP BY 1) "
        f"SELECT r.t AS time, a.flagged_attacks::float / NULLIF(a.flagged, 0) AS \"Precision\", "
        f"a.flagged_attacks::float / NULLIF(r.attacks, 0) AS \"Recall\", "
        f"a.flagged::float / NULLIF(r.readings, 0) AS \"Share flagged\" "
        f"FROM r LEFT JOIN a ON a.t = r.t ORDER BY 1", "time_series")],
        "Buckets Flink has not processed yet show low values; judge the older buckets.", unit="percentunit",
        colors={"Precision": C["purple"], "Recall": C["amber"], "Share flagged": C["grey"]})
    barchart(d, "RRCF score of flagged readings", [sql(
        f"SELECT CASE WHEN anomaly_score < 0.4 THEN '< 0.40' "
        f"ELSE to_char(LEAST(FLOOR(anomaly_score * 20) / 20, 0.95), 'FM0.00') END AS \"Score\", "
        f"COUNT(*) AS \"Anomalies\" FROM anomalies WHERE $__timeFilter(ts) AND {DEVICE_FILTER} "
        f"GROUP BY 1 ORDER BY MIN(anomaly_score)")],
        "Scores are percentile ranks of the forest's collusive displacement (0 for the bottom 90%).",
        x_field="Score", color=C["orange"])

    d.row("Devices")
    table(d, "Devices with the most anomalies", [sql(
        "SELECT device_id AS \"Device\", COUNT(*) AS \"Anomalies\", "
        "COUNT(*) FILTER (WHERE severity = 'critical') AS \"Critical\", "
        "ROUND(AVG(label)::numeric * 100, 1) AS \"Attacks %\", ROUND(AVG(anomaly_score)::numeric, 3) AS \"Avg score\", "
        "MAX(ts) AS \"Last flagged\" FROM anomalies WHERE $__timeFilter(ts) "
        "GROUP BY device_id ORDER BY 2 DESC LIMIT 25")],
        "Click a device to open it in the Devices dashboard.", w=12, h=10,
        overrides=[{"matcher": {"id": "byName", "options": "Device"}, "properties": DEVICE_LINK}])
    # The stored z-score is capped at 1 (|z| / 6), so the table shows how often a device was out of line instead
    table(d, "Fleet z-score: devices flagged most", [sql(
        "SELECT device_id AS \"Device\", COUNT(*) FILTER (WHERE is_anomaly) AS \"Flagged windows\", "
        "COUNT(*) AS \"Windows\", ROUND(100.0 * COUNT(*) FILTER (WHERE is_anomaly) / COUNT(*), 1) AS \"Flagged %\" "
        "FROM stream_analysis_results WHERE $__timeFilter(timestamp) GROUP BY device_id "
        "HAVING COUNT(*) FILTER (WHERE is_anomaly) > 0 ORDER BY 2 DESC LIMIT 25")],
        "Spark's fleet comparison, independent of RRCF.", w=12, h=10,
        overrides=[{"matcher": {"id": "byName", "options": "Device"}, "properties": DEVICE_LINK}])
    timeseries(d, "Fleet z-score: devices out of line (Spark)", [sql(
        "SELECT time_bucket('1 minute', timestamp) AS time, COUNT(*) FILTER (WHERE is_anomaly) AS \"Flagged windows\" "
        "FROM stream_analysis_results WHERE $__timeFilter(timestamp) GROUP BY 1 ORDER BY 1", "time_series")],
        "30-second windows in which a device's mean stream metric was more than 3 standard deviations from the "
        "fleet mean.", w=24, h=6, style="bars", colors={"Flagged windows": C["purple"]}, min_value=0)

    d.row("Feed")
    table(d, "Latest anomalies", [sql(
        f"SELECT ts AS \"Reading time\", device_id AS \"Device\", ROUND(anomaly_score::numeric, 3) AS \"RRCF score\", "
        f"severity AS \"Severity\", CASE label WHEN 1 THEN 'attack' WHEN 0 THEN 'benign' END AS \"Label\" "
        f"FROM anomalies WHERE $__timeFilter(ts) AND {DEVICE_FILTER} ORDER BY ts DESC LIMIT 100")],
        "The label is the dataset's ground truth for the flagged reading.", w=24, h=10,
        overrides=[{"matcher": {"id": "byName", "options": "Severity"}, "properties": COLORED_TEXT},
                   {"matcher": {"id": "byName", "options": "Label"}, "properties": COLORED_TEXT},
                   {"matcher": {"id": "byName", "options": "Device"}, "properties": DEVICE_LINK}])
    return d


def devices():
    d = Dashboard("flead-device", "FLEAD - Devices",
                  "One device at a time: its stream, RRCF scores, local training and held-out accuracy.",
                  ["devices"], variables=[device_variable(include_all=False)])
    text(d, "### Devices\nPick a device above, or click one in a table. Per-device held-out accuracy rests on a "
            "small sample (a few readings per evaluation); the fleet line is the reliable measure.  \n" + NAV)

    d.row("$device in the selected range")
    stat(d, "Readings stored", [sql("SELECT COUNT(*) AS value FROM iot_data WHERE device_id = '$device' AND $__timeFilter(ts)")],
         "Readings of this device stored in range (one every ~16 s).", decimals=0)
    stat(d, "Attack readings", [sql("SELECT AVG(label)::float AS value FROM iot_data WHERE device_id = '$device' AND $__timeFilter(ts)")],
         "Share of this device's stored readings labelled as attacks.", unit="percentunit", decimals=1,
         thresholds=steps((None, C["grey"])))
    stat(d, "Anomalies flagged", [sql("SELECT COUNT(*) AS value FROM anomalies WHERE device_id = '$device' AND $__timeFilter(ts)")],
         "Readings RRCF flagged for this device in range.", decimals=0)
    stat(d, "RRCF precision", [sql("SELECT AVG(label)::float AS value FROM anomalies WHERE device_id = '$device' AND $__timeFilter(ts)")],
         "Precision of RRCF for this device.", unit="percentunit", decimals=1)
    stat(d, "Local models", [sql("SELECT COUNT(*) AS value FROM local_models WHERE device_id = '$device' AND $__timeFilter(created_at)")],
         "Local training rounds of this device in range.", decimals=0)
    stat(d, "Held-out accuracy", [sql(
        "SELECT model_accuracy AS value FROM model_evaluations WHERE device_id = '$device' "
        "ORDER BY evaluation_timestamp DESC LIMIT 1")],
        "Latest global model on this device's held-out sample; a small sample, so it jumps.",
        unit="percentunit", decimals=0)

    timeseries(d, "Stream metric", [sql(
        "SELECT ts AS time, value AS \"tcp.ack (standardized)\" FROM iot_data WHERE device_id = '$device' "
        "AND $__timeFilter(ts) ORDER BY 1", "time_series")],
        "The stream metric of each reading. RRCF scores all 46 features, not only this one.",
        colors={"tcp.ack (standardized)": C["blue"]})
    timeseries(d, "RRCF scores of flagged readings", [sql(
        "SELECT ts AS time, anomaly_score AS \"RRCF score\" FROM anomalies WHERE device_id = '$device' "
        "AND $__timeFilter(ts) ORDER BY 1", "time_series")],
        "One point per flagged reading.", style="points", min_value=0, max_value=1, decimals=2,
        colors={"RRCF score": C["orange"]})
    timeseries(d, "Local training accuracy", [sql(
        "SELECT created_at AS time, accuracy AS \"Local training accuracy\" FROM local_models "
        "WHERE device_id = '$device' AND $__timeFilter(created_at) ORDER BY 1", "time_series")],
        "Accuracy on the device's own recent training window.", unit="percentunit",
        colors={"Local training accuracy": C["teal"]}, style="points")
    timeseries(d, "Held-out accuracy: device vs fleet", [
        sql("SELECT evaluation_timestamp AS time, model_accuracy AS \"This device\" FROM model_evaluations "
            "WHERE device_id = '$device' AND $__timeFilter(evaluation_timestamp) ORDER BY 1", "time_series", "A"),
        sql("SELECT evaluation_timestamp AS time, model_accuracy AS \"Fleet\" FROM model_evaluations "
            "WHERE device_id = 'ALL' AND $__timeFilter(evaluation_timestamp) ORDER BY 1", "time_series", "B"),
    ], "The fleet line uses about 20,000 held-out readings; the device line only this device's few.",
        unit="percentunit", colors={"This device": C["amber"], "Fleet": C["blue"]}, style="line")

    d.row("Fleet")
    table(d, "Devices by anomalies in range", [sql(
        "SELECT device_id AS \"Device\", COUNT(*) AS \"Anomalies\", COUNT(*) FILTER (WHERE severity = 'critical') "
        "AS \"Critical\", ROUND(AVG(label)::numeric * 100, 1) AS \"Attacks %\", MAX(ts) AS \"Last flagged\" "
        "FROM anomalies WHERE $__timeFilter(ts) GROUP BY device_id ORDER BY 2 DESC LIMIT 50")],
        "Click a device to select it.", w=24, h=10,
        overrides=[{"matcher": {"id": "byName", "options": "Device"}, "properties": DEVICE_LINK}])
    return d


def operations():
    d = Dashboard("flead-ops", "FLEAD - Operations",
                  "Is the pipeline keeping up? Throughput, Flink lag, data freshness, platform health and alerts.",
                  ["operations"], time_from="now-30m", refresh="15s")
    text(d, "### Operations\nStream throughput and lag (Prometheus), freshness of each stage (TimescaleDB), "
            "JVM memory and alert states.  \n" + NAV)

    d.row("Stream")
    stat(d, "Readings / s", [sql(snapshot_rate_latest("total_iot_count"))],
         "Collector throughput (15-second snapshots). The producer sends 150 per second.", unit="short", decimals=0,
         thresholds=steps((None, C["red"]), (100, C["green"])))
    stat(d, "Flink reads / s", [promql(FLINK_CONSUMED, instant=True)],
         "Readings the Flink job pulls from Kafka per second; under backpressure this is its processing rate.",
         unit="short", decimals=0, thresholds=steps((None, C["red"]), (140, C["green"])))
    stat(d, "Flink lag", [promql(FLINK_LAG, instant=True)],
         "Readings waiting in Kafka for Flink. Above 9,000 (one minute of stream) for 10 minutes, FlinkFallingBehind fires.",
         unit="short", decimals=0, thresholds=steps((None, C["green"]), (9000, C["red"])))
    stat(d, "Backpressure", [promql("max(flink_taskmanager_job_task_isBackPressured)", instant=True)],
         "Whether a Flink task is waiting on a slower downstream task.",
         mappings=[{"type": "value", "options": {"0": {"text": "No", "color": C["green"]},
                                                 "1": {"text": "Yes", "color": C["amber"]}}}])
    stat(d, "Anomaly age", [sql(newest_age("anomalies", "ts"))],
         "Age of the newest flagged reading (reading time). Grows with Flink lag.", unit="s", decimals=0,
         thresholds=steps((None, C["green"]), (300, C["red"])))
    stat(d, "Alerts firing", [promql(ALERTS_FIRING, instant=True)], "Prometheus alert rules currently firing.",
         thresholds=steps((None, C["green"]), (1, C["red"])))

    timeseries(d, "Stored vs processed per second", [
        sql(snapshot_rate_series("total_iot_count", "Stored by collector"), "time_series", "A"),
        promql(FLINK_CONSUMED, "Read by Flink", "B"),
    ], "If Flink reads less than the collector stores, the lag grows.", unit="short", decimals=0,
        colors={"Stored by collector": C["blue"], "Read by Flink": C["teal"]}, min_value=0)
    timeseries(d, "Flink lag", [promql(FLINK_LAG, "Readings waiting")],
        "Kafka consumer lag of the Flink job; the shaded area is above the alert threshold.", unit="short",
        decimals=0, colors={"Readings waiting": C["red"]}, min_value=0, threshold_line=9000)

    d.row("Freshness")
    stat(d, "Reading age", [sql(newest_age("iot_data", "ts"))],
         "Age of the newest stored reading.", unit="s", decimals=0, thresholds=steps((None, C["green"]), (60, C["red"])))
    stat(d, "Local model age", [sql(newest_age("local_models", "created_at"))],
         "Age of the newest local model the aggregator received.", unit="s", decimals=0,
         thresholds=steps((None, C["green"]), (600, C["red"])))
    stat(d, "Round age", [sql("SELECT EXTRACT(EPOCH FROM NOW() - MAX(created_at)) AS value FROM federated_models")],
         "Rounds run every 60 s once 200 devices have reported.", unit="s", decimals=0,
         thresholds=steps((None, C["green"]), (300, C["red"])))
    stat(d, "Evaluation age", [sql(newest_age("model_evaluations", "evaluation_timestamp", "device_id = 'ALL'"))],
         "Spark checks for a new global version every 120 s.", unit="s", decimals=0,
         thresholds=steps((None, C["green"]), (600, C["red"])))
    stat(d, "Z-score age", [sql(newest_age("stream_analysis_results", "timestamp"))],
         "Spark writes 30-second windows every 30 s after a one-minute watermark.", unit="s", decimals=0,
         thresholds=steps((None, C["green"]), (300, C["red"])))
    stat(d, "Active devices", [sql(snapshot("active_devices_5m"))],
         "Devices with a reading stored in the last 5 minutes.", decimals=0,
         thresholds=steps((None, C["red"]), (2000, C["green"])))

    timeseries(d, "Local models and anomalies per minute", [
        sql(snapshot_rate_series("total_local_models_count", "Local models", 60), "time_series", "A"),
        sql(snapshot_rate_series("total_anomalies_count", "Anomalies stored", 60), "time_series", "B"),
    ], "From the 15-second KPI snapshots (insert time).", decimals=0,
        colors={"Local models": C["teal"], "Anomalies stored": C["orange"]}, min_value=0)
    timeseries(d, "Devices per federated round", [sql(
        "SELECT created_at AS time, num_devices AS \"Devices\" FROM federated_models "
        "WHERE $__timeFilter(created_at) ORDER BY 1", "time_series")],
        "Rounds need at least 200 devices.", style="bars", colors={"Devices": C["teal"]}, min_value=0)

    d.row("Platform")
    stat(d, "Flink job uptime", [promql("max(flink_jobmanager_job_uptime)", instant=True)],
         "Time since the Flink training job (re)started.", unit="ms", decimals=0,
         thresholds=steps((None, C["amber"]), (60000, C["green"])))
    stat(d, "Flink restarts", [promql("max(flink_jobmanager_job_numRestarts)", instant=True)],
         "Restarts of the Flink job since submission.", decimals=0,
         thresholds=steps((None, C["green"]), (1, C["amber"]), (4, C["red"])))
    stat(d, "Spark workers", [promql("max(metrics_master_aliveWorkers_Value)", instant=True)],
         "Workers registered with the Spark master.", decimals=0,
         thresholds=steps((None, C["red"]), (1, C["green"])))
    stat(d, "Scrape targets up", [promql("sum(up)", instant=True)],
         "Endpoints Prometheus scraped successfully (5 expected).", decimals=0,
         thresholds=steps((None, C["red"]), (5, C["green"])))
    stat(d, "DB errors", [promql("max(flead_db_connection_errors_total)", instant=True)],
         "Failed TimescaleDB connections from the monitor since it started.", decimals=0,
         thresholds=steps((None, C["green"]), (1, C["amber"])))
    stat(d, "KPI snapshot age", [promql("min(flead_metrics_snapshot_age_seconds)", instant=True)],
         "Age of the newest fleet KPI snapshot (dashboard-metrics-updater, every 15 s).", unit="s", decimals=0,
         thresholds=steps((None, C["green"]), (120, C["red"])))

    timeseries(d, "JVM heap used", [
        promql("max(flink_taskmanager_Status_JVM_Memory_Heap_Used / flink_taskmanager_Status_JVM_Memory_Heap_Max)",
               "Flink TaskManager", "A"),
        promql("max(flink_jobmanager_Status_JVM_Memory_Heap_Used / flink_jobmanager_Status_JVM_Memory_Heap_Max)",
               "Flink JobManager", "B"),
    ], "The FlinkTaskManagerHeapHigh alert fires above 90%.", unit="percentunit", min_value=0, max_value=1,
        colors={"Flink TaskManager": C["purple"], "Flink JobManager": C["blue"]})
    alerts = table(d, "Alert rules pending or firing", [promql("ALERTS", instant=True)],
                   "Rules are defined in prometheus/alerts.yml.", w=12, h=8)
    alerts["fieldConfig"]["defaults"]["noValue"] = "No alert rules are pending or firing"

    d.row("Live feeds")
    table(d, "Latest readings", [sql(
        "SELECT ts AS \"Time\", device_id AS \"Device\", ROUND(value::numeric, 3) AS \"tcp.ack\", "
        "CASE label WHEN 1 THEN 'attack' ELSE 'benign' END AS \"Label\" FROM iot_data "
        "WHERE ts > NOW() - INTERVAL '5 minutes' ORDER BY ts DESC LIMIT 50")],
        "Readings stored in the last 5 minutes.", w=12, h=11,
        overrides=[{"matcher": {"id": "byName", "options": "Time"}, "properties": clock_column(90)},
                   {"matcher": {"id": "byName", "options": "Label"}, "properties": COLORED_TEXT},
                   {"matcher": {"id": "byName", "options": "Device"}, "properties": DEVICE_LINK}])
    labels = " ".join(f"WHEN '{name}' THEN '{label}'" for name, label in KPI_LABELS.items())
    table(d, "Current KPI snapshot", [sql(
        f"SELECT DISTINCT ON (metric_name) CASE metric_name {labels} ELSE metric_name END AS \"Metric\", "
        f"CASE WHEN metric_name LIKE '%f1' THEN to_char(metric_value, 'FM0.000') "
        f"WHEN metric_unit = 'ratio' THEN to_char(metric_value * 100, 'FM990.0') || ' %' "
        f"ELSE to_char(metric_value, 'FM999,999,990') END AS \"Value\", timestamp AS \"Updated\" "
        f"FROM dashboard_metrics WHERE timestamp > NOW() - INTERVAL '10 minutes' "
        f"ORDER BY metric_name, timestamp DESC")],
        "Latest fleet KPIs from dashboard-metrics-updater and Spark.", w=12, h=11,
        overrides=[{"matcher": {"id": "byName", "options": "Value"},
                    "properties": [{"id": "custom.align", "value": "right"}, {"id": "custom.width", "value": 110}]},
                   {"matcher": {"id": "byName", "options": "Updated"}, "properties": clock_column(90)}])
    return d


DASHBOARDS = {
    "flead_home.json": overview,
    "flead_federated_learning.json": federated_learning,
    "flead_anomaly_detection.json": anomalies,
    "flead_device_explorer.json": devices,
    "flead_operations.json": operations,
}
# Dashboards merged into the ones above
RETIRED = ["flead_anomaly_tracker.json", "flead_executive_overview.json", "pipeline_health.json"]


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for filename, build in DASHBOARDS.items():
        dashboard = build().to_json()
        (OUT_DIR / filename).write_text(json.dumps(dashboard, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"wrote {filename}: {dashboard['title']} ({len(dashboard['panels'])} panels)")
    for filename in RETIRED:
        path = OUT_DIR / filename
        if path.exists():
            path.unlink()
            print(f"removed {filename} (merged)")


if __name__ == "__main__":
    main()
