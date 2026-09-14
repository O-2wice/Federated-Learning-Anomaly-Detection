"""The provisioned Grafana dashboards reference real data sources, lay out cleanly and query cheaply."""
import json
import re
from pathlib import Path

import pytest

DASHBOARD_DIR = Path(__file__).resolve().parent.parent / "grafana" / "dashboards"
DATASOURCE_UIDS = {"flead-timescaledb", "flead-prometheus", "-- Mixed --", "-- Grafana --"}
EXPECTED_UIDS = {"flead-home", "flead-fl-lab", "flead-anomaly", "flead-device", "flead-ops"}
# Tables that grow with the stream; queries must bound them by time, device or LIMIT
LARGE_TABLES = ("iot_data", "anomalies", "local_models", "local_model_updates", "stream_analysis_results",
                "model_evaluations")


def dashboards():
    return {path.name: json.loads(path.read_text(encoding="utf-8")) for path in sorted(DASHBOARD_DIR.glob("*.json"))}


def panels(dashboard):
    stack = list(dashboard.get("panels", []))
    while stack:
        panel = stack.pop()
        stack.extend(panel.get("panels", []))
        yield panel


def test_expected_dashboards_and_unique_uids():
    loaded = dashboards()
    uids = [d["uid"] for d in loaded.values()]
    assert len(uids) == len(set(uids))
    assert set(uids) == EXPECTED_UIDS


@pytest.mark.parametrize("name", sorted(dashboards()))
def test_panels_use_provisioned_datasources(name):
    for panel in panels(dashboards()[name]):
        if panel.get("type") in ("row", "text"):
            continue
        assert panel["datasource"]["uid"] in DATASOURCE_UIDS, (name, panel["title"])
        for target in panel.get("targets", []):
            assert target["datasource"]["uid"] in DATASOURCE_UIDS, (name, panel["title"])


@pytest.mark.parametrize("name", sorted(dashboards()))
def test_layout_fits_grid_and_ids_are_unique(name):
    ids = []
    for panel in panels(dashboards()[name]):
        pos = panel["gridPos"]
        assert 0 <= pos["x"] and pos["x"] + pos["w"] <= 24, (name, panel.get("title"))
        ids.append(panel["id"])
    assert len(ids) == len(set(ids))


@pytest.mark.parametrize("name", sorted(dashboards()))
def test_stat_titles_fit_their_panels(name):
    # Stat panels are 4 of 24 columns wide; longer titles are cut off on a 1600 px screen
    for panel in panels(dashboards()[name]):
        if panel.get("type") == "stat":
            assert len(panel["title"]) <= 18, (name, panel["title"])


@pytest.mark.parametrize("name", sorted(dashboards()))
def test_large_tables_are_never_scanned_unbounded(name):
    bounds = re.compile(r"\$__timeFilter|NOW\(\) - INTERVAL|device_id = |LIMIT 1\b", re.IGNORECASE)
    for panel in panels(dashboards()[name]):
        for target in panel.get("targets", []):
            query = target.get("rawSql") or ""
            # Check each FROM clause up to the next FROM, so subqueries are judged separately
            for match in re.finditer(r"FROM\s+(\w+)", query, re.IGNORECASE):
                table = match.group(1).lower()
                if table not in LARGE_TABLES:
                    continue
                following = re.split(r"\bFROM\b", query[match.end():], maxsplit=1, flags=re.IGNORECASE)[0]
                assert bounds.search(following), (name, panel.get("title"), table, query)
