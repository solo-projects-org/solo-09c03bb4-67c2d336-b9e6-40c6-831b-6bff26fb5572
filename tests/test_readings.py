"""Tests for monthly readings upload, distribution maintenance, and quantiles."""

from __future__ import annotations

import csv
import sqlite3
from pathlib import Path

import pytest

from app import db


SAMPLE_ROWS = [
    ("H01", "A", "T1", 9),
    ("H02", "A", "T1", 1),
    ("H03", "A", "T2", 5),
    ("H04", "B", "T1", 4),
    ("H05", "B", "T2", 6),
]


def write_sample(path: Path) -> Path:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["样本号", "街道", "住房类型", "初始权重"])
        writer.writerows(SAMPLE_ROWS)
    return path


@pytest.fixture()
def database(tmp_path: Path) -> Path:
    path = tmp_path / "survey.sqlite3"
    db.initialize(path, write_sample(tmp_path / "sample.csv"))
    return path


def readings_csv(rows) -> str:
    lines = ["样本号,用电量"] + [f"{sid},{usage}" for sid, usage in rows]
    return "\n".join(lines) + "\n"


def submit(db_path: Path, month: str, rows) -> db.UploadOutcome:
    return db.submit_readings(db_path, month, db.parse_readings_csv(readings_csv(rows)))


# ---------------------------------------------------------------------------
# Recompute helpers: derive everything from committed bills + weights.
# ---------------------------------------------------------------------------


def recompute_distribution(db_path: Path) -> dict:
    accrued = {}
    for bill in db.get_bills(db_path):
        key = (bill["month"], bill["street"], bill["usage_wh"])
        accrued[key] = accrued.get(key, 0) + bill["current_weight"]
    return accrued


def materialized_distribution(db_path: Path) -> dict:
    return {
        (row["month"], row["street"], row["usage_wh"]): row["weight_sum"]
        for row in db.get_distribution(db_path)
    }


def recompute_report(db_path: Path, month: str) -> dict:
    data = db.get_dashboard(db_path)
    totals = {}
    for household in data.households:
        totals[household["street"]] = (
            totals.get(household["street"], 0) + household["current_weight"]
        )
    entries = {}
    for bill in db.get_bills(db_path):
        if bill["month"] != month:
            continue
        entries.setdefault(bill["street"], []).append(
            (bill["usage_wh"], bill["current_weight"])
        )
    report = {}
    for street, total in totals.items():
        street_entries = entries.get(street, [])
        reported_weight = sum(weight for _, weight in street_entries)
        report[street] = {
            "reported": reported_weight > 0,
            "reported_weight": reported_weight,
            "total_weight": total,
            "p90_usage": db.weighted_quantile_usage(street_entries),
        }
    return report


def assert_consistent(db_path: Path):
    """Materialized state must equal a recompute from committed bills+weights."""
    assert materialized_distribution(db_path) == recompute_distribution(db_path)
    months = {bill["month"] for bill in db.get_bills(db_path)}
    for month in months:
        actual = {row["street"]: row for row in db.get_monthly_report(db_path, month)}
        expected = recompute_report(db_path, month)
        for street, want in expected.items():
            got = actual[street]
            assert got["reported"] == want["reported"]
            assert got["reported_weight"] == want["reported_weight"]
            assert got["total_weight"] == want["total_weight"]
            assert got["p90_usage"] == want["p90_usage"]


def snapshot(db_path: Path) -> tuple:
    return (
        db.get_bills(db_path),
        db.get_distribution(db_path),
        db.get_dashboard(db_path).batches,
    )


# ---------------------------------------------------------------------------
# Parsing and validation
# ---------------------------------------------------------------------------


def test_parse_rejects_duplicate_sample_ids():
    with pytest.raises(db.ValidationError, match="重复"):
        db.parse_readings_csv(readings_csv([("H01", 100), ("H01", 200)]))


def test_parse_rejects_negative_and_fractional_usage():
    with pytest.raises(db.ValidationError, match="非负整数"):
        db.parse_readings_csv(readings_csv([("H01", -5)]))
    with pytest.raises(db.ValidationError, match="非负整数"):
        db.parse_readings_csv(readings_csv([("H01", "3.5")]))
    with pytest.raises(db.ValidationError, match="非负整数"):
        db.parse_readings_csv(readings_csv([("H01", "")]))


def test_parse_accepts_zero_usage():
    rows = db.parse_readings_csv(readings_csv([("H01", 0)]))
    assert rows == [{"sample_id": "H01", "usage_wh": 0}]


def test_parse_rejects_missing_column_and_empty_file():
    with pytest.raises(db.ValidationError, match="缺少列"):
        db.parse_readings_csv("样本号\nH01\n")
    with pytest.raises(db.ValidationError, match="没有表头"):
        db.parse_readings_csv("")
    with pytest.raises(db.ValidationError, match="没有任何数据行"):
        db.parse_readings_csv("样本号,用电量\n")


def test_parse_accepts_alias_headers_and_bom():
    rows = db.parse_readings_csv("\ufeff样本编号,用电量（Wh）\nH01,120\n")
    assert rows == [{"sample_id": "H01", "usage_wh": 120}]


def test_validate_month():
    assert db.validate_month(" 2026-10 ") == "2026-10"
    for bad in ("2026-13", "2026-00", "2026-1", "26-10", "2026/10", ""):
        with pytest.raises(db.ValidationError):
            db.validate_month(bad)


def test_quantile_integer_math():
    # First usage whose cumulative weight reaches exactly 90% must be taken.
    assert db.weighted_quantile_usage([(100, 9), (200, 1)]) == 100
    assert db.weighted_quantile_usage([(200, 1), (100, 9)]) == 100
    assert db.weighted_quantile_usage([(100, 8), (200, 2)]) == 200
    assert db.weighted_quantile_usage([(100, 1)]) == 100
    assert db.weighted_quantile_usage([]) is None


# ---------------------------------------------------------------------------
# Batch submission semantics
# ---------------------------------------------------------------------------


def test_submit_inserts_bills_and_distribution(database: Path):
    outcome = submit(database, "2026-10", [("H01", 100), ("H02", 200), ("H04", 150)])
    assert (outcome.inserted, outcome.replaced, outcome.rows) == (3, 0, 3)
    assert materialized_distribution(database) == {
        ("2026-10", "A", 100): 9,
        ("2026-10", "A", 200): 1,
        ("2026-10", "B", 150): 4,
    }
    assert_consistent(database)


def test_last_successful_write_wins_same_month(database: Path):
    submit(database, "2026-10", [("H01", 100), ("H02", 200)])
    outcome = submit(database, "2026-10", [("H01", 300)])
    assert (outcome.inserted, outcome.replaced) == (0, 1)
    bills = {bill["sample_id"]: bill["usage_wh"] for bill in db.get_bills(database)}
    assert bills == {"H01": 300, "H02": 200}
    # The stale 100 Wh entry is gone; 300 Wh carries H01's weight.
    assert materialized_distribution(database) == {
        ("2026-10", "A", 200): 1,
        ("2026-10", "A", 300): 9,
    }
    assert_consistent(database)


def test_same_usage_overwrite_keeps_distribution(database: Path):
    submit(database, "2026-10", [("H01", 100)])
    submit(database, "2026-10", [("H01", 100)])
    assert materialized_distribution(database) == {("2026-10", "A", 100): 9}
    assert_consistent(database)


def test_unknown_sample_id_rejects_whole_batch(database: Path):
    before = snapshot(database)
    with pytest.raises(db.ValidationError, match="不存在"):
        submit(database, "2026-10", [("H01", 100), ("H99", 200)])
    assert snapshot(database) == before


def test_months_are_independent(database: Path):
    submit(database, "2026-10", [("H01", 100)])
    submit(database, "2026-11", [("H01", 900)])
    assert materialized_distribution(database) == {
        ("2026-10", "A", 100): 9,
        ("2026-11", "A", 900): 9,
    }
    assert_consistent(database)


# ---------------------------------------------------------------------------
# Monthly report: coverage, quantile, unreported streets
# ---------------------------------------------------------------------------


def test_monthly_report_quantile_boundary_and_unreported(database: Path):
    # Street A: weights 9 (H01) and 1 (H02) -> reported total 10, 90% = 9.
    # Cumulative reaches exactly 9 at 100 Wh, so P90 = 100.
    submit(database, "2026-10", [("H01", 100), ("H02", 200)])
    report = {row["street"]: row for row in db.get_monthly_report(database, "2026-10")}
    street_a = report["A"]
    assert street_a["reported"] is True
    assert street_a["reported_weight"] == 10
    assert street_a["total_weight"] == 15
    assert street_a["reported_households"] == 2
    assert street_a["household_count"] == 3
    assert street_a["p90_usage"] == 100
    street_b = report["B"]
    assert street_b["reported"] is False
    assert street_b["reported_weight"] == 0
    assert street_b["total_weight"] == 10
    assert street_b["p90_usage"] is None


def test_report_uses_initial_weights_before_first_publish(database: Path):
    submit(database, "2026-10", [("H04", 500)])
    data = db.get_dashboard(database)
    assert data.summary["publication_count"] == 0
    report = {row["street"]: row for row in db.get_monthly_report(database, "2026-10")}
    assert report["B"]["reported_weight"] == 4  # initial weight of H04


# ---------------------------------------------------------------------------
# Publication refreshes the distribution for all reported months
# ---------------------------------------------------------------------------


def publish_ok(db_path: Path, street_targets: dict, type_targets: dict):
    outcome = db.publish(db_path, street_targets, type_targets)
    assert outcome.ok, outcome.conflict
    return outcome


def test_publish_refreshes_distribution_across_months(database: Path):
    submit(database, "2026-10", [("H01", 100), ("H02", 200), ("H04", 150)])
    submit(database, "2026-11", [("H01", 800), ("H05", 600)])
    before_weights = {
        h["sample_id"]: h["current_weight"] for h in db.get_dashboard(database).households
    }
    publish_ok(database, {"A": 12, "B": 13}, {"T1": 13, "T2": 12})
    after_weights = {
        h["sample_id"]: h["current_weight"] for h in db.get_dashboard(database).households
    }
    changed = {sid for sid in after_weights if after_weights[sid] != before_weights[sid]}
    assert changed, "测试前提：发布必须改变至少一户权重"

    assert_consistent(database)
    # Every reported month of a changed household reflects the new weight.
    dist = materialized_distribution(database)
    for bill in db.get_bills(database):
        sid_weight = after_weights[bill["sample_id"]]
        key = (bill["month"], bill["street"], bill["usage_wh"])
        assert dist[key] >= sid_weight or sid_weight != before_weights[bill["sample_id"]]


def test_failed_publish_leaves_distribution_untouched(database: Path):
    submit(database, "2026-10", [("H01", 100), ("H04", 150)])
    before = snapshot(database)
    outcome = db.publish(database, {"A": 100, "B": 1}, {"T1": 50, "T2": 50})
    assert not outcome.ok
    assert snapshot(database) == before
    assert_consistent(database)


def test_failed_batch_between_publishes_leaves_state(database: Path):
    submit(database, "2026-10", [("H01", 100)])
    publish_ok(database, {"A": 12, "B": 13}, {"T1": 13, "T2": 12})
    before = snapshot(database)
    with pytest.raises(db.ValidationError):
        submit(database, "2026-10", [("H02", 5), ("H02", 6)])  # duplicate in batch
    with pytest.raises(db.ValidationError):
        submit(database, "2026-13", [("H02", 5)])  # bad month
    assert snapshot(database) == before
    assert_consistent(database)


# ---------------------------------------------------------------------------
# Interleaved writes: quantiles and coverage recomputable from committed state
# ---------------------------------------------------------------------------


def test_interleaved_uploads_and_publishes(database: Path):
    submit(database, "2026-10", [("H01", 100), ("H02", 200), ("H04", 150)])
    assert_consistent(database)

    publish_ok(database, {"A": 12, "B": 13}, {"T1": 13, "T2": 12})
    assert_consistent(database)

    # Back-report: overwrite one October reading, add a new street, open a
    # new month.
    submit(database, "2026-10", [("H01", 900), ("H03", 300)])
    submit(database, "2026-11", [("H01", 700), ("H05", 600), ("H04", 150)])
    assert_consistent(database)

    publish_ok(database, {"A": 15, "B": 10}, {"T1": 14, "T2": 11})
    assert_consistent(database)

    # Spot-check the October quantile against a manual recompute.
    report = {row["street"]: row for row in db.get_monthly_report(database, "2026-10")}
    expected = recompute_report(database, "2026-10")
    assert report["A"]["p90_usage"] == expected["A"]["p90_usage"]
    assert report["A"]["reported_weight"] == expected["A"]["reported_weight"]


def test_state_survives_reopen(database: Path):
    submit(database, "2026-10", [("H01", 100), ("H04", 150)])
    publish_ok(database, {"A": 12, "B": 13}, {"T1": 13, "T2": 12})
    submit(database, "2026-11", [("H02", 50)])
    expected_distribution = materialized_distribution(database)
    expected_report = db.get_monthly_report(database, "2026-10")

    # Fresh connections only; nothing in memory is shared between calls, but
    # also verify via a raw sqlite3 connection that the rows are on disk.
    raw = sqlite3.connect(str(database))
    try:
        dist_rows = raw.execute(
            "SELECT month, street, usage_wh, weight_sum FROM distribution"
        ).fetchall()
        bill_rows = raw.execute("SELECT COUNT(*) FROM bills").fetchone()[0]
    finally:
        raw.close()
    assert len(dist_rows) == len(expected_distribution)
    assert bill_rows == 3
    assert db.get_monthly_report(database, "2026-10") == expected_report
    assert_consistent(database)


def test_dashboard_lists_months_and_batches(database: Path):
    submit(database, "2026-10", [("H01", 100)])
    submit(database, "2026-11", [("H01", 200), ("H02", 300)])
    data = db.get_dashboard(database)
    assert data.months == ["2026-11", "2026-10"]
    assert data.selected_month == "2026-11"
    assert data.summary["bill_count"] == 3
    assert data.summary["month_count"] == 2
    assert [batch["month"] for batch in data.batches] == ["2026-11", "2026-10"]
    assert data.batches[0]["inserted"] == 2

    october = db.get_dashboard(database, month="2026-10")
    assert october.selected_month == "2026-10"
    streets = {row["street"] for row in october.month_report}
    assert streets == {"A", "B"}
