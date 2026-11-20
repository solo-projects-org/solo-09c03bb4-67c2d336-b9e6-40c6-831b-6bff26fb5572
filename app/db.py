"""SQLite persistence layer for the calibration tool."""

from __future__ import annotations

import csv
import io
import json
import re
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Set

from . import solver

SCHEMA = """
CREATE TABLE IF NOT EXISTS households (
    id             INTEGER PRIMARY KEY,
    sample_id      TEXT NOT NULL UNIQUE,
    street         TEXT NOT NULL,
    housing_type   TEXT NOT NULL,
    initial_weight INTEGER NOT NULL CHECK (initial_weight > 0),
    current_weight INTEGER NOT NULL CHECK (current_weight > 0),
    row_order      INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_households_street ON households (street, row_order);
CREATE INDEX IF NOT EXISTS idx_households_type ON households (housing_type, row_order);

CREATE TABLE IF NOT EXISTS street_locks (
    street          TEXT PRIMARY KEY,
    locked_at       TEXT NOT NULL,
    household_count INTEGER NOT NULL CHECK (household_count > 0),
    total_weight    INTEGER NOT NULL CHECK (total_weight > 0)
);

CREATE TABLE IF NOT EXISTS locked_weights (
    household_id INTEGER PRIMARY KEY REFERENCES households (id) ON DELETE CASCADE,
    street       TEXT NOT NULL,
    weight       INTEGER NOT NULL CHECK (weight > 0)
);
CREATE INDEX IF NOT EXISTS idx_locked_weights_street ON locked_weights (street);

CREATE TABLE IF NOT EXISTS publications (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    published_at        TEXT NOT NULL,
    household_count     INTEGER NOT NULL,
    total_weight        INTEGER NOT NULL,
    street_targets_json TEXT NOT NULL,
    type_targets_json   TEXT NOT NULL,
    locked_streets_json TEXT NOT NULL,
    weights_json        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS batches (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    month       TEXT NOT NULL,
    row_count   INTEGER NOT NULL CHECK (row_count > 0),
    inserted    INTEGER NOT NULL CHECK (inserted >= 0),
    replaced    INTEGER NOT NULL CHECK (replaced >= 0),
    accepted_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bills (
    month        TEXT NOT NULL,
    household_id INTEGER NOT NULL REFERENCES households (id) ON DELETE CASCADE,
    usage_wh     INTEGER NOT NULL CHECK (usage_wh >= 0),
    batch_id     INTEGER NOT NULL REFERENCES batches (id) ON DELETE CASCADE,
    updated_at   TEXT NOT NULL,
    PRIMARY KEY (month, household_id)
);
CREATE INDEX IF NOT EXISTS idx_bills_household ON bills (household_id, month);

-- Materialized distribution: current-weight totals per (month, street, usage).
-- Maintained incrementally inside the same transaction as every bill upsert
-- and every weight publication, so it always equals a full recompute from the
-- committed bills and household weights.  Entries never persist with a
-- non-positive sum: _distribution_adjust deletes zero rows and aborts the
-- transaction on a negative one.
CREATE TABLE IF NOT EXISTS distribution (
    month      TEXT NOT NULL,
    street     TEXT NOT NULL,
    usage_wh   INTEGER NOT NULL CHECK (usage_wh >= 0),
    weight_sum INTEGER NOT NULL,
    PRIMARY KEY (month, street, usage_wh)
);
"""

MAX_TARGET = 1_000_000_000
MAX_READING_ROWS = 200_000
MAX_UPLOAD_BYTES = 5 * 1024 * 1024

MONTH_RE = re.compile(r"[0-9]{4}-(0[1-9]|1[0-2])")

_HEADER_ALIASES = {
    "sample_id": {"样本号", "样本编号", "sample_id", "sample id", "id"},
    "street": {"街道", "街道名称", "street"},
    "housing_type": {"住房类型", "房屋类型", "housing_type", "housing type", "type"},
    "initial_weight": {"初始权重", "初始值", "initial_weight", "initial weight"},
}

_READING_HEADER_ALIASES = {
    "sample_id": {"样本号", "样本编号", "sample_id", "sample id", "id"},
    "usage_wh": {
        "用电量",
        "用电量（Wh）",
        "用电量(Wh)",
        "电量",
        "usage",
        "usage_wh",
        "usage wh",
    },
}


class ValidationError(ValueError):
    """Raised when user input or the sample file is invalid."""


@dataclass
class LockOutcome:
    street: str
    count: int
    total: int
    already: bool


@dataclass
class PublishOutcome:
    ok: bool
    updated: int = 0
    total: int = 0
    publication_id: Optional[int] = None
    conflict: Optional[List[dict]] = None


@dataclass
class UploadOutcome:
    month: str
    batch_id: int
    rows: int
    inserted: int
    replaced: int


@dataclass
class DashboardData:
    households: List[dict] = field(default_factory=list)
    streets: List[dict] = field(default_factory=list)
    types: List[dict] = field(default_factory=list)
    locks: Dict[str, dict] = field(default_factory=dict)
    publications: List[dict] = field(default_factory=list)
    summary: dict = field(default_factory=dict)
    months: List[str] = field(default_factory=list)
    selected_month: Optional[str] = None
    month_report: List[dict] = field(default_factory=list)
    batches: List[dict] = field(default_factory=list)


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def _now() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


# ---------------------------------------------------------------------------
# Sample import
# ---------------------------------------------------------------------------


def _canonical_headers(fieldnames: Optional[Iterable[str]]) -> Dict[str, str]:
    if not fieldnames:
        raise ValidationError("样本 CSV 没有表头")
    mapping: Dict[str, str] = {}
    for raw in fieldnames:
        name = (raw or "").strip()
        for canonical, aliases in _HEADER_ALIASES.items():
            if name in aliases:
                if canonical in mapping:
                    raise ValidationError(f"样本 CSV 表头重复：{name}")
                mapping[canonical] = raw
                break
    missing = [c for c in _HEADER_ALIASES if c not in mapping]
    if missing:
        readable = {
            "sample_id": "样本号",
            "street": "街道",
            "housing_type": "住房类型",
            "initial_weight": "初始权重",
        }
        raise ValidationError(
            "样本 CSV 缺少列：" + "、".join(readable[item] for item in missing)
        )
    return mapping


def parse_sample_csv(path: Path) -> List[dict]:
    if not path.is_file():
        raise ValidationError(f"找不到样本 CSV：{path}")
    rows: List[dict] = []
    seen_ids: Set[str] = set()
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        headers = _canonical_headers(reader.fieldnames)
        for line_no, raw in enumerate(reader, start=2):
            sample_id = (raw.get(headers["sample_id"]) or "").strip()
            street = (raw.get(headers["street"]) or "").strip()
            housing_type = (raw.get(headers["housing_type"]) or "").strip()
            weight_text = (raw.get(headers["initial_weight"]) or "").strip()
            if not sample_id:
                raise ValidationError(f"样本 CSV 第 {line_no} 行：样本号为空")
            if sample_id in seen_ids:
                raise ValidationError(f"样本 CSV 第 {line_no} 行：样本号重复（{sample_id}）")
            if not street:
                raise ValidationError(f"样本 CSV 第 {line_no} 行：街道为空")
            if not housing_type:
                raise ValidationError(f"样本 CSV 第 {line_no} 行：住房类型为空")
            if not re.fullmatch(r"[1-9][0-9]*", weight_text):
                raise ValidationError(
                    f"样本 CSV 第 {line_no} 行：初始权重必须是正整数（当前为 {weight_text!r}）"
                )
            seen_ids.add(sample_id)
            rows.append(
                {
                    "sample_id": sample_id,
                    "street": street,
                    "housing_type": housing_type,
                    "initial_weight": int(weight_text),
                }
            )
    if not rows:
        raise ValidationError("样本 CSV 没有任何数据行")
    if len(rows) > 200_000:
        raise ValidationError("样本 CSV 行数过多（上限 200000 行）")
    return rows


def initialize(db_path: Path, sample_csv_path: Path) -> None:
    """Create the schema and seed the fixed sample once."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = _connect(db_path)
    try:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.executescript(SCHEMA)
        count = conn.execute("SELECT COUNT(*) FROM households").fetchone()[0]
        if count:
            return
        rows = parse_sample_csv(Path(sample_csv_path))
        conn.execute("BEGIN IMMEDIATE")
        try:
            # Re-check under the write lock in case another process seeded first.
            if conn.execute("SELECT COUNT(*) FROM households").fetchone()[0]:
                conn.execute("COMMIT")
                return
            conn.executemany(
                """
                INSERT INTO households
                    (sample_id, street, housing_type, initial_weight, current_weight, row_order)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        row["sample_id"],
                        row["street"],
                        row["housing_type"],
                        row["initial_weight"],
                        row["initial_weight"],
                        index,
                    )
                    for index, row in enumerate(rows)
                ],
            )
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Monthly readings (bills) and the materialized distribution
# ---------------------------------------------------------------------------


def validate_month(month: str) -> str:
    text = (month or "").strip()
    if not MONTH_RE.fullmatch(text):
        raise ValidationError(f"月份必须是 YYYY-MM 格式：{month!r}")
    return text


def _canonical_reading_headers(fieldnames: Optional[Iterable[str]]) -> Dict[str, str]:
    if not fieldnames:
        raise ValidationError("读数 CSV 没有表头")
    mapping: Dict[str, str] = {}
    for raw in fieldnames:
        name = (raw or "").strip()
        for canonical, aliases in _READING_HEADER_ALIASES.items():
            if name in aliases:
                if canonical in mapping:
                    raise ValidationError(f"读数 CSV 表头重复：{name}")
                mapping[canonical] = raw
                break
    missing = [c for c in _READING_HEADER_ALIASES if c not in mapping]
    if missing:
        readable = {"sample_id": "样本号", "usage_wh": "用电量"}
        raise ValidationError(
            "读数 CSV 缺少列：" + "、".join(readable[item] for item in missing)
        )
    return mapping


def parse_readings_csv(text: str) -> List[dict]:
    """Parse one uploaded readings CSV.

    The whole batch is rejected (ValidationError) if any row is bad, any
    sample id appears twice, or the file is structurally invalid.  Unknown
    sample ids are rejected later inside the submit transaction.
    """
    rows: List[dict] = []
    seen_ids: Set[str] = set()
    reader = csv.DictReader(io.StringIO(text.lstrip("\ufeff")))
    headers = _canonical_reading_headers(reader.fieldnames)
    for line_no, raw in enumerate(reader, start=2):
        sample_id = (raw.get(headers["sample_id"]) or "").strip()
        usage_text = (raw.get(headers["usage_wh"]) or "").strip()
        if not sample_id:
            raise ValidationError(f"读数 CSV 第 {line_no} 行：样本号为空")
        if sample_id in seen_ids:
            raise ValidationError(
                f"读数 CSV 第 {line_no} 行：批内样本号重复（{sample_id}），整批退回"
            )
        if not re.fullmatch(r"[0-9]+", usage_text):
            raise ValidationError(
                f"读数 CSV 第 {line_no} 行：用电量必须是非负整数（单位 Wh，当前为 {usage_text!r}）"
            )
        seen_ids.add(sample_id)
        rows.append({"sample_id": sample_id, "usage_wh": int(usage_text)})
        if len(rows) > MAX_READING_ROWS:
            raise ValidationError(f"读数 CSV 行数过多（上限 {MAX_READING_ROWS} 行）")
    if not rows:
        raise ValidationError("读数 CSV 没有任何数据行")
    return rows


def _distribution_adjust(
    conn: sqlite3.Connection, month: str, street: str, usage_wh: int, delta: int
) -> None:
    """Add ``delta`` to exactly one distribution entry (incremental refresh)."""
    if delta == 0:
        return
    conn.execute(
        """
        INSERT INTO distribution (month, street, usage_wh, weight_sum)
        VALUES (?, ?, ?, ?)
        ON CONFLICT (month, street, usage_wh)
        DO UPDATE SET weight_sum = weight_sum + excluded.weight_sum
        """,
        (month, street, usage_wh, delta),
    )
    row = conn.execute(
        "SELECT weight_sum FROM distribution WHERE month = ? AND street = ? AND usage_wh = ?",
        (month, street, usage_wh),
    ).fetchone()
    if row is None or row["weight_sum"] < 0:
        raise RuntimeError("内部错误：分布权重出现负值")
    if row["weight_sum"] == 0:
        conn.execute(
            "DELETE FROM distribution WHERE month = ? AND street = ? AND usage_wh = ?",
            (month, street, usage_wh),
        )


def submit_readings(db_path: Path, month: str, rows: List[dict]) -> UploadOutcome:
    """Store one batch of readings; the batch and the distribution commit together.

    Last write wins per (month, household).  Any unknown sample id aborts the
    whole batch with nothing written.
    """
    month = validate_month(month)
    if not rows:
        raise ValidationError("读数批次为空")
    conn = _connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            households = conn.execute(
                "SELECT id, sample_id, street, current_weight FROM households"
            ).fetchall()
            by_sample = {row["sample_id"]: row for row in households}
            unknown = sorted({row["sample_id"] for row in rows} - set(by_sample))
            if unknown:
                preview = "、".join(unknown[:5])
                suffix = f" 等 {len(unknown)} 个" if len(unknown) > 5 else ""
                raise ValidationError(
                    f"读数 CSV 含样本中不存在的样本号：{preview}{suffix}，整批退回"
                )

            now = _now()
            cursor = conn.execute(
                "INSERT INTO batches (month, row_count, inserted, replaced, accepted_at) "
                "VALUES (?, ?, 0, 0, ?)",
                (month, len(rows), now),
            )
            batch_id = cursor.lastrowid
            inserted = 0
            replaced = 0
            for row in rows:
                household = by_sample[row["sample_id"]]
                existing = conn.execute(
                    "SELECT usage_wh FROM bills WHERE month = ? AND household_id = ?",
                    (month, household["id"]),
                ).fetchone()
                if existing is None:
                    inserted += 1
                else:
                    replaced += 1
                    _distribution_adjust(
                        conn,
                        month,
                        household["street"],
                        existing["usage_wh"],
                        -household["current_weight"],
                    )
                conn.execute(
                    """
                    INSERT INTO bills (month, household_id, usage_wh, batch_id, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT (month, household_id)
                    DO UPDATE SET usage_wh = excluded.usage_wh,
                                  batch_id = excluded.batch_id,
                                  updated_at = excluded.updated_at
                    """,
                    (month, household["id"], row["usage_wh"], batch_id, now),
                )
                _distribution_adjust(
                    conn,
                    month,
                    household["street"],
                    row["usage_wh"],
                    household["current_weight"],
                )
            conn.execute(
                "UPDATE batches SET inserted = ?, replaced = ? WHERE id = ?",
                (inserted, replaced, batch_id),
            )
            conn.execute("COMMIT")
            return UploadOutcome(
                month=month,
                batch_id=batch_id,
                rows=len(rows),
                inserted=inserted,
                replaced=replaced,
            )
        except BaseException:
            conn.execute("ROLLBACK")
            raise
    finally:
        conn.close()


def _apply_weight_deltas(
    conn: sqlite3.Connection,
    deltas: Mapping[int, int],
    street_by_household: Mapping[int, str],
) -> None:
    """Refresh distribution entries for households whose weight changed.

    Only the (month, street, usage) entries those households actually
    reported are touched, across every month they reported.
    """
    aggregated: Dict[tuple, int] = {}
    for household_id, delta in deltas.items():
        if delta == 0:
            continue
        street = street_by_household[household_id]
        for bill in conn.execute(
            "SELECT month, usage_wh FROM bills WHERE household_id = ?", (household_id,)
        ):
            key = (bill["month"], street, bill["usage_wh"])
            aggregated[key] = aggregated.get(key, 0) + delta
    for (month, street, usage_wh), delta in aggregated.items():
        _distribution_adjust(conn, month, street, usage_wh, delta)


def get_bills(db_path: Path) -> List[dict]:
    conn = _connect(db_path)
    try:
        return [
            dict(row)
            for row in conn.execute(
                """
                SELECT b.month, h.sample_id, h.street, b.usage_wh, h.current_weight,
                       b.batch_id, b.updated_at
                FROM bills b JOIN households h ON h.id = b.household_id
                ORDER BY b.month, h.sample_id
                """
            )
        ]
    finally:
        conn.close()


def get_distribution(db_path: Path) -> List[dict]:
    conn = _connect(db_path)
    try:
        return [
            dict(row)
            for row in conn.execute(
                "SELECT month, street, usage_wh, weight_sum FROM distribution "
                "ORDER BY month, street, usage_wh"
            )
        ]
    finally:
        conn.close()


def weighted_quantile_usage(
    entries: Iterable[tuple], numerator: int = 9, denominator: int = 10
) -> Optional[int]:
    """Smallest usage whose low-to-high cumulative weight reaches the fraction.

    ``entries`` are (usage, weight) pairs; the threshold is
    ``numerator / denominator`` of the total weight (default 90%).  Returns
    ``None`` when there is nothing reported.  Integer arithmetic only.
    """
    items = sorted((int(usage), int(weight)) for usage, weight in entries)
    total = sum(weight for _, weight in items)
    if total <= 0:
        return None
    cumulative = 0
    for usage, weight in items:
        cumulative += weight
        if cumulative * denominator >= numerator * total:
            return usage
    return None  # pragma: no cover - unreachable when total > 0


def _monthly_report_rows(conn: sqlite3.Connection, month: str) -> List[dict]:
    streets = conn.execute(
        """
        SELECT street, COUNT(*) AS household_count, SUM(current_weight) AS total_weight
        FROM households GROUP BY street ORDER BY street
        """
    ).fetchall()
    reported_counts = {
        row["street"]: row["n"]
        for row in conn.execute(
            """
            SELECT h.street AS street, COUNT(*) AS n
            FROM bills b JOIN households h ON h.id = b.household_id
            WHERE b.month = ? GROUP BY h.street
            """,
            (month,),
        )
    }
    entries_by_street: Dict[str, List[tuple]] = {}
    for row in conn.execute(
        "SELECT street, usage_wh, weight_sum FROM distribution WHERE month = ? "
        "ORDER BY street, usage_wh",
        (month,),
    ):
        entries_by_street.setdefault(row["street"], []).append(
            (row["usage_wh"], row["weight_sum"])
        )

    report: List[dict] = []
    for street in streets:
        name = street["street"]
        entries = entries_by_street.get(name, [])
        reported_weight = sum(weight for _, weight in entries)
        reported = reported_weight > 0
        total_weight = street["total_weight"]
        coverage_pct = (
            round(100.0 * reported_weight / total_weight, 1) if total_weight else None
        )
        report.append(
            {
                "street": name,
                "household_count": street["household_count"],
                "reported_households": reported_counts.get(name, 0),
                "reported_weight": reported_weight,
                "total_weight": total_weight,
                "reported": reported,
                "coverage_pct": coverage_pct,
                "coverage_text": f"{coverage_pct:.1f}%" if coverage_pct is not None else "—",
                "p90_usage": weighted_quantile_usage(entries) if reported else None,
            }
        )
    return report


def get_monthly_report(db_path: Path, month: str) -> List[dict]:
    month = validate_month(month)
    conn = _connect(db_path)
    try:
        conn.execute("BEGIN")
        try:
            report = _monthly_report_rows(conn, month)
            conn.execute("COMMIT")
            return report
        except BaseException:
            conn.execute("ROLLBACK")
            raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Read helpers
# ---------------------------------------------------------------------------


def _load_locks(conn: sqlite3.Connection) -> Dict[str, dict]:
    locks: Dict[str, dict] = {}
    for row in conn.execute("SELECT * FROM street_locks ORDER BY street"):
        locks[row["street"]] = {
            "street": row["street"],
            "locked_at": row["locked_at"],
            "household_count": row["household_count"],
            "total_weight": row["total_weight"],
            "weights": {},
        }
    for row in conn.execute("SELECT household_id, street, weight FROM locked_weights"):
        if row["street"] in locks:
            locks[row["street"]]["weights"][row["household_id"]] = row["weight"]
    return locks


def _households(conn: sqlite3.Connection) -> List[dict]:
    return [
        dict(row)
        for row in conn.execute(
            """
            SELECT id, sample_id, street, housing_type, initial_weight, current_weight
            FROM households ORDER BY row_order
            """
        )
    ]


def get_dashboard(db_path: Path, month: Optional[str] = None) -> DashboardData:
    conn = _connect(db_path)
    try:
        conn.execute("BEGIN")
        households = _households(conn)
        locks = _load_locks(conn)
        publication_rows = [
            {
                "id": row["id"],
                "published_at": row["published_at"],
                "household_count": row["household_count"],
                "total_weight": row["total_weight"],
            }
            for row in conn.execute(
                "SELECT id, published_at, household_count, total_weight "
                "FROM publications ORDER BY id DESC LIMIT 5"
            )
        ]
        publication_count = conn.execute("SELECT COUNT(*) FROM publications").fetchone()[0]
        months = [
            row["month"]
            for row in conn.execute(
                "SELECT DISTINCT month FROM bills ORDER BY month DESC"
            )
        ]
        bill_count = conn.execute("SELECT COUNT(*) FROM bills").fetchone()[0]
        batches = [
            dict(row)
            for row in conn.execute(
                "SELECT id, month, row_count, inserted, replaced, accepted_at "
                "FROM batches ORDER BY id DESC LIMIT 5"
            )
        ]
        selected_month: Optional[str] = None
        if month and MONTH_RE.fullmatch(month.strip()):
            selected_month = month.strip()
        elif months:
            selected_month = months[0]
        month_report = (
            _monthly_report_rows(conn, selected_month) if selected_month else []
        )
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()

    street_groups: Dict[str, dict] = {}
    type_groups: Dict[str, dict] = {}
    for household in households:
        household["locked"] = household["street"] in locks
        street = household["street"]
        if street not in street_groups:
            street_groups[street] = {
                "name": street,
                "households": [],
                "count": 0,
                "initial_sum": 0,
                "current_sum": 0,
                "locked": street in locks,
            }
        group = street_groups[street]
        group["households"].append(household)
        group["count"] += 1
        group["initial_sum"] += household["initial_weight"]
        group["current_sum"] += household["current_weight"]

        housing_type = household["housing_type"]
        if housing_type not in type_groups:
            type_groups[housing_type] = {
                "name": housing_type,
                "households": [],
                "count": 0,
                "initial_sum": 0,
                "current_sum": 0,
            }
        type_group = type_groups[housing_type]
        type_group["households"].append(household)
        type_group["count"] += 1
        type_group["initial_sum"] += household["initial_weight"]
        type_group["current_sum"] += household["current_weight"]

    for name, group in street_groups.items():
        group["lock"] = locks.get(name)
        group["target_value"] = group["current_sum"]
        group["min_target"] = group["count"]
        group["max_target"] = 2 * group["initial_sum"]
        if name in locks:
            group["target_value"] = locks[name]["total_weight"]
            group["min_target"] = locks[name]["total_weight"]
            group["max_target"] = locks[name]["total_weight"]

    for group in type_groups.values():
        group["min_target"] = group["count"]
        group["max_target"] = 2 * group["initial_sum"]

    summary = {
        "household_count": len(households),
        "street_count": len(street_groups),
        "type_count": len(type_groups),
        "total_initial": sum(item["initial_weight"] for item in households),
        "total_current": sum(item["current_weight"] for item in households),
        "locked_streets": len(locks),
        "publication_count": publication_count,
        "last_published_at": publication_rows[0]["published_at"] if publication_rows else None,
        "bill_count": bill_count,
        "month_count": len(months),
    }

    return DashboardData(
        households=households,
        streets=list(street_groups.values()),
        types=list(type_groups.values()),
        locks=locks,
        publications=publication_rows,
        summary=summary,
        months=months,
        selected_month=selected_month,
        month_report=month_report,
        batches=batches,
    )


# ---------------------------------------------------------------------------
# Locking
# ---------------------------------------------------------------------------


def lock_street(db_path: Path, street: str) -> LockOutcome:
    conn = _connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            rows = conn.execute(
                "SELECT id, current_weight FROM households WHERE street = ? ORDER BY row_order",
                (street,),
            ).fetchall()
            if not rows:
                raise ValidationError(f"街道不存在：{street}")
            existing = conn.execute(
                "SELECT * FROM street_locks WHERE street = ?", (street,)
            ).fetchone()
            if existing:
                conn.execute("COMMIT")
                return LockOutcome(
                    street=street,
                    count=existing["household_count"],
                    total=existing["total_weight"],
                    already=True,
                )
            total = sum(row["current_weight"] for row in rows)
            conn.execute(
                "INSERT INTO street_locks (street, locked_at, household_count, total_weight) "
                "VALUES (?, ?, ?, ?)",
                (street, _now(), len(rows), total),
            )
            conn.executemany(
                "INSERT INTO locked_weights (household_id, street, weight) VALUES (?, ?, ?)",
                [(row["id"], street, row["current_weight"]) for row in rows],
            )
            conn.execute("COMMIT")
            return LockOutcome(street=street, count=len(rows), total=total, already=False)
        except BaseException:
            conn.execute("ROLLBACK")
            raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Publishing
# ---------------------------------------------------------------------------


def _solver_households(rows: List[dict]) -> List[solver.Household]:
    return [
        solver.Household(
            id=row["id"],
            sample_id=row["sample_id"],
            street=row["street"],
            housing_type=row["housing_type"],
            initial_weight=row["initial_weight"],
            current_weight=row["current_weight"],
        )
        for row in rows
    ]


def _describe_conflict(
    constraints: List[solver.Constraint],
    street_targets: Mapping[str, int],
    type_targets: Mapping[str, int],
    locks: Mapping[str, dict],
) -> List[dict]:
    described: List[dict] = []
    for kind, name in constraints:
        if kind == "street":
            described.append(
                {
                    "kind": "street",
                    "name": name,
                    "label": f"街道「{name}」目标户数 = {street_targets[name]:,}",
                }
            )
        elif kind == "type":
            described.append(
                {
                    "kind": "type",
                    "name": name,
                    "label": f"住房类型「{name}」目标户数 = {type_targets[name]:,}",
                }
            )
        elif kind == "lock":
            lock = locks[name]
            described.append(
                {
                    "kind": "lock",
                    "name": name,
                    "label": (
                        f"街道「{name}」锁定（{lock['household_count']} 户样本，"
                        f"锁定合计 {lock['total_weight']:,}）"
                    ),
                }
            )
    return described


def publish(
    db_path: Path,
    street_targets: Mapping[str, int],
    type_targets: Mapping[str, int],
) -> PublishOutcome:
    """Atomically calibrate and publish; never partially writes weights."""
    conn = _connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            rows = _households(conn)
            if not rows:
                raise ValidationError("样本为空，无法校准")
            locks = _load_locks(conn)
            unknown = set(street_targets) - {row["street"] for row in rows}
            if unknown:
                raise ValidationError("目标中出现样本里不存在的街道：" + "、".join(sorted(unknown)))
            # A locked street's weights are frozen, so its total is already
            # implied by the lock; its submitted value is ignored and only the
            # remaining streets are target conditions the statistician adjusts.
            street_targets = {
                name: value for name, value in street_targets.items() if name not in locks
            }
            streets = {row["street"] for row in rows} - set(locks)
            types = {row["housing_type"] for row in rows}
            if set(street_targets) != streets:
                raise ValidationError("街道目标与样本中的街道不一致")
            if set(type_targets) != types:
                raise ValidationError("住房类型目标与样本中的类型不一致")
            for name, value in street_targets.items():
                if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                    raise ValidationError(f"街道「{name}」的目标户数必须是非负整数")
                if value > MAX_TARGET:
                    raise ValidationError(f"街道「{name}」的目标户数过大")
            for name, value in type_targets.items():
                if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                    raise ValidationError(f"住房类型「{name}」的目标户数必须是非负整数")
                if value > MAX_TARGET:
                    raise ValidationError(f"住房类型「{name}」的目标户数过大")

            # Only the unlocked streets contribute target constraints; the
            # locked ones contribute their frozen per-household weights.
            lock_weights = {
                household_id: weight
                for lock in locks.values()
                for household_id, weight in lock["weights"].items()
            }
            households = _solver_households(rows)
            locked_streets = set(locks)

            result = solver.solve(
                households, street_targets, type_targets, lock_weights, locked_streets
            )
            if not result.feasible:
                constraints = solver.find_conflict(
                    households, street_targets, type_targets, lock_weights, locked_streets
                )
                conflict = (
                    _describe_conflict(constraints, street_targets, type_targets, locks)
                    if constraints
                    else []
                )
                conn.execute("ROLLBACK")
                return PublishOutcome(ok=False, conflict=conflict)

            weights = result.weights or {}
            for street, lock in locks.items():
                for household_id, locked_weight in lock["weights"].items():
                    if weights.get(household_id) != locked_weight:
                        raise RuntimeError("内部错误：求解结果偏离锁定权重")

            updated = len(weights)
            total = sum(weights.values())
            # Refresh the distribution for every household whose weight
            # changes, across all months that household has reported, inside
            # the same transaction so weights and distribution commit together.
            deltas = {
                row["id"]: weights[row["id"]] - row["current_weight"]
                for row in rows
                if weights.get(row["id"]) != row["current_weight"]
            }
            if deltas:
                street_by_household = {row["id"]: row["street"] for row in rows}
                _apply_weight_deltas(conn, deltas, street_by_household)
            conn.executemany(
                "UPDATE households SET current_weight = ? WHERE id = ?",
                [(weight, household_id) for household_id, weight in weights.items()],
            )
            conn.execute(
                """
                INSERT INTO publications
                    (published_at, household_count, total_weight, street_targets_json,
                     type_targets_json, locked_streets_json, weights_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    _now(),
                    updated,
                    total,
                    json.dumps(dict(street_targets), ensure_ascii=False, sort_keys=True),
                    json.dumps(dict(type_targets), ensure_ascii=False, sort_keys=True),
                    json.dumps(sorted(locked_streets), ensure_ascii=False),
                    json.dumps(weights, ensure_ascii=False, sort_keys=True),
                ),
            )
            publication_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            conn.execute("COMMIT")
            return PublishOutcome(
                ok=True, updated=updated, total=total, publication_id=publication_id
            )
        except BaseException:
            conn.execute("ROLLBACK")
            raise
    finally:
        conn.close()
