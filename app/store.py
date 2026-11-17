"""SQLite 读写：批次快照、运行清单、隔离行、键值状态。

只放数据访问，不放判定；调用方拿到的是普通字典。
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
from pathlib import Path

from app import config

_LOCK = threading.Lock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshots (
    schema_id      TEXT PRIMARY KEY,
    version_label  TEXT NOT NULL,
    source_path    TEXT NOT NULL,
    source_sha256  TEXT NOT NULL,
    encoding       TEXT NOT NULL,
    bom            INTEGER NOT NULL,
    field_count    INTEGER NOT NULL,
    blocked_count  INTEGER NOT NULL,
    created_at     TEXT NOT NULL,
    payload_json   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS runs (
    run_id         TEXT PRIMARY KEY,
    batch_key      TEXT NOT NULL UNIQUE,
    old_schema_id  TEXT NOT NULL,
    new_schema_id  TEXT NOT NULL,
    version_seq    INTEGER NOT NULL,
    input_sha256   TEXT NOT NULL,
    ruleset        TEXT NOT NULL,
    created_at     TEXT NOT NULL,
    effective_from TEXT NOT NULL,
    effective_to   TEXT,
    summary_json   TEXT NOT NULL,
    payload_json   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS quarantine (
    schema_id     TEXT NOT NULL,
    line          INTEGER,
    table_name    TEXT,
    column_name   TEXT,
    reason        TEXT NOT NULL,
    blocking      INTEGER NOT NULL,
    detail        TEXT,
    raw           TEXT
);
CREATE TABLE IF NOT EXISTS kv (
    k TEXT PRIMARY KEY,
    v TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS orders (
    order_id        TEXT PRIMARY KEY,
    run_id          TEXT NOT NULL,
    revision        INTEGER NOT NULL,
    supersedes      TEXT,
    status          TEXT NOT NULL,
    content_text    TEXT NOT NULL,
    content_sha256  TEXT NOT NULL,
    reason          TEXT,
    amendments_json TEXT NOT NULL,
    idempotency_key TEXT UNIQUE,
    issued_at       TEXT NOT NULL,
    voided_at       TEXT,
    void_reason     TEXT,
    settled_at      TEXT
);
CREATE TABLE IF NOT EXISTS signoffs (
    order_id        TEXT PRIMARY KEY,
    signer          TEXT NOT NULL,
    signed_at       TEXT NOT NULL,
    receipt         TEXT NOT NULL UNIQUE,
    content_sha256  TEXT NOT NULL,
    idempotency_key TEXT UNIQUE
);
CREATE TABLE IF NOT EXISTS compensations (
    comp_id         TEXT PRIMARY KEY,
    order_id        TEXT NOT NULL,
    new_order_id    TEXT NOT NULL,
    status          TEXT NOT NULL,
    idempotency_key TEXT UNIQUE,
    created_at      TEXT NOT NULL,
    confirmed_at    TEXT
);
CREATE INDEX IF NOT EXISTS idx_quarantine_schema ON quarantine(schema_id);
CREATE INDEX IF NOT EXISTS idx_orders_run ON orders(run_id, revision);
CREATE INDEX IF NOT EXISTS idx_compensations_order ON compensations(order_id);
"""


def db_path() -> Path:
    raw = os.environ.get("LINEAGEGUARD_DB", "").strip()
    path = Path(raw) if raw else config.DEFAULT_DB
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(db_path(), timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db() -> None:
    with _LOCK, _connect() as conn:
        conn.executescript(SCHEMA)


def reset() -> None:
    """测试与重建用：清空全部业务表。"""
    with _LOCK, _connect() as conn:
        conn.executescript(SCHEMA)
        for table in ("snapshots", "runs", "quarantine", "kv",
                      "orders", "signoffs", "compensations"):
            conn.execute(f"DELETE FROM {table}")


def _dumps(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _loads(value):
    if not value:
        return None
    return json.loads(value)


# ---------- 快照 ----------

def save_snapshot(snapshot: dict) -> None:
    with _LOCK, _connect() as conn:
        conn.execute(
            """INSERT INTO snapshots (schema_id, version_label, source_path, source_sha256,
                   encoding, bom, field_count, blocked_count, created_at, payload_json)
               VALUES (?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(schema_id) DO UPDATE SET
                   version_label=excluded.version_label,
                   source_path=excluded.source_path,
                   source_sha256=excluded.source_sha256,
                   encoding=excluded.encoding,
                   bom=excluded.bom,
                   field_count=excluded.field_count,
                   blocked_count=excluded.blocked_count,
                   created_at=excluded.created_at,
                   payload_json=excluded.payload_json""",
            (
                snapshot["schema_id"], snapshot["version_label"], snapshot["source_path"],
                snapshot["source_sha256"], snapshot["encoding"], int(bool(snapshot["bom"])),
                int(snapshot["field_count"]), int(snapshot["blocked_count"]),
                snapshot["created_at"], _dumps(snapshot),
            ),
        )


def load_snapshot(schema_id: str) -> dict | None:
    with _LOCK, _connect() as conn:
        row = conn.execute(
            "SELECT payload_json FROM snapshots WHERE schema_id=?", (schema_id,)
        ).fetchone()
    return _loads(row["payload_json"]) if row else None


def list_snapshots(limit: int = 20) -> list[dict]:
    with _LOCK, _connect() as conn:
        rows = conn.execute(
            """SELECT schema_id, version_label, source_path, source_sha256, encoding, bom,
                      field_count, blocked_count, created_at
               FROM snapshots ORDER BY created_at DESC, schema_id LIMIT ?""",
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def save_quarantine(schema_id: str, rows: list[dict]) -> None:
    with _LOCK, _connect() as conn:
        conn.execute("DELETE FROM quarantine WHERE schema_id=?", (schema_id,))
        conn.executemany(
            """INSERT INTO quarantine (schema_id, line, table_name, column_name, reason,
                   blocking, detail, raw) VALUES (?,?,?,?,?,?,?,?)""",
            [
                (
                    schema_id, int(row.get("line") or 0), row.get("table") or "",
                    row.get("column") or "", row["reason"], int(bool(row.get("blocking"))),
                    row.get("detail") or "", row.get("raw") or "",
                )
                for row in rows
            ],
        )


def list_quarantine(schema_id: str) -> list[dict]:
    with _LOCK, _connect() as conn:
        rows = conn.execute(
            """SELECT line, table_name, column_name, reason, blocking, detail, raw
               FROM quarantine WHERE schema_id=? ORDER BY line, reason""",
            (schema_id,),
        ).fetchall()
    return [
        {
            "line": row["line"],
            "table": row["table_name"],
            "column": row["column_name"],
            "reason": row["reason"],
            "blocking": bool(row["blocking"]),
            "detail": row["detail"],
            "raw": row["raw"],
        }
        for row in rows
    ]


# ---------- 运行清单 ----------

def save_run(run: dict) -> None:
    with _LOCK, _connect() as conn:
        conn.execute(
            """INSERT INTO runs (run_id, batch_key, old_schema_id, new_schema_id, version_seq,
                   input_sha256, ruleset, created_at, effective_from, effective_to,
                   summary_json, payload_json)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                run["run_id"], run["batch_key"], run["old_schema_id"], run["new_schema_id"],
                int(run["version_seq"]), run["input_sha256"], run["ruleset"],
                run["created_at"], run["effective_from"], run["effective_to"],
                _dumps(run["summary"]), _dumps(run),
            ),
        )


def find_run_by_key(batch_key: str) -> dict | None:
    with _LOCK, _connect() as conn:
        row = conn.execute(
            "SELECT payload_json FROM runs WHERE batch_key=?", (batch_key,)
        ).fetchone()
    return _loads(row["payload_json"]) if row else None


def run_by_id(run_id: str) -> dict | None:
    with _LOCK, _connect() as conn:
        row = conn.execute(
            "SELECT payload_json FROM runs WHERE run_id=?", (run_id,)
        ).fetchone()
    return _loads(row["payload_json"]) if row else None


def latest_run() -> dict | None:
    with _LOCK, _connect() as conn:
        row = conn.execute(
            "SELECT payload_json FROM runs ORDER BY version_seq DESC LIMIT 1"
        ).fetchone()
    return _loads(row["payload_json"]) if row else None


def list_runs(limit: int = 30) -> list[dict]:
    with _LOCK, _connect() as conn:
        rows = conn.execute(
            """SELECT summary_json FROM runs ORDER BY version_seq DESC, run_id LIMIT ?""",
            (limit,),
        ).fetchall()
    return [_loads(row["summary_json"]) for row in rows]


def count_runs() -> int:
    with _LOCK, _connect() as conn:
        row = conn.execute("SELECT COUNT(*) AS n FROM runs").fetchone()
    return int(row["n"])


def next_version_seq() -> int:
    with _LOCK, _connect() as conn:
        row = conn.execute("SELECT COALESCE(MAX(version_seq), 0) AS n FROM runs").fetchone()
    return int(row["n"]) + 1


def current_version_seq() -> int:
    with _LOCK, _connect() as conn:
        row = conn.execute("SELECT COALESCE(MAX(version_seq), 0) AS n FROM runs").fetchone()
    return int(row["n"])


def list_windows() -> list[dict]:
    with _LOCK, _connect() as conn:
        rows = conn.execute(
            "SELECT run_id, effective_from, effective_to FROM runs ORDER BY version_seq"
        ).fetchall()
    return [dict(row) for row in rows]


# ---------- 键值 ----------

def set_kv(key: str, value) -> None:
    with _LOCK, _connect() as conn:
        conn.execute(
            "INSERT INTO kv (k, v) VALUES (?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v",
            (key, _dumps(value)),
        )


def get_kv(key: str, default=None):
    with _LOCK, _connect() as conn:
        row = conn.execute("SELECT v FROM kv WHERE k=?", (key,)).fetchone()
    return _loads(row["v"]) if row else default


# ---------- 迁移单流转 ----------

def _order_from_row(row) -> dict:
    return {
        "order_id": row["order_id"],
        "run_id": row["run_id"],
        "revision": int(row["revision"]),
        "supersedes": row["supersedes"],
        "status": row["status"],
        "content_text": row["content_text"],
        "content_sha256": row["content_sha256"],
        "reason": row["reason"],
        "amendments": _loads(row["amendments_json"]) or [],
        "idempotency_key": row["idempotency_key"],
        "issued_at": row["issued_at"],
        "voided_at": row["voided_at"],
        "void_reason": row["void_reason"],
        "settled_at": row["settled_at"],
    }


def _signoff_from_row(row) -> dict:
    return {
        "order_id": row["order_id"],
        "signer": row["signer"],
        "signed_at": row["signed_at"],
        "receipt": row["receipt"],
        "content_sha256": row["content_sha256"],
        "idempotency_key": row["idempotency_key"],
    }


def _comp_from_row(row) -> dict:
    return {
        "comp_id": row["comp_id"],
        "order_id": row["order_id"],
        "new_order_id": row["new_order_id"],
        "status": row["status"],
        "idempotency_key": row["idempotency_key"],
        "created_at": row["created_at"],
        "confirmed_at": row["confirmed_at"],
    }


def _insert_order(conn, order: dict) -> None:
    conn.execute(
        """INSERT INTO orders (order_id, run_id, revision, supersedes, status,
               content_text, content_sha256, reason, amendments_json,
               idempotency_key, issued_at, voided_at, void_reason, settled_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            order["order_id"], order["run_id"], int(order["revision"]),
            order.get("supersedes"), order["status"], order["content_text"],
            order["content_sha256"], order.get("reason"),
            _dumps(order.get("amendments") or []), order.get("idempotency_key"),
            order["issued_at"], order.get("voided_at"), order.get("void_reason"),
            order.get("settled_at"),
        ),
    )


def _insert_signoff(conn, signoff: dict) -> None:
    conn.execute(
        """INSERT INTO signoffs (order_id, signer, signed_at, receipt,
               content_sha256, idempotency_key) VALUES (?,?,?,?,?,?)""",
        (
            signoff["order_id"], signoff["signer"], signoff["signed_at"],
            signoff["receipt"], signoff["content_sha256"],
            signoff.get("idempotency_key"),
        ),
    )


def _insert_compensation(conn, comp: dict) -> None:
    conn.execute(
        """INSERT INTO compensations (comp_id, order_id, new_order_id, status,
               idempotency_key, created_at, confirmed_at) VALUES (?,?,?,?,?,?,?)""",
        (
            comp["comp_id"], comp["order_id"], comp["new_order_id"], comp["status"],
            comp.get("idempotency_key"), comp["created_at"], comp.get("confirmed_at"),
        ),
    )


def save_order(order: dict) -> None:
    """签发落库；单号或幂等键撞车时由调用方按冲突处理。"""
    with _LOCK, _connect() as conn:
        _insert_order(conn, order)


def order_by_id(order_id: str) -> dict | None:
    with _LOCK, _connect() as conn:
        row = conn.execute(
            "SELECT * FROM orders WHERE order_id=?", (order_id,)
        ).fetchone()
    return _order_from_row(row) if row else None


def order_by_idempotency(key: str) -> dict | None:
    with _LOCK, _connect() as conn:
        row = conn.execute(
            "SELECT * FROM orders WHERE idempotency_key=?", (key,)
        ).fetchone()
    return _order_from_row(row) if row else None


def orders_for_run(run_id: str) -> list[dict]:
    with _LOCK, _connect() as conn:
        rows = conn.execute(
            "SELECT * FROM orders WHERE run_id=? ORDER BY revision", (run_id,)
        ).fetchall()
    return [_order_from_row(row) for row in rows]


def latest_order_for_run(run_id: str) -> dict | None:
    with _LOCK, _connect() as conn:
        row = conn.execute(
            "SELECT * FROM orders WHERE run_id=? ORDER BY revision DESC LIMIT 1",
            (run_id,),
        ).fetchone()
    return _order_from_row(row) if row else None


def orders_unsigned() -> list[dict]:
    with _LOCK, _connect() as conn:
        rows = conn.execute(
            "SELECT * FROM orders WHERE status='issued' ORDER BY issued_at, order_id"
        ).fetchall()
    return [_order_from_row(row) for row in rows]


def signoff_for_order(order_id: str) -> dict | None:
    with _LOCK, _connect() as conn:
        row = conn.execute(
            "SELECT * FROM signoffs WHERE order_id=?", (order_id,)
        ).fetchone()
    return _signoff_from_row(row) if row else None


def signoff_by_idempotency(key: str) -> dict | None:
    with _LOCK, _connect() as conn:
        row = conn.execute(
            "SELECT * FROM signoffs WHERE idempotency_key=?", (key,)
        ).fetchone()
    return _signoff_from_row(row) if row else None


def compensation_by_id(comp_id: str) -> dict | None:
    with _LOCK, _connect() as conn:
        row = conn.execute(
            "SELECT * FROM compensations WHERE comp_id=?", (comp_id,)
        ).fetchone()
    return _comp_from_row(row) if row else None


def compensation_for_order(order_id: str) -> dict | None:
    """该单被作废时登记的待补偿，一单最多一条。"""
    with _LOCK, _connect() as conn:
        row = conn.execute(
            "SELECT * FROM compensations WHERE order_id=?", (order_id,)
        ).fetchone()
    return _comp_from_row(row) if row else None


def compensation_by_idempotency(key: str) -> dict | None:
    with _LOCK, _connect() as conn:
        row = conn.execute(
            "SELECT * FROM compensations WHERE idempotency_key=?", (key,)
        ).fetchone()
    return _comp_from_row(row) if row else None


def compensations_pending() -> list[dict]:
    with _LOCK, _connect() as conn:
        rows = conn.execute(
            "SELECT * FROM compensations WHERE status='pending'"
            " ORDER BY created_at, comp_id"
        ).fetchall()
    return [_comp_from_row(row) for row in rows]


def correct_order_tx(*, old_order_id: str, expected_revision: int, voided_at: str,
                     void_reason: str, new_order: dict,
                     compensation: dict | None) -> dict:
    """原子更正：作废旧单、写入新单与待补偿，一步事务。

    旧单状态或修订号不符时一行不动，返回当前单让调用方决定报错形态。
    """
    with _LOCK, _connect() as conn:
        cur = conn.execute(
            """UPDATE orders SET status='voided', voided_at=?, void_reason=?
               WHERE order_id=? AND revision=? AND status IN ('issued','signed')""",
            (voided_at, void_reason, old_order_id, int(expected_revision)),
        )
        if cur.rowcount != 1:
            row = conn.execute(
                "SELECT * FROM orders WHERE order_id=?", (old_order_id,)
            ).fetchone()
            return {"ok": False, "current": _order_from_row(row) if row else None}
        _insert_order(conn, new_order)
        if compensation is not None:
            _insert_compensation(conn, compensation)
        return {"ok": True}


def signoff_tx(*, signoff: dict) -> dict:
    """原子签收：在事务里按当前状态分流，迟到签收作废单直接拒。"""
    with _LOCK, _connect() as conn:
        row = conn.execute(
            "SELECT status FROM orders WHERE order_id=?", (signoff["order_id"],)
        ).fetchone()
        if row is None:
            return {"outcome": "missing"}
        status = row["status"]
        if status in ("voided", "settled"):
            return {"outcome": "voided", "status": status}
        if status == "signed":
            existing = conn.execute(
                "SELECT * FROM signoffs WHERE order_id=?", (signoff["order_id"],)
            ).fetchone()
            return {"outcome": "already",
                    "signoff": _signoff_from_row(existing) if existing else None}
        _insert_signoff(conn, signoff)
        conn.execute(
            "UPDATE orders SET status='signed' WHERE order_id=? AND status='issued'",
            (signoff["order_id"],),
        )
        return {"outcome": "signed", "signoff": dict(signoff)}


def confirm_compensation_tx(comp_id: str, confirmed_at: str,
                            idempotency_key: str | None = None) -> dict:
    """原子确认补偿：补偿转已确认，对应旧单结清；重复确认原样返回。"""
    with _LOCK, _connect() as conn:
        row = conn.execute(
            "SELECT * FROM compensations WHERE comp_id=?", (comp_id,)
        ).fetchone()
        if row is None:
            return {"outcome": "missing"}
        comp = _comp_from_row(row)
        if comp["status"] == "confirmed":
            return {"outcome": "already", "compensation": comp}
        conn.execute(
            """UPDATE compensations SET status='confirmed', confirmed_at=?,
                   idempotency_key=COALESCE(?, idempotency_key)
               WHERE comp_id=? AND status='pending'""",
            (confirmed_at, idempotency_key, comp_id),
        )
        conn.execute(
            "UPDATE orders SET status='settled', settled_at=?"
            " WHERE order_id=? AND status='voided'",
            (confirmed_at, comp["order_id"]),
        )
        comp["status"] = "confirmed"
        comp["confirmed_at"] = confirmed_at
        if idempotency_key:
            comp["idempotency_key"] = idempotency_key
        return {"outcome": "confirmed", "compensation": comp}
