"""Local SQLite store for per-turn call snapshots."""
import json
import os
import sqlite3
from datetime import datetime

from config.settings import DB_PATH, DB_TIMEOUT_SECS

_SCHEMA = """
CREATE TABLE IF NOT EXISTS calls (
    asterisk_id          TEXT PRIMARY KEY,
    timestamp            TEXT,
    customer_name        TEXT,
    customer_name_source TEXT,
    call_outcome         TEXT,
    rpc_status           TEXT,
    payment_intent       TEXT,
    ptp_amount           TEXT,
    ptp_date             TEXT,
    transfer_reason      TEXT,
    refusal_outcome      TEXT,
    callback_required    INTEGER,
    callback_number      TEXT,
    callback_time        TEXT,
    callback_details     TEXT,
    alternate_number     TEXT,
    interaction_notes    TEXT,
    conversation_log     TEXT,
    updated_at           TEXT
)
"""

_COLUMNS = (
    "timestamp",
    "customer_name",
    "customer_name_source",
    "call_outcome",
    "rpc_status",
    "payment_intent",
    "ptp_amount",
    "ptp_date",
    "transfer_reason",
    "refusal_outcome",
    "callback_required",
    "callback_number",
    "callback_time",
    "callback_details",
    "alternate_number",
    "interaction_notes",
    "conversation_log",
)

_UPDATE_SQL = ", ".join(f"{c} = excluded.{c}" for c in _COLUMNS)


def init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=DB_TIMEOUT_SECS)
    try:
        conn.execute(_SCHEMA)
        conn.commit()
        # Auto-migration: ensure newly added columns exist in older DB files
        existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(calls)").fetchall()}
        if "callback_details" not in existing_cols:
            conn.execute("ALTER TABLE calls ADD COLUMN callback_details TEXT")
        if "alternate_number" not in existing_cols:
            conn.execute("ALTER TABLE calls ADD COLUMN alternate_number TEXT")
        conn.commit()
    finally:
        conn.close()


def upsert_call(record: dict) -> None:
    """Insert or update the row for this asterisk_id with the latest snapshot."""
    cols = _COLUMNS
    values = [
        record.get("timestamp", datetime.now().isoformat(timespec="seconds")),
        record.get("customer_name"),
        record.get("customer_name_source"),
        record.get("call_outcome"),
        record.get("rpc_status"),
        record.get("payment_intent"),
        record.get("ptp_amount"),
        record.get("ptp_date"),
        record.get("transfer_reason"),
        record.get("refusal_outcome"),
        int(bool(record.get("callback_required"))),
        record.get("callback_number"),
        record.get("callback_time"),
        record.get("callback_details"),
        record.get("alternate_number") or record.get("callback_number"),
        json.dumps(record.get("interaction_notes") or [], ensure_ascii=False, default=str),
        json.dumps(record.get("conversation_log") or [], ensure_ascii=False, default=str),
    ]
    placeholders = ", ".join("?" for _ in cols)
    sql = (
        f"INSERT INTO calls (asterisk_id, {', '.join(cols)}, updated_at) "
        f"VALUES (?, {placeholders}, ?) "
        f"ON CONFLICT(asterisk_id) DO UPDATE SET {_UPDATE_SQL}, updated_at = excluded.updated_at"
    )
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(sql, (record.get("asterisk_id"),) + tuple(values) + (datetime.now().isoformat(timespec="seconds"),))
        conn.commit()
    finally:
        conn.close()


init_db()
