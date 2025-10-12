# -*- coding: utf-8 -*-
import hashlib
import shutil
import json
import sqlite3
import threading
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

DOMAINS = ("naver", "daum")

DEFAULT_DOMAIN_CONFIG: Dict[str, Any] = {
    "helo": "",
    "smtp_host": "",
    "smtp_port": 25,
    "mail_from": "",
    "header": "",
    "session_count": 1,
    "bcc_count": 0,
    "anchor_interval": 0,
    "anchor_email": "",
    "rcpt_to": "",
    "stop_schedule_enabled": False,
    "stop_schedule_time": "",
    "stop_schedule_last_run": None,
}


app = FastAPI(title="MailSender Control Server")

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "control.db"
STORAGE_ROOT = BASE_DIR / "storage"

templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
db_lock = threading.RLock()

DEVICE_HEARTBEAT_TIMEOUT_SECONDS = 15
JOB_STALE_GRACE_SECONDS = DEVICE_HEARTBEAT_TIMEOUT_SECONDS * 2
GLOBAL_CONFIG_KEY = "common_config"
GLOBAL_CONFIG_DEFAULTS: Dict[str, Any] = {
    "helo": "",
    "mail_from": "",
    "header": "",
    "bcc_count": 0,
    "session_count": 1,
    "active_domain": "naver",
    "stop_schedule_enabled": False,
    "stop_schedule_time": "",
}
GLOBAL_CONFIG_DEVICE_FIELDS = ("helo", "mail_from", "header", "bcc_count", "session_count")
MAX_DEVICE_LOG_HISTORY = 10


def ensure_storage_root() -> None:
    STORAGE_ROOT.mkdir(parents=True, exist_ok=True)
    (STORAGE_ROOT / "devices").mkdir(exist_ok=True)


ensure_storage_root()


def now_ts() -> str:
    return datetime.now(timezone.utc).isoformat()


def clamp_bcc_count(value: Any) -> int:
    try:
        count = int(value)
    except (TypeError, ValueError):
        return 0
    return max(0, min(30, count))


def clamp_anchor_interval(value: Any) -> int:
    try:
        interval = int(value)
    except (TypeError, ValueError):
        return 0
    return max(0, min(1000, interval))


def normalize_anchor_email(value: Any) -> str:
    if value is None:
        return ""
    candidate = str(value).strip()
    if not candidate:
        return ""
    return candidate


def sanitize_session_count(value: Any) -> int:
    try:
        count = int(value)
    except (TypeError, ValueError):
        return 1
    return max(1, count)


def sanitize_stop_schedule_enabled(value: Any) -> bool:
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"", "0", "false", "off", "no"}:
            return False
        if lowered in {"1", "true", "on", "yes"}:
            return True
    return bool(value)


def sanitize_stop_schedule_time(value: Any) -> str:
    if value is None:
        return ""
    candidate = str(value).strip()
    if not candidate:
        return ""
    try:
        parsed = datetime.strptime(candidate, "%H:%M")
    except ValueError:
        return ""
    return parsed.strftime("%H:%M")


def sanitize_stop_schedule_last_run(value: Any) -> Optional[str]:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    candidate = str(value).strip()
    if not candidate:
        return None
    try:
        parsed = datetime.strptime(candidate, "%Y-%m-%d")
        return parsed.date().isoformat()
    except ValueError:
        try:
            parsed_dt = datetime.fromisoformat(candidate)
            return parsed_dt.date().isoformat()
        except ValueError:
            return None


def get_local_now() -> datetime:
    return datetime.now().astimezone()


def compute_next_stop_schedule(time_str: str, *, ref: Optional[datetime] = None) -> Optional[str]:
    sanitized = sanitize_stop_schedule_time(time_str)
    if not sanitized:
        return None
    base = ref.astimezone() if ref else get_local_now()
    try:
        target_time = datetime.strptime(sanitized, "%H:%M").time()
    except ValueError:
        return None
    candidate = datetime.combine(base.date(), target_time, tzinfo=base.tzinfo)
    if candidate <= base:
        candidate = candidate + timedelta(days=1)
    return candidate.isoformat()


def sanitize_global_active_domain(value: Any) -> str:
    if isinstance(value, str):
        candidate = value.strip().lower()
    elif value is None:
        candidate = ""
    else:
        candidate = str(value).strip().lower()
    if candidate in DOMAINS:
        return candidate
    return "naver"


def to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def sanitize_global_config_payload(raw: Dict[str, Any]) -> Dict[str, Any]:
    sanitized: Dict[str, Any] = {}
    sanitized["helo"] = str(raw.get("helo") or "").strip()
    sanitized["mail_from"] = str(raw.get("mail_from") or "").strip()
    sanitized["header"] = str(raw.get("header") or "")
    sanitized["bcc_count"] = clamp_bcc_count(raw.get("bcc_count"))
    sanitized["session_count"] = sanitize_session_count(raw.get("session_count"))
    sanitized["active_domain"] = sanitize_global_active_domain(raw.get("active_domain"))
    schedule_time = sanitize_stop_schedule_time(raw.get("stop_schedule_time"))
    sanitized["stop_schedule_time"] = schedule_time
    schedule_enabled = sanitize_stop_schedule_enabled(raw.get("stop_schedule_enabled"))
    sanitized["stop_schedule_enabled"] = schedule_enabled if schedule_time else False
    return sanitized


def _parse_iso_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def prune_device_logs(conn: sqlite3.Connection, device_id: str, limit: int = MAX_DEVICE_LOG_HISTORY) -> None:
    if limit <= 0:
        return
    conn.execute(
        """
        DELETE FROM send_logs
        WHERE device_id=?
          AND id NOT IN (
              SELECT id
              FROM send_logs
              WHERE device_id=?
              ORDER BY id DESC
              LIMIT ?
          )
        """,
        (device_id, device_id, limit),
    )


def load_global_config(*, conn: Optional[sqlite3.Connection] = None) -> Dict[str, Any]:
    owns_conn = conn is None
    if owns_conn:
        conn = get_conn()
    assert conn is not None
    row = conn.execute(
        "SELECT value, updated_at FROM global_settings WHERE key=?",
        (GLOBAL_CONFIG_KEY,),
    ).fetchone()
    payload: Dict[str, Any] = {}
    updated_at: Optional[str] = None
    if row:
        updated_at = row["updated_at"]
        raw_value = row["value"] or "{}"
        try:
            payload = json.loads(raw_value)
        except json.JSONDecodeError:
            payload = {}
    sanitized = sanitize_global_config_payload(payload)
    config: Dict[str, Any] = {**sanitized}
    config["updated_at"] = updated_at
    try:
        last_row = conn.execute(
            "SELECT MAX(stop_schedule_last_run) AS last_run FROM device_configs WHERE stop_schedule_last_run IS NOT NULL"
        ).fetchone()
        raw_last_run = last_row["last_run"] if last_row else None
        config["stop_schedule_last_run"] = sanitize_stop_schedule_last_run(raw_last_run)
    except sqlite3.Error:
        config["stop_schedule_last_run"] = None
    if sanitized.get("stop_schedule_enabled") and sanitized.get("stop_schedule_time"):
        config["stop_schedule_next_run"] = compute_next_stop_schedule(sanitized["stop_schedule_time"])
    else:
        config["stop_schedule_next_run"] = None
    if owns_conn:
        conn.close()
    return config


def save_global_config(conn: sqlite3.Connection, config: Dict[str, Any]) -> str:
    now = now_ts()
    sanitized = sanitize_global_config_payload(config)
    payload: Dict[str, Any] = {}
    for field, default in GLOBAL_CONFIG_DEFAULTS.items():
        payload[field] = sanitized.get(field, default)
    conn.execute(
        """
        INSERT INTO global_settings (key, value, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
        """,
        (
            GLOBAL_CONFIG_KEY,
            json.dumps(payload, ensure_ascii=False),
            now,
        ),
    )
    return now


def apply_global_config_to_devices(
    conn: sqlite3.Connection,
    values: Dict[str, str],
) -> Tuple[int, int]:
    if not values:
        return 0, 0
    fields = tuple(values.keys())
    now = now_ts()
    device_rows = conn.execute(
        "SELECT id FROM devices",
    ).fetchall()
    device_count = len(device_rows)
    if device_count == 0:
        return 0, 0
    assignments = ", ".join(f"{field}=?" for field in fields)
    query = f"UPDATE device_configs SET {assignments}, updated_at=? WHERE device_id=? AND domain=?"
    base_params = [values[field] for field in fields]
    total_updates = 0
    for row in device_rows:
        device_id = row["id"]
        for domain in DOMAINS:
            params = base_params + [now, device_id, domain]
            conn.execute(query, params)
            total_updates += 1
    return device_count, total_updates


def apply_global_active_domain(conn: sqlite3.Connection, domain: str) -> int:
    now = now_ts()
    cursor = conn.execute(
        "UPDATE devices SET active_domain=?, updated_at=?",
        (domain, now),
    )
    return cursor.rowcount


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _init_db() -> None:
    with db_lock, get_conn() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS devices (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                active_domain TEXT NOT NULL DEFAULT 'naver',
                status TEXT NOT NULL DEFAULT 'disconnected',
                last_seen TEXT,
                worker_state TEXT,
                last_error TEXT,
                public_ip TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS device_configs (
                device_id TEXT NOT NULL,
                domain TEXT NOT NULL,
                helo TEXT DEFAULT '',
                smtp_host TEXT DEFAULT '',
                smtp_port INTEGER DEFAULT 25,
                mail_from TEXT DEFAULT '',
                header TEXT DEFAULT '',
                session_count INTEGER DEFAULT 1,
                bcc_count INTEGER DEFAULT 0,
                rcpt_to TEXT DEFAULT '',
                client_db_version INTEGER DEFAULT 0,
                client_total INTEGER DEFAULT 0,
                client_pending INTEGER DEFAULT 0,
                client_sent INTEGER DEFAULT 0,
                client_failed INTEGER DEFAULT 0,
                client_block INTEGER DEFAULT 0,
                client_removed INTEGER DEFAULT 0,
                client_updated_at TEXT,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (device_id, domain),
                FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS device_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT NOT NULL,
                domain TEXT NOT NULL,
                filename TEXT NOT NULL,
                stored_name TEXT NOT NULL,
                size INTEGER NOT NULL,
                checksum TEXT,
                version INTEGER NOT NULL,
                uploaded_at TEXT NOT NULL,
                uploaded_by TEXT,
                last_injected_at TEXT,
                last_injected_status TEXT,
                last_injected_job_id TEXT,
                preview_cache TEXT,
                FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE,
                UNIQUE (device_id, domain, filename, version)
            );

            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                device_id TEXT NOT NULL,
                domain TEXT,
                job_type TEXT NOT NULL,
                status TEXT NOT NULL,
                payload TEXT,
                result TEXT,
                error TEXT,
                created_at TEXT NOT NULL,
                queued_at TEXT,
                started_at TEXT,
                finished_at TEXT,
                FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_jobs_device_status ON jobs(device_id, status);
            CREATE INDEX IF NOT EXISTS idx_jobs_created ON jobs(created_at DESC);

            CREATE TABLE IF NOT EXISTS send_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT NOT NULL,
                domain TEXT,
                job_id TEXT,
                rcpt_to TEXT,
                success INTEGER NOT NULL,
                response TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS job_progress_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL,
                device_id TEXT NOT NULL,
                domain TEXT,
                status TEXT NOT NULL,
                message TEXT,
                data TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS global_settings (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_job_progress_job
                ON job_progress_logs(job_id, created_at DESC);
            """
        )
        config_columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(device_configs)").fetchall()
        }
        device_columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(devices)").fetchall()
        }
        if "public_ip" not in device_columns:
            conn.execute("ALTER TABLE devices ADD COLUMN public_ip TEXT")
        if "bcc_count" not in config_columns:
            conn.execute(
                "ALTER TABLE device_configs ADD COLUMN bcc_count INTEGER DEFAULT 0"
            )
        if "anchor_interval" not in config_columns:
            conn.execute(
                "ALTER TABLE device_configs ADD COLUMN anchor_interval INTEGER DEFAULT 0"
            )
        if "anchor_email" not in config_columns:
            conn.execute(
                "ALTER TABLE device_configs ADD COLUMN anchor_email TEXT DEFAULT ''"
            )
        if "client_reserved" not in config_columns:
            conn.execute(
                "ALTER TABLE device_configs ADD COLUMN client_reserved INTEGER DEFAULT 0"
            )
        if "client_remaining" not in config_columns:
            conn.execute(
                "ALTER TABLE device_configs ADD COLUMN client_remaining INTEGER DEFAULT 0"
            )
        if "client_cycle_completed" not in config_columns:
            conn.execute(
                "ALTER TABLE device_configs ADD COLUMN client_cycle_completed INTEGER DEFAULT 0"
            )
        if "client_cycle_count" not in config_columns:
            conn.execute(
                "ALTER TABLE device_configs ADD COLUMN client_cycle_count INTEGER DEFAULT 0"
            )
        if "client_last_cycle_at" not in config_columns:
            conn.execute(
                "ALTER TABLE device_configs ADD COLUMN client_last_cycle_at TEXT"
            )
        if "client_last_cycle_processed" not in config_columns:
            conn.execute(
                "ALTER TABLE device_configs ADD COLUMN client_last_cycle_processed INTEGER DEFAULT 0"
            )
        if "stop_schedule_enabled" not in config_columns:
            conn.execute(
                "ALTER TABLE device_configs ADD COLUMN stop_schedule_enabled INTEGER NOT NULL DEFAULT 0"
            )
        if "stop_schedule_time" not in config_columns:
            conn.execute(
                "ALTER TABLE device_configs ADD COLUMN stop_schedule_time TEXT DEFAULT ''"
            )
        if "stop_schedule_last_run" not in config_columns:
            conn.execute(
                "ALTER TABLE device_configs ADD COLUMN stop_schedule_last_run TEXT"
            )
        job_columns = {
            row["name"]
            for row in conn.execute("PRAGMA table_info(jobs)").fetchall()
        }
        if "cancel_requested" not in job_columns:
            conn.execute(
                "ALTER TABLE jobs ADD COLUMN cancel_requested INTEGER NOT NULL DEFAULT 0"
            )
        conn.commit()


_init_db()


def normalize_domain(domain: str) -> str:
    lowered = (domain or "").strip().lower()
    if lowered not in DOMAINS:
        raise HTTPException(status_code=400, detail="지원하지 않는 도메인입니다.")
    return lowered


def ensure_device(device_id: str, name: str, public_ip: Optional[str] = None) -> Dict[str, Any]:
    device_id = (device_id or "").strip() or uuid.uuid4().hex
    name = (name or "").strip() or f"Device-{device_id[:6]}"
    public_ip = (public_ip or "").strip() or None
    now = now_ts()
    with db_lock, get_conn() as conn:
        conn.execute(
            """
            INSERT INTO devices (id, name, active_domain, status, last_seen, public_ip, created_at, updated_at)
            VALUES (?, ?, 'naver', 'connected', ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name=excluded.name,
                public_ip=COALESCE(excluded.public_ip, devices.public_ip),
                updated_at=excluded.updated_at
            """,
            (device_id, name, now, public_ip, now, now),
        )
        for domain in DOMAINS:
            conn.execute(
                """
                INSERT INTO device_configs (
                    device_id, domain, helo, smtp_host, smtp_port,
                    mail_from, header, session_count, bcc_count,
                    anchor_interval, anchor_email, rcpt_to,
                    stop_schedule_enabled, stop_schedule_time, stop_schedule_last_run,
                    updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(device_id, domain) DO NOTHING
                """,
                (
                    device_id,
                    domain,
                    DEFAULT_DOMAIN_CONFIG["helo"],
                    DEFAULT_DOMAIN_CONFIG["smtp_host"],
                    DEFAULT_DOMAIN_CONFIG["smtp_port"],
                    DEFAULT_DOMAIN_CONFIG["mail_from"],
                    DEFAULT_DOMAIN_CONFIG["header"],
                    DEFAULT_DOMAIN_CONFIG["session_count"],
                    DEFAULT_DOMAIN_CONFIG["bcc_count"],
                    DEFAULT_DOMAIN_CONFIG["anchor_interval"],
                    DEFAULT_DOMAIN_CONFIG["anchor_email"],
                    DEFAULT_DOMAIN_CONFIG["rcpt_to"],
                    1 if DEFAULT_DOMAIN_CONFIG["stop_schedule_enabled"] else 0,
                    DEFAULT_DOMAIN_CONFIG["stop_schedule_time"],
                    DEFAULT_DOMAIN_CONFIG["stop_schedule_last_run"],
                    now,
                ),
            )
        row = conn.execute(
            "SELECT * FROM devices WHERE id=?",
            (device_id,),
        ).fetchone()
        conn.commit()
    if not row:
        raise HTTPException(status_code=500, detail="디바이스 정보를 생성하지 못했습니다.")
    return to_dict(row)


def get_device(device_id: str, *, conn: Optional[sqlite3.Connection] = None) -> Optional[Dict[str, Any]]:
    owns_conn = conn is None
    if owns_conn:
        conn = get_conn()
    assert conn is not None
    row = conn.execute("SELECT * FROM devices WHERE id=?", (device_id,)).fetchone()
    if owns_conn:
        conn.close()
    return to_dict(row) if row else None


def load_device_configs(device_id: str, *, conn: Optional[sqlite3.Connection] = None) -> Dict[str, Dict[str, Any]]:
    owns_conn = conn is None
    if owns_conn:
        conn = get_conn()
    assert conn is not None
    rows = conn.execute(
        "SELECT * FROM device_configs WHERE device_id=?",
        (device_id,),
    ).fetchall()
    if owns_conn:
        conn.close()
    result: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        result[row["domain"]] = to_dict(row)
    return result


def serialize_config(row: Dict[str, Any]) -> Dict[str, Any]:
    raw_session = row.get("session_count", 1)
    try:
        session_value = int(raw_session)
    except (TypeError, ValueError):
        session_value = 1
    session_value = max(1, session_value)
    schedule_enabled = sanitize_stop_schedule_enabled(row.get("stop_schedule_enabled"))
    schedule_time = sanitize_stop_schedule_time(row.get("stop_schedule_time"))
    schedule_last_run = sanitize_stop_schedule_last_run(row.get("stop_schedule_last_run"))
    schedule_next_run = compute_next_stop_schedule(schedule_time) if schedule_enabled and schedule_time else None
    return {
        "domain": row["domain"],
        "helo": row.get("helo", ""),
        "smtp_host": row.get("smtp_host", ""),
        "smtp_port": row.get("smtp_port", 25),
        "mail_from": row.get("mail_from", ""),
        "header": row.get("header", ""),
        "session_count": session_value,
        "bcc_count": clamp_bcc_count(row.get("bcc_count", 0)),
        "anchor_interval": clamp_anchor_interval(row.get("anchor_interval", 0)),
        "anchor_email": normalize_anchor_email(row.get("anchor_email", "")),
        "rcpt_to": row.get("rcpt_to", ""),
        "stop_schedule_enabled": schedule_enabled,
        "stop_schedule_time": schedule_time,
        "stop_schedule_last_run": schedule_last_run,
        "stop_schedule_next_run": schedule_next_run,
        "updated_at": row.get("updated_at"),
        "client_db_version": row.get("client_db_version", 0),
        "client_total": row.get("client_total", 0),
        "client_pending": row.get("client_pending", 0),
        "client_sent": row.get("client_sent", 0),
        "client_failed": row.get("client_failed", 0),
        "client_block": row.get("client_block", 0),
        "client_removed": row.get("client_removed", 0),
        "client_reserved": row.get("client_reserved", 0),
        "client_remaining": row.get("client_remaining", 0),
        "client_cycle_completed": bool(row.get("client_cycle_completed", 0)),
        "client_cycle_count": row.get("client_cycle_count", 0),
        "client_last_cycle_at": row.get("client_last_cycle_at"),
        "client_last_cycle_processed": row.get("client_last_cycle_processed", 0),
        "client_updated_at": row.get("client_updated_at"),
    }


def load_device_summary() -> Dict[str, Any]:
    with db_lock, get_conn() as conn:
        device_rows = [
            to_dict(row)
            for row in conn.execute(
                "SELECT * FROM devices ORDER BY name COLLATE NOCASE"
            ).fetchall()
        ]
        config_rows = conn.execute(
            "SELECT * FROM device_configs"
        ).fetchall()
        recent_threshold = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()
        job_rows = conn.execute(
            """
            SELECT *
            FROM jobs
            WHERE status IN ('pending','dispatched','running')
               OR created_at >= ?
            ORDER BY created_at DESC
            """,
            (recent_threshold,),
        ).fetchall()
        job_overrides: Dict[str, sqlite3.Row] = {}
        job_ids = [row["id"] for row in job_rows]
        progress_rows: List[sqlite3.Row] = []
        if job_ids:
            placeholders = ",".join(["?"] * len(job_ids))
            progress_rows = conn.execute(
                f"""
                SELECT job_id, status, message, data, created_at
                FROM job_progress_logs
                WHERE job_id IN ({placeholders})
                ORDER BY created_at DESC
                LIMIT 500
                """,
                job_ids,
            ).fetchall()
        log_rows = conn.execute(
            """
            SELECT *
            FROM send_logs
            WHERE created_at >= ?
            ORDER BY created_at DESC
            LIMIT 500
            """,
            (recent_threshold,),
        ).fetchall()
        file_rows = conn.execute(
            """
            SELECT device_id, domain, MAX(last_injected_at) AS last_injected_at,
                   MAX(last_injected_status) AS last_injected_status
            FROM device_files
            GROUP BY device_id, domain
            """
        ).fetchall()
        now_utc = datetime.now(timezone.utc)
        stale_cutoff = now_utc - timedelta(seconds=DEVICE_HEARTBEAT_TIMEOUT_SECONDS)
        stale_updates: List[str] = []
        for device in device_rows:
            last_seen_dt = _parse_iso_dt(device.get("last_seen"))
            if last_seen_dt is None or last_seen_dt < stale_cutoff:
                if device.get("status") != "disconnected":
                    stale_updates.append(device["id"])
                device["status"] = "disconnected"
            elif not device.get("status"):
                device["status"] = "connected"
        if stale_updates:
            stamp = now_ts()
            conn.executemany(
                "UPDATE devices SET status='disconnected', updated_at=? WHERE id=?",
                [(stamp, device_id) for device_id in stale_updates],
            )
            conn.commit()
        device_lookup = {device["id"]: device for device in device_rows}
        stale_message = "디바이스 연결이 끊겨 작업이 중단되었습니다."
        for job_row in job_rows:
            if job_row["status"] not in {"running", "dispatched"}:
                continue
            device_info = device_lookup.get(job_row["device_id"])
            if not device_info or device_info.get("status") == "connected":
                continue
            last_seen_dt = _parse_iso_dt(device_info.get("last_seen"))
            if last_seen_dt and now_utc - last_seen_dt <= timedelta(seconds=JOB_STALE_GRACE_SECONDS):
                continue
            updated_row = update_job_status(
                conn,
                job_row["device_id"],
                job_row["id"],
                "failed",
                stale_message,
                None,
                stale_message,
            )
            if updated_row:
                job_overrides[job_row["id"]] = updated_row
                if updated_row["job_type"] in {"single_send", "batch_send"}:
                    failure_report = JobReportPayload(
                        job_id=updated_row["id"],
                        status="failed",
                        message=stale_message,
                        result=None,
                        error=stale_message,
                    )
                    handle_job_completion(conn, updated_row, failure_report)
        if job_overrides:
            conn.commit()
    config_map: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for row in config_rows:
        config_map.setdefault(row["device_id"], {})[row["domain"]] = serialize_config(to_dict(row))
    file_map: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for row in file_rows:
        file_map.setdefault(row["device_id"], {})[row["domain"]] = {
            "last_injected_at": row["last_injected_at"],
            "last_injected_status": row["last_injected_status"],
        }
    progress_map: Dict[str, List[Dict[str, Any]]] = {}
    for row in progress_rows:
        data_payload: Optional[Dict[str, Any]] = None
        if row["data"]:
            try:
                data_payload = json.loads(row["data"])
            except json.JSONDecodeError:
                data_payload = None
        progress_map.setdefault(row["job_id"], []).append(
            {
                "status": row["status"],
                "message": row["message"],
                "data": data_payload,
                "created_at": row["created_at"],
            }
        )
    for job_id, entries in list(progress_map.items()):
        trimmed = entries[:20]
        progress_map[job_id] = list(reversed(trimmed))
    job_map: Dict[str, List[Dict[str, Any]]] = {}
    for original_row in job_rows:
        row = job_overrides.get(original_row["id"], original_row)
        payload: Optional[Dict[str, Any]] = None
        try:
            payload = json.loads(row["payload"]) if row["payload"] else None
        except json.JSONDecodeError:
            payload = None
        result_payload: Optional[Dict[str, Any]] = None
        try:
            result_payload = json.loads(row["result"]) if row["result"] else None
        except json.JSONDecodeError:
            result_payload = None
        row_keys = row.keys()
        cancel_requested_flag = bool(row["cancel_requested"]) if "cancel_requested" in row_keys else False
        job_map.setdefault(row["device_id"], []).append(
            {
                "id": row["id"],
                "job_type": row["job_type"],
                "domain": row["domain"],
                "status": row["status"],
                "payload": payload,
                "result": result_payload,
                "created_at": row["created_at"],
                "queued_at": row["queued_at"],
                "started_at": row["started_at"],
                "finished_at": row["finished_at"],
                "cancel_requested": cancel_requested_flag,
                "error": row["error"],
                "progress": progress_map.get(row["id"], []),
            }
        )
    log_map: Dict[str, List[Dict[str, Any]]] = {}
    for row in log_rows:
        log_map.setdefault(row["device_id"], []).append(
            {
                "id": row["id"],
                "domain": row["domain"],
                "rcpt_to": row["rcpt_to"],
                "success": bool(row["success"]),
                "response": row["response"],
                "created_at": row["created_at"],
            }
        )
    devices: List[Dict[str, Any]] = []
    online_count = 0
    for device in device_rows:
        configs = config_map.get(device["id"], {})
        if device.get("status") == "connected":
            online_count += 1
        devices.append(
            {
                "id": device["id"],
                "name": device["name"],
                "active_domain": device["active_domain"],
                "status": device["status"],
                "last_seen": device.get("last_seen"),
                "worker_state": device.get("worker_state"),
                "last_error": device.get("last_error"),
                "public_ip": device.get("public_ip"),
                "configs": configs,
                "jobs": job_map.get(device["id"], []),
                "files": file_map.get(device["id"], {}),
                "logs": log_map.get(device["id"], [])[:MAX_DEVICE_LOG_HISTORY],
            }
        )
    global_config = load_global_config()
    return {
        "devices": devices,
        "counts": {
            "total": len(devices),
            "online": online_count,
        },
        "global_config": global_config,
    }


def remove_device(device_id: str) -> None:
    safe_device = device_id.replace("/", "_")
    device_root = STORAGE_ROOT / "devices" / safe_device
    with db_lock, get_conn() as conn:
        row = conn.execute(
            "SELECT id FROM devices WHERE id=?",
            (device_id,),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="디바이스를 찾을 수 없습니다.")
        conn.execute("DELETE FROM devices WHERE id=?", (device_id,))
        conn.commit()
    if device_root.exists():
        shutil.rmtree(device_root, ignore_errors=True)


def get_next_file_version(conn: sqlite3.Connection, device_id: str, domain: str) -> int:
    row = conn.execute(
        """
        SELECT MAX(version) AS max_version
        FROM device_files
        WHERE device_id=? AND domain=?
        """,
        (device_id, domain),
    ).fetchone()
    if not row or row["max_version"] is None:
        return 1
    return int(row["max_version"]) + 1


def compute_checksum(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def build_storage_path(device_id: str, domain: str, stored_name: str) -> Path:
    safe_device = device_id.replace("/", "_")
    safe_domain = normalize_domain(domain)
    device_root = STORAGE_ROOT / "devices" / safe_device / safe_domain
    device_root.mkdir(parents=True, exist_ok=True)
    return device_root / stored_name


def create_job(
    conn: sqlite3.Connection,
    device_id: str,
    domain: Optional[str],
    job_type: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    job_id = uuid.uuid4().hex
    now = now_ts()
    conn.execute(
        """
        INSERT INTO jobs (id, device_id, domain, job_type, status, payload, created_at)
        VALUES (?, ?, ?, ?, 'pending', ?, ?)
        """,
        (
            job_id,
            device_id,
            domain,
            job_type,
            json.dumps(payload, ensure_ascii=False),
            now,
        ),
    )
    return {
        "id": job_id,
        "device_id": device_id,
        "domain": domain,
        "job_type": job_type,
        "status": "pending",
        "payload": payload,
        "created_at": now,
    }


def load_job_payload(row: sqlite3.Row) -> Dict[str, Any]:
    if not row["payload"]:
        return {}
    try:
        return json.loads(row["payload"])
    except json.JSONDecodeError:
        return {}


def serialize_job_dispatch(row: sqlite3.Row) -> Dict[str, Any]:
    return {
        "job_id": row["id"],
        "job_type": row["job_type"],
        "domain": row["domain"],
        "payload": load_job_payload(row),
        "created_at": row["created_at"],
    }


def update_job_status(
    conn: sqlite3.Connection,
    device_id: str,
    job_id: str,
    status: str,
    message: Optional[str],
    result: Optional[Dict[str, Any]],
    error: Optional[str],
) -> Optional[sqlite3.Row]:
    row = conn.execute(
        "SELECT * FROM jobs WHERE id=? AND device_id=?",
        (job_id, device_id),
    ).fetchone()
    if not row:
        return None
    now = now_ts()
    fields: Dict[str, Any] = {"status": status}
    if status == "running" and not row["started_at"]:
        fields["started_at"] = now
    if status in {"success", "failed", "cancelled"}:
        fields["finished_at"] = now
        fields["cancel_requested"] = 0
    if status in {"success", "failed", "cancelled"}:
        if result is not None:
            fields["result"] = json.dumps(result, ensure_ascii=False)
        elif message:
            fields["result"] = json.dumps({"message": message}, ensure_ascii=False)
    if error:
        fields["error"] = error
    assignments = ", ".join(f"{key}=?" for key in fields)
    params = list(fields.values()) + [job_id]
    conn.execute(f"UPDATE jobs SET {assignments} WHERE id=?", params)
    updated = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    record_job_progress(conn, updated or row, status, message, result)
    return updated


def record_job_progress(
    conn: sqlite3.Connection,
    job_row: Optional[sqlite3.Row],
    status: str,
    message: Optional[str],
    result: Optional[Dict[str, Any]],
) -> None:
    if not job_row:
        return
    if result and isinstance(result, dict):
        logs_payload = result.get("logs")
        if isinstance(logs_payload, list):
            now_point = now_ts()
            entries: List[Tuple[str, Optional[str], str, Optional[str], int, str, str]] = []
            for item in logs_payload:
                if not isinstance(item, dict):
                    continue
                log_line = str(item.get("log") or "").strip()
                if not log_line:
                    continue
                email = item.get("email")
                delivery = str(item.get("delivery_status") or "").lower()
                success_flag = 1 if delivery == "sent" or log_line.lower().startswith("sent|") else 0
                entries.append(
                    (
                        job_row["device_id"],
                        job_row["domain"],
                        job_row["id"],
                        email,
                        success_flag,
                        log_line,
                        now_point,
                    )
                )
            if entries:
                conn.executemany(
                    """
                    INSERT INTO send_logs (device_id, domain, job_id, rcpt_to, success, response, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    entries,
                )
                device_ids = {entry[0] for entry in entries if entry and entry[0]}
                for device_id in device_ids:
                    prune_device_logs(conn, device_id)
    if job_row["job_type"] != "batch_send":
        return
    data_json: Optional[str] = None
    if result is not None:
        try:
            if isinstance(result, dict):
                filtered = {key: value for key, value in result.items() if key != "logs"}
                data_json = json.dumps(filtered, ensure_ascii=False) if filtered else None
            else:
                data_json = json.dumps(result, ensure_ascii=False)
        except (TypeError, ValueError):
            data_json = None
    conn.execute(
        """
        INSERT INTO job_progress_logs (job_id, device_id, domain, status, message, data, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            job_row["id"],
            job_row["device_id"],
            job_row["domain"],
            status,
            message,
            data_json,
            now_ts(),
        ),
    )


def handle_job_completion(
    conn: sqlite3.Connection,
    job_row: sqlite3.Row,
    report: "JobReportPayload",
) -> None:
    status = report.status.lower()
    payload = load_job_payload(job_row)
    now = now_ts()
    if job_row["job_type"] == "inject_file":
        file_id = payload.get("file_id")
        if file_id:
            conn.execute(
                """
                UPDATE device_files
                SET last_injected_at=?,
                    last_injected_status=?,
                    last_injected_job_id=?
                WHERE id=?
                """,
                (now, status, job_row["id"], file_id),
            )
    elif job_row["job_type"] in {"single_send", "batch_send"}:
        has_logs = False
        if report.result and isinstance(report.result, dict):
            logs_payload = report.result.get("logs")
            if isinstance(logs_payload, list) and logs_payload:
                has_logs = True
        if not has_logs:
            rcpt_to = payload.get("rcpt_to")
            if report.result and isinstance(report.result, dict):
                rcpt_to = report.result.get("rcpt_to") or rcpt_to
            conn.execute(
                """
                INSERT INTO send_logs (device_id, domain, job_id, rcpt_to, success, response, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_row["device_id"],
                    job_row["domain"],
                    job_row["id"],
                    rcpt_to,
                    1 if status == "success" else 0,
                    report.message or "",
                    now,
                ),
            )
            prune_device_logs(conn, job_row["device_id"])


class RegisterRequest(BaseModel):
    device_name: str = Field(..., min_length=1)
    device_id: Optional[str] = Field(default=None, description="기존 디바이스 ID (선택)")
    public_ip: Optional[str] = Field(default=None, description="현재 공인 IP 주소")


class RegisterResponse(BaseModel):
    device_id: str
    name: str
    active_domain: str
    configs: Dict[str, Dict[str, Any]]
    public_ip: Optional[str] = None


class DeviceConfigPayload(BaseModel):
    helo: Optional[str] = ""
    smtp_host: Optional[str] = ""
    smtp_port: Optional[int] = 25
    mail_from: Optional[str] = ""
    header: Optional[str] = ""
    session_count: Optional[int] = 1
    bcc_count: Optional[int] = 0
    anchor_interval: Optional[int] = 0
    anchor_email: Optional[str] = ""
    rcpt_to: Optional[str] = ""


class UpdateConfigRequest(DeviceConfigPayload):
    pass


class ActiveDomainRequest(BaseModel):
    domain: str


class DeviceScheduleUpdateRequest(BaseModel):
    enabled: bool
    time: Optional[str] = None


class DomainStatePayload(BaseModel):
    domain: str
    local_db_version: Optional[int] = None
    total: Optional[int] = None
    pending: Optional[int] = None
    reserved: Optional[int] = None
    sent: Optional[int] = None
    failed: Optional[int] = None
    block: Optional[int] = None
    removed: Optional[int] = None
    remaining: Optional[int] = None
    cycle_completed: Optional[bool] = None
    cycle_count: Optional[int] = None
    last_cycle_completed_at: Optional[str] = None
    last_cycle_processed: Optional[int] = None
    stop_schedule_last_run: Optional[str] = None


class JobReportPayload(BaseModel):
    job_id: str
    status: str
    message: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class HeartbeatRequest(BaseModel):
    device_name: str
    active_domain: Optional[str] = "naver"
    domain_states: List[DomainStatePayload] = Field(default_factory=list)
    job_reports: List[JobReportPayload] = Field(default_factory=list)
    public_ip: Optional[str] = Field(default=None, description="현재 공인 IP 주소")


class JobDispatchPayload(BaseModel):
    job_id: str
    job_type: str
    domain: Optional[str]
    payload: Dict[str, Any]
    created_at: str


class JobControlPayload(BaseModel):
    job_id: str
    cancel_requested: bool = False


class HeartbeatResponse(BaseModel):
    active_domain: str
    configs: Dict[str, Dict[str, Any]]
    jobs: List[JobDispatchPayload]
    job_controls: List[JobControlPayload] = Field(default_factory=list)
    public_ip: Optional[str] = None


class SingleSendRequest(BaseModel):
    domain: str
    rcpt_to: Optional[str] = None
    header_override: Optional[str] = None


class BatchSendRequest(BaseModel):
    domain: str


class FileInfo(BaseModel):
    id: int
    filename: str
    size: int
    checksum: Optional[str]
    version: int
    uploaded_at: str
    uploaded_by: Optional[str]
    last_injected_at: Optional[str]
    last_injected_status: Optional[str]
    last_injected_job_id: Optional[str]


class FileListResponse(BaseModel):
    files: List[FileInfo]


class GlobalConfigPayload(BaseModel):
    helo: Optional[str] = ""
    mail_from: Optional[str] = ""
    header: Optional[str] = ""
    bcc_count: Optional[int] = 0
    session_count: Optional[int] = 1
    active_domain: Optional[str] = None
    stop_schedule_enabled: Optional[bool] = None
    stop_schedule_time: Optional[str] = None


class GlobalConfigResponse(BaseModel):
    helo: str
    mail_from: str
    header: str
    bcc_count: int
    session_count: int
    active_domain: str
    stop_schedule_enabled: bool
    stop_schedule_time: str
    updated_at: Optional[str] = None
    stop_schedule_last_run: Optional[str] = None
    stop_schedule_next_run: Optional[str] = None


class GlobalBatchRequest(BaseModel):
    domain: Optional[str] = Field(
        default="active",
        description="명시하면 해당 도메인으로 전체 발송을 예약합니다. 기본은 각 디바이스의 활성 도메인입니다.",
    )


class GlobalStopRequest(BaseModel):
    reason: Optional[str] = None


class ClearLogsRequest(BaseModel):
    domain: Optional[str] = None


class JobCancelRequest(BaseModel):
    device_id: str
    reason: Optional[str] = None


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("dashboard.html", {"request": request})


@app.get("/api/devices")
def list_devices() -> Dict[str, Any]:
    return load_device_summary()


@app.get("/api/global/config", response_model=GlobalConfigResponse)
def get_global_config_endpoint() -> GlobalConfigResponse:
    config = load_global_config()
    schedule_time = sanitize_stop_schedule_time(config.get("stop_schedule_time"))
    schedule_enabled = sanitize_stop_schedule_enabled(config.get("stop_schedule_enabled"))
    if schedule_enabled and not schedule_time:
        schedule_enabled = False
    return GlobalConfigResponse(
        helo=config.get("helo", ""),
        mail_from=config.get("mail_from", ""),
        header=config.get("header", ""),
        bcc_count=clamp_bcc_count(config.get("bcc_count", 0)),
        session_count=sanitize_session_count(config.get("session_count", 1)),
        active_domain=sanitize_global_active_domain(config.get("active_domain")),
        stop_schedule_enabled=schedule_enabled,
        stop_schedule_time=schedule_time or "",
        updated_at=config.get("updated_at"),
        stop_schedule_last_run=config.get("stop_schedule_last_run"),
        stop_schedule_next_run=config.get("stop_schedule_next_run"),
    )


@app.post("/api/global/config/apply")
def apply_global_config_endpoint(payload: GlobalConfigPayload) -> Dict[str, Any]:
    raw_inputs = {
        "helo": (payload.helo or "").strip(),
        "mail_from": (payload.mail_from or "").strip(),
        "header": payload.header or "",
        "bcc_count": clamp_bcc_count(payload.bcc_count),
        "session_count": sanitize_session_count(payload.session_count),
    }
    domain_update_requested = payload.active_domain is not None
    schedule_reset_last_run = False
    with db_lock, get_conn() as conn:
        current_config = load_global_config(conn=conn)
        stored_config = sanitize_global_config_payload(current_config)
        previous_active_domain = stored_config.get("active_domain", "naver")
        requested_active_domain = (
            sanitize_global_active_domain(payload.active_domain)
            if domain_update_requested
            else previous_active_domain
        )
        active_domain_changed = (
            domain_update_requested and requested_active_domain != previous_active_domain
        )
        apply_values: Dict[str, Any] = {}
        applied_fields: List[str] = []
        for field in GLOBAL_CONFIG_DEVICE_FIELDS:
            raw_value = raw_inputs.get(field)
            if field == "header":
                should_apply = isinstance(raw_value, str) and bool(raw_value.strip())
                value_to_apply = raw_value
            elif field in ("helo", "mail_from"):
                value_to_apply = (raw_value or "").strip()
                should_apply = bool(value_to_apply)
            elif field == "bcc_count":
                value_to_apply = clamp_bcc_count(raw_value)
                should_apply = value_to_apply != stored_config[field]
            elif field == "session_count":
                value_to_apply = sanitize_session_count(raw_value)
                should_apply = value_to_apply != stored_config[field]
            else:
                continue
            if should_apply:
                apply_values[field] = value_to_apply
                stored_config[field] = value_to_apply
                applied_fields.append(field)
        current_schedule_enabled = sanitize_stop_schedule_enabled(stored_config.get("stop_schedule_enabled"))
        current_schedule_time = sanitize_stop_schedule_time(stored_config.get("stop_schedule_time"))
        previous_last_run = sanitize_stop_schedule_last_run(current_config.get("stop_schedule_last_run"))
        schedule_requested = (
            payload.stop_schedule_enabled is not None or payload.stop_schedule_time is not None
        )
        incoming_schedule_time = current_schedule_time
        if payload.stop_schedule_time is not None:
            incoming_schedule_time = sanitize_stop_schedule_time(payload.stop_schedule_time)
            raw_schedule_str = str(payload.stop_schedule_time or "").strip()
            if raw_schedule_str and not incoming_schedule_time:
                raise HTTPException(status_code=400, detail="HH:MM 형식으로 시간을 입력하세요.")
        incoming_schedule_enabled = current_schedule_enabled
        if payload.stop_schedule_enabled is not None:
            incoming_schedule_enabled = sanitize_stop_schedule_enabled(payload.stop_schedule_enabled)
        if incoming_schedule_enabled and not incoming_schedule_time:
            raise HTTPException(status_code=400, detail="자동 중지를 활성화하려면 HH:MM 형식의 시간을 입력하세요.")
        final_schedule_time = incoming_schedule_time
        final_schedule_enabled = incoming_schedule_enabled and bool(final_schedule_time)
        schedule_changed = (
            final_schedule_enabled != current_schedule_enabled
            or final_schedule_time != current_schedule_time
        )
        if not final_schedule_enabled:
            if current_schedule_enabled or previous_last_run:
                schedule_reset_last_run = True
        elif final_schedule_time != current_schedule_time:
            schedule_reset_last_run = True
        if schedule_requested or schedule_changed or schedule_reset_last_run:
            apply_values["stop_schedule_enabled"] = 1 if final_schedule_enabled else 0
            apply_values["stop_schedule_time"] = final_schedule_time or ""
            if schedule_reset_last_run:
                apply_values["stop_schedule_last_run"] = None
            stored_config["stop_schedule_enabled"] = final_schedule_enabled
            stored_config["stop_schedule_time"] = final_schedule_time or ""
            if "stop_schedule" not in applied_fields:
                applied_fields.append("stop_schedule")
        else:
            stored_config["stop_schedule_enabled"] = current_schedule_enabled
            stored_config["stop_schedule_time"] = current_schedule_time or ""
        device_count, update_count = apply_global_config_to_devices(conn, apply_values)
        domain_update_count = 0
        if active_domain_changed:
            domain_update_count = apply_global_active_domain(conn, requested_active_domain)
            applied_fields.append("active_domain")
        stored_config["active_domain"] = requested_active_domain
        device_count = max(device_count, domain_update_count)
        updated_at = save_global_config(conn, stored_config)
        conn.commit()
        refreshed_config = load_global_config(conn=conn)
    schedule_state = {
        "enabled": sanitize_stop_schedule_enabled(refreshed_config.get("stop_schedule_enabled")),
        "time": sanitize_stop_schedule_time(refreshed_config.get("stop_schedule_time")),
        "next_run": refreshed_config.get("stop_schedule_next_run"),
        "last_run": refreshed_config.get("stop_schedule_last_run"),
        "reset_last_run": schedule_reset_last_run,
    }
    unique_fields = sorted(set(applied_fields))
    field_log = ", ".join(unique_fields) if unique_fields else "none"
    schedule_mode = "on" if schedule_state["enabled"] else "off"
    schedule_time = schedule_state["time"] or "-"
    print(
        f"[{updated_at}] Applied global config (fields={field_log}, devices={device_count}, updates={update_count}) "
        f"schedule={schedule_mode}({schedule_time})"
    )
    return {
        "config": refreshed_config,
        "updated_at": updated_at,
        "device_count": device_count,
        "config_updates": update_count,
        "applied_fields": unique_fields,
        "schedule_state": schedule_state,
    }


@app.post("/api/devices/register", response_model=RegisterResponse)
def register_device(payload: RegisterRequest) -> RegisterResponse:
    device = ensure_device(payload.device_id or uuid.uuid4().hex, payload.device_name, payload.public_ip)
    raw_configs = load_device_configs(device["id"])
    configs = {domain: serialize_config(row) for domain, row in raw_configs.items()}
    return RegisterResponse(
        device_id=device["id"],
        name=device["name"],
        active_domain=device.get("active_domain", "naver"),
        configs=configs,
        public_ip=device.get("public_ip"),
    )


@app.delete("/api/devices/{device_id}")
def delete_device_endpoint(device_id: str) -> Dict[str, Any]:
    remove_device(device_id)
    return {"device_id": device_id, "deleted": True}


@app.put("/api/devices/{device_id}/domains/{domain}/config")
def update_device_config(device_id: str, domain: str, payload: UpdateConfigRequest) -> Dict[str, Any]:
    normalized = normalize_domain(domain)
    with db_lock, get_conn() as conn:
        device = get_device(device_id, conn=conn)
        if not device:
            raise HTTPException(status_code=404, detail="디바이스를 찾을 수 없습니다.")
        config_row = conn.execute(
            "SELECT * FROM device_configs WHERE device_id=? AND domain=?",
            (device_id, normalized),
        ).fetchone()
        if not config_row:
            raise HTTPException(status_code=404, detail="도메인 설정을 찾을 수 없습니다.")
        config_data = to_dict(config_row)
        now = now_ts()
        sanitized_bcc = clamp_bcc_count(payload.bcc_count or 0)
        sanitized_interval = clamp_anchor_interval(payload.anchor_interval or 0)
        sanitized_anchor = normalize_anchor_email(payload.anchor_email or "")
        conn.execute(
            """
            UPDATE device_configs
            SET helo=?, smtp_host=?, smtp_port=?, mail_from=?, header=?, session_count=?, bcc_count=?, anchor_interval=?, anchor_email=?, rcpt_to=?,
                updated_at=?
            WHERE device_id=? AND domain=?
            """,
            (
                payload.helo or "",
                payload.smtp_host or "",
                int(payload.smtp_port or 25),
                payload.mail_from or "",
                payload.header or "",
                max(1, int(payload.session_count or 1)),
                sanitized_bcc,
                sanitized_interval,
                sanitized_anchor,
                payload.rcpt_to or "",
                now,
                device_id,
                normalized,
            ),
        )
        conn.commit()
        config_row = conn.execute(
            "SELECT * FROM device_configs WHERE device_id=? AND domain=?",
            (device_id, normalized),
        ).fetchone()
    if not config_row:
        raise HTTPException(status_code=404, detail="도메인 설정을 찾을 수 없습니다.")
    return serialize_config(to_dict(config_row))


@app.post("/api/devices/{device_id}/domains/{domain}/schedule")
def update_device_schedule(device_id: str, domain: str, payload: DeviceScheduleUpdateRequest) -> Dict[str, Any]:
    normalized = normalize_domain(domain)
    with db_lock, get_conn() as conn:
        device = get_device(device_id, conn=conn)
        if not device:
            raise HTTPException(status_code=404, detail="디바이스를 찾을 수 없습니다.")
        config_row = conn.execute(
            "SELECT * FROM device_configs WHERE device_id=? AND domain=?",
            (device_id, normalized),
        ).fetchone()
        if not config_row:
            raise HTTPException(status_code=404, detail="도메인 설정을 찾을 수 없습니다.")
        config_data = to_dict(config_row)
        previous_enabled = sanitize_stop_schedule_enabled(config_data.get("stop_schedule_enabled"))
        previous_time = sanitize_stop_schedule_time(config_data.get("stop_schedule_time"))
        previous_last_run = sanitize_stop_schedule_last_run(config_data.get("stop_schedule_last_run"))
        requested_enabled = sanitize_stop_schedule_enabled(payload.enabled)
        requested_time = sanitize_stop_schedule_time(payload.time)
        if requested_enabled and not requested_time:
            raise HTTPException(status_code=400, detail="예약을 켜려면 유효한 HH:MM 형식의 시간을 입력하세요.")
        schedule_time = requested_time if requested_enabled else (requested_time or previous_time or "")
        schedule_enabled_flag = 1 if requested_enabled and schedule_time else 0
        schedule_last_run: Optional[str]
        if schedule_enabled_flag == 0:
            schedule_last_run = None
        elif not previous_enabled or previous_time != schedule_time:
            schedule_last_run = None
        else:
            schedule_last_run = previous_last_run
        now = now_ts()
        conn.execute(
            """
            UPDATE device_configs
            SET stop_schedule_enabled=?,
                stop_schedule_time=?,
                stop_schedule_last_run=?,
                updated_at=?
            WHERE device_id=? AND domain=?
            """,
            (
                schedule_enabled_flag,
                schedule_time or "",
                schedule_last_run,
                now,
                device_id,
                normalized,
            ),
        )
        conn.commit()
        refreshed = conn.execute(
            "SELECT * FROM device_configs WHERE device_id=? AND domain=?",
            (device_id, normalized),
        ).fetchone()
    if not refreshed:
        raise HTTPException(status_code=404, detail="도메인 설정을 찾을 수 없습니다.")
    return {"config": serialize_config(to_dict(refreshed))}


@app.post("/api/devices/{device_id}/active-domain")
def set_active_domain(device_id: str, payload: ActiveDomainRequest) -> Dict[str, Any]:
    normalized = normalize_domain(payload.domain)
    with db_lock, get_conn() as conn:
        device = get_device(device_id, conn=conn)
        if not device:
            raise HTTPException(status_code=404, detail="디바이스를 찾을 수 없습니다.")
        conn.execute(
            "UPDATE devices SET active_domain=?, updated_at=? WHERE id=?",
            (normalized, now_ts(), device_id),
        )
        conn.commit()
    return {"device_id": device_id, "active_domain": normalized}


def build_config_snapshot(configs: Dict[str, Dict[str, Any]], domain: str) -> Dict[str, Any]:
    base = configs.get(domain) or {}
    bcc_count = clamp_bcc_count(base.get("bcc_count", 0))
    raw_session = base.get("session_count", 1)
    try:
        session_count = int(raw_session)
    except (TypeError, ValueError):
        session_count = 1
    session_count = max(1, session_count)
    anchor_interval = clamp_anchor_interval(base.get("anchor_interval", 0))
    anchor_email = normalize_anchor_email(base.get("anchor_email", ""))
    return {
        "helo": base.get("helo", ""),
        "smtp_host": base.get("smtp_host", ""),
        "smtp_port": base.get("smtp_port", 25),
        "mail_from": base.get("mail_from", ""),
        "header": base.get("header", ""),
        "session_count": session_count,
        "bcc_count": bcc_count,
        "anchor_interval": anchor_interval,
        "anchor_email": anchor_email,
        "rcpt_to": base.get("rcpt_to", ""),
    }


@app.post("/api/global/actions/send-batch")
def enqueue_global_batch(payload: GlobalBatchRequest) -> Dict[str, Any]:
    mode = (payload.domain or "active").strip().lower() or "active"
    forced_domain: Optional[str] = None
    if mode != "active":
        forced_domain = normalize_domain(mode)
    created_jobs: List[Dict[str, Any]] = []
    with db_lock, get_conn() as conn:
        device_rows = conn.execute(
            "SELECT id, active_domain FROM devices ORDER BY name COLLATE NOCASE",
        ).fetchall()
        for device_row in device_rows:
            device_id = device_row["id"]
            active = device_row["active_domain"] or "naver"
            domain = forced_domain or normalize_domain(active)
            configs = load_device_configs(device_id, conn=conn)
            config_snapshot = build_config_snapshot(configs, domain)
            job = create_job(
                conn,
                device_id,
                domain,
                "batch_send",
                {"config": config_snapshot},
            )
            created_jobs.append(job)
        conn.commit()
    return {
        "jobs": [job["id"] for job in created_jobs],
        "device_count": len(created_jobs),
        "mode": forced_domain or "active",
    }


@app.post("/api/devices/{device_id}/actions/send-single")
def enqueue_single_send(device_id: str, payload: SingleSendRequest) -> Dict[str, Any]:
    domain = normalize_domain(payload.domain)
    with db_lock, get_conn() as conn:
        device = get_device(device_id, conn=conn)
        if not device:
            raise HTTPException(status_code=404, detail="디바이스를 찾을 수 없습니다.")
        configs = load_device_configs(device_id, conn=conn)
        config_snapshot = build_config_snapshot(configs, domain)
        config_snapshot["bcc_count"] = 0
        config_snapshot["anchor_interval"] = 0
        rcpt_to = (payload.rcpt_to or "").strip() or config_snapshot.get("rcpt_to")
        if not rcpt_to:
            raise HTTPException(status_code=400, detail="RCPT TO 주소가 필요합니다.")
        if payload.header_override:
            config_snapshot["header"] = payload.header_override
        job = create_job(
            conn,
            device_id,
            domain,
            "single_send",
            {
                "rcpt_to": rcpt_to,
                "config": config_snapshot,
            },
        )
        conn.commit()
    return {"job": job}


@app.post("/api/devices/{device_id}/actions/send-batch")
def enqueue_batch_send(device_id: str, payload: BatchSendRequest) -> Dict[str, Any]:
    domain = normalize_domain(payload.domain)
    with db_lock, get_conn() as conn:
        device = get_device(device_id, conn=conn)
        if not device:
            raise HTTPException(status_code=404, detail="디바이스를 찾을 수 없습니다.")
        configs = load_device_configs(device_id, conn=conn)
        config_snapshot = build_config_snapshot(configs, domain)
        job = create_job(
            conn,
            device_id,
            domain,
            "batch_send",
            {"config": config_snapshot},
        )
        conn.commit()
    return {"job": job}


@app.post("/api/devices/{device_id}/actions/change-ip")
def enqueue_change_ip(device_id: str) -> Dict[str, Any]:
    with db_lock, get_conn() as conn:
        device = get_device(device_id, conn=conn)
        if not device:
            raise HTTPException(status_code=404, detail="디바이스를 찾을 수 없습니다.")
        job = create_job(conn, device_id, None, "change_ip", {})
        conn.commit()
    return {"job": job}


@app.post("/api/jobs/{job_id}/cancel")
def cancel_job(job_id: str, payload: JobCancelRequest) -> Dict[str, Any]:
    cancel_message = payload.reason or "사용자가 작업 중지를 요청했습니다."
    with db_lock, get_conn() as conn:
        job_row = conn.execute(
            "SELECT * FROM jobs WHERE id=?",
            (job_id,),
        ).fetchone()
        if not job_row:
            raise HTTPException(status_code=404, detail="작업을 찾을 수 없습니다.")
        if job_row["device_id"] != payload.device_id:
            raise HTTPException(status_code=400, detail="디바이스 ID가 일치하지 않습니다.")
        device_row = conn.execute(
            "SELECT status, last_seen FROM devices WHERE id=?",
            (payload.device_id,),
        ).fetchone()
        device_connected = bool(device_row and device_row["status"] == "connected")
        status = job_row["status"]
        if status in {"success", "failed", "cancelled"}:
            return {
                "job_id": job_id,
                "status": status,
                "cancel_requested": False,
            }
        if status == "pending":
            updated_row = update_job_status(
                conn,
                payload.device_id,
                job_id,
                "cancelled",
                cancel_message,
                None,
                cancel_message,
            )
            if updated_row:
                pseudo_report = JobReportPayload(
                    job_id=job_id,
                    status="cancelled",
                    message=cancel_message,
                    result=None,
                    error=cancel_message,
                )
                handle_job_completion(conn, updated_row, pseudo_report)
            conn.commit()
            return {
                "job_id": job_id,
                "status": "cancelled",
                "cancel_requested": False,
            }
        if not device_connected:
            updated_row = update_job_status(
                conn,
                payload.device_id,
                job_id,
                "cancelled",
                cancel_message,
                None,
                cancel_message,
            )
            if updated_row:
                pseudo_report = JobReportPayload(
                    job_id=job_id,
                    status="cancelled",
                    message=cancel_message,
                    result=None,
                    error=cancel_message,
                )
                handle_job_completion(conn, updated_row, pseudo_report)
            conn.commit()
            return {
                "job_id": job_id,
                "status": "cancelled",
                "cancel_requested": False,
            }
        if job_row["cancel_requested"]:
            conn.commit()
            return {
                "job_id": job_id,
                "status": status,
                "cancel_requested": True,
            }
        conn.execute(
            "UPDATE jobs SET cancel_requested=1 WHERE id=?",
            (job_id,),
        )
        record_job_progress(conn, job_row, "cancel_requested", cancel_message, None)
        conn.commit()
    return {
        "job_id": job_id,
        "status": status,
        "cancel_requested": True,
    }


@app.post("/api/global/actions/stop")
def request_global_stop(payload: GlobalStopRequest) -> Dict[str, Any]:
    cancel_message = (payload.reason or "").strip() or "사용자가 전체 중지를 요청했습니다."
    with db_lock, get_conn() as conn:
        job_rows = conn.execute(
            """
            SELECT jobs.*, devices.status AS device_status
            FROM jobs
            JOIN devices ON devices.id = jobs.device_id
            WHERE jobs.job_type IN ('batch_send', 'single_send')
              AND jobs.status IN ('pending', 'dispatched', 'running')
            """,
        ).fetchall()
        cancelled = 0
        cancel_requested = 0
        for job_row in job_rows:
            status = job_row["status"]
            job_id = job_row["id"]
            device_id = job_row["device_id"]
            device_status = job_row["device_status"]
            if status == "pending" or device_status != "connected":
                updated_row = update_job_status(
                    conn,
                    device_id,
                    job_id,
                    "cancelled",
                    cancel_message,
                    None,
                    cancel_message,
                )
                if updated_row:
                    pseudo_report = JobReportPayload(
                        job_id=updated_row["id"],
                        status="cancelled",
                        message=cancel_message,
                        result=None,
                        error=cancel_message,
                    )
                    handle_job_completion(conn, updated_row, pseudo_report)
                cancelled += 1
                continue
            if job_row["cancel_requested"]:
                continue
            conn.execute(
                "UPDATE jobs SET cancel_requested=1 WHERE id=?",
                (job_id,),
            )
            refreshed_row = conn.execute(
                "SELECT * FROM jobs WHERE id=?",
                (job_id,),
            ).fetchone()
            record_job_progress(conn, refreshed_row or job_row, "cancel_requested", cancel_message, None)
            cancel_requested += 1
        conn.commit()
    return {
        "total_jobs": len(job_rows),
        "cancelled": cancelled,
        "cancel_requested": cancel_requested,
    }


def fetch_device_file(conn: sqlite3.Connection, device_id: str, domain: str, file_id: int) -> sqlite3.Row:
    row = conn.execute(
        """
        SELECT *
        FROM device_files
        WHERE id=? AND device_id=? AND domain=?
        """,
        (file_id, device_id, domain),
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="파일을 찾을 수 없습니다.")
    return row


@app.get("/api/devices/{device_id}/domains/{domain}/files", response_model=FileListResponse)
def list_domain_files(device_id: str, domain: str) -> FileListResponse:
    normalized = normalize_domain(domain)
    with db_lock, get_conn() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM device_files
            WHERE device_id=? AND domain=?
            ORDER BY uploaded_at DESC
            """,
            (device_id, normalized),
        ).fetchall()
    files = [
        FileInfo(
            id=row["id"],
            filename=row["filename"],
            size=row["size"],
            checksum=row["checksum"],
            version=row["version"],
            uploaded_at=row["uploaded_at"],
            uploaded_by=row["uploaded_by"],
            last_injected_at=row["last_injected_at"],
            last_injected_status=row["last_injected_status"],
            last_injected_job_id=row["last_injected_job_id"],
        )
        for row in rows
    ]
    return FileListResponse(files=files)


@app.post("/api/devices/{device_id}/logs/clear")
def clear_device_logs(device_id: str, payload: ClearLogsRequest) -> Dict[str, Any]:
    normalized_domain: Optional[str] = None
    if payload.domain:
        normalized_domain = normalize_domain(payload.domain)
    with db_lock, get_conn() as conn:
        device = get_device(device_id, conn=conn)
        if not device:
            raise HTTPException(status_code=404, detail="디바이스를 찾을 수 없습니다.")
        if normalized_domain:
            cursor = conn.execute(
                "DELETE FROM send_logs WHERE device_id=? AND domain=?",
                (device_id, normalized_domain),
            )
        else:
            cursor = conn.execute(
                "DELETE FROM send_logs WHERE device_id=?",
                (device_id,),
            )
        deleted = cursor.rowcount if cursor.rowcount is not None else 0
        job_payload = {"domains": [normalized_domain]} if normalized_domain else {"domains": []}
        reset_job = create_job(
            conn,
            device_id,
            normalized_domain,
            "reset_sent_sequence",
            job_payload,
        )
        conn.commit()
    return {
        "device_id": device_id,
        "domain": normalized_domain,
        "cleared": deleted,
        "reset_job_id": reset_job["id"],
    }


@app.post("/api/devices/{device_id}/domains/{domain}/files")
async def upload_domain_file(
    device_id: str,
    domain: str,
    file: UploadFile = File(...),
) -> Dict[str, Any]:
    normalized = normalize_domain(domain)
    data = await file.read()
    if not data:
        raise HTTPException(status_code=400, detail="파일이 비어 있습니다.")
    stored_name = f"{uuid.uuid4().hex}_{file.filename}"
    checksum = compute_checksum(data)
    now = now_ts()
    with db_lock, get_conn() as conn:
        device = get_device(device_id, conn=conn)
        if not device:
            raise HTTPException(status_code=404, detail="디바이스를 찾을 수 없습니다.")
        version = get_next_file_version(conn, device_id, normalized)
        storage_path = build_storage_path(device_id, normalized, stored_name)
        storage_path.write_bytes(data)
        conn.execute(
            """
            INSERT INTO device_files (
                device_id, domain, filename, stored_name, size, checksum,
                version, uploaded_at, uploaded_by
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                device_id,
                normalized,
                file.filename,
                stored_name,
                len(data),
                checksum,
                version,
                now,
                "dashboard",
            ),
        )
        conn.commit()
    return {
        "filename": file.filename,
        "size": len(data),
        "checksum": checksum,
        "version": version,
        "uploaded_at": now,
    }


@app.get("/api/devices/{device_id}/domains/{domain}/files/{file_id}/preview")
def preview_domain_file(device_id: str, domain: str, file_id: int) -> Dict[str, Any]:
    normalized = normalize_domain(domain)
    with db_lock, get_conn() as conn:
        row = fetch_device_file(conn, device_id, normalized, file_id)
    storage_path = build_storage_path(device_id, normalized, row["stored_name"])
    if not storage_path.exists():
        raise HTTPException(status_code=404, detail="파일이 서버에 존재하지 않습니다.")
    preview_lines: List[str] = []
    try:
        with storage_path.open("r", encoding="utf-8") as fp:
            for idx, line in enumerate(fp):
                preview_lines.append(line.rstrip("\n"))
                if idx >= 99:
                    break
    except UnicodeDecodeError:
        with storage_path.open("r", encoding="euc-kr", errors="ignore") as fp:
            for idx, line in enumerate(fp):
                preview_lines.append(line.rstrip("\n"))
                if idx >= 99:
                    break
    return {
        "preview": preview_lines,
        "filename": row["filename"],
        "size": row["size"],
        "version": row["version"],
    }


@app.get("/api/devices/{device_id}/domains/{domain}/files/{file_id}/download")
def download_domain_file(device_id: str, domain: str, file_id: int) -> FileResponse:
    normalized = normalize_domain(domain)
    with db_lock, get_conn() as conn:
        row = fetch_device_file(conn, device_id, normalized, file_id)
    storage_path = build_storage_path(device_id, normalized, row["stored_name"])
    if not storage_path.exists():
        raise HTTPException(status_code=404, detail="파일이 서버에 존재하지 않습니다.")
    return FileResponse(
        storage_path,
        media_type="application/octet-stream",
        filename=row["filename"],
    )


@app.delete("/api/devices/{device_id}/domains/{domain}/files/{file_id}")
def delete_domain_file(device_id: str, domain: str, file_id: int) -> Dict[str, Any]:
    normalized = normalize_domain(domain)
    with db_lock, get_conn() as conn:
        row = fetch_device_file(conn, device_id, normalized, file_id)
        storage_path = build_storage_path(device_id, normalized, row["stored_name"])
        conn.execute(
            "DELETE FROM device_files WHERE id=?",
            (file_id,),
        )
        conn.commit()
    if storage_path.exists():
        storage_path.unlink()
    return {"deleted": file_id}


@app.post("/api/devices/{device_id}/domains/{domain}/files/{file_id}/inject")
def enqueue_inject_file(device_id: str, domain: str, file_id: int) -> Dict[str, Any]:
    normalized = normalize_domain(domain)
    with db_lock, get_conn() as conn:
        device = get_device(device_id, conn=conn)
        if not device:
            raise HTTPException(status_code=404, detail="디바이스를 찾을 수 없습니다.")
        row = fetch_device_file(conn, device_id, normalized, file_id)
        job = create_job(
            conn,
            device_id,
            normalized,
            "inject_file",
            {
                "file_id": row["id"],
                "filename": row["filename"],
                "version": row["version"],
                "download_path": f"/api/devices/{device_id}/domains/{normalized}/files/{row['id']}/download",
            },
        )
        conn.execute(
            """
            UPDATE device_files
            SET last_injected_status='pending',
                last_injected_job_id=?,
                last_injected_at=NULL
            WHERE id=?
            """,
            (job["id"], row["id"]),
        )
        conn.commit()
    return {"job": job}


@app.post("/api/devices/{device_id}/domains/{domain}/actions/purge")
def enqueue_domain_purge(device_id: str, domain: str) -> Dict[str, Any]:
    normalized = normalize_domain(domain)
    with db_lock, get_conn() as conn:
        device = get_device(device_id, conn=conn)
        if not device:
            raise HTTPException(status_code=404, detail="디바이스를 찾을 수 없습니다.")
        job = create_job(
            conn,
            device_id,
            normalized,
            "purge_domain",
            {"domain": normalized},
        )
        conn.commit()
    return {"job": job}


@app.post("/api/devices/{device_id}/heartbeat", response_model=HeartbeatResponse)
def heartbeat(device_id: str, payload: HeartbeatRequest) -> HeartbeatResponse:
    normalized_active = normalize_domain(payload.active_domain or "naver")
    public_ip = (payload.public_ip or "").strip() or None
    with db_lock, get_conn() as conn:
        device = get_device(device_id, conn=conn)
        if not device:
            raise HTTPException(status_code=404, detail="디바이스를 찾을 수 없습니다.")
        now = now_ts()
        conn.execute(
            """
            UPDATE devices
            SET name=?,
                active_domain=?,
                status='connected',
                last_seen=?,
                public_ip=COALESCE(?, public_ip),
                updated_at=?
            WHERE id=?
            """,
            (payload.device_name, normalized_active, now, public_ip, now, device_id),
        )
        for state in payload.domain_states:
            domain = normalize_domain(state.domain)
            cycle_completed_value: Optional[int]
            if state.cycle_completed is None:
                cycle_completed_value = None
            else:
                cycle_completed_value = 1 if state.cycle_completed else 0
            state_last_run = sanitize_stop_schedule_last_run(state.stop_schedule_last_run)
            conn.execute(
                """
                UPDATE device_configs
                SET client_db_version=COALESCE(?, client_db_version),
                    client_total=COALESCE(?, client_total),
                    client_pending=COALESCE(?, client_pending),
                    client_sent=COALESCE(?, client_sent),
                    client_failed=COALESCE(?, client_failed),
                    client_block=COALESCE(?, client_block),
                    client_removed=COALESCE(?, client_removed),
                    client_reserved=COALESCE(?, client_reserved),
                    client_remaining=COALESCE(?, client_remaining),
                    client_cycle_completed=COALESCE(?, client_cycle_completed),
                    client_cycle_count=COALESCE(?, client_cycle_count),
                    client_last_cycle_at=COALESCE(?, client_last_cycle_at),
                    client_last_cycle_processed=COALESCE(?, client_last_cycle_processed),
                    stop_schedule_last_run=COALESCE(?, stop_schedule_last_run),
                    client_updated_at=?
                WHERE device_id=? AND domain=?
                """,
                (
                    state.local_db_version,
                    state.total,
                    state.pending,
                    state.sent,
                    state.failed,
                    state.block,
                    state.removed,
                    state.reserved,
                    state.remaining,
                    cycle_completed_value,
                    state.cycle_count,
                    state.last_cycle_completed_at,
                    state.last_cycle_processed,
                    state_last_run,
                    now,
                    device_id,
                    domain,
                ),
            )
        dispatched_jobs: List[JobDispatchPayload] = []
        for report in payload.job_reports:
            updated_row = update_job_status(
                conn,
                device_id,
                report.job_id,
                report.status.lower(),
                report.message,
                report.result,
                report.error,
            )
            if updated_row and report.status.lower() in {"success", "failed", "cancelled"}:
                handle_job_completion(conn, updated_row, report)
        pending_jobs = conn.execute(
            """
            SELECT *
            FROM jobs
            WHERE device_id=? AND status='pending' AND cancel_requested=0
            ORDER BY
                CASE job_type
                    WHEN 'single_send' THEN 0
                    WHEN 'batch_send' THEN 1
                    ELSE 2
                END,
                created_at ASC
            """,
            (device_id,),
        ).fetchall()
        for row in pending_jobs:
            conn.execute(
                "UPDATE jobs SET status='dispatched', queued_at=? WHERE id=?",
                (now, row["id"]),
            )
            dispatched_jobs.append(JobDispatchPayload(**serialize_job_dispatch(row)))
        control_rows = conn.execute(
            """
            SELECT id, cancel_requested
            FROM jobs
            WHERE device_id=? AND cancel_requested=1 AND status IN ('dispatched', 'running')
            """,
            (device_id,),
        ).fetchall()
        config_rows = conn.execute(
            "SELECT * FROM device_configs WHERE device_id=?",
            (device_id,),
        ).fetchall()
        device_row = conn.execute(
            "SELECT active_domain, public_ip FROM devices WHERE id=?",
            (device_id,),
        ).fetchone()
        conn.commit()
    active_domain = device_row["active_domain"] if device_row else normalized_active
    public_ip_value = device_row["public_ip"] if device_row else public_ip
    configs = {row["domain"]: serialize_config(to_dict(row)) for row in config_rows}
    job_controls = [
        JobControlPayload(job_id=row["id"], cancel_requested=bool(row["cancel_requested"]))
        for row in control_rows
        if row["cancel_requested"]
    ]
    return HeartbeatResponse(
        active_domain=active_domain,
        configs=configs,
        jobs=dispatched_jobs,
        job_controls=job_controls,
        public_ip=public_ip_value,
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
