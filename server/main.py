# -*- coding: utf-8 -*-
import hashlib
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
    "rcpt_to": "",
}


app = FastAPI(title="MailSender Control Server")

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "control.db"
STORAGE_ROOT = BASE_DIR / "storage"

templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
db_lock = threading.RLock()

DEVICE_HEARTBEAT_TIMEOUT_SECONDS = 15
JOB_STALE_GRACE_SECONDS = DEVICE_HEARTBEAT_TIMEOUT_SECONDS * 2


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
    return max(0, min(10, count))


def to_dict(row: sqlite3.Row) -> Dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _parse_iso_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


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
                    mail_from, header, session_count, bcc_count, rcpt_to, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    DEFAULT_DOMAIN_CONFIG["rcpt_to"],
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
    return {
        "domain": row["domain"],
        "helo": row.get("helo", ""),
        "smtp_host": row.get("smtp_host", ""),
        "smtp_port": row.get("smtp_port", 25),
        "mail_from": row.get("mail_from", ""),
        "header": row.get("header", ""),
        "session_count": session_value,
        "bcc_count": clamp_bcc_count(row.get("bcc_count", 0)),
        "rcpt_to": row.get("rcpt_to", ""),
        "updated_at": row.get("updated_at"),
        "client_db_version": row.get("client_db_version", 0),
        "client_total": row.get("client_total", 0),
        "client_pending": row.get("client_pending", 0),
        "client_sent": row.get("client_sent", 0),
        "client_failed": row.get("client_failed", 0),
        "client_block": row.get("client_block", 0),
        "client_removed": row.get("client_removed", 0),
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
                "logs": log_map.get(device["id"], [])[:40],
            }
        )
    return {
        "devices": devices,
        "counts": {
            "total": len(devices),
            "online": online_count,
        },
    }


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
    rcpt_to: Optional[str] = ""


class UpdateConfigRequest(DeviceConfigPayload):
    pass


class ActiveDomainRequest(BaseModel):
    domain: str


class DomainStatePayload(BaseModel):
    domain: str
    local_db_version: Optional[int] = None
    total: Optional[int] = None
    pending: Optional[int] = None
    sent: Optional[int] = None
    failed: Optional[int] = None
    block: Optional[int] = None
    removed: Optional[int] = None


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


@app.put("/api/devices/{device_id}/domains/{domain}/config")
def update_device_config(device_id: str, domain: str, payload: UpdateConfigRequest) -> Dict[str, Any]:
    normalized = normalize_domain(domain)
    with db_lock, get_conn() as conn:
        device = get_device(device_id, conn=conn)
        if not device:
            raise HTTPException(status_code=404, detail="디바이스를 찾을 수 없습니다.")
        now = now_ts()
        sanitized_bcc = clamp_bcc_count(payload.bcc_count or 0)
        conn.execute(
            """
            UPDATE device_configs
            SET helo=?, smtp_host=?, smtp_port=?, mail_from=?, header=?, session_count=?, bcc_count=?, rcpt_to=?, updated_at=?
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
    return {
        "helo": base.get("helo", ""),
        "smtp_host": base.get("smtp_host", ""),
        "smtp_port": base.get("smtp_port", 25),
        "mail_from": base.get("mail_from", ""),
        "header": base.get("header", ""),
        "session_count": session_count,
        "bcc_count": bcc_count,
        "rcpt_to": base.get("rcpt_to", ""),
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
        conn.commit()
    return {
        "device_id": device_id,
        "domain": normalized_domain,
        "cleared": deleted,
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
            ORDER BY created_at ASC
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
