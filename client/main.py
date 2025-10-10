# -*- coding: utf-8 -*-
import hashlib
import json
import re
import sqlite3
import sys
import time
import uuid
from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Deque, Dict, Iterable, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from smtp_utils import send_via_telnet
from urllib.parse import urlparse, urlunparse
from lib.change_ip import change_mobile_ip_at_phone, get_public_ipv4


APP_VERSION = "0.0.4"

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "settings.json"
DATA_DIR = BASE_DIR / "data"
LOG_DIR = BASE_DIR / "logs"

DOMAINS = ("naver", "daum")
EMAIL_STATUSES = ("pending", "reserved", "sent", "block", "failed", "removed")

DEFAULT_CONFIG: Dict[str, object] = {
    "server_url": "http://127.0.0.1:8000",
    "device_name": "MyPhone",
    "device_id": "",
    "interval": 5,
    "timeout": 15,
    "active_domain": "naver",
    "local_versions": {},
}

DOMAIN_DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS emails (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT NOT NULL UNIQUE,
    source_file TEXT,
    version INTEGER,
    status TEXT CHECK(status IN ('pending','reserved','sent','block','failed','removed')) NOT NULL DEFAULT 'pending',
    priority INTEGER DEFAULT 100,
    reserved_by TEXT,
    reserved_at TEXT,
    next_retry_at TEXT,
    attempts INTEGER DEFAULT 0,
    last_error TEXT,
    meta TEXT DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS injection_meta (
    version INTEGER PRIMARY KEY,
    created_at TEXT NOT NULL,
    total_count INTEGER NOT NULL,
    files TEXT NOT NULL,
    notes TEXT
);
CREATE TABLE IF NOT EXISTS domain_config (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""

DOMAIN_CONFIG_DEFAULTS = {
    "batch_size": "200",
    "reserved_timeout_seconds": "300",
    "block_retry_seconds": "60",
    "pending_retry_seconds": "120",
    "sent_retry_seconds": "3600",
    "smtp_port": "25",
    "smtp_host": "",
    "helo": "",
    "max_workers": "3",
    "report_interval_seconds": "60",
    "mail_from": "",
    "header_template": "",
}

EMAIL_PATTERN = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z0-9-]{2,}")
SQLITE_HEADER = b"SQLite format 3\x00"


def load_config() -> Dict[str, object]:
    if CONFIG_PATH.exists():
        try:
            data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
            merged = DEFAULT_CONFIG.copy()
            merged.update(data)
            return merged
        except (json.JSONDecodeError, OSError):
            return DEFAULT_CONFIG.copy()
    return DEFAULT_CONFIG.copy()


def save_config(config: Dict[str, object]) -> None:
    CONFIG_PATH.write_text(
        json.dumps(config, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def ensure_directories() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    for domain in DOMAINS:
        (DATA_DIR / domain).mkdir(parents=True, exist_ok=True)


def join_url(base: str, path: str) -> str:
    if path.startswith("http://") or path.startswith("https://"):
        return path
    if not path.startswith("/"):
        path = "/" + path
    return base.rstrip("/") + path


def normalize_server_url(raw: str) -> str:
    value = (raw or "").strip()
    if not value:
        return value
    if "://" not in value:
        value = f"http://{value}"
    parsed = urlparse(value)
    scheme = parsed.scheme or "http"
    netloc = parsed.netloc
    path = parsed.path or ""
    if not netloc and parsed.path:
        netloc = parsed.path
        path = ""
    hostname = parsed.hostname
    port = parsed.port
    userinfo = ""
    if parsed.username:
        userinfo = parsed.username
        if parsed.password:
            userinfo += f":{parsed.password}"
    if hostname:
        host_repr = hostname
        if ":" in host_repr and not host_repr.startswith("["):
            host_repr = f"[{host_repr}]"
        if userinfo:
            host_repr = f"{userinfo}@{host_repr}"
        if port is None:
            netloc = f"{host_repr}:8000"
        else:
            netloc = f"{host_repr}:{port}"
    elif netloc and ":" not in netloc:
        netloc = f"{netloc}:8000"
    normalized = urlunparse((scheme, netloc, path, parsed.params, parsed.query, parsed.fragment))
    return normalized


def prompt_value(message: str, current: Optional[str] = None) -> Optional[str]:
    hint = f" (현재: {current})" if current else ""
    value = input(f"{message}{hint}> ").strip()
    return value or None


def configure_server_url(config: Dict[str, object]) -> Dict[str, object]:
    current = str(config.get("server_url") or "")
    new_value = prompt_value("서버 주소를 입력하세요 (예: http://127.0.0.1:8000)", current)
    if new_value is not None:
        if not new_value:
            print("서버 주소는 비울 수 없습니다.")
        else:
            normalized = normalize_server_url(new_value)
            config["server_url"] = normalized
            save_config(config)
    return config


def configure_device_name(config: Dict[str, object]) -> Dict[str, object]:
    current = str(config.get("device_name") or "")
    new_value = prompt_value("디바이스 이름을 입력하세요", current)
    if new_value is not None:
        if not new_value:
            print("디바이스 이름은 비울 수 없습니다.")
        else:
            config["device_name"] = new_value
            answer = prompt_value("해당 이름으로 디바이스 ID를 새로 발급할까요? (y/N)")
            if answer and answer.lower().startswith("y"):
                config["device_id"] = ""
                print("디바이스 ID를 재발급하도록 설정했습니다. 다음 연결 시 새로운 ID가 생성됩니다.")
            save_config(config)
    return config


def ensure_required_config(config: Dict[str, object]) -> Dict[str, object]:
    working = config
    if not working.get("server_url"):
        working = configure_server_url(working)
    if not working.get("device_name"):
        working = configure_device_name(working)
    return load_config()


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())


@dataclass
class JobResult:
    job_id: str
    status: str
    message: Optional[str] = None
    result: Optional[Dict[str, object]] = None
    error: Optional[str] = None


@dataclass
class DispatchEmail:
    id: int
    email: str
    previous_status: str


@dataclass
class DispatchGroup:
    primary: DispatchEmail
    bcc: List[DispatchEmail]


@dataclass
class DispatchOutcome:
    success: bool
    response_text: str
    delivery_status: str
    status_line: str
    detail_line: Optional[str]


class MailClient:
    def __init__(self, config: Dict[str, object]) -> None:
        self.config = config
        raw_server_url = str(config.get("server_url") or "").strip()
        normalized_server_url = normalize_server_url(raw_server_url) if raw_server_url else ""
        if normalized_server_url and normalized_server_url != raw_server_url:
            config["server_url"] = normalized_server_url
            save_config(config)
        self.server_url = normalized_server_url.rstrip("/")
        self.device_name = str(config.get("device_name") or "").strip() or "Device"
        self.device_id = str(config.get("device_id") or "").strip()
        self.interval = int(config.get("interval") or 5)
        self.timeout = int(config.get("timeout") or 15)
        self.active_domain = str(config.get("active_domain") or "naver")
        self.local_versions: Dict[str, Optional[int]] = {
            key: value for key, value in config.get("local_versions", {}).items()
        }
        for domain in DOMAINS:
            self.local_versions.setdefault(domain, None)
        self.session = requests.Session()
        self._configure_session()
        self.domain_paths: Dict[str, Path] = {
            domain: DATA_DIR / domain / f"{domain}.db" for domain in DOMAINS
        }
        self.connected = False
        self.job_controls: Dict[str, Dict[str, object]] = {}
        self._state_errors: Dict[str, str] = {}
        self._recovery_failures: Dict[str, str] = {}
        self.public_ip: Optional[str] = None
        self._last_ip_refresh: float = 0.0

    # ------------------------------------------------------------------ #
    # 설정/환경 관리
    # ------------------------------------------------------------------ #
    def _configure_session(self) -> None:
        self.session.headers.update({"Connection": "keep-alive"})
        retry_strategy = Retry(
            total=2,
            connect=2,
            read=2,
            status=0,
            backoff_factor=0.5,
            allowed_methods=None,
            raise_on_status=False,
            respect_retry_after_header=False,
        )
        adapter = HTTPAdapter(pool_connections=5, pool_maxsize=5, max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def _reset_session(self) -> None:
        try:
            self.session.close()
        except Exception:
            pass
        self.session = requests.Session()
        self._configure_session()

    def refresh_public_ip(self, force: bool = False) -> Optional[str]:
        now = time.time()
        if not force and self.public_ip and (now - self._last_ip_refresh) < 60:
            return self.public_ip
        try:
            ip = get_public_ipv4()
        except Exception as exc:  # pylint: disable=broad-except
            print(f"[IP 조회 오류] {exc}")
            ip = None
        self._last_ip_refresh = now
        if ip:
            if ip != self.public_ip:
                print(f"[공인 IP] {ip}")
            self.public_ip = ip
        return self.public_ip

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        url = join_url(self.server_url, path)
        timeout = kwargs.pop("timeout", self.timeout)
        max_attempts = 3
        attempt = 0
        last_exc: Optional[requests.RequestException] = None
        response: Optional[requests.Response] = None
        while attempt < max_attempts:
            attempt += 1
            try:
                response = self.session.request(method=method, url=url, timeout=timeout, **kwargs)
                break
            except requests.RequestException as exc:
                last_exc = exc
                print(f"[HTTP {method.upper()}] 요청 실패 (시도 {attempt}/{max_attempts}): {exc}")
                if attempt >= max_attempts or not self._should_retry(exc):
                    raise
                self._reset_session()
                time.sleep(min(1.0, 0.2 * attempt))
        if response is None:
            if last_exc:
                raise last_exc
            raise RuntimeError("알 수 없는 HTTP 요청 오류가 발생했습니다.")
        return response

    @staticmethod
    def _should_retry(exc: requests.RequestException) -> bool:
        if isinstance(exc, (requests.exceptions.Timeout, requests.exceptions.HTTPError)):
            return False
        message = str(exc).lower()
        if isinstance(exc, requests.exceptions.ConnectionError):
            return True
        retry_signatures = (
            "remotedisconnected",
            "connection aborted",
            "broken pipe",
            "badstatusline",
        )
        return any(signature in message for signature in retry_signatures)

    def persist(self) -> None:
        snapshot = self.config.copy()
        snapshot["server_url"] = self.server_url
        snapshot["device_name"] = self.device_name
        snapshot["device_id"] = self.device_id
        snapshot["interval"] = self.interval
        snapshot["timeout"] = self.timeout
        snapshot["active_domain"] = self.active_domain
        snapshot["local_versions"] = self.local_versions
        save_config(snapshot)

    # ------------------------------------------------------------------ #
    # 내부 유틸리티
    # ------------------------------------------------------------------ #
    @staticmethod
    def _sanitize_bcc_count(value: object) -> int:
        try:
            count = int(value or 0)
        except (TypeError, ValueError):
            return 0
        return max(0, min(10, count))

    @staticmethod
    def _sanitize_session_count(value: object) -> int:
        if value is None:
            return 1
        if isinstance(value, str):
            candidate = value.strip()
            if not candidate:
                return 1
            try:
                parsed = int(candidate)
            except ValueError:
                return 1
            return max(1, parsed)
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            return 1
        return max(1, parsed)

    @staticmethod
    def _classify_delivery(success: bool, response_text: str) -> str:
        if success:
            return "sent"
        text = (response_text or "").lower()
        if "block" in text:
            return "block"
        return "failed"

    def _select_bcc_candidates(
        self,
        domain: str,
        limit: int,
        exclude: Optional[Iterable[str]] = None,
    ) -> List[sqlite3.Row]:
        if limit <= 0:
            return []
        normalized = (domain or "").lower()
        db_path = self.domain_paths.get(normalized)
        if not db_path or not db_path.exists():
            return []
        exclude_set = {
            email.strip().lower()
            for email in (exclude or [])
            if email and email.strip()
        }
        placeholders = ",".join("?" for _ in exclude_set)
        params: List[object] = []
        query_parts = [
            "SELECT id, email",
            "FROM emails",
            "WHERE status IN ('pending','block')",
        ]
        if placeholders:
            query_parts.append(f"AND lower(email) NOT IN ({placeholders})")
            params.extend(sorted(exclude_set))
        query_parts.append("ORDER BY priority ASC, id ASC")
        query_parts.append("LIMIT ?")
        params.append(limit)
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            statement = "\n".join(query_parts)
            return conn.execute(statement, params).fetchall()

    def _update_email_rows(
        self,
        domain: str,
        rows: Iterable[sqlite3.Row],
        status: str,
        summary: Optional[str],
    ) -> None:
        row_list: List[sqlite3.Row] = []
        for row in rows:
            if not row:
                continue
            try:
                _ = row["id"]
            except (KeyError, TypeError, IndexError):
                continue
            row_list.append(row)
        if not row_list:
            return
        normalized = (domain or "").lower()
        db_path = self.domain_paths.get(normalized)
        if not db_path or not db_path.exists():
            return
        last_error = None if status == "sent" else (summary[-500:] if summary else None)
        with sqlite3.connect(db_path) as conn:
            now = now_iso()
            for row in row_list:
                conn.execute(
                    """
                    UPDATE emails
                    SET status=?,
                        updated_at=?,
                        attempts = attempts + 1,
                        last_error=?,
                        reserved_by=NULL,
                        reserved_at=NULL
                    WHERE id=?
                    """,
                    (
                        status,
                        now,
                        last_error,
                        row["id"],
                    ),
                )
            conn.commit()

    def _build_domain_state_snapshot(
        self,
        domain: str,
        totals: Dict[str, int],
    ) -> Dict[str, object]:
        total_count = sum(totals.values())
        normalized = (domain or "").lower()
        return {
            "domain": normalized,
            "local_db_version": self.local_versions.get(normalized),
            "total": total_count,
            "pending": totals.get("pending", 0),
            "sent": totals.get("sent", 0),
            "failed": totals.get("failed", 0),
            "block": totals.get("block", 0),
            "removed": totals.get("removed", 0),
        }

    def register(self) -> None:
        self.refresh_public_ip(force=True)
        payload = {
            "device_name": self.device_name,
            "device_id": self.device_id or None,
            "public_ip": self.public_ip,
        }
        response = self._request("post", "/api/devices/register", json=payload)
        response.raise_for_status()
        data = response.json()
        self.device_id = data["device_id"]
        self.device_name = data.get("name", self.device_name)
        self.active_domain = data.get("active_domain", self.active_domain)
        if data.get("public_ip"):
            self.public_ip = data["public_ip"]
        print(f"[등록] 디바이스 ID: {self.device_id}")
        self.persist()

    # ------------------------------------------------------------------ #
    # 하트비트 & 상태 보고
    # ------------------------------------------------------------------ #
    def heartbeat(
        self,
        domain_states: List[Dict[str, object]],
        job_reports: List[JobResult],
    ) -> Dict[str, object]:
        self.refresh_public_ip()
        payload = {
            "device_name": self.device_name,
            "active_domain": self.active_domain,
            "domain_states": domain_states,
            "job_reports": [
                {
                    "job_id": report.job_id,
                    "status": report.status,
                    "message": report.message,
                    "result": report.result,
                    "error": report.error,
                }
                for report in job_reports
            ],
            "public_ip": self.public_ip,
        }
        response = self._request("post", f"/api/devices/{self.device_id}/heartbeat", json=payload)
        response.raise_for_status()
        data = response.json()
        self.active_domain = data.get("active_domain", self.active_domain)
        if data.get("public_ip"):
            self.public_ip = data["public_ip"]
        controls = data.get("job_controls") or []
        if isinstance(controls, Iterable):
            self._update_job_controls(controls)
        return data

    def send_job_report(
        self,
        report: JobResult,
        domain_states: Optional[List[Dict[str, object]]] = None,
    ) -> None:
        try:
            self.heartbeat(domain_states or [], [report])
        except Exception as exc:  # pylint: disable=broad-except
            print(f"[경고] 작업 보고 실패: {exc}")

    def _update_job_controls(self, controls: Iterable[Dict[str, object]]) -> None:
        snapshot: Dict[str, Dict[str, object]] = {}
        for control in controls:
            if not isinstance(control, dict):
                continue
            job_id = str(control.get("job_id") or "").strip()
            if not job_id:
                continue
            snapshot[job_id] = control
        self.job_controls = snapshot

    def _is_cancel_requested(self, job_id: str) -> bool:
        if not job_id:
            return False
        control = self.job_controls.get(job_id)
        if not isinstance(control, dict):
            return False
        return bool(control.get("cancel_requested"))

    # ------------------------------------------------------------------ #
    # 로컬 DB / 상태 수집
    # ------------------------------------------------------------------ #
    def collect_domain_states(self) -> List[Dict[str, object]]:
        states: List[Dict[str, object]] = []
        for domain, db_path in self.domain_paths.items():
            if not db_path.exists():
                states.append(
                    {
                        "domain": domain,
                        "local_db_version": self.local_versions.get(domain),
                        "total": 0,
                        "pending": 0,
                        "sent": 0,
                        "failed": 0,
                        "block": 0,
                        "removed": 0,
                    }
                )
                continue
            try:
                with sqlite3.connect(db_path) as conn:
                    conn.row_factory = sqlite3.Row
                    total_row = conn.execute("SELECT COUNT(*) AS cnt FROM emails").fetchone()
                    totals = {status: 0 for status in EMAIL_STATUSES}
                    for row in conn.execute(
                        "SELECT status, COUNT(*) AS cnt FROM emails GROUP BY status"
                    ):
                        totals[row["status"]] = row["cnt"]
                    states.append(
                        {
                            "domain": domain,
                            "local_db_version": self.local_versions.get(domain),
                            "total": total_row["cnt"] if total_row else 0,
                            "pending": totals.get("pending", 0),
                            "sent": totals.get("sent", 0),
                            "failed": totals.get("failed", 0),
                            "block": totals.get("block", 0),
                            "removed": totals.get("removed", 0),
                        }
                    )
                    if domain in self._state_errors:
                        self._state_errors.pop(domain, None)
            except sqlite3.Error as error:
                rebuilt = self._attempt_recover_domain_db(domain, db_path)
                if rebuilt:
                    return self.collect_domain_states()
                message = f"{domain} DB 상태를 읽는 중 문제 발생: {error}"
                if self._state_errors.get(domain) != message:
                    print(f"[오류] {message}")
                    self._state_errors[domain] = message
                states.append(
                    {
                        "domain": domain,
                        "local_db_version": self.local_versions.get(domain),
                        "total": 0,
                        "pending": 0,
                        "sent": 0,
                        "failed": 0,
                        "block": 0,
                        "removed": 0,
                    }
                )
        return states

    # ------------------------------------------------------------------ #
    # 작업 처리
    # ------------------------------------------------------------------ #
    def process_job(self, job: Dict[str, object]) -> JobResult:
        job_type = job.get("job_type")
        domain = job.get("domain")
        payload = job.get("payload") or {}
        job_id = str(job.get("job_id"))
        print(f"[작업 수신] {job_type} (ID: {job_id})")
        if job_type == "inject_file":
            return self.handle_inject(domain, payload, job_id)
        if job_type == "single_send":
            return self.handle_single_send(domain, payload, job_id)
        if job_type == "batch_send":
            return self.handle_batch_send(domain, payload, job_id)
        if job_type == "change_ip":
            return self.handle_change_ip(job_id)
        return JobResult(job_id=job_id, status="failed", message="지원하지 않는 작업 유형입니다.")

    def handle_inject(self, domain: Optional[str], payload: Dict[str, object], job_id: str) -> JobResult:
        if not domain:
            return JobResult(job_id=job_id, status="failed", message="도메인 정보가 없습니다.")
        normalized = domain.lower()
        download_path = payload.get("download_path")
        version = payload.get("version")
        filename = payload.get("filename") or f"{normalized}.db"
        if not download_path:
            return JobResult(job_id=job_id, status="failed", message="다운로드 경로가 없습니다.")
        print(f"[Inject] 파일 다운로드 경로: {download_path}")
        response = self._request("get", str(download_path), timeout=max(self.timeout, 30))
        response.raise_for_status()
        data = response.content
        target_dir = DATA_DIR / normalized
        target_dir.mkdir(parents=True, exist_ok=True)
        target_path = target_dir / f"{normalized}.db"
        backup_path = target_dir / f"{normalized}.db.bak"
        if target_path.exists():
            target_path.replace(backup_path)

        try:
            target_path.write_bytes(data)
            inserted_count = self._ensure_sqlite_db(target_path, data, filename)
        except Exception as error:  # pylint: disable=broad-except
            print(f"[오류] Inject 처리 중 문제 발생: {error}")
            if backup_path.exists():
                backup_path.replace(target_path)
            return JobResult(job_id=job_id, status="failed", message=f"DB 갱신 실패: {error}")

        self.local_versions[normalized] = version
        self.persist()
        print(f"[Inject 완료] {filename} -> {target_path}")

        message = f"{DOMAIN_LABELS.get(normalized, normalized)} DB 동기화 완료"
        if inserted_count >= 0:
            message += f" ({inserted_count}건 변환)"

        total_records = self._count_emails_in_db(target_path)
        if total_records is not None:
            message += f" · 총 {total_records}건"

        result_payload = {
            "bytes": len(data),
            "version": version,
            "filename": filename,
            "records": inserted_count if inserted_count >= 0 else None,
            "total_records": total_records,
        }

        return JobResult(
            job_id=job_id,
            status="success",
            message=message,
            result=result_payload,
        )

    def handle_single_send(self, domain: Optional[str], payload: Dict[str, object], job_id: str) -> JobResult:
        config = payload.get("config") or {}
        rcpt_to = payload.get("rcpt_to")
        if not rcpt_to:
            return JobResult(job_id=job_id, status="failed", message="RCPT TO 정보가 없습니다.")
        normalized_domain = (domain or "").lower()
        bcc_count = self._sanitize_bcc_count(config.get("bcc_count"))
        bcc_rows: List[sqlite3.Row] = []
        if normalized_domain:
            bcc_rows = self._select_bcc_candidates(normalized_domain, bcc_count, [rcpt_to])
        bcc_emails = [row["email"] for row in bcc_rows]
        success, response_text = send_via_telnet(
            smtp_host=config.get("smtp_host", ""),
            smtp_port=int(config.get("smtp_port") or 25),
            helo=config.get("helo", ""),
            mail_from=config.get("mail_from", ""),
            rcpt_to=rcpt_to,
            header_text=config.get("header", ""),
            bcc_emails=bcc_emails,
        )
        response_text = response_text or ""
        status = "success" if success else "failed"
        status_line, detail_line = self._smtp_status_and_detail(response_text)
        delivery_status = self._classify_delivery(success, response_text)
        if bcc_rows and normalized_domain:
            self._update_email_rows(normalized_domain, bcc_rows, delivery_status, detail_line or status_line)
        recipients = [rcpt_to] + bcc_emails
        dispatch_logs: List[Dict[str, object]] = []
        bcc_total = len(bcc_emails)
        if delivery_status == "sent":
            for offset, recipient in enumerate(recipients, start=1):
                is_primary = offset == 1
                meta_items: List[Tuple[str, object]] = []
                if is_primary and bcc_total > 0:
                    meta_items = [("bcc", bcc_total), ("primary", 1)]
                log_line = self._format_dispatch_log_line("Sent", offset, recipient, meta_items)
                display_line = self._format_dispatch_display_line("Sent", recipient, bcc_total, is_primary)
                dispatch_logs.append(
                    {
                        "log": log_line,
                        "display": display_line,
                        "email": recipient,
                        "sequence": offset,
                        "delivery_status": "sent",
                        "detail": detail_line or status_line,
                        "bcc_total": bcc_total if is_primary else 0,
                        "is_primary": is_primary,
                    }
                )
        else:
            label = "Block" if delivery_status == "block" else "Fail"
            for recipient in recipients:
                log_line = self._format_dispatch_log_line(label, 0, recipient)
                display_line = self._format_dispatch_display_line(label, recipient, 0, False)
                dispatch_logs.append(
                    {
                        "log": log_line,
                        "display": display_line,
                        "email": recipient,
                        "sequence": 0,
                        "delivery_status": delivery_status,
                        "detail": detail_line or status_line,
                        "bcc_total": 0,
                        "is_primary": False,
                    }
                )
        for entry in dispatch_logs:
            print(entry.get("display") or entry["log"])
        if bcc_emails:
            print(f"  ↳ BCC 대상 {len(bcc_emails)}건 포함")
        if not success and detail_line and detail_line != status_line:
            print(f"  ↳ {detail_line}")
        result_payload = {
            "rcpt_to": rcpt_to,
            "domain": domain,
            "summary": status_line,
            "detail": detail_line,
            "bcc": bcc_emails,
            "delivery_status": delivery_status,
            "logs": dispatch_logs,
        }
        error_message = None if success else (detail_line or status_line or "발송 실패")
        primary_log = dispatch_logs[0]["log"] if dispatch_logs else self._format_dispatch_log_line("Fail", 0, rcpt_to)
        return JobResult(
            job_id=job_id,
            status=status,
            message=primary_log,
            result=result_payload,
            error=error_message,
        )

    def handle_batch_send(self, domain: Optional[str], payload: Dict[str, object], job_id: str) -> JobResult:
        if not domain:
            return JobResult(job_id=job_id, status="failed", message="도메인 정보가 없습니다.")
        normalized = domain.lower()
        domain_label = DOMAIN_LABELS.get(normalized, normalized)
        db_path = self.domain_paths.get(normalized)
        if not db_path or not db_path.exists():
            return JobResult(job_id=job_id, status="failed", message="로컬 DB가 없습니다.")

        config = payload.get("config") or {}
        session_count = max(1, self._sanitize_session_count(config.get("session_count")))
        bcc_count = self._sanitize_bcc_count(config.get("bcc_count"))
        group_size = max(1, 1 + bcc_count)

        processed = 0
        sent_count = 0
        block_count = 0
        failed_count = 0
        last_error: Optional[str] = None
        bcc_processed = 0
        progress_interval = min(3.0, max(1.0, float(self.interval or 3)))
        last_report_at = time.monotonic()
        domain_totals: Dict[str, int] = {status: 0 for status in EMAIL_STATUSES}

        def build_summary() -> Dict[str, int]:
            return {
                "processed": processed,
                "sent": sent_count,
                "failed": failed_count,
                "block": block_count,
                "bcc": bcc_processed,
            }

        def format_summary(prefix: str) -> str:
            return (
                f"{prefix} 처리={processed} 성공={sent_count} "
                f"실패={failed_count} 차단={block_count} BCC={bcc_processed}"
            )

        def emit_progress(force: bool = False) -> None:
            nonlocal last_report_at
            now_point = time.monotonic()
            if not force and now_point - last_report_at < progress_interval:
                return
            summary_snapshot = build_summary()
            progress_message = (
                f"[진행] {domain_label} 처리={summary_snapshot['processed']} "
                f"성공={summary_snapshot['sent']} 실패={summary_snapshot['failed']} "
                f"차단={summary_snapshot['block']} BCC={summary_snapshot['bcc']}"
            )
            domain_state = self._build_domain_state_snapshot(normalized, domain_totals.copy())
            self.send_job_report(
                JobResult(job_id=job_id, status="running", message=progress_message, result=summary_snapshot),
                [domain_state],
            )
            last_report_at = now_point

        stop_requested = False
        cancel_requested = False
        stop_reason: Optional[str] = None
        fatal_error: Optional[str] = None

        try:
            with sqlite3.connect(db_path) as conn:
                conn.row_factory = sqlite3.Row
                for row in conn.execute(
                    "SELECT status, COUNT(*) AS cnt FROM emails GROUP BY status"
                ):
                    status_key = row["status"]
                    count_value = row["cnt"] or 0
                    domain_totals[status_key] = count_value

                session_token = uuid.uuid4().hex
                pending_queue: Deque[DispatchEmail] = deque()
                inflight: Dict[Future[DispatchOutcome], DispatchGroup] = {}
                fetch_batch_size = max(group_size * session_count, group_size)

                def reserve_candidates(limit: int) -> List[DispatchEmail]:
                    limit = max(1, int(limit or 0))
                    rows = conn.execute(
                        """
                        SELECT id, email, status
                        FROM emails
                        WHERE status IN ('pending','block')
                        ORDER BY priority ASC, id ASC
                        LIMIT ?
                        """,
                        (limit,),
                    ).fetchall()
                    if not rows:
                        return []
                    now_stamp = now_iso()
                    emails = [
                        DispatchEmail(
                            id=int(row["id"]),
                            email=row["email"],
                            previous_status=row["status"],
                        )
                        for row in rows
                    ]
                    conn.executemany(
                        """
                        UPDATE emails
                        SET status='reserved',
                            reserved_by=?,
                            reserved_at=?
                        WHERE id=?
                        """,
                        [(session_token, now_stamp, record.id) for record in emails],
                    )
                    conn.commit()
                    return emails

                def release_reserved_rows(rows: Iterable[DispatchEmail]) -> None:
                    release_list = list(rows)
                    if not release_list:
                        return
                    now_stamp = now_iso()
                    conn.executemany(
                        """
                        UPDATE emails
                        SET status=?,
                            reserved_by=NULL,
                            reserved_at=NULL,
                            updated_at=?
                        WHERE id=?
                        """,
                        [
                            (record.previous_status, now_stamp, record.id)
                            for record in release_list
                        ],
                    )
                    conn.commit()

                def next_group() -> Optional[DispatchGroup]:
                    if stop_requested:
                        return None
                    while not pending_queue:
                        newly_reserved = reserve_candidates(fetch_batch_size)
                        if not newly_reserved:
                            return None
                        pending_queue.extend(newly_reserved)
                    primary_email = pending_queue.popleft()
                    bcc_items: List[DispatchEmail] = []
                    while len(bcc_items) < bcc_count:
                        if pending_queue:
                            bcc_items.append(pending_queue.popleft())
                            continue
                        replenished = reserve_candidates(bcc_count - len(bcc_items))
                        if not replenished:
                            break
                        pending_queue.extend(replenished)
                    return DispatchGroup(primary=primary_email, bcc=bcc_items)

                def deliver_group(group: DispatchGroup) -> DispatchOutcome:
                    rcpt_to = group.primary.email
                    bcc_emails = [item.email for item in group.bcc if item.email]
                    success, response_text = send_via_telnet(
                        smtp_host=config.get("smtp_host", ""),
                        smtp_port=int(config.get("smtp_port") or 25),
                        helo=config.get("helo", ""),
                        mail_from=config.get("mail_from", ""),
                        rcpt_to=rcpt_to,
                        header_text=config.get("header", ""),
                        bcc_emails=bcc_emails,
                    )
                    response_text = response_text or ""
                    delivery_status = self._classify_delivery(success, response_text)
                    status_line, detail_line = self._smtp_status_and_detail(response_text)
                    return DispatchOutcome(
                        success=success,
                        response_text=response_text,
                        delivery_status=delivery_status,
                        status_line=status_line,
                        detail_line=detail_line,
                    )

                initial_rows = reserve_candidates(fetch_batch_size)
                if not initial_rows:
                    empty_summary = build_summary()
                    empty_message = format_summary("처리할 대상이 없습니다.")
                    return JobResult(
                        job_id=job_id,
                        status="success",
                        message=empty_message,
                        result=empty_summary,
                    )
                pending_queue.extend(initial_rows)
                emit_progress(force=True)

                def process_future(future: Future, group: DispatchGroup) -> None:
                    nonlocal processed, sent_count, block_count, failed_count, last_error
                    nonlocal stop_requested, fatal_error, stop_reason, bcc_processed
                    try:
                        outcome = future.result()
                    except Exception as exc:  # pylint: disable=broad-except
                        outcome = DispatchOutcome(
                            success=False,
                            response_text=f"ERROR: {exc}",
                            delivery_status="failed",
                            status_line="예외 발생",
                            detail_line=str(exc),
                        )
                    recipients = [group.primary] + group.bcc
                    previous_statuses = [item.previous_status for item in recipients]
                    bcc_processed += len(group.bcc)
                    now_stamp = now_iso()
                    detail_for_log = outcome.detail_line or outcome.status_line
                    error_text = None if outcome.delivery_status == "sent" else (detail_for_log or outcome.status_line or "")[-500:]
                    for prev_status in previous_statuses:
                        if prev_status:
                            domain_totals[prev_status] = max(0, domain_totals.get(prev_status, 0) - 1)
                    for record in recipients:
                        persist_status = outcome.delivery_status
                        if persist_status == "block":
                            persist_status = "pending"
                        conn.execute(
                            """
                            UPDATE emails
                            SET status=?,
                                updated_at=?,
                                attempts = attempts + 1,
                                last_error=?,
                                reserved_by=NULL,
                                reserved_at=NULL
                            WHERE id=?
                            """,
                            (
                                persist_status,
                                now_stamp,
                                None if outcome.delivery_status == "sent" else error_text,
                                record.id,
                            ),
                        )
                        domain_totals[persist_status] = domain_totals.get(persist_status, 0) + 1
                    conn.commit()
                    group_size_actual = len(recipients)
                    processed += group_size_actual
                    dispatch_logs: List[Dict[str, object]] = []
                    recipient_emails = [record.email for record in recipients]
                    if outcome.delivery_status == "sent":
                        sent_base = sent_count
                        sent_count += group_size_actual
                        bcc_total = len(group.bcc)
                        for offset, recipient in enumerate(recipient_emails, start=1):
                            sequence = sent_base + offset
                            is_primary = offset == 1
                            meta_items: List[Tuple[str, object]] = []
                            if is_primary and bcc_total > 0:
                                meta_items = [("bcc", bcc_total), ("primary", 1)]
                            log_line = self._format_dispatch_log_line("Sent", sequence, recipient, meta_items)
                            display_line = self._format_dispatch_display_line("Sent", recipient, bcc_total, is_primary)
                            dispatch_logs.append(
                                {
                                    "log": log_line,
                                    "display": display_line,
                                    "email": recipient,
                                    "sequence": sequence,
                                    "delivery_status": outcome.delivery_status,
                                    "detail": detail_for_log,
                                    "bcc_total": bcc_total if is_primary else 0,
                                    "is_primary": is_primary,
                                }
                            )
                            print(display_line)
                    else:
                        is_block = outcome.delivery_status == "block"
                        label = "Block" if is_block else "Fail"
                        for offset, recipient in enumerate(recipient_emails, start=1):
                            sequence_for_log = processed - group_size_actual + offset
                            log_line = self._format_dispatch_log_line(label, sequence_for_log, recipient)
                            display_line = self._format_dispatch_display_line(label, recipient, 0, False)
                            dispatch_logs.append(
                                {
                                    "log": log_line,
                                    "display": display_line,
                                    "email": recipient,
                                    "sequence": sequence_for_log,
                                    "delivery_status": outcome.delivery_status,
                                    "detail": detail_for_log,
                                    "bcc_total": 0,
                                    "is_primary": False,
                                }
                            )
                            print(display_line)
                        if detail_for_log and detail_for_log != outcome.status_line:
                            print(f"  ↳ {detail_for_log}")
                        if is_block:
                            block_count += group_size_actual
                            last_error = detail_for_log
                        else:
                            failed_count += group_size_actual
                            last_error = detail_for_log
                    if group.bcc:
                        print(f"  ↳ BCC 대상 {len(group.bcc)}건 포함")
                    lower_response = (outcome.response_text or "").lower()
                    status_lower = (outcome.status_line or "").lower()
                    detail_lower = (detail_for_log or "").lower()
                    fatal_marker_messages = {
                        "452 4.7.1 sent too many messages": "네이버 '452 4.7.1 Sent too many messages' 응답 감지",
                        "452 4.7.1 sent too many message": "네이버 '452 4.7.1 Sent too many messages' 응답 감지",
                        "421 4.7.1 this email has been temporarily blocked": "네이버 '421 4.7.1 This email has been temporarily blocked' 응답 감지",
                    }
                    fatal_error = next(
                        (
                            message
                            for text in (lower_response, status_lower, detail_lower)
                            if text
                            for marker, message in fatal_marker_messages.items()
                            if marker in text
                        ),
                        fatal_error,
                    )
                    if fatal_error:
                        stop_reason = fatal_error
                        stop_requested = True
                    emit_progress()
                    if dispatch_logs:
                        summary_snapshot = build_summary()
                        try:
                            self.send_job_report(
                                JobResult(
                                    job_id=job_id,
                                    status="running",
                                    message=dispatch_logs[0]["log"],
                                    result={"logs": dispatch_logs, "summary": summary_snapshot},
                                )
                            )
                        except Exception:
                            pass

                try:
                    with ThreadPoolExecutor(max_workers=session_count) as executor:
                        while True:
                            if not stop_requested:
                                while len(inflight) < session_count:
                                    group = next_group()
                                    if not group:
                                        break
                                    future = executor.submit(deliver_group, group)
                                    inflight[future] = group
                                    if stop_requested:
                                        break
                            if inflight:
                                done, _ = wait(inflight.keys(), timeout=0.5, return_when=FIRST_COMPLETED)
                                if not done:
                                    if not stop_requested and self._is_cancel_requested(job_id):
                                        stop_requested = True
                                        cancel_requested = True
                                        stop_reason = "사용자 중지 요청"
                                    continue
                                for future in done:
                                    group = inflight.pop(future)
                                    process_future(future, group)
                                if fatal_error and not inflight:
                                    break
                                continue
                            if stop_requested:
                                break
                            group = next_group()
                            if not group:
                                break
                            future = executor.submit(deliver_group, group)
                            inflight[future] = group
                        while inflight:
                            done, _ = wait(inflight.keys(), timeout=None, return_when=FIRST_COMPLETED)
                            for future in done:
                                group = inflight.pop(future)
                                process_future(future, group)
                finally:
                    if pending_queue:
                        release_reserved_rows(list(pending_queue))
                        pending_queue.clear()
        except sqlite3.Error as exc:
            return JobResult(job_id=job_id, status="failed", message=f"DB 오류: {exc}")

        emit_progress(force=True)
        summary = build_summary()
        if cancel_requested:
            final_message = format_summary("사용자 요청으로 배치 발송 중단")
            print(f"[배치 발송] {domain_label} · {final_message}")
            return JobResult(
                job_id=job_id,
                status="cancelled",
                message=final_message,
                result=summary,
                error="사용자 요청으로 발송을 중단했습니다.",
            )
        success = failed_count == 0 and fatal_error is None
        base_message = "배치 발송 완료" if success else "배치 발송 중 오류"
        final_message = format_summary(base_message)
        error_message = None
        if fatal_error:
            error_message = fatal_error
            final_message = f"{final_message} · {fatal_error}"
        elif not success and last_error:
            error_message = last_error
            final_message = f"{final_message} · 마지막 오류: {last_error}"
        print(f"[배치 발송] {domain_label} · {final_message}")
        return JobResult(
            job_id=job_id,
            status="success" if success else "failed",
            message=final_message,
            result=summary,
            error=error_message,
        )

    def handle_change_ip(self, job_id: str) -> JobResult:
        print("[IP 변경] 비행기 모드 토글을 시작합니다.")
        previous_ip = self.public_ip
        try:
            new_ip = change_mobile_ip_at_phone()
        except Exception as exc:  # pylint: disable=broad-except
            return JobResult(job_id=job_id, status="failed", message=f"IP 변경 중 예외: {exc}")
        if not new_ip:
            return JobResult(job_id=job_id, status="failed", message="새 공인 IP를 확인하지 못했습니다.")
        self.public_ip = new_ip
        self._last_ip_refresh = time.time()
        message = f"공인 IP 변경 완료: {previous_ip or '-'} → {new_ip}"
        print(f"[IP 변경 완료] {message}")
        return JobResult(
            job_id=job_id,
            status="success",
            message=message,
            result={"public_ip": new_ip},
        )

    # ------------------------------------------------------------------ #
    # 실행 루프
    # ------------------------------------------------------------------ #
    @staticmethod
    def _looks_like_sqlite(raw: bytes) -> bool:
        return len(raw) >= len(SQLITE_HEADER) and raw.startswith(SQLITE_HEADER)

    def _ensure_sqlite_db(self, db_path: Path, original_bytes: bytes, source_name: str) -> int:
        if self._looks_like_sqlite(original_bytes):
            try:
                with sqlite3.connect(db_path) as conn:
                    conn.execute("PRAGMA schema_version")
                return -1
            except sqlite3.DatabaseError as exc:
                raise ValueError(f"SQLite DB 형식이지만 열 수 없습니다: {exc}") from exc

        try:
            with sqlite3.connect(db_path) as conn:
                conn.execute("SELECT name FROM sqlite_master WHERE type='table' LIMIT 1")
            return -1
        except sqlite3.DatabaseError:
            emails = self._extract_emails_from_bytes(original_bytes)
            if not emails:
                raise ValueError("텍스트에서 이메일을 추출하지 못했습니다.")
            self._build_sqlite_from_emails(db_path, emails, source_name)
            return len(emails)

    def _extract_emails_from_bytes(self, data: bytes) -> List[str]:
        candidates = ["utf-8", "euc-kr", "cp949", "latin-1"]
        text = ""
        for encoding in candidates:
            try:
                text = data.decode(encoding)
                break
            except UnicodeDecodeError:
                continue
        else:
            text = data.decode("utf-8", errors="ignore")

        seen = set()
        emails: List[str] = []
        for line in text.splitlines():
            for match in EMAIL_PATTERN.findall(line):
                email = match.strip(" <>\"'.,;:\t")
                key = email.lower()
                if email and key not in seen:
                    seen.add(key)
                    emails.append(email)
        return emails

    @staticmethod
    def _smtp_status_and_detail(response_text: Optional[str]) -> Tuple[str, Optional[str]]:
        if not response_text:
            return "응답 없음", None
        raw_lines = [line.strip() for line in response_text.splitlines() if line.strip()]
        if not raw_lines:
            return "응답 없음", None
        processed: List[str] = []
        for line in raw_lines:
            lower = line.lower()
            content = line.split(":", 1)[-1].strip() if ":" in line else line
            if lower.startswith("connect"):
                continue
            if lower.startswith("helo"):
                continue
            if lower.startswith("mail from"):
                continue
            if lower.startswith("rcpt to"):
                continue
            if lower == "data":
                continue
            if lower.startswith("data end"):
                if content:
                    processed.append(content)
                continue
            if lower.startswith("quit"):
                continue
            if content:
                processed.append(content)
        if not processed:
            return "응답 없음", None
        status_line = processed[-1]
        if len(status_line) > 180:
            status_line = status_line[:177] + "..."
        detail: Optional[str] = None
        if len(processed) > 1:
            detail = " / ".join(processed[:-1][-3:])
            if len(detail) > 300:
                detail = detail[:297] + "..."
        return status_line or "응답 없음", detail

    @staticmethod
    def _format_smtp_log_line(domain: Optional[str], rcpt_to: Optional[str], status_line: str) -> str:
        label = DOMAIN_LABELS.get((domain or "").lower(), (domain or "-") or "-")
        target = (rcpt_to or "").strip() or "-"
        status_segment = status_line or "응답 없음"
        timestamp = now_iso().replace("T", " ")
        return f"{label} {target} {status_segment} {timestamp}"

    @staticmethod
    def _format_dispatch_log_line(
        label: str,
        sequence: int,
        email: Optional[str],
        meta_items: Optional[Iterable[Tuple[str, object]]] = None,
    ) -> str:
        safe_label = (label or "").strip() or "Sent"
        safe_sequence = max(0, int(sequence or 0))
        target = (email or "").strip() or "-"
        timestamp = time.strftime("%m-%d %H:%M:%S", time.localtime())
        segments = [safe_label, str(safe_sequence), target, timestamp]
        if meta_items:
            for key, value in meta_items:
                key_text = str(key or "").strip()
                if not key_text:
                    continue
                if value is None:
                    continue
                segments.append(f"{key_text}={value}")
        return "|".join(segments)

    @staticmethod
    def _format_dispatch_display_line(
        label: str,
        email: Optional[str],
        bcc_total: int,
        is_primary: bool,
    ) -> str:
        safe_label = (label or "").strip() or "Sent"
        target = (email or "").strip() or "-"
        display = f"{safe_label} - {target}"
        if is_primary and bcc_total > 0:
            display += f" 외 {bcc_total}"
        return display

    def _build_sqlite_from_emails(self, db_path: Path, emails: List[str], source_name: str) -> None:
        if db_path.exists():
            db_path.unlink()
        with sqlite3.connect(db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.executescript(DOMAIN_DB_SCHEMA)
            now = now_iso()
            conn.executemany(
                """
                INSERT INTO emails (
                    email, source_file, version, status, priority, reserved_by,
                    reserved_at, next_retry_at, attempts, last_error, meta, created_at, updated_at
                )
                VALUES (?, ?, ?, 'pending', 100, NULL, NULL, NULL, 0, NULL, '{}', ?, ?)
                """,
                [(email, source_name, 1, now, now) for email in emails],
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO injection_meta (version, created_at, total_count, files, notes)
                VALUES (?, ?, ?, ?, NULL)
                """,
                (1, now, len(emails), json.dumps([source_name], ensure_ascii=False)),
            )
            conn.executemany(
                """
                INSERT INTO domain_config (key, value)
                VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value
                """,
                list(DOMAIN_CONFIG_DEFAULTS.items()),
            )
            conn.commit()

    @staticmethod
    def _count_emails_in_db(db_path: Path) -> Optional[int]:
        try:
            with sqlite3.connect(db_path) as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute("SELECT COUNT(*) AS cnt FROM emails").fetchone()
                return row["cnt"] if isinstance(row, sqlite3.Row) else (row[0] if row else 0)
        except sqlite3.Error:
            return None

    def _attempt_recover_domain_db(self, domain: str, db_path: Path) -> bool:
        if not db_path.exists():
            return False
        try:
            raw = db_path.read_bytes()
        except OSError:
            return False
        checksum = hashlib.sha1(raw).hexdigest()
        if self._recovery_failures.get(domain) == checksum:
            return False

        try:
            inserted = self._ensure_sqlite_db(db_path, raw, db_path.name)
        except Exception as error:  # pylint: disable=broad-except
            print(f"[오류] {domain} DB 복구 실패: {error}")
            self._recovery_failures[domain] = checksum
            return False

        if inserted >= 0:
            print(f"[복구] {domain} DB를 텍스트에서 변환했습니다. ({inserted}건)")
        self._state_errors.pop(domain, None)
        self._recovery_failures.pop(domain, None)
        self.persist()
        return True

    def run(self) -> None:
        ensure_directories()
        if not self.device_id:
            self.device_id = uuid.uuid4().hex
        try:
            self.register()
        except requests.RequestException as exc:
            print(f"[연결 실패] 서버({self.server_url or '-'})에 접속하지 못했습니다: {exc}")
            print("서버가 실행 중인지, 방화벽 또는 네트워크 설정을 확인한 뒤 다시 실행하세요.")
            self.connected = False
            self.persist()
            sys.exit(1)

        print("[시작] 서버와 동기화를 시작합니다. 중단: Ctrl+C")
        try:
            while True:
                try:
                    domain_states = self.collect_domain_states()
                    response = self.heartbeat(domain_states, [])
                    if not self.connected:
                        print("[연결] 서버와 동기화되었습니다.")
                    self.connected = True
                    configs = response.get("configs") or {}
                    self.apply_configs(configs)
                    jobs = response.get("jobs") or []
                    for job in jobs:
                        job_id = str(job.get("job_id"))
                        self.send_job_report(
                            JobResult(job_id=job_id, status="running", message="작업 시작")
                        )
                        result = self.process_job(job)
                        domain_states = self.collect_domain_states()
                        self.send_job_report(result, domain_states)
                        if job_id:
                            self.job_controls.pop(job_id, None)
                    time.sleep(self.interval)
                except requests.RequestException as exc:
                    if self.connected:
                        print(f"[네트워크 오류] {exc}. {self.interval}초 후 재시도합니다.")
                    try:
                        self.session.close()
                    except Exception:
                        pass
                    self.session = requests.Session()
                    self._configure_session()
                    self.connected = False
                    time.sleep(self.interval)
                except KeyboardInterrupt:
                    raise
                except Exception as exc:  # pylint: disable=broad-except
                    print(f"[예외] 루프 처리 중 오류: {exc}")
                    self.connected = False
                    time.sleep(self.interval)
        except KeyboardInterrupt:
            print("\n[정지] 사용자가 종료했습니다.")
        finally:
            self.persist()

    def apply_configs(self, configs: Dict[str, Dict[str, object]]) -> None:
        if not configs:
            return
        # 현재는 서버 설정을 참조용으로만 사용
        # 향후 필요시 로컬 동기화 로직을 추가할 수 있습니다.
        pass


DOMAIN_LABELS = {"naver": "네이버", "daum": "다음"}


def main() -> None:
    config = load_config()
    while True:
        print("\n========================")
        print(f" 메일 발송 클라이언트 v{APP_VERSION}")
        print("========================")
        print(f"1. 서버 연결 (현재: {config.get('server_url') or '미설정'})")
        print(f"2. 서버 주소 설정")
        print(f"3. 디바이스 이름 설정 (현재: {config.get('device_name') or '미설정'})")
        print("0. 종료")
        choice = input("선택> ").strip()

        if choice == "1":
            config = ensure_required_config(config)
            if not config.get("server_url") or not config.get("device_name"):
                print("서버 주소와 디바이스 이름을 먼저 설정해야 합니다.")
                continue
            client = MailClient(config)
            try:
                client.run()
            except KeyboardInterrupt:
                print("\n연결을 종료했습니다.")
            config = load_config()
        elif choice == "2":
            config = configure_server_url(config)
        elif choice == "3":
            config = configure_device_name(config)
        elif choice in {"0", "q", "Q"}:
            print("종료합니다.")
            break
        else:
            print("알 수 없는 선택입니다. 다시 입력하세요.")


if __name__ == "__main__":
    main()
