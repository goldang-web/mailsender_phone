# -*- coding: utf-8 -*-
import hashlib
import json
import re
import sqlite3
import sys
import time
import uuid
import os
import threading
import socket
from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from pathlib import Path
from typing import Deque, Dict, Iterable, List, Optional, Set, Tuple
from datetime import datetime, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from smtp_utils import send_via_telnet
from urllib.parse import urlparse, urlunparse
from lib.change_ip import change_mobile_ip_at_phone, get_public_ipv4
from lib.naver_imap import probe_imap_connection, verify_delivery, fetch_latest_message_summary


APP_VERSION = "0.0.40"

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "settings.json"
DATA_DIR = BASE_DIR / "data"
LOG_DIR = BASE_DIR / "logs"

DOMAINS = ("naver", "daum")
EMAIL_STATUSES = ("pending", "reserved", "sent", "block", "failed", "removed")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat().replace("+00:00", "Z")

DEFAULT_CONFIG: Dict[str, object] = {
    "server_url": "http://127.0.0.1:8000",
    "device_name": "MyPhone",
    "device_id": "",
    "interval": 5,
    "timeout": 15,
    "active_domain": "naver",
    "local_versions": {},
    "domain_cycles": {},
    "stop_schedule": {},
    "imap_settings": {},
}

IMAP_ALLOWED_LATENCY_MIN_SECONDS = 5
IMAP_ALLOWED_LATENCY_MAX_SECONDS = 600
IMAP_DEFAULT_ALLOWED_LATENCY_SECONDS = 20
IMAP_DEFAULT_SINGLE_DELAY_SECONDS = 20
IMAP_DEFAULT_BATCH_DELAY_SECONDS = 20

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


def acquire_wake_lock() -> bool:
    """디스플레이가 꺼져도 연결을 유지하도록 웨이크락을 확보합니다."""
    try:
        result = os.system('su -c "echo \'email_sender\' > /sys/power/wake_lock"')
        if result == 0:
            print("웨이크락이 획득되었습니다.")
            return True
        print(f"웨이크락 획득 실패: exit code {result}")
    except Exception as exc:  # pylint: disable=broad-except
        print(f"웨이크락 획득 실패: {exc}")
    return False


def release_wake_lock() -> None:
    """프로그램 종료 시 웨이크락을 해제합니다."""
    try:
        result = os.system('su -c "echo \'email_sender\' > /sys/power/wake_unlock"')
        if result == 0:
            print("웨이크락이 해제되었습니다.")
        else:
            print(f"웨이크락 해제 실패: exit code {result}")
    except Exception as exc:  # pylint: disable=broad-except
        print(f"웨이크락 해제 실패: {exc}")


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


def sanitize_imap_delay(
    value: Optional[object],
    *,
    default: Optional[int] = None,
    minimum: Optional[int] = None,
) -> int:
    effective_default = default if default is not None else IMAP_DEFAULT_BATCH_DELAY_SECONDS
    effective_min = minimum if minimum is not None else 0
    try:
        delay = int(value) if value is not None else effective_default
    except (TypeError, ValueError):
        delay = effective_default
    return max(effective_min, min(IMAP_ALLOWED_LATENCY_MAX_SECONDS, delay))


def sanitize_imap_allowed_latency(
    value: Optional[object],
    *,
    default: Optional[int] = None,
) -> int:
    effective_default = default if default is not None else IMAP_DEFAULT_ALLOWED_LATENCY_SECONDS
    try:
        latency = int(value) if value is not None else effective_default
    except (TypeError, ValueError):
        latency = effective_default
    return max(IMAP_ALLOWED_LATENCY_MIN_SECONDS, min(IMAP_ALLOWED_LATENCY_MAX_SECONDS, latency))


def sanitize_imap_failure_action(value: Optional[object]) -> str:
    if value is None:
        return "none"
    candidate = str(value).strip().lower()
    if candidate in {"none", "stop_device", "stop_all"}:
        return candidate
    return "none"


def sanitize_bool_flag(value: Optional[object]) -> bool:
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "on", "yes"}:
            return True
        if lowered in {"0", "false", "off", "no"}:
            return False
    return bool(value)


def normalize_imap_string(value: Optional[object]) -> str:
    if value is None:
        return ""
    return str(value).strip()


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
    injected: List[str] = field(default_factory=list)


@dataclass
class DispatchOutcome:
    success: bool
    response_text: str
    delivery_status: str
    status_line: str
    detail_line: Optional[str]
    sent_at: datetime


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
        raw_cycle_config = config.get("domain_cycles") or {}
        self.domain_cycles: Dict[str, Dict[str, object]] = {}
        for domain in DOMAINS:
            entry = raw_cycle_config.get(domain) or {}
            cycles_value = entry.get("cycles", 0)
            try:
                cycles_count = int(cycles_value)
            except (TypeError, ValueError):
                cycles_count = 0
            self.domain_cycles[domain] = {
                "cycles": max(0, cycles_count),
                "last_completed_at": entry.get("last_completed_at"),
                "last_processed": entry.get("last_processed"),
            }
        raw_schedule_config = config.get("stop_schedule") or {}
        self.stop_schedules: Dict[str, Dict[str, object]] = {}
        for domain in DOMAINS:
            entry = raw_schedule_config.get(domain) or {}
            schedule_time = self._sanitize_schedule_time(entry.get("time"))
            enabled_flag = self._sanitize_schedule_enabled(entry.get("enabled")) if entry else False
            enabled = bool(enabled_flag) and bool(schedule_time)
            last_run_local = self._sanitize_schedule_date(entry.get("last_run"))
            server_last_run = self._sanitize_schedule_date(entry.get("server_last_run"))
            if self._is_date_newer(server_last_run, last_run_local):
                last_run_local = server_last_run
            self.stop_schedules[domain] = {
                "enabled": enabled,
                "time": schedule_time or "",
                "last_run": last_run_local,
                "server_last_run": server_last_run,
                "needs_sync": bool(last_run_local and last_run_local != server_last_run),
            }
        raw_imap_settings = config.get("imap_settings") or {}
        self.imap_settings: Dict[str, Dict[str, object]] = {}
        for domain in DOMAINS:
            entry = raw_imap_settings.get(domain) or {}
            legacy_delay = sanitize_imap_delay(entry.get("delay_seconds"))
            allowed_latency = sanitize_imap_allowed_latency(
                entry.get("allowed_latency_seconds"),
                default=legacy_delay if legacy_delay else IMAP_DEFAULT_ALLOWED_LATENCY_SECONDS,
            )
            single_delay = sanitize_imap_delay(
                entry.get("single_delay_seconds"),
                default=IMAP_DEFAULT_SINGLE_DELAY_SECONDS,
                minimum=0,
            )
            batch_delay = sanitize_imap_delay(
                entry.get("batch_delay_seconds"),
                default=legacy_delay if legacy_delay else IMAP_DEFAULT_BATCH_DELAY_SECONDS,
                minimum=0,
            )
            self.imap_settings[domain] = {
                "enabled": bool(entry.get("enabled")),
                "username": normalize_imap_string(entry.get("username")),
                "password": entry.get("password") or "",
                "single_delay_seconds": single_delay,
                "batch_delay_seconds": batch_delay,
                "allowed_latency_seconds": allowed_latency,
                "failure_action": sanitize_imap_failure_action(entry.get("failure_action")),
                "notify_before_stop_all": sanitize_bool_flag(entry.get("notify_before_stop_all")),
                "delay_seconds": legacy_delay,
            }
        self._schedule_events: Dict[str, Dict[str, object]] = {}
        self.session = requests.Session()
        self._configure_session()
        self.domain_paths: Dict[str, Path] = {
            domain: DATA_DIR / domain / f"{domain}.db" for domain in DOMAINS
        }
        self.connected = False
        self._disconnect_logged = False
        self.job_controls: Dict[str, Dict[str, object]] = {}
        self._state_errors: Dict[str, str] = {}
        self._recovery_failures: Dict[str, str] = {}
        self.public_ip: Optional[str] = None
        self._last_ip_refresh: float = 0.0
        self._active_job_ids: Set[str] = set()
        self._active_jobs: Dict[str, str] = {}
        self._priority_running: bool = False
        self._cancel_ack_sent: Set[str] = set()
        self._pending_jobs: Deque[Dict[str, object]] = deque()
        self._pending_job_ids: Set[str] = set()
        raw_sequences = config.get("sent_sequences")
        if not isinstance(raw_sequences, dict):
            raw_sequences = {}
        self.sent_sequences: Dict[str, int] = {}
        for key, value in raw_sequences.items():
            try:
                self.sent_sequences[key] = max(0, int(value))
            except (TypeError, ValueError):
                self.sent_sequences[key] = 0
        for domain in DOMAINS:
            self.sent_sequences.setdefault(domain, 0)
        self._sequence_dirty: Set[str] = set()
        self._last_sequence_flush: float = 0.0
        self._imap_executor = ThreadPoolExecutor(max_workers=2)
        self._imap_lock = threading.Lock()
        self._imap_reports: Deque[Dict[str, object]] = deque()
        self._imap_futures: Set[Future] = set()

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

    def _get_cycle_entry(self, domain: str) -> Dict[str, object]:
        normalized = (domain or "").lower()
        entry = self.domain_cycles.get(normalized)
        if not isinstance(entry, dict):
            entry = {"cycles": 0, "last_completed_at": None}
        cycles_value = entry.get("cycles", 0)
        try:
            cycles_count = int(cycles_value)
        except (TypeError, ValueError):
            cycles_count = 0
        entry["cycles"] = max(0, cycles_count)
        if "last_completed_at" not in entry:
            entry["last_completed_at"] = entry.get("last_completed_at")
        if entry.get("last_processed") is not None:
            try:
                entry["last_processed"] = int(entry.get("last_processed"))
            except (TypeError, ValueError):
                entry["last_processed"] = None
        self.domain_cycles[normalized] = entry
        return entry

    def _record_cycle_completion(self, domain: str, processed_total: int) -> Dict[str, object]:
        entry = self._get_cycle_entry(domain)
        entry["cycles"] = int(entry.get("cycles", 0) or 0) + 1
        entry["last_completed_at"] = now_iso()
        try:
            entry["last_processed"] = int(processed_total)
        except (TypeError, ValueError):
            entry["last_processed"] = processed_total
        return entry

    def _reset_cycle_stats(self, domain: str) -> Dict[str, object]:
        entry = self._get_cycle_entry(domain)
        entry["cycles"] = 0
        entry["last_completed_at"] = None
        entry.pop("last_processed", None)
        return entry

    def _cycle_snapshot(self, domain: str) -> Dict[str, object]:
        entry = self._get_cycle_entry(domain)
        cycles_count = int(entry.get("cycles", 0) or 0)
        last_completed = entry.get("last_completed_at")
        return {
            "cycle_count": cycles_count,
            "last_cycle_completed_at": last_completed,
            "last_cycle_processed": entry.get("last_processed"),
            "cycle_completed": cycles_count > 0,
        }

    def persist(self) -> None:
        snapshot = self.config.copy()
        snapshot["server_url"] = self.server_url
        snapshot["device_name"] = self.device_name
        snapshot["device_id"] = self.device_id
        snapshot["interval"] = self.interval
        snapshot["timeout"] = self.timeout
        snapshot["active_domain"] = self.active_domain
        snapshot["local_versions"] = self.local_versions
        snapshot["domain_cycles"] = self.domain_cycles
        snapshot["stop_schedule"] = self._serialize_stop_schedule()
        snapshot["sent_sequences"] = self.sent_sequences
        snapshot["imap_settings"] = self._serialize_imap_settings()
        save_config(snapshot)
        self.config["sent_sequences"] = self.sent_sequences
        self.config["imap_settings"] = snapshot["imap_settings"]
        self._sequence_dirty.clear()
        self._last_sequence_flush = time.monotonic()

    # ------------------------------------------------------------------ #
    # 내부 유틸리티
    # ------------------------------------------------------------------ #
    def _serialize_stop_schedule(self) -> Dict[str, Dict[str, object]]:
        serialized: Dict[str, Dict[str, object]] = {}
        for domain, state in self.stop_schedules.items():
            serialized[domain] = {
                "enabled": bool(state.get("enabled")),
                "time": state.get("time") or "",
                "last_run": state.get("last_run"),
                "server_last_run": state.get("server_last_run"),
            }
        return serialized

    def _serialize_imap_settings(self) -> Dict[str, Dict[str, object]]:
        serialized: Dict[str, Dict[str, object]] = {}
        for domain, settings in self.imap_settings.items():
            serialized[domain] = {
                "enabled": bool(settings.get("enabled")),
                "username": normalize_imap_string(settings.get("username")),
                "password": settings.get("password") or "",
                "single_delay_seconds": sanitize_imap_delay(
                    settings.get("single_delay_seconds"),
                    default=IMAP_DEFAULT_SINGLE_DELAY_SECONDS,
                    minimum=0,
                ),
                "batch_delay_seconds": sanitize_imap_delay(
                    settings.get("batch_delay_seconds"),
                    default=IMAP_DEFAULT_BATCH_DELAY_SECONDS,
                    minimum=0,
                ),
                "allowed_latency_seconds": sanitize_imap_allowed_latency(
                    settings.get("allowed_latency_seconds"),
                    default=IMAP_DEFAULT_ALLOWED_LATENCY_SECONDS,
                ),
                "failure_action": sanitize_imap_failure_action(settings.get("failure_action")),
                "notify_before_stop_all": sanitize_bool_flag(settings.get("notify_before_stop_all")),
                "delay_seconds": sanitize_imap_delay(settings.get("delay_seconds")),
            }
        return serialized

    def _collect_imap_reports(self) -> List[Dict[str, object]]:
        with self._imap_lock:
            if not self._imap_reports:
                return []
            reports = list(self._imap_reports)
            self._imap_reports.clear()
        return reports

    def _queue_imap_report(self, report: Dict[str, object]) -> None:
        if not isinstance(report, dict):
            return
        with self._imap_lock:
            while len(self._imap_reports) >= 100:
                self._imap_reports.popleft()
            self._imap_reports.append(report)

    def _imap_settings_for_domain(self, domain: str) -> Dict[str, object]:
        normalized = (domain or "").lower()
        settings = self.imap_settings.get(normalized)
        if settings is None:
            settings = {
                "enabled": False,
                "username": "",
                "password": "",
                "single_delay_seconds": IMAP_DEFAULT_SINGLE_DELAY_SECONDS,
                "batch_delay_seconds": IMAP_DEFAULT_BATCH_DELAY_SECONDS,
                "allowed_latency_seconds": IMAP_DEFAULT_ALLOWED_LATENCY_SECONDS,
                "failure_action": "none",
                "notify_before_stop_all": False,
                "delay_seconds": IMAP_DEFAULT_BATCH_DELAY_SECONDS,
            }
            self.imap_settings[normalized] = settings
        return settings

    def _update_imap_settings_from_server(self, domain: str, payload: Dict[str, object]) -> None:
        if not isinstance(payload, dict):
            return
        settings = self._imap_settings_for_domain(domain)
        settings["enabled"] = bool(payload.get("imap_enabled"))
        settings["username"] = normalize_imap_string(payload.get("imap_username"))
        raw_password = payload.get("imap_password")
        password_saved = bool(payload.get("imap_password_saved"))
        if raw_password is not None:
            if raw_password == "********" and password_saved and settings.get("password"):
                pass
            else:
                settings["password"] = raw_password or ""
        settings["delay_seconds"] = sanitize_imap_delay(
            payload.get("imap_batch_delay_seconds"),
            default=IMAP_DEFAULT_BATCH_DELAY_SECONDS,
            minimum=0,
        )
        settings["single_delay_seconds"] = sanitize_imap_delay(
            payload.get("imap_single_delay_seconds"),
            default=IMAP_DEFAULT_SINGLE_DELAY_SECONDS,
            minimum=0,
        )
        settings["batch_delay_seconds"] = sanitize_imap_delay(
            payload.get("imap_batch_delay_seconds"),
            default=settings.get("delay_seconds"),
            minimum=0,
        )
        settings["allowed_latency_seconds"] = sanitize_imap_allowed_latency(
            payload.get("imap_allowed_latency_seconds"),
            default=payload.get("imap_delay_seconds"),
        )
        settings["failure_action"] = sanitize_imap_failure_action(payload.get("imap_failure_action"))
        settings["notify_before_stop_all"] = sanitize_bool_flag(
            payload.get("imap_notify_before_stop_all")
        )
        settings["last_status"] = payload.get("imap_last_status")
        settings["last_checked_at"] = payload.get("imap_last_checked_at")
        settings["last_latency"] = payload.get("imap_last_latency")
        settings["last_error"] = payload.get("imap_last_error")
        settings["last_mail_from"] = payload.get("imap_last_mail_from")
        settings["last_sent_at"] = payload.get("imap_last_sent_at")
        settings["last_received_at"] = payload.get("imap_last_received_at")

    def _imap_enabled(self, domain: str) -> bool:
        settings = self._imap_settings_for_domain(domain)
        return bool(
            settings.get("enabled")
            and normalize_imap_string(settings.get("username"))
            and (settings.get("password") or "")
        )

    def _on_imap_future_done(self, future: Future) -> None:
        with self._imap_lock:
            self._imap_futures.discard(future)

    def _shutdown_imap_executor(self) -> None:
        try:
            self._imap_executor.shutdown(wait=False)
        except Exception:  # pylint: disable=broad-except
            pass

    def _submit_imap_check(
        self,
        *,
        domain: str,
        job_id: Optional[str],
        send_type: str,
        mail_from: str,
        sent_at: datetime,
        has_anchor: bool,
        delay_before_check: Optional[float],
        allowed_delay: Optional[int],
        context_reason: Optional[str] = None,
    ) -> None:
        normalized = (domain or "").lower()
        if normalized != "naver":
            return
        if not mail_from:
            return
        if not isinstance(sent_at, datetime):
            return
        settings = self._imap_settings_for_domain(normalized)
        if not settings.get("enabled"):
            return
        username = normalize_imap_string(settings.get("username"))
        password = settings.get("password") or ""
        if not username or not password:
            return
        allowed_setting = sanitize_imap_allowed_latency(settings.get("allowed_latency_seconds"))
        allowed_delay_value = sanitize_imap_allowed_latency(
            allowed_delay if allowed_delay is not None else allowed_setting,
            default=allowed_setting,
        )
        single_delay_setting = sanitize_imap_delay(
            settings.get("single_delay_seconds"),
            default=IMAP_DEFAULT_SINGLE_DELAY_SECONDS,
            minimum=0,
        )
        batch_delay_setting = sanitize_imap_delay(
            settings.get("batch_delay_seconds"),
            default=IMAP_DEFAULT_BATCH_DELAY_SECONDS,
            minimum=0,
        )
        if delay_before_check is None:
            use_single = send_type == "single" and not has_anchor
            delay_seconds = float(single_delay_setting if use_single else batch_delay_setting)
        else:
            try:
                delay_seconds = float(delay_before_check)
            except (TypeError, ValueError):
                use_single = send_type == "single" and not has_anchor
                delay_seconds = float(single_delay_setting if use_single else batch_delay_setting)
            delay_seconds = max(0.0, delay_seconds)
        sent_at_value = sent_at
        failure_action_value = sanitize_imap_failure_action(settings.get("failure_action"))

        def task() -> None:
            self._log_imap_console(
                "자동 확인 시작 · "
                f"도메인 {normalized} · "
                f"발송시각 {sent_at_value.isoformat()} · "
                f"대기 {delay_seconds:.1f}s · "
                f"허용지연 {allowed_delay_value}s · "
                f"유형 {send_type}"
            )
            print(f"[디버그] IMAP check delay={delay_seconds}")
            def has_network() -> bool:
                try:
                    with socket.create_connection(("8.8.8.8", 53), timeout=1):
                        return True
                except OSError:
                    return False

            def wait_for_delay_and_network(max_delay: float) -> None:
                if max_delay > 0:
                    deadline = time.monotonic() + max_delay
                    while True:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            break
                        time.sleep(min(1.0, remaining))
                if has_network():
                    return
                self._log_imap_console("  ↳ 네트워크 연결 대기 중…")
                while not has_network():
                    time.sleep(1.0)

            wait_for_delay_and_network(delay_seconds)
            checked_at_iso = utc_now_iso()
            try:
                result = verify_delivery(
                    email_id=username,
                    password=password,
                    mail_from=mail_from,
                    sent_at=sent_at_value,
                    allowed_delay=allowed_delay_value,
                    max_messages=25,
                    check_delay=delay_seconds,
                )
                status = result.get("status", "error")
                latency = result.get("latency")
                received_at = result.get("received_at")
                reason_text = result.get("reason")
                latency_label = f"{latency:.1f}s" if isinstance(latency, (int, float)) else "-"
                self._log_imap_console(
                    "자동 확인 완료 · "
                    f"상태 {status} · "
                    f"지연 {latency_label} · "
                    f"허용 {allowed_delay_value}s"
                )
                if status != "success" and reason_text:
                    self._log_imap_console(f"  ↳ 사유: {reason_text}")
            except Exception as exc:  # pylint: disable=broad-except
                status = "error"
                latency = None
                received_at = None
                reason_text = f"IMAP 확인 실패: {exc}"
            if status not in {"success", "failure", "error"}:
                status = "error"
            trigger_stop = bool(
                failure_action_value in {"stop_device", "stop_all"}
                and status in {"failure", "error"}
            )
            report = {
                "domain": normalized,
                "status": status,
                "latency": latency,
                "received_at": received_at,
                "sent_at": sent_at_value.isoformat(),
                "reason": (reason_text or context_reason or ""),
                "job_id": job_id,
                "send_type": send_type,
                "mail_from": mail_from,
                "anchor": has_anchor,
                "trigger_stop": trigger_stop,
                "checked_at": checked_at_iso,
                "delay_seconds": int(delay_seconds),
                "allowed_latency_seconds": allowed_delay_value,
                "failure_action": failure_action_value,
            }
            self._queue_imap_report(report)

        future = self._imap_executor.submit(task)
        with self._imap_lock:
            self._imap_futures.add(future)
        future.add_done_callback(self._on_imap_future_done)
    def _maybe_flush_sent_sequences(self, force: bool = False) -> None:
        if not force and not self._sequence_dirty:
            return
        now_point = time.monotonic()
        if not force and now_point - self._last_sequence_flush < 10:
            return
        self.persist()
        self._last_sequence_flush = now_point

    def _next_sent_sequence(self, domain: Optional[str], increment: int = 1) -> int:
        normalized = (domain or self.active_domain or "naver").lower()
        try:
            step = int(increment)
        except (TypeError, ValueError):
            step = 1
        if step <= 0:
            step = 1
        current = max(0, int(self.sent_sequences.get(normalized, 0)))
        new_value = current + step
        self.sent_sequences[normalized] = new_value
        self._sequence_dirty.add(normalized)
        self._maybe_flush_sent_sequences()
        return new_value

    def _reset_sent_sequences(self, domains: Optional[Iterable[str]] = None) -> None:
        if domains is None:
            target_domains = set(self.sent_sequences.keys()) | set(DOMAINS)
        else:
            target_domains = {str(domain or "").lower() for domain in domains if domain}
        if not target_domains:
            target_domains = set(DOMAINS)
        for domain in target_domains:
            if not domain:
                continue
            self.sent_sequences[domain] = 0
            self._sequence_dirty.add(domain)
        self._maybe_flush_sent_sequences(force=True)

    @staticmethod
    def _sanitize_bcc_count(value: object) -> int:
        try:
            count = int(value or 0)
        except (TypeError, ValueError):
            return 0
        return max(0, min(30, count))

    @staticmethod
    def _sanitize_anchor_interval(value: object) -> int:
        try:
            interval = int(value or 0)
        except (TypeError, ValueError):
            return 0
        return max(0, min(1000, interval))

    @staticmethod
    def _sanitize_anchor_email(value: object) -> Optional[str]:
        if value is None:
            return None
        candidate = str(value).strip()
        if not candidate:
            return None
        if not EMAIL_PATTERN.fullmatch(candidate):
            return None
        return candidate.lower()

    @staticmethod
    def _sanitize_schedule_enabled(value: object) -> bool:
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"", "0", "false", "no", "off"}:
                return False
            if lowered in {"1", "true", "yes", "on"}:
                return True
        return bool(value)

    @staticmethod
    def _sanitize_schedule_time(value: object) -> Optional[str]:
        if value is None:
            return None
        candidate = str(value).strip()
        if not candidate:
            return None
        try:
            parsed = datetime.strptime(candidate, "%H:%M")
        except ValueError:
            return None
        return parsed.strftime("%H:%M")

    @staticmethod
    def _sanitize_schedule_date(value: object) -> Optional[str]:
        if value is None:
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
            except ValueError:
                return None
            return parsed_dt.date().isoformat()

    @staticmethod
    def _is_date_newer(candidate: Optional[str], reference: Optional[str]) -> bool:
        if not candidate:
            return False
        if not reference:
            return True
        try:
            candidate_dt = datetime.fromisoformat(candidate)
        except ValueError:
            try:
                candidate_dt = datetime.strptime(candidate, "%Y-%m-%d")
            except ValueError:
                return False
        try:
            reference_dt = datetime.fromisoformat(reference)
        except ValueError:
            try:
                reference_dt = datetime.strptime(reference, "%Y-%m-%d")
            except ValueError:
                return True
        return candidate_dt.date() > reference_dt.date()

    def _get_schedule_state(self, domain: str) -> Dict[str, object]:
        normalized = (domain or "").lower()
        state = self.stop_schedules.get(normalized)
        if state is None:
            state = {
                "enabled": False,
                "time": "",
                "last_run": None,
                "server_last_run": None,
                "needs_sync": False,
            }
            self.stop_schedules[normalized] = state
        return state

    def _local_now(self) -> datetime:
        return datetime.now().astimezone()

    def _schedule_due(self, domain: str) -> bool:
        state = self._get_schedule_state(domain)
        if not state.get("enabled") or not state.get("time"):
            return False
        try:
            schedule_time = datetime.strptime(state["time"], "%H:%M").time()
        except (TypeError, ValueError):
            return False
        now_point = self._local_now()
        scheduled_dt = datetime.combine(now_point.date(), schedule_time, tzinfo=now_point.tzinfo)
        if now_point < scheduled_dt:
            return False
        today_iso = now_point.date().isoformat()
        if state.get("last_run") == today_iso:
            return False
        if state.get("server_last_run") == today_iso:
            state["last_run"] = today_iso
            state["needs_sync"] = False
            return False
        state["last_run"] = today_iso
        state["needs_sync"] = True
        self._schedule_events[domain] = {
            "reason": "예약된 자동 정지 실행",
            "triggered_at": now_iso(),
        }
        label = DOMAIN_LABELS.get(domain, domain)
        print(f"[예약 중지] {label} · {state['time']} 스케줄 도달, 발송을 중단합니다.")
        self.persist()
        return True

    def _schedule_block_reason(self, domain: str) -> Optional[str]:
        state = self._get_schedule_state(domain)
        if not state.get("enabled"):
            return None
        today_iso = self._local_now().date().isoformat()
        if state.get("last_run") != today_iso:
            return None
        event = self._schedule_events.get(domain)
        if event and event.get("reason"):
            return str(event["reason"])
        return "예약된 자동 정지가 이미 실행되었습니다."

    def _schedule_guard_for_job(self, domain: Optional[str], job_id: str, job_type: str) -> Optional["JobResult"]:
        if job_type != "batch_send":
            return None
        if not domain:
            return None
        normalized = domain.lower()
        if normalized not in DOMAINS:
            return None
        self._schedule_due(normalized)
        block_reason = self._schedule_block_reason(normalized)
        if not block_reason:
            return None
        label = DOMAIN_LABELS.get(normalized, normalized)
        print(f"[예약 중지] {label} · {block_reason}")
        summary = f"{label} 예약 중지가 활성화되어 작업을 취소했습니다."
        return JobResult(
            job_id=job_id,
            status="cancelled",
            message=summary,
            result={"domain": normalized, "job_type": job_type, "reason": block_reason},
            error=block_reason,
        )

    def _evaluate_idle_schedules(self) -> None:
        for domain in DOMAINS:
            self._schedule_due(domain)

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
        pending = totals.get("pending", 0)
        reserved = totals.get("reserved", 0)
        block = totals.get("block", 0)
        remaining = pending + reserved + block
        cycle_info = self._cycle_snapshot(normalized)
        return {
            "domain": normalized,
            "local_db_version": self.local_versions.get(normalized),
            "total": total_count,
            "pending": totals.get("pending", 0),
            "reserved": reserved,
            "sent": totals.get("sent", 0),
            "failed": totals.get("failed", 0),
            "block": totals.get("block", 0),
            "removed": totals.get("removed", 0),
            "remaining": remaining,
            "cycle_completed": cycle_info.get("cycle_completed"),
            "cycle_count": cycle_info.get("cycle_count"),
            "last_cycle_completed_at": cycle_info.get("last_cycle_completed_at"),
            "last_cycle_processed": cycle_info.get("last_cycle_processed"),
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
        configs = data.get("configs")
        if isinstance(configs, dict):
            self.apply_configs(configs)
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
        imap_reports = self._collect_imap_reports()
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
        if imap_reports:
            payload["imap_reports"] = imap_reports
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
            data = self.heartbeat(domain_states or [], [report])
            configs = data.get("configs") if isinstance(data, dict) else None
            if isinstance(configs, dict) and configs:
                self.apply_configs(configs)
            new_jobs = data.get("jobs") if isinstance(data, dict) else None
            if isinstance(new_jobs, list):
                self._queue_jobs(new_jobs)
            self._run_priority_jobs()
        except Exception as exc:  # pylint: disable=broad-except
            print(f"[경고] 작업 보고 실패: {exc}")

    def _update_job_controls(self, controls: Iterable[Dict[str, object]]) -> None:
        previous_ids = set(self.job_controls.keys())
        snapshot: Dict[str, Dict[str, object]] = {}
        for control in controls:
            if not isinstance(control, dict):
                continue
            job_id = str(control.get("job_id") or "").strip()
            if not job_id:
                continue
            snapshot[job_id] = control
        self.job_controls = snapshot
        removed_ids = previous_ids - set(snapshot.keys())
        for job_id in removed_ids:
            self._cancel_ack_sent.discard(job_id)

    def _acknowledge_cancelled_jobs(self) -> None:
        if not self.job_controls:
            return
        pending: List[str] = []
        for job_id, control in self.job_controls.items():
            if not isinstance(control, dict):
                continue
            if not control.get("cancel_requested"):
                continue
            if job_id in self._active_job_ids:
                continue
            if job_id in self._cancel_ack_sent:
                continue
            pending.append(job_id)
        if not pending:
            return
        message = "사용자 요청으로 발송을 중단했습니다. (클라이언트 재접속)"
        for job_id in pending:
            self._cancel_ack_sent.add(job_id)
            try:
                print(f"[작업 정리] 중지 요청된 작업 {job_id}을(를) 취소 상태로 보고합니다.")
                self.send_job_report(
                    JobResult(
                        job_id=job_id,
                        status="cancelled",
                        message=message,
                        result=None,
                        error="사용자 요청으로 발송을 중단했습니다.",
                    )
                )
            except Exception as exc:  # pylint: disable=broad-except
                print(f"[경고] 작업 취소 보고 실패: {exc}")
                self._cancel_ack_sent.discard(job_id)

    def _queue_jobs(self, jobs: Iterable[Dict[str, object]]) -> None:
        if not jobs:
            return
        for job in jobs:
            if not isinstance(job, dict):
                continue
            job_id = str(job.get("job_id") or "").strip()
            if not job_id or job_id in self._active_job_ids:
                continue
            if job_id in self._pending_job_ids:
                continue
            self._pending_jobs.append(job)
            self._pending_job_ids.add(job_id)
        if self._active_jobs and any(active_type == "batch_send" for active_type in self._active_jobs.values()):
            self._run_priority_jobs()

    def _run_priority_jobs(self) -> None:
        if self._priority_running:
            return
        if not self._pending_jobs:
            return
        if not any(job_type == "batch_send" for job_type in self._active_jobs.values()):
            return
        singles: List[Dict[str, object]] = []
        remaining: Deque[Dict[str, object]] = deque()
        remaining_ids: Set[str] = set()
        while self._pending_jobs:
            job = self._pending_jobs.popleft()
            if not isinstance(job, dict):
                continue
            job_id = str(job.get("job_id") or "").strip()
            if not job_id:
                continue
            job_type = str(job.get("job_type") or "")
            if job_type == "single_send" and job_id not in self._active_job_ids:
                singles.append(job)
                self._pending_job_ids.discard(job_id)
                continue
            remaining.append(job)
            remaining_ids.add(job_id)
        self._pending_jobs = remaining
        self._pending_job_ids = remaining_ids
        if not singles:
            return
        self._priority_running = True
        try:
            for job in singles:
                self._execute_priority_job(job)
        finally:
            self._priority_running = False

    def _execute_priority_job(self, job: Dict[str, object]) -> None:
        job_id = str(job.get("job_id") or "").strip()
        if not job_id or job_id in self._active_job_ids:
            return
        job_type = str(job.get("job_type") or "")
        guard_result: Optional[JobResult] = None
        if job_type in {"single_send", "batch_send"}:
            guard_result = self._schedule_guard_for_job(job.get("domain"), job_id, job_type)
        if guard_result:
            domain_states = self.collect_domain_states()
            self.send_job_report(guard_result, domain_states)
            self.job_controls.pop(job_id, None)
            self._cancel_ack_sent.discard(job_id)
            return
        self._active_job_ids.add(job_id)
        self._active_jobs[job_id] = job_type
        self._cancel_ack_sent.discard(job_id)
        self.send_job_report(JobResult(job_id=job_id, status="running", message="작업 시작"))
        try:
            result = self.process_job(job)
        finally:
            self._active_job_ids.discard(job_id)
            self._active_jobs.pop(job_id, None)
        domain_states = self.collect_domain_states()
        self.send_job_report(result, domain_states)
        self.job_controls.pop(job_id, None)
        self._cancel_ack_sent.discard(job_id)

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

        def attach_schedule(snapshot: Dict[str, object], domain_key: str) -> Dict[str, object]:
            schedule_state = self.stop_schedules.get(domain_key)
            if not schedule_state:
                return snapshot
            schedule_last_run = schedule_state.get("last_run")
            if schedule_last_run:
                snapshot["stop_schedule_last_run"] = schedule_last_run
                if schedule_state.get("needs_sync"):
                    schedule_state["needs_sync"] = False
                if self._is_date_newer(schedule_last_run, schedule_state.get("server_last_run")):
                    schedule_state["server_last_run"] = schedule_last_run
            return snapshot

        for domain, db_path in self.domain_paths.items():
            totals: Dict[str, int] = {status: 0 for status in EMAIL_STATUSES}
            if not db_path.exists():
                snapshot = self._build_domain_state_snapshot(domain, totals)
                states.append(attach_schedule(snapshot, domain))
                continue
            try:
                with sqlite3.connect(db_path) as conn:
                    conn.row_factory = sqlite3.Row
                    for row in conn.execute(
                        "SELECT status, COUNT(*) AS cnt FROM emails GROUP BY status"
                    ):
                        totals[row["status"]] = row["cnt"]
                    snapshot = self._build_domain_state_snapshot(domain, totals)
                    states.append(attach_schedule(snapshot, domain))
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
                snapshot = self._build_domain_state_snapshot(domain, totals)
                states.append(attach_schedule(snapshot, domain))
        return states

    # ------------------------------------------------------------------ #
    # 작업 처리
    # ------------------------------------------------------------------ #
    def process_job(self, job: Dict[str, object]) -> JobResult:
        job_type = job.get("job_type")
        domain = job.get("domain")
        payload = job.get("payload") or {}
        job_id = str(job.get("job_id"))
        print(f"[작업 수신] {job_type} (ID: {job_id})", flush=True)
        if job_type == "inject_file":
            return self.handle_inject(domain, payload, job_id)
        if job_type == "purge_domain":
            return self.handle_purge_domain(domain, payload, job_id)
        if job_type == "single_send":
            return self.handle_single_send(domain, payload, job_id)
        if job_type == "batch_send":
            return self.handle_batch_send(domain, payload, job_id)
        if job_type == "imap_test":
            return self.handle_imap_test(domain, payload, job_id)
        if job_type == "imap_fetch_latest":
            return self.handle_imap_fetch_latest(domain, payload, job_id)
        if job_type == "change_ip":
            return self.handle_change_ip(job_id)
        if job_type == "reset_sent_sequence":
            return self.handle_reset_sent_sequence(domain, payload, job_id)
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
        self._reset_cycle_stats(normalized)
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

    def handle_purge_domain(self, domain: Optional[str], payload: Dict[str, object], job_id: str) -> JobResult:
        if not domain:
            return JobResult(job_id=job_id, status="failed", message="도메인 정보가 없습니다.")
        normalized = domain.lower()
        domain_label = DOMAIN_LABELS.get(normalized, normalized)
        db_path = self.domain_paths.get(normalized)
        if not db_path or not db_path.exists():
            self.local_versions[normalized] = None
            self._reset_cycle_stats(normalized)
            cycle_snapshot = self._cycle_snapshot(normalized)
            self.persist()
            message = f"{domain_label} DB가 존재하지 않아 초기화할 항목이 없습니다."
            return JobResult(
                job_id=job_id,
                status="success",
                message=message,
                result={
                    "domain": normalized,
                    "cleared_emails": 0,
                    "cleared_meta": 0,
                    "remaining": 0,
                    "cycles_completed": cycle_snapshot.get("cycle_count"),
                },
            )
        try:
            with sqlite3.connect(db_path) as conn:
                conn.row_factory = sqlite3.Row
                email_row = conn.execute("SELECT COUNT(*) AS cnt FROM emails").fetchone()
                meta_row = conn.execute("SELECT COUNT(*) AS cnt FROM injection_meta").fetchone()
                email_total = int(email_row["cnt"] if email_row else 0)
                meta_total = int(meta_row["cnt"] if meta_row else 0)
                conn.execute("DELETE FROM emails")
                conn.execute("DELETE FROM injection_meta")
                try:
                    conn.execute("DELETE FROM sqlite_sequence WHERE name IN ('emails','injection_meta')")
                except sqlite3.Error:
                    pass
                conn.commit()
            try:
                with sqlite3.connect(db_path) as vacuum_conn:
                    vacuum_conn.execute("VACUUM")
            except sqlite3.Error:
                pass
            self.local_versions[normalized] = None
            self._reset_cycle_stats(normalized)
            cycle_snapshot = self._cycle_snapshot(normalized)
            self.persist()
            message = (
                f"{domain_label} DB 전체 삭제 완료 (이메일 {email_total}건, 메타 {meta_total}건 제거)"
            )
            return JobResult(
                job_id=job_id,
                status="success",
                message=message,
                result={
                    "domain": normalized,
                    "cleared_emails": email_total,
                    "cleared_meta": meta_total,
                    "remaining": 0,
                    "cycles_completed": cycle_snapshot.get("cycle_count"),
                },
            )
        except sqlite3.Error as exc:
            return JobResult(
                job_id=job_id,
                status="failed",
                message=f"{domain_label} DB 삭제 실패: {exc}",
                error=str(exc),
            )

    def handle_single_send(self, domain: Optional[str], payload: Dict[str, object], job_id: str) -> JobResult:
        config = payload.get("config") or {}
        rcpt_to = payload.get("rcpt_to")
        if not rcpt_to:
            return JobResult(job_id=job_id, status="failed", message="RCPT TO 정보가 없습니다.")
        normalized_domain = (domain or "").lower()
        bcc_rows: List[sqlite3.Row] = []
        bcc_emails: List[str] = []
        success, response_text = send_via_telnet(
            smtp_host=config.get("smtp_host", ""),
            smtp_port=int(config.get("smtp_port") or 25),
            helo=config.get("helo", ""),
            mail_from=config.get("mail_from", ""),
            rcpt_to=rcpt_to,
            header_text=config.get("header", ""),
            bcc_emails=bcc_emails,
        )
        sent_at = utc_now()
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
            total_increment = 1 + bcc_total
            sequence_value = self._next_sent_sequence(normalized_domain or None, total_increment)
            meta_items: List[Tuple[str, object]] = [("primary", 1)]
            if bcc_total > 0:
                meta_items.append(("bcc", bcc_total))
            log_line = self._format_dispatch_log_line("Sent", sequence_value, rcpt_to, meta_items)
            display_line = self._format_dispatch_display_line(
                "Sent",
                rcpt_to,
                bcc_total,
                0,
                True,
                self.device_name,
                sequence=sequence_value,
            )
            dispatch_logs.append(
                {
                    "log": log_line,
                    "display": display_line,
                    "email": rcpt_to,
                    "sequence": sequence_value,
                    "delivery_status": "sent",
                    "detail": detail_line or status_line,
                    "bcc_total": bcc_total,
                    "anchor_total": 0,
                    "is_primary": True,
                }
            )
            print(display_line)
        else:
            label = "Block" if delivery_status == "block" else "Fail"
            meta_items: List[Tuple[str, object]] = [("primary", 1)]
            if bcc_total > 0:
                meta_items.append(("bcc", bcc_total))
            sequence_domain = (normalized_domain or (self.active_domain or "naver")).lower()
            base_sequence = max(0, int(self.sent_sequences.get(sequence_domain, 0)))
            sequence_for_log = base_sequence + 1
            log_line = self._format_dispatch_log_line(label, sequence_for_log, rcpt_to, meta_items)
            display_line = self._format_dispatch_display_line(
                label,
                rcpt_to,
                bcc_total,
                0,
                True,
                self.device_name,
            )
            dispatch_logs.append(
                {
                    "log": log_line,
                    "display": display_line,
                    "email": rcpt_to,
                    "sequence": sequence_for_log,
                    "delivery_status": delivery_status,
                    "detail": detail_line or status_line,
                    "bcc_total": bcc_total,
                    "anchor_total": 0,
                    "is_primary": True,
                    "bcc_recipients": list(bcc_emails),
                }
            )
        if delivery_status != "sent":
            for entry in dispatch_logs:
                print(entry.get("display") or entry["log"])
        if bcc_emails:
            print(f"  ↳ BCC 대상 {len(bcc_emails)}건 포함")
        if not success and detail_line and detail_line != status_line:
            print(f"  ↳ {detail_line}")
        if delivery_status == "sent" and normalized_domain:
            mail_from_value = config.get("mail_from", "")
            settings = self._imap_settings_for_domain(normalized_domain)
            allowed_latency = sanitize_imap_allowed_latency(settings.get("allowed_latency_seconds"))
            single_delay = sanitize_imap_delay(
                settings.get("single_delay_seconds"),
                default=IMAP_DEFAULT_SINGLE_DELAY_SECONDS,
                minimum=0,
            )
            # 단일 발송 확인은 설정한 대기 시간(single_delay) 뒤에 시작하고, 허용 지연은 최대 판정 시간을 의미합니다.
            self._submit_imap_check(
                domain=normalized_domain,
                job_id=job_id,
                send_type="single",
                mail_from=mail_from_value,
                sent_at=sent_at,
                has_anchor=False,
                delay_before_check=single_delay,
                allowed_delay=allowed_latency,
                context_reason=detail_line or status_line,
            )
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
        job_result = JobResult(
            job_id=job_id,
            status=status,
            message=primary_log,
            result=result_payload,
            error=error_message,
        )
        if delivery_status == "sent":
            self._maybe_flush_sent_sequences(force=True)
        return job_result

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
        anchor_interval = self._sanitize_anchor_interval(config.get("anchor_interval"))
        anchor_email = self._sanitize_anchor_email(config.get("anchor_email"))
        anchor_enabled = bool(anchor_interval and anchor_email)

        processed = 0
        sent_count = 0
        block_count = 0
        failed_count = 0
        last_error: Optional[str] = None
        bcc_processed = 0
        anchor_processed = 0
        anchor_retry_pending = 0
        dispatched_db_total = 0
        progress_interval = min(3.0, max(1.0, float(self.interval or 3)))
        last_report_at = time.monotonic()
        poll_interval = max(1.0, float(self.interval or 3))
        last_poll_at = time.monotonic()
        domain_totals: Dict[str, int] = {status: 0 for status in EMAIL_STATUSES}

        def build_summary() -> Dict[str, object]:
            remaining_count = (
                domain_totals.get("pending", 0)
                + domain_totals.get("reserved", 0)
                + domain_totals.get("block", 0)
            )
            total_candidates = sum(domain_totals.values())
            cycle_snapshot = self._cycle_snapshot(normalized)
            absolute_sent = max(0, int(self.sent_sequences.get(normalized, 0)))
            return {
                "processed": processed,
                "sent": sent_count,
                "sent_sequence": absolute_sent,
                "sent_absolute": absolute_sent,
                "failed": failed_count,
                "block": block_count,
                "bcc": bcc_processed,
                "anchor": anchor_processed,
                "remaining": remaining_count,
                "total": total_candidates,
                "reserved": domain_totals.get("reserved", 0),
                "cycles_completed": cycle_snapshot.get("cycle_count"),
                "last_cycle_at": cycle_snapshot.get("last_cycle_completed_at"),
            }

        def format_summary(prefix: str, snapshot: Optional[Dict[str, object]] = None) -> str:
            summary_snapshot = snapshot or build_summary()
            processed_count = int(summary_snapshot.get("processed") or 0)
            sent_total = int(summary_snapshot.get("sent") or 0)
            absolute_sent = int(
                summary_snapshot.get("sent_sequence")
                or summary_snapshot.get("sent_absolute")
                or summary_snapshot.get("sent_total")
                or 0
            )
            failed_total = int(summary_snapshot.get("failed") or 0)
            block_total = int(summary_snapshot.get("block") or 0)
            bcc_total = int(summary_snapshot.get("bcc") or 0)
            anchor_total = int(summary_snapshot.get("anchor") or 0)
            remaining_total = int(summary_snapshot.get("remaining") or 0)
            cycles_completed = int(summary_snapshot.get("cycles_completed") or 0)
            message = (
                f"{prefix} 처리={processed_count} 성공={sent_total} "
                f"실패={failed_total} 차단={block_total} BCC={bcc_total} "
                f"알박기={anchor_total} 잔여={remaining_total}"
            )
            if absolute_sent:
                message = f"{message} · 누적={absolute_sent}"
            if cycles_completed > 0:
                message = f"{message} · {cycles_completed}회 순환 완료"
            return message

        def emit_progress(force: bool = False) -> None:
            nonlocal last_report_at
            now_point = time.monotonic()
            if not force and now_point - last_report_at < progress_interval:
                return
            summary_snapshot = build_summary()
            progress_message = format_summary(f"[진행] {domain_label}", summary_snapshot)
            domain_state = self._build_domain_state_snapshot(normalized, domain_totals.copy())
            self.send_job_report(
                JobResult(job_id=job_id, status="running", message=progress_message, result=summary_snapshot),
                [domain_state],
            )
            self._maybe_flush_sent_sequences()
            last_report_at = now_point

        def maybe_poll_updates(force: bool = False) -> None:
            nonlocal last_poll_at
            now_point = time.monotonic()
            if not force and now_point - last_poll_at < poll_interval:
                return
            try:
                response = self.heartbeat([], [])
            except requests.RequestException as exc:
                print(f"[진행] 상태 동기화 실패: {exc}")
                last_poll_at = now_point
                return
            except Exception as exc:  # pylint: disable=broad-except
                print(f"[진행] 상태 동기화 중 예외: {exc}")
                last_poll_at = now_point
                return
            last_poll_at = now_point
            configs = response.get("configs") if isinstance(response, dict) else None
            if isinstance(configs, dict) and configs:
                self.apply_configs(configs)
            new_jobs = response.get("jobs") if isinstance(response, dict) else None
            if isinstance(new_jobs, list):
                self._queue_jobs(new_jobs)
            controls = response.get("job_controls") if isinstance(response, dict) else None
            if isinstance(controls, list):
                self._update_job_controls(controls)
                self._acknowledge_cancelled_jobs()

        stop_requested = False
        cancel_requested = False
        stop_reason: Optional[str] = None
        fatal_error: Optional[str] = None

        def check_schedule_trigger() -> None:
            nonlocal stop_requested, cancel_requested, stop_reason
            if stop_requested:
                return
            if self._schedule_due(normalized):
                stop_requested = True
                cancel_requested = True
                stop_reason = "예약된 자동 정지 실행"

        check_schedule_trigger()

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

                def recycle_consumed_rows() -> bool:
                    summary_row = conn.execute(
                        """
                        SELECT
                            SUM(CASE WHEN status='pending' THEN 1 ELSE 0 END) AS pending_count,
                            SUM(CASE WHEN status='reserved' THEN 1 ELSE 0 END) AS reserved_count,
                            SUM(CASE WHEN status='block' THEN 1 ELSE 0 END) AS block_count,
                            SUM(CASE WHEN status='sent' THEN 1 ELSE 0 END) AS sent_count,
                            SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed_count
                        FROM emails
                        """
                    ).fetchone()
                    if not summary_row:
                        return False
                    pending_candidates = (
                        (summary_row["pending_count"] or 0)
                        + (summary_row["reserved_count"] or 0)
                        + (summary_row["block_count"] or 0)
                    )
                    if pending_candidates > 0:
                        return False
                    sent_total = summary_row["sent_count"] or 0
                    failed_total = summary_row["failed_count"] or 0
                    reset_total = sent_total + failed_total
                    if reset_total == 0:
                        return False
                    now_stamp = now_iso()
                    conn.execute(
                        """
                        UPDATE emails
                        SET status='pending',
                            reserved_by=NULL,
                            reserved_at=NULL,
                            updated_at=?
                        WHERE status IN ('sent','failed')
                        """,
                        (now_stamp,),
                    )
                    conn.commit()
                    domain_totals["pending"] = domain_totals.get("pending", 0) + reset_total
                    domain_totals["sent"] = max(0, domain_totals.get("sent", 0) - sent_total)
                    domain_totals["failed"] = max(0, domain_totals.get("failed", 0) - failed_total)
                    domain_totals["reserved"] = 0
                    cycle_entry = self._record_cycle_completion(normalized, reset_total)
                    cycle_count = int(cycle_entry.get("cycles", 0) or 0)
                    cycle_label = "1회 순환 완료" if cycle_count == 1 else f"{cycle_count}회 순환 완료"
                    restart_message = (
                        f"{domain_label} {cycle_label} · sent {sent_total}건, failed {failed_total}건을 pending으로 전환"
                    )
                    print(f"[배치 발송] {restart_message}")
                    self.persist()
                    try:
                        self.send_job_report(
                            JobResult(
                                job_id=job_id,
                                status="running",
                                message=restart_message,
                                result={
                                    "logs": [
                                        {"log": restart_message, "delivery_status": "info"},
                                    ],
                                    "summary": build_summary(),
                                },
                            )
                        )
                    except Exception:
                        pass
                    emit_progress(force=True)
                    return True

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
                        if recycle_consumed_rows():
                            return reserve_candidates(limit)
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
                    for row in rows:
                        status_key = (row["status"] or "pending").lower()
                        domain_totals[status_key] = max(0, domain_totals.get(status_key, 0) - 1)
                    domain_totals["reserved"] = domain_totals.get("reserved", 0) + len(rows)
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
                    for record in release_list:
                        domain_totals["reserved"] = max(0, domain_totals.get("reserved", 0) - 1)
                        previous_status = (record.previous_status or "pending").lower()
                        domain_totals[previous_status] = domain_totals.get(previous_status, 0) + 1

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
                    injected_emails = [email for email in (group.injected or []) if email]
                    if injected_emails:
                        bcc_emails.extend(injected_emails)
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
                    sent_at = utc_now()
                    delivery_status = self._classify_delivery(success, response_text)
                    status_line, detail_line = self._smtp_status_and_detail(response_text)
                    return DispatchOutcome(
                        success=success,
                        response_text=response_text,
                        delivery_status=delivery_status,
                        status_line=status_line,
                        detail_line=detail_line,
                        sent_at=sent_at,
                    )

                def prepare_group_for_dispatch(group: DispatchGroup) -> DispatchGroup:
                    nonlocal dispatched_db_total, anchor_retry_pending
                    if not group:
                        return group
                    group.injected = []
                    group_db_size = 1 + len(group.bcc)
                    start_total = dispatched_db_total
                    end_total = start_total + group_db_size
                    if not anchor_enabled:
                        anchor_retry_pending = 0
                        dispatched_db_total = end_total
                        return group
                    anchors_needed = 0
                    if anchor_interval > 0:
                        anchors_needed = (end_total // anchor_interval) - (start_total // anchor_interval)
                    if anchor_retry_pending > 0:
                        anchors_needed = max(anchors_needed, anchor_retry_pending)
                    if anchors_needed > 0 and anchor_email:
                        group.injected = [anchor_email] * anchors_needed
                        anchor_retry_pending = max(0, anchor_retry_pending - len(group.injected))
                    dispatched_db_total = end_total
                    return group

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
                    nonlocal stop_requested, fatal_error, stop_reason, bcc_processed, anchor_processed
                    nonlocal anchor_retry_pending
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
                    lower_response = (outcome.response_text or "").lower()
                    status_lower = (outcome.status_line or "").lower()
                    detail_lower = (detail_for_log or "").lower()
                    throttle_marker_messages = {
                        "550 5.7.2": "네이버 '550 5.7.2' 응답 감지",  # Your email has been blocked because the sender is unauthenticated
                        "452 4.7.1 sent too many messages": "네이버 '452 4.7.1 Sent too many messages' 응답 감지",
                        "452 4.7.1 sent too many message": "네이버 '452 4.7.1 Sent too many messages' 응답 감지",
                        "421 4.3.2 your ip blocked from this server": "네이버 '421 4.3.2 Your IP blocked from this server' 응답 감지",
                    }
                    fatal_stop_marker_messages = {
                        "421 4.7.1 this email has been temporarily blocked": "네이버 '421 4.7.1 This email has been temporarily blocked' 응답 감지",
                    }
                    matched_marker: Optional[str] = None
                    matched_message: Optional[str] = None
                    for text in (lower_response, status_lower, detail_lower):
                        if not text:
                            continue
                        for marker, message in throttle_marker_messages.items():
                            if marker in text:
                                matched_marker = marker
                                matched_message = message
                                break
                        if matched_marker:
                            break
                    if matched_marker is None:
                        for text in (lower_response, status_lower, detail_lower):
                            if not text:
                                continue
                            for marker, message in fatal_stop_marker_messages.items():
                                if marker in text:
                                    matched_marker = marker
                                    matched_message = message
                                    break
                            if matched_marker:
                                break
                    throttle_detected = matched_marker in throttle_marker_messages if matched_marker else False
                    error_text = None if outcome.delivery_status == "sent" else (detail_for_log or outcome.status_line or "")[-500:]
                    for prev_status in previous_statuses:
                        status_key = (prev_status or "pending").lower()
                        domain_totals[status_key] = max(0, domain_totals.get(status_key, 0) - 1)
                    for record in recipients:
                        persist_status = outcome.delivery_status
                        if persist_status == "block" or throttle_detected:
                            persist_status = "pending"
                        status_key = (persist_status or "pending").lower()
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
                        domain_totals[status_key] = domain_totals.get(status_key, 0) + 1
                    conn.commit()
                    group_size_actual = len(recipients)
                    processed += group_size_actual
                    dispatch_logs: List[Dict[str, object]] = []
                    recipient_emails = [record.email for record in recipients]
                    bcc_count = len(group.bcc)
                    anchor_count = len(group.injected)
                    if outcome.delivery_status == "sent":
                        sent_count += group_size_actual
                        primary_email = recipient_emails[0] if recipient_emails else "-"
                        sequence = self._next_sent_sequence(normalized, group_size_actual)
                        meta_items: List[Tuple[str, object]] = [("primary", 1)]
                        if bcc_count > 0:
                            meta_items.append(("bcc", bcc_count))
                        if anchor_count > 0:
                            meta_items.append(("anchor", anchor_count))
                        log_line = self._format_dispatch_log_line("Sent", sequence, primary_email, meta_items)
                        display_line = self._format_dispatch_display_line(
                            "Sent",
                            primary_email,
                            bcc_count,
                            anchor_count,
                            True,
                            self.device_name,
                            sequence=sequence,
                        )
                        dispatch_logs.append(
                            {
                                "log": log_line,
                                "display": display_line,
                                "email": primary_email,
                                "sequence": sequence,
                                "delivery_status": outcome.delivery_status,
                                "detail": detail_for_log,
                                "bcc_total": bcc_count,
                                "anchor_total": anchor_count,
                                "is_primary": True,
                                "bcc_recipients": [record.email for record in group.bcc],
                                "anchor": list(group.injected),
                            }
                        )
                        print(display_line)
                        if group.injected and normalized == "naver":
                            mail_from_value = config.get("mail_from", "")
                            settings = self._imap_settings_for_domain(normalized)
                            allowed_latency = sanitize_imap_allowed_latency(settings.get("allowed_latency_seconds"))
                            batch_delay = sanitize_imap_delay(
                                settings.get("batch_delay_seconds"),
                                default=IMAP_DEFAULT_BATCH_DELAY_SECONDS,
                                minimum=0,
                            )
                            self._submit_imap_check(
                                domain=normalized,
                                job_id=job_id,
                                send_type="batch-anchor",
                                mail_from=mail_from_value,
                                sent_at=outcome.sent_at,
                                has_anchor=True,
                                delay_before_check=batch_delay,
                                allowed_delay=allowed_latency,
                                context_reason=detail_for_log or outcome.status_line,
                            )
                    else:
                        is_block = outcome.delivery_status == "block"
                        label = "Block" if is_block else "Fail"
                        primary_email = recipient_emails[0] if recipient_emails else "-"
                        current_sequence = max(0, int(self.sent_sequences.get(normalized, 0)))
                        sequence_for_log = current_sequence + 1
                        meta_items: List[Tuple[str, object]] = [("primary", 1)]
                        if bcc_count > 0:
                            meta_items.append(("bcc", bcc_count))
                        if anchor_count > 0:
                            meta_items.append(("anchor", anchor_count))
                        log_line = self._format_dispatch_log_line(label, sequence_for_log, primary_email, meta_items)
                        display_line = self._format_dispatch_display_line(
                            label,
                            primary_email,
                            bcc_count,
                            anchor_count,
                            True,
                            self.device_name,
                        )
                        dispatch_logs.append(
                            {
                                "log": log_line,
                                "display": display_line,
                                "email": primary_email,
                                "sequence": sequence_for_log,
                                "delivery_status": "throttle" if throttle_detected else outcome.delivery_status,
                                "detail": detail_for_log,
                                "bcc_total": bcc_count,
                                "anchor_total": anchor_count,
                                "is_primary": True,
                                "bcc_recipients": [record.email for record in group.bcc],
                                "anchor": list(group.injected),
                                "failed_recipients": recipient_emails,
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
                    if group.injected:
                        if outcome.delivery_status == "sent":
                            anchor_processed += len(group.injected)
                        else:
                            anchor_retry_pending += len(group.injected)
                        anchor_display = f"  ↳ 알박기 대상 {len(group.injected)}건 포함"
                        print(anchor_display)
                        anchor_log_line = self._format_dispatch_log_line(
                            "Anchor",
                            processed,
                            group.injected[0] if group.injected else None,
                            [("count", len(group.injected))],
                        )
                        dispatch_logs.append(
                            {
                                "log": anchor_log_line,
                                "display": anchor_display,
                                "email": group.injected[0] if group.injected else None,
                                "sequence": processed,
                                "delivery_status": outcome.delivery_status,
                                "detail": detail_for_log,
                                "bcc_total": 0,
                                "anchor_total": len(group.injected),
                                "is_primary": False,
                                "anchor": list(group.injected),
                            }
                        )
                    if throttle_detected:
                        throttle_notice = matched_message or "네이버 발송 제한 응답 감지"
                        info_message = f"{throttle_notice} · IP 변경 시도"
                        print(f"[배치 발송] {domain_label} · {info_message}")
                        try:
                            self.send_job_report(
                                JobResult(
                                    job_id=job_id,
                                    status="running",
                                    message=info_message,
                                    result={
                                        "logs": [
                                            {"log": info_message, "delivery_status": "warning"},
                                        ]
                                    },
                                )
                            )
                        except Exception:
                            pass
                        print("[IP 변경] 비행기 모드 토글을 시작합니다.")
                        ip_success, ip_message, new_ip = self._perform_ip_change()
                        log_status = "sent" if ip_success else "error"
                        report_payload: Dict[str, object] = {
                            "logs": [
                                {"log": ip_message, "delivery_status": log_status},
                            ]
                        }
                        if ip_success and new_ip:
                            report_payload["public_ip"] = new_ip
                        try:
                            self.send_job_report(
                                JobResult(
                                    job_id=job_id,
                                    status="running" if ip_success else "failed",
                                    message=ip_message,
                                    result=report_payload,
                                    error=None if ip_success else ip_message,
                                )
                            )
                        except Exception:
                            pass
                        print(f"[배치 발송] {domain_label} · {ip_message}")
                        if not ip_success:
                            fatal_error = f"{throttle_notice} · {ip_message}"
                            stop_reason = fatal_error
                            stop_requested = True
                        else:
                            emit_progress(force=True)
                    elif matched_message:
                        fatal_error = matched_message
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
                            check_schedule_trigger()
                            if not stop_requested:
                                while len(inflight) < session_count:
                                    group = next_group()
                                    if not group:
                                        break
                                    group = prepare_group_for_dispatch(group)
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
                                    check_schedule_trigger()
                                    maybe_poll_updates()
                                    continue
                                for future in done:
                                    group = inflight.pop(future)
                                    process_future(future, group)
                                if fatal_error and not inflight:
                                    break
                                check_schedule_trigger()
                                maybe_poll_updates()
                                continue
                            check_schedule_trigger()
                            maybe_poll_updates()
                            if stop_requested:
                                break
                            group = next_group()
                            if not group:
                                break
                            group = prepare_group_for_dispatch(group)
                            future = executor.submit(deliver_group, group)
                            inflight[future] = group
                        while inflight:
                            done, _ = wait(inflight.keys(), timeout=None, return_when=FIRST_COMPLETED)
                            for future in done:
                                group = inflight.pop(future)
                                process_future(future, group)
                            check_schedule_trigger()
                            maybe_poll_updates()
                finally:
                    if pending_queue:
                        release_reserved_rows(list(pending_queue))
                        pending_queue.clear()
        except sqlite3.Error as exc:
            return JobResult(job_id=job_id, status="failed", message=f"DB 오류: {exc}")

        emit_progress(force=True)
        summary = build_summary()
        if cancel_requested:
            headline = stop_reason or "사용자 요청으로 배치 발송 중단"
            final_message = format_summary(headline, summary)
            error_text = stop_reason or "사용자 요청으로 발송을 중단했습니다."
            print(f"[배치 발송] {domain_label} · {final_message}")
            self._maybe_flush_sent_sequences(force=True)
            return JobResult(
                job_id=job_id,
                status="cancelled",
                message=final_message,
                result=summary,
                error=error_text,
            )
        success = failed_count == 0 and fatal_error is None
        base_message = "배치 발송 완료" if success else "배치 발송 중 오류"
        final_message = format_summary(base_message, summary)
        error_message = None
        if fatal_error:
            error_message = fatal_error
            final_message = f"{final_message} · {fatal_error}"
        elif not success and last_error:
            error_message = last_error
            final_message = f"{final_message} · 마지막 오류: {last_error}"
        print(f"[배치 발송] {domain_label} · {final_message}")
        self._maybe_flush_sent_sequences(force=True)
        return JobResult(
            job_id=job_id,
            status="success" if success else "failed",
            message=final_message,
            result=summary,
            error=error_message,
        )

    def _log_imap_console(self, message: str) -> None:
        print(f"[IMAP 테스트] {message}", flush=True)

    def handle_imap_test(self, domain: Optional[str], payload: Dict[str, object], job_id: str) -> JobResult:
        normalized = (domain or "naver").lower()
        if normalized != "naver":
            message = "네이버 도메인에서만 IMAP 테스트를 지원합니다."
            self._log_imap_console(message)
            return JobResult(job_id=job_id, status="failed", message=message, error=message)
        payload = payload or {}
        username = normalize_imap_string(payload.get("username"))
        settings = self._imap_settings_for_domain(normalized)
        if not username:
            username = normalize_imap_string(settings.get("username"))
        if not username:
            message = "IMAP 계정 ID가 설정되지 않았습니다."
            self._log_imap_console(message)
            return JobResult(job_id=job_id, status="failed", message=message, error=message)
        use_saved_password = bool(payload.get("use_saved_password"))
        password = str(payload.get("password") or "")
        used_saved_password = False
        if not password and use_saved_password:
            password = settings.get("password") or ""
            used_saved_password = bool(password)
        if not password:
            message = "IMAP 비밀번호를 확인할 수 없습니다."
            self._log_imap_console(message)
            return JobResult(job_id=job_id, status="failed", message=message, error=message)
        folder = str(payload.get("folder") or "Junk").strip() or "Junk"
        self._log_imap_console(f"계정 {username} · 폴더 {folder} · 연결 시도")
        diagnostics = probe_imap_connection(username, password, folder=folder)
        success = bool(diagnostics.get("success"))
        latency = diagnostics.get("latency")
        checked_at = diagnostics.get("checked_at")
        reason = diagnostics.get("reason")
        result_payload: Dict[str, object] = {
            "username": username,
            "folder": folder,
            "success": success,
            "latency": latency,
            "checked_at": checked_at,
            "used_saved_password": used_saved_password,
        }
        if reason:
            result_payload["reason"] = reason
        if success:
            latency_text = f"{latency:.1f}s" if isinstance(latency, (int, float)) else "-"
            message = f"IMAP 연결 성공 · {folder} 접근 · 지연 {latency_text}"
            self._log_imap_console(f"성공 - {message}")
            return JobResult(job_id=job_id, status="success", message=message, result=result_payload)
        error_message = reason or "IMAP 연결 실패"
        self._log_imap_console(f"실패 - {error_message}")
        return JobResult(
            job_id=job_id,
            status="failed",
            message=error_message,
            result=result_payload,
            error=error_message,
        )

    def handle_imap_fetch_latest(
        self,
        domain: Optional[str],
        payload: Dict[str, object],
        job_id: str,
    ) -> JobResult:
        normalized = (domain or "naver").lower()
        if normalized != "naver":
            message = "네이버 도메인에서만 최신 메일 확인을 지원합니다."
            self._log_imap_console(message)
            return JobResult(job_id=job_id, status="failed", message=message, error=message)
        payload = payload or {}
        settings = self._imap_settings_for_domain(normalized)
        username = normalize_imap_string(payload.get("username")) or normalize_imap_string(
            settings.get("username")
        )
        if not username:
            message = "IMAP 계정 ID가 설정되지 않았습니다."
            self._log_imap_console(message)
            return JobResult(job_id=job_id, status="failed", message=message, error=message)
        use_saved_password = bool(payload.get("use_saved_password"))
        password = str(payload.get("password") or "")
        used_saved_password = False
        if not password and use_saved_password:
            password = settings.get("password") or ""
            used_saved_password = bool(password)
        if not password:
            message = "IMAP 비밀번호를 확인할 수 없습니다."
            self._log_imap_console(message)
            return JobResult(job_id=job_id, status="failed", message=message, error=message)
        folder = str(payload.get("folder") or "Junk").strip() or "Junk"
        try:
            limit_candidate = payload.get("limit")
            limit_value = int(limit_candidate) if limit_candidate is not None else 1
        except (TypeError, ValueError):
            limit_value = 1
        limit_value = max(1, min(10, limit_value))
        self._log_imap_console(f"계정 {username} · 폴더 {folder} · 최신 메일 확인")
        summary = fetch_latest_message_summary(username, password, folder=folder, limit=limit_value)
        success = bool(summary.get("success"))
        mail_info = summary.get("mail") or {}
        reason = summary.get("reason")
        total = summary.get("total")
        used_saved = summary.get("used_saved_password") or False
        self._log_imap_console(
            "최신 메일 확인 결과 · "
            f"성공 {success} · "
            f"총 {total}건 · "
            f"ID {mail_info.get('sequence','-')} · "
            f"저장비번 {used_saved}"
        )
        if mail_info:
            self._log_imap_console(
                "  ↳ 헤더 비교 · "
                f"From {mail_info.get('from') or mail_info.get('from_address') or '-'} · "
                f"Subject {mail_info.get('subject') or '-'} · "
                f"수신 {mail_info.get('received_at_iso') or mail_info.get('date_header') or '-'}"
            )
        if reason:
            self._log_imap_console(f"  ↳ 실패 사유: {reason}")

        def _shorten(value: Optional[str], *, length: int = 60) -> str:
            if not value:
                return ""
            text = str(value).strip()
            return text if len(text) <= length else text[: length - 1] + "…"

        received_label = ""
        raw_received = mail_info.get("received_at_local") or mail_info.get("received_at_iso")
        if raw_received:
            try:
                received_dt = datetime.fromisoformat(str(raw_received).replace("Z", "+00:00"))
                received_label = received_dt.strftime("%Y-%m-%d %H:%M:%S%z")
            except ValueError:
                received_label = str(raw_received)
        elif mail_info.get("date_header"):
            received_label = str(mail_info.get("date_header"))

        from_label = _shorten(
            mail_info.get("from")
            or mail_info.get("from_name")
            or mail_info.get("from_address"),
            length=80,
        )
        subject_label = _shorten(mail_info.get("subject"), length=80)

        detail_parts = []
        if from_label:
            detail_parts.append(f"발신자 {from_label}")
        if subject_label:
            detail_parts.append(f"제목 {subject_label}")
        if received_label:
            detail_parts.append(f"수신 {received_label}")

        result_payload: Dict[str, object] = {
            "username": username,
            "folder": folder,
            "success": success,
            "used_saved_password": used_saved_password,
            "mail": mail_info or None,
            "limit": summary.get("limit", limit_value),
            "total": summary.get("total"),
            "reason": reason,
        }

        if success:
            message = "최신 메일 확인 성공"
            if detail_parts:
                message = f"{message} · {' · '.join(detail_parts)}"
            self._log_imap_console(f"성공 - {message}")
            return JobResult(job_id=job_id, status="success", message=message, result=result_payload)

        error_message = reason or "최신 메일을 가져오지 못했습니다."
        self._log_imap_console(f"실패 - {error_message}")
        return JobResult(
            job_id=job_id,
            status="failed",
            message=error_message,
            result=result_payload,
            error=error_message,
        )

    def _perform_ip_change(self) -> Tuple[bool, str, Optional[str]]:
        previous_ip = self.public_ip
        try:
            new_ip = change_mobile_ip_at_phone()
        except Exception as exc:  # pylint: disable=broad-except
            return False, f"IP 변경 실패: {exc}", None
        if not new_ip:
            return False, "IP 변경 실패: 새 공인 IP를 확인하지 못했습니다.", None
        self.public_ip = new_ip
        self._last_ip_refresh = time.time()
        message = f"공인 IP 변경 완료: {previous_ip or '-'} → {new_ip}"
        return True, message, new_ip

    def handle_change_ip(self, job_id: str) -> JobResult:
        print("[IP 변경] 비행기 모드 토글을 시작합니다.")
        self.send_job_report(
            JobResult(
                job_id=job_id,
                status="running",
                message="IP 변경 중...",
                result={
                    "logs": [
                        {
                            "log": "IP 변경 중...",
                            "delivery_status": "info",
                        }
                    ]
                },
            )
        )
        success, message, new_ip = self._perform_ip_change()
        if success:
            print(f"[IP 변경 완료] {message}")
        else:
            print(f"[IP 변경 실패] {message}")
        log_entry = {
            "log": f"IP 변경 완료: {new_ip}" if success and new_ip else message,
            "delivery_status": "sent" if success else "error",
        }
        result_payload: Dict[str, object] = {"logs": [log_entry]}
        if success and new_ip:
            result_payload["public_ip"] = new_ip
        return JobResult(
            job_id=job_id,
            status="success" if success else "failed",
            message=message,
            result=result_payload,
            error=None if success else message,
        )

    def handle_reset_sent_sequence(self, domain: Optional[str], payload: Dict[str, object], job_id: str) -> JobResult:
        targets: Set[str] = set()
        if domain:
            targets.add(str(domain).lower())
        raw_domains = payload.get("domains")
        if isinstance(raw_domains, (list, tuple, set)):
            for item in raw_domains:
                if not item:
                    continue
                targets.add(str(item).lower())
        if not targets:
            targets = set(self.sent_sequences.keys()) | set(DOMAINS)
        self._reset_sent_sequences(targets)
        labels = [DOMAIN_LABELS.get(key, key) for key in sorted(targets)]
        if labels:
            label_text = ", ".join(labels)
        else:
            label_text = "모든 도메인"
        print(f"[로그 초기화] {label_text} 누적 발송 카운터를 0으로 재설정했습니다.")
        result = {
            "domains": sorted(targets),
            "sent_sequences": {key: self.sent_sequences.get(key, 0) for key in sorted(targets)},
        }
        return JobResult(
            job_id=job_id,
            status="success",
            message=f"{label_text} 발송 카운터 초기화",
            result=result,
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
        anchor_total: int,
        is_primary: bool,
        device_name: Optional[str] = None,
        sequence: Optional[int] = None,
    ) -> str:
        safe_label = (label or "").strip() or "Sent"
        target = (email or "").strip() or "-"
        label_display = safe_label
        if sequence is not None and sequence > 0 and safe_label.lower() == "sent":
            label_display = f"{safe_label}({sequence})"
        display = f"{label_display} - {target}"
        if is_primary:
            if bcc_total > 0:
                display += f" 외 {bcc_total}"
            if anchor_total > 0:
                display += f" + 알박기 {anchor_total}"
        device_label = (device_name or "").strip()
        if device_label:
            display += f" | {device_label}"
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
                        self._disconnect_logged = False
                    self.connected = True
                    configs = response.get("configs") or {}
                    self.apply_configs(configs)
                    self._evaluate_idle_schedules()
                    controls = response.get("job_controls") or []
                    if isinstance(controls, Iterable):
                        self._update_job_controls(controls)
                    self._acknowledge_cancelled_jobs()
                    jobs: List[Dict[str, object]] = []
                    while self._pending_jobs:
                        pending_job = self._pending_jobs.popleft()
                        pending_id = str(pending_job.get("job_id") or "").strip()
                        if pending_id:
                            self._pending_job_ids.discard(pending_id)
                        jobs.append(pending_job)
                    response_jobs = response.get("jobs")
                    if isinstance(response_jobs, list):
                        jobs.extend(job for job in response_jobs if isinstance(job, dict))
                    jobs.sort(key=lambda job: 0 if str(job.get("job_type") or "") == "single_send" else 1)
                    for job in jobs:
                        job_id = str(job.get("job_id") or "").strip()
                        if job_id:
                            self._pending_job_ids.discard(job_id)
                        job_type = str(job.get("job_type") or "")
                        guard_result: Optional[JobResult] = None
                        if job_type in {"single_send", "batch_send"}:
                            guard_result = self._schedule_guard_for_job(job.get("domain"), job_id, job_type)
                        if guard_result:
                            domain_states = self.collect_domain_states()
                            self.send_job_report(guard_result, domain_states)
                            if job_id:
                                self.job_controls.pop(job_id, None)
                                self._cancel_ack_sent.discard(job_id)
                            continue
                        if job_id:
                            self._active_job_ids.add(job_id)
                            self._active_jobs[job_id] = job_type
                            self._cancel_ack_sent.discard(job_id)
                        self.send_job_report(
                            JobResult(job_id=job_id, status="running", message="작업 시작")
                        )
                        try:
                            result = self.process_job(job)
                        finally:
                            if job_id:
                                self._active_job_ids.discard(job_id)
                                self._active_jobs.pop(job_id, None)
                        domain_states = self.collect_domain_states()
                        self.send_job_report(result, domain_states)
                        if job_id:
                            self.job_controls.pop(job_id, None)
                            self._cancel_ack_sent.discard(job_id)
                    time.sleep(self.interval)
                except requests.RequestException:
                    if not self._disconnect_logged:
                        print("[연결 끊김] 대시보드 서버가 닫혀 있습니다. 백그라운드 발송은 계속하고 재연결을 시도합니다.")
                        self._disconnect_logged = True
                    self._reset_session()
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
            self._shutdown_imap_executor()

    def apply_configs(self, configs: Dict[str, Dict[str, object]]) -> None:
        if not configs:
            return
        changed = False
        for domain, payload in configs.items():
            normalized = (domain or "").lower()
            if normalized not in DOMAINS:
                continue
            state = self._get_schedule_state(normalized)
            server_enabled = self._sanitize_schedule_enabled(payload.get("stop_schedule_enabled"))
            server_time = self._sanitize_schedule_time(payload.get("stop_schedule_time"))
            if not server_time:
                server_enabled = False
            server_last_run = self._sanitize_schedule_date(payload.get("stop_schedule_last_run"))
            previous_enabled = bool(state.get("enabled"))
            previous_time = state.get("time") or ""
            desired_time = server_time or ""
            if server_enabled != previous_enabled or desired_time != previous_time:
                state["enabled"] = server_enabled
                state["time"] = desired_time
                if not server_enabled:
                    state["last_run"] = None
                    state["needs_sync"] = False
                    self._schedule_events.pop(normalized, None)
                elif desired_time != previous_time:
                    state["last_run"] = None
                    state["needs_sync"] = False
                    self._schedule_events.pop(normalized, None)
                    server_last_run = None
                changed = True
            if server_last_run:
                if self._is_date_newer(server_last_run, state.get("last_run")):
                    state["last_run"] = server_last_run
                    state["needs_sync"] = False
                    changed = True
            if server_last_run is None and not server_enabled:
                state["server_last_run"] = None
            else:
                state["server_last_run"] = server_last_run
            self._update_imap_settings_from_server(normalized, payload)
        if changed:
            self.persist()


DOMAIN_LABELS = {"naver": "네이버", "daum": "다음"}


def main() -> None:
    config = load_config()
    wake_acquired = acquire_wake_lock()
    try:
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
    finally:
        if not wake_acquired:
            print("웨이크락 상태가 불확실하여 해제를 시도합니다.")
        release_wake_lock()


if __name__ == "__main__":
    main()
