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
import ssl
from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from email import policy
from email.parser import Parser
from email.utils import format_datetime, make_msgid
from pathlib import Path
from typing import Deque, Dict, Iterable, List, Optional, Set, Tuple
from datetime import datetime, timezone, timedelta

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import smtp_utils
from smtp_utils import send_via_telnet, set_telnet_debug_mode
from urllib.parse import urlparse, urlunparse
from lib.change_ip import change_mobile_ip_at_phone, get_public_ipv4
from lib.naver_imap import (
    IMAPNetworkError,
    probe_imap_connection,
    verify_delivery,
    fetch_latest_message_summary,
)


APP_VERSION = "0.0.71"

smtp_utils.TELNET_READ_TIMEOUT_SECONDS = 5

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "settings.json"
DATA_DIR = BASE_DIR / "data"
LOG_DIR = BASE_DIR / "logs"

DOMAINS = ("naver", "daum")
EMAIL_STATUSES = ("pending", "reserved", "sent", "block", "failed", "nouser", "removed")
SMTP_RECIPIENT_LIMIT = 25

THROTTLE_MARKER_MESSAGES = {
    "550 5.7.2": "네이버 '550 5.7.2' 응답 감지",
    "452 4.7.1 sent too many messages": "네이버 '452 4.7.1 Sent too many messages' 응답 감지",
    "452 4.7.1 sent too many message": "네이버 '452 4.7.1 Sent too many messages' 응답 감지",
    "421 4.3.2 your ip blocked from this server": "네이버 '421 4.3.2 Your IP blocked from this server' 응답 감지",
}

FATAL_STOP_MARKER_MESSAGES = {
    "421 4.7.1 this email has been temporarily blocked": "네이버 '421 4.7.1 This email has been temporarily blocked' 응답 감지",
}

RECIPIENT_LIMIT_MARKERS = (
    "too many recipients",
    "too many rcpt",
    "recipient limit",
    "rcpt limit",
    "too many recipients on this connection",
    "수신자 수 초과",
)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat().replace("+00:00", "Z")


def to_utc_iso(dt: datetime) -> str:
    if not isinstance(dt, datetime):
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")

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
    "telnet_debug_mode": False,
}

IMAP_ALLOWED_LATENCY_MIN_SECONDS = 5
IMAP_ALLOWED_LATENCY_MAX_SECONDS = 600
IMAP_DEFAULT_ALLOWED_LATENCY_SECONDS = 20
IMAP_DEFAULT_SINGLE_DELAY_SECONDS = 20
IMAP_DEFAULT_SENT_THRESHOLD = 90
IMAP_SENT_THRESHOLD_MIN = 1
IMAP_SENT_THRESHOLD_MAX = 1000

OFFLINE_PROBE_INTERVAL_SECONDS = 10
OFFLINE_HEALTH_PATH = "/health"

DOMAIN_DB_SCHEMA = """
CREATE TABLE IF NOT EXISTS emails (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT NOT NULL UNIQUE,
    source_file TEXT,
    version INTEGER,
    status TEXT CHECK(status IN ('pending','reserved','sent','block','failed','nouser','removed')) NOT NULL DEFAULT 'pending',
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


def configure_settings_menu(config: Dict[str, object]) -> Dict[str, object]:
    while True:
        current = bool(config.get("telnet_debug_mode"))
        state_label = "ON" if current else "OFF"
        print("\n--- 설정 ---")
        print(f"1. 텔넷 디버그 모드 {state_label} (토글)")
        print("0. 뒤로")
        choice = input("선택> ").strip()
        if choice == "1":
            new_state = not current
            config["telnet_debug_mode"] = new_state
            save_config(config)
            set_telnet_debug_mode(new_state)
            print(f"텔넷 디버그 모드를 {'ON' if new_state else 'OFF'}로 설정했습니다.")
        elif choice in {"0", "q", "Q"}:
            break
        else:
            print("알 수 없는 선택입니다. 다시 입력하세요.")
    return load_config()


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
    effective_default = default if default is not None else IMAP_DEFAULT_SINGLE_DELAY_SECONDS
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


def sanitize_imap_sent_threshold(
    value: Optional[object],
    *,
    default: Optional[int] = None,
) -> int:
    effective_default = default if default is not None else IMAP_DEFAULT_SENT_THRESHOLD
    try:
        threshold = int(value) if value is not None else effective_default
    except (TypeError, ValueError):
        threshold = effective_default
    return max(IMAP_SENT_THRESHOLD_MIN, min(IMAP_SENT_THRESHOLD_MAX, threshold))


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


def normalize_imap_recipient(domain: str, value: Optional[object]) -> str:
    candidate = normalize_imap_string(value)
    if not candidate:
        return ""
    if "@" in candidate:
        return candidate
    normalized_domain = (domain or "").lower()
    if normalized_domain == "naver":
        return f"{candidate}@naver.com"
    return candidate


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
    deferred_bcc: int = 0


@dataclass
class DispatchOutcome:
    success: bool
    response_text: str
    delivery_status: str
    status_line: str
    detail_line: Optional[str]
    sent_at: datetime
    rcpt_details: Optional[List[Dict[str, object]]] = None
    data_response_code: str = ""
    data_response_message: Optional[str] = None


@dataclass
class SentProbeResult:
    success: bool
    sent_at: Optional[datetime]
    status_line: str
    detail_line: Optional[str]
    throttle_marker: Optional[str] = None
    throttle_detail: Optional[str] = None
    ip_change_attempted: bool = False
    ip_change_success: bool = False
    ip_change_message: Optional[str] = None
    ip_after_change: Optional[str] = None
    attempts: int = 0
    message_id: Optional[str] = None
    mail_from: Optional[str] = None
    rcpt_to: Optional[str] = None


@dataclass
class ImapGuardOutcome:
    probe: Optional[SentProbeResult]
    future: Optional[Future]
    sent_window_count: Optional[int]
    sent_threshold: Optional[int]
    scheduled: bool
    failure_report_enqueued: bool = False


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
            sent_threshold = sanitize_imap_sent_threshold(
                entry.get("sent_threshold"),
                default=IMAP_DEFAULT_SENT_THRESHOLD,
            )
            try:
                sent_since_last_check = int(entry.get("sent_since_last_check") or 0)
            except (TypeError, ValueError):
                sent_since_last_check = 0
            sent_since_last_check = max(0, sent_since_last_check)
            try:
                last_threshold_multiple = int(entry.get("last_threshold_multiple") or 0)
            except (TypeError, ValueError):
                last_threshold_multiple = 0
            last_threshold_multiple = max(0, last_threshold_multiple)
            self.imap_settings[domain] = {
                "enabled": bool(entry.get("enabled")),
                "username": normalize_imap_string(entry.get("username")),
                "password": entry.get("password") or "",
                "single_delay_seconds": single_delay,
                "allowed_latency_seconds": allowed_latency,
                "failure_action": sanitize_imap_failure_action(entry.get("failure_action")),
                "notify_before_stop_all": sanitize_bool_flag(entry.get("notify_before_stop_all")),
                "sent_threshold": sent_threshold,
                "sent_since_last_check": sent_since_last_check,
                "sent_last_reset_at": entry.get("sent_last_reset_at"),
                "last_threshold_multiple": last_threshold_multiple,
            }
        self._imap_sent_counters: Dict[str, int] = {}
        self._imap_threshold_multiples: Dict[str, int] = {}
        for domain in DOMAINS:
            try:
                self._imap_sent_counters[domain] = max(
                    0,
                    int(self.imap_settings.get(domain, {}).get("sent_since_last_check") or 0),
                )
            except (TypeError, ValueError):
                self._imap_sent_counters[domain] = 0
            try:
                self._imap_threshold_multiples[domain] = max(
                    0,
                    int(self.imap_settings.get(domain, {}).get("last_threshold_multiple") or 0),
                )
            except (TypeError, ValueError):
                self._imap_threshold_multiples[domain] = 0
        self._imap_settings_dirty: Set[str] = set()
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
        self._pending_job_reports: Deque[Tuple[JobResult, List[Dict[str, object]]]] = deque()
        self._report_flush_in_progress: bool = False
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
        self._sent_log_bases: Dict[str, int] = {}
        for domain in DOMAINS:
            try:
                self._sent_log_bases[domain] = max(0, int(self.sent_sequences.get(domain, 0)))
            except (TypeError, ValueError):
                self._sent_log_bases[domain] = 0
        self.telnet_debug_mode = bool(config.get("telnet_debug_mode"))
        smtp_utils.set_telnet_debug_mode(self.telnet_debug_mode)
        self._sequence_dirty: Set[str] = set()
        self._last_sequence_flush: float = 0.0
        self._imap_executor = ThreadPoolExecutor(max_workers=2)
        self._imap_lock = threading.Lock()
        self._imap_reports: Deque[Dict[str, object]] = deque()
        self._imap_futures: Set[Future] = set()
        self._imap_protection_lock = threading.Lock()
        self._imap_throttle_flags: Dict[str, Dict[str, object]] = {}
        self._sent_guard_lock = threading.Lock()
        self._sent_guard_condition = threading.Condition()
        self._sent_guard_paused_domains: Set[str] = set()
        self._sent_guard_pending: Dict[str, Dict[str, object]] = {}

        self.offline_mode: bool = False
        self.next_offline_probe_at: float = 0.0
        self._offline_probe_executor = ThreadPoolExecutor(max_workers=1)
        self._offline_probe_future: Optional[Future] = None
        self._offline_probe_lock = threading.Lock()
        self._offline_probe_last_error: Optional[str] = None

        self._upgrade_domain_schemas()

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
        suppress_log = bool(kwargs.pop("suppress_log", False))
        log_context = kwargs.pop("log_context", None)
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
                if not suppress_log:
                    if isinstance(exc, (requests.exceptions.ConnectionError, requests.exceptions.Timeout)):
                        message = self._summarize_http_connection_error(method, url, exc, log_context=log_context)
                    else:
                        reason = self._short_error_reason(exc)
                        if log_context:
                            message = f"[네트워크] {log_context} 실패 - {reason}"
                        else:
                            target = urlparse(url).path or "/"
                            message = f"[네트워크] {method.upper()} {target} 실패 - {reason}"
                    print(message)
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

    @staticmethod
    def _short_error_reason(exc: Exception) -> str:
        message = str(exc).strip()
        message_lower = message.lower()
        if isinstance(exc, requests.exceptions.Timeout) or "timed out" in message_lower:
            return "타임아웃"
        if "connection refused" in message_lower:
            return "연결 거부"
        if "name or service not known" in message_lower or "temporary failure in name resolution" in message_lower:
            return "DNS 오류"
        if "remotedisconnected" in message_lower:
            return "원격 연결 종료"
        if "broken pipe" in message_lower or "connection aborted" in message_lower:
            return "연결 끊김"
        if "max retries exceeded" in message_lower:
            return "재시도 한도 초과"
        if "certificate verify failed" in message_lower or "ssl: wrong version number" in message_lower:
            return "TLS 인증 오류"
        if message:
            first_line = message.splitlines()[0]
            if len(first_line) > 60:
                return f"{first_line[:57]}..."
            return first_line
        return exc.__class__.__name__

    @staticmethod
    def _summarize_http_connection_error(
        method: str,
        url: str,
        exc: requests.RequestException,
        log_context: Optional[str] = None,
    ) -> str:
        parsed = urlparse(url)
        reason = MailClient._short_error_reason(exc)
        if log_context:
            return f"[네트워크] {log_context} 실패 - {reason}"
        target = parsed.path or "/"
        return f"[네트워크] {method.upper()} {target} 실패 - {reason}"

    def _enter_offline_mode(self, context: str, exc: Optional[Exception] = None) -> None:
        if self.offline_mode:
            return
        self.offline_mode = True
        self.next_offline_probe_at = time.time() + OFFLINE_PROBE_INTERVAL_SECONDS
        self.connected = False
        self._report_flush_in_progress = False
        self._schedule_offline_probe()
        try:
            self._reset_session()
        except Exception:
            pass
        summary = f"{context} 요청 실패"
        if exc:
            summary = f"{context} 요청 실패 - {self._short_error_reason(exc)}"
        print(f"[오프라인 전환] {summary} · 오프라인 모드 진입")

    def _exit_offline_mode(self) -> None:
        if not self.offline_mode:
            return
        self.offline_mode = False
        self.next_offline_probe_at = 0.0
        self._disconnect_logged = False
        print("[오프라인 복구] 서버 응답을 확인했습니다. 동기화를 재개합니다.")

    def _schedule_offline_probe(self) -> None:
        if not self.server_url:
            return
        with self._offline_probe_lock:
            if self._offline_probe_future and not self._offline_probe_future.done():
                return
            self._offline_probe_last_error = None
            self._offline_probe_future = self._offline_probe_executor.submit(
                self._perform_offline_probe
            )

    def _collect_offline_probe_result(self) -> Optional[Tuple[bool, Optional[str]]]:
        with self._offline_probe_lock:
            future = self._offline_probe_future
            if not future or not future.done():
                return None
        try:
            result = future.result()
        except Exception as exc:  # pylint: disable=broad-except
            result = (False, self._short_error_reason(exc))
        with self._offline_probe_lock:
            self._offline_probe_future = None
        if isinstance(result, tuple) and len(result) == 2:
            return result  # type: ignore[return-value]
        return None

    def _perform_offline_probe(self) -> Tuple[bool, Optional[str]]:
        session = requests.Session()
        try:
            adapter = HTTPAdapter(
                pool_connections=1,
                pool_maxsize=1,
                max_retries=Retry(
                    total=0,
                    connect=0,
                    read=0,
                    status=0,
                    backoff_factor=0.0,
                    allowed_methods=None,
                    raise_on_status=False,
                    respect_retry_after_header=False,
                ),
            )
            session.headers.update({"Connection": "close"})
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            url = join_url(self.server_url, OFFLINE_HEALTH_PATH)
            try:
                timeout_candidate = float(self.timeout or 5)
            except (TypeError, ValueError):
                timeout_candidate = 5.0
            timeout = max(1.0, min(3.0, timeout_candidate))
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            return True, None
        except requests.RequestException as exc:
            return False, self._short_error_reason(exc)
        finally:
            try:
                session.close()
            except Exception:  # pylint: disable=broad-except
                pass

    def _probe_server_if_due(self, domain_states: List[Dict[str, object]]) -> Optional[Dict[str, object]]:
        if not self.offline_mode:
            return None

        probe_result = self._collect_offline_probe_result()
        if probe_result is not None:
            success, error_message = probe_result
            if success:
                self._offline_probe_last_error = None
                self._exit_offline_mode()
                try:
                    fresh_states = self.collect_domain_states()
                except Exception:  # pylint: disable=broad-except
                    fresh_states = domain_states
                try:
                    data = self.heartbeat(fresh_states, [])
                except requests.RequestException as exc:
                    self._enter_offline_mode("하트비트", exc)
                    return None
                self._flush_pending_job_reports()
                return data
            if error_message and error_message != self._offline_probe_last_error:
                print(f"오프라인 모드 · 서버 핑 실패 - {error_message}")
                self._offline_probe_last_error = error_message

        now = time.time()
        if now < self.next_offline_probe_at:
            return None
        self.next_offline_probe_at = now + OFFLINE_PROBE_INTERVAL_SECONDS
        self._schedule_offline_probe()
        return None

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
        snapshot["telnet_debug_mode"] = self.telnet_debug_mode
        save_config(snapshot)
        self.config["sent_sequences"] = self.sent_sequences
        self.config["imap_settings"] = snapshot["imap_settings"]
        self._sequence_dirty.clear()
        self._imap_settings_dirty.clear()
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
            sent_threshold = sanitize_imap_sent_threshold(
                settings.get("sent_threshold"),
                default=IMAP_DEFAULT_SENT_THRESHOLD,
            )
            try:
                sent_since = int(settings.get("sent_since_last_check") or 0)
            except (TypeError, ValueError):
                sent_since = 0
            sent_since = max(0, sent_since)
            try:
                last_multiple = int(settings.get("last_threshold_multiple") or 0)
            except (TypeError, ValueError):
                last_multiple = 0
            last_multiple = max(0, last_multiple)
            serialized[domain] = {
                "enabled": bool(settings.get("enabled")),
                "username": normalize_imap_string(settings.get("username")),
                "password": settings.get("password") or "",
                "single_delay_seconds": sanitize_imap_delay(
                    settings.get("single_delay_seconds"),
                    default=IMAP_DEFAULT_SINGLE_DELAY_SECONDS,
                    minimum=0,
                ),
                "allowed_latency_seconds": sanitize_imap_allowed_latency(
                    settings.get("allowed_latency_seconds"),
                    default=IMAP_DEFAULT_ALLOWED_LATENCY_SECONDS,
                ),
                "failure_action": sanitize_imap_failure_action(settings.get("failure_action")),
                "notify_before_stop_all": sanitize_bool_flag(settings.get("notify_before_stop_all")),
                "sent_threshold": sent_threshold,
                "sent_since_last_check": sent_since,
                "sent_last_reset_at": settings.get("sent_last_reset_at"),
                "last_threshold_multiple": last_multiple,
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
        if self._try_push_imap_reports_immediately([report]):
            return
        with self._imap_lock:
            while len(self._imap_reports) >= 100:
                self._imap_reports.popleft()
            self._imap_reports.append(report)

    def _try_push_imap_reports_immediately(self, reports: List[Dict[str, object]]) -> bool:
        if not reports:
            return True
        if self.offline_mode:
            return False
        if not self.device_id:
            return False
        payload = {
            "device_name": self.device_name,
            "active_domain": self.active_domain,
            "domain_states": [],
            "job_reports": [],
            "public_ip": self.public_ip,
            "imap_reports": reports,
        }
        try:
            data = self._post_heartbeat(payload, log_context="IMAP 즉시 보고")
        except requests.RequestException as exc:  # type: ignore[attr-defined]
            print(f"[경고] 즉시 IMAP 보고 실패: {exc}")
            return False
        except Exception as exc:  # pylint: disable=broad-except
            print(f"[경고] 즉시 IMAP 보고 예외: {exc}")
            return False
        self._process_heartbeat_payload(data)
        return True

    def _report_imap_probe_failure(
        self,
        *,
        domain: str,
        job_id: Optional[str],
        send_type: str,
        mail_from: str,
        header_from: Optional[str],
        has_anchor: bool,
        context_reason: Optional[str],
        delay_seconds: float,
        allowed_latency_seconds: int,
        failure_action: str,
        sent_window_count: Optional[int],
        sent_threshold: Optional[int],
        probe_result: SentProbeResult,
    ) -> Dict[str, object]:
        checked_at_iso = utc_now_iso()
        sent_iso = to_utc_iso(probe_result.sent_at) if probe_result.sent_at else checked_at_iso
        reason_components: List[str] = []
        status_line = probe_result.status_line or ""
        detail_line = probe_result.detail_line or ""
        throttle_detail = probe_result.throttle_detail or ""
        if status_line:
            reason_components.append(status_line)
        if detail_line and detail_line != status_line:
            reason_components.append(detail_line)
        if throttle_detail:
            reason_components.append(throttle_detail)
        if context_reason:
            reason_components.append(context_reason)
        reason_text = " · ".join(component for component in reason_components if component)
        delay_seconds_value = int(max(0, round(delay_seconds)))
        report = {
            "domain": (domain or "").lower(),
            "status": "error",
            "latency": None,
            "received_at": None,
            "sent_at": sent_iso,
            "reason": reason_text,
            "job_id": job_id,
            "send_type": send_type,
            "mail_from": mail_from,
            "header_from": header_from or "",
            "anchor": has_anchor,
            "trigger_stop": False,
            "checked_at": checked_at_iso,
            "delay_seconds": delay_seconds_value,
            "allowed_latency_seconds": int(max(0, allowed_latency_seconds)),
            "failure_action": failure_action,
            "sent_window_count": sent_window_count,
            "sent_threshold": sent_threshold,
            "ip_change_attempted": probe_result.ip_change_attempted,
            "ip_change_success": probe_result.ip_change_success,
            "ip_change_message": probe_result.ip_change_message,
            "ip_change_marker": probe_result.throttle_marker,
            "ip_change_reason": probe_result.throttle_detail,
            "ip_after_change": probe_result.ip_after_change,
            "probe_mail_sent": bool(probe_result.success),
            "probe_mail_error": probe_result.detail_line or probe_result.status_line,
            "probe_status_line": probe_result.status_line,
            "probe_detail_line": probe_result.detail_line,
            "probe_attempts": probe_result.attempts,
        }
        self._queue_imap_report(report)
        return report

    def _execute_imap_guard_flow(
        self,
        *,
        domain: str,
        job_id: Optional[str],
        send_type: str,
        mail_from: str,
        header_from: Optional[str],
        has_anchor: bool,
        context_reason: Optional[str],
        delay_before_check: Optional[float],
        allowed_delay: Optional[int],
        smtp_context: Optional[Dict[str, object]],
        force: bool,
        counter_mode: Optional[str] = None,
        counter_current: Optional[int] = None,
        counter_threshold: Optional[int] = None,
        sent_window_count: Optional[int] = None,
        probe_result: Optional[SentProbeResult] = None,
        report_probe_failure: bool = False,
    ) -> ImapGuardOutcome:
        normalized = (domain or "").lower()
        settings = self._imap_settings_for_domain(normalized)
        allowed_setting = sanitize_imap_allowed_latency(
            settings.get("allowed_latency_seconds"),
            default=IMAP_DEFAULT_ALLOWED_LATENCY_SECONDS,
        )
        allowed_delay_value = sanitize_imap_allowed_latency(
            allowed_delay if allowed_delay is not None else allowed_setting,
            default=allowed_setting,
        )
        single_delay_setting = sanitize_imap_delay(
            settings.get("single_delay_seconds"),
            default=IMAP_DEFAULT_SINGLE_DELAY_SECONDS,
            minimum=0,
        )
        if delay_before_check is None:
            delay_seconds = float(single_delay_setting)
        else:
            try:
                delay_seconds = float(delay_before_check)
            except (TypeError, ValueError):
                delay_seconds = float(single_delay_setting)
            delay_seconds = max(0.0, delay_seconds)
        threshold_value = sanitize_imap_sent_threshold(
            counter_threshold if counter_threshold is not None else settings.get("sent_threshold"),
            default=IMAP_DEFAULT_SENT_THRESHOLD,
        )
        failure_action_value = sanitize_imap_failure_action(settings.get("failure_action"))
        username_value = normalize_imap_string(settings.get("username"))
        rcpt_username = normalize_imap_recipient(normalized, username_value)
        provided_counter = counter_current if counter_current is not None else sent_window_count
        if counter_mode == "manual":
            base_counter = self._get_sent_counter(normalized)
        else:
            base_counter = provided_counter if provided_counter is not None else self._get_sent_counter(normalized)
        updated_counter: Optional[int] = base_counter
        if not self._imap_enabled(normalized):
            detail_message = "IMAP 도착 확인이 비활성화되어 있어 확인 메일을 발송하지 않습니다."
            probe_payload: Optional[SentProbeResult] = None
            if report_probe_failure:
                probe_payload = SentProbeResult(
                    success=False,
                    sent_at=None,
                    status_line="IMAP 비활성",
                    detail_line=detail_message,
                )
            return ImapGuardOutcome(
                probe=probe_payload,
                future=None,
                sent_window_count=updated_counter,
                sent_threshold=threshold_value,
                scheduled=False,
                failure_report_enqueued=False,
            )
        probe_override = probe_result if (
            probe_result
            and probe_result.success
            and probe_result.message_id
        ) else None
        did_send_probe = False
        with self._sent_guard_lock:
            if probe_override is None:
                probe_result = self._run_sent_probe_mail(
                    domain=normalized,
                    mail_from=mail_from,
                    smtp_context=smtp_context,
                    rcpt_to=rcpt_username,
                )
                did_send_probe = bool(probe_result.success and probe_result.message_id)
            else:
                probe_result = probe_override
            probe_success = bool(
                probe_result
                and probe_result.success
                and probe_result.message_id
                and (probe_result.status_line or "").strip().lower().startswith("250 2.0.0")
            )
            updated_counter = base_counter
            if counter_mode == "threshold":
                self._set_sent_counter(normalized, updated_counter)
            elif counter_mode == "manual":
                self._set_sent_counter(normalized, updated_counter)
        if not probe_success or not probe_result:
            failure_enqueued = False
            if report_probe_failure:
                self._report_imap_probe_failure(
                    domain=normalized,
                    job_id=job_id,
                    send_type=send_type,
                    mail_from=mail_from,
                    header_from=header_from,
                    has_anchor=has_anchor,
                    context_reason=context_reason,
                    delay_seconds=delay_seconds,
                    allowed_latency_seconds=allowed_delay_value,
                    failure_action=failure_action_value,
                    sent_window_count=updated_counter,
                    sent_threshold=threshold_value,
                    probe_result=probe_result,
                )
                failure_enqueued = True
            return ImapGuardOutcome(
                probe=probe_result,
                future=None,
                sent_window_count=updated_counter,
                sent_threshold=threshold_value,
                scheduled=False,
                failure_report_enqueued=failure_enqueued,
            )
        sent_at_value = probe_result.sent_at or utc_now()
        future = self._submit_imap_check(
            domain=normalized,
            job_id=job_id,
            send_type=send_type,
            mail_from=mail_from,
            header_from=header_from,
            sent_at=sent_at_value,
            has_anchor=has_anchor,
            delay_before_check=delay_seconds,
            allowed_delay=allowed_delay_value,
            context_reason=context_reason,
            force=force,
            sent_window_count=updated_counter,
            sent_threshold=threshold_value,
            smtp_context=smtp_context,
            probe_result=probe_result,
        )
        return ImapGuardOutcome(
            probe=probe_result,
            future=future,
            sent_window_count=updated_counter,
            sent_threshold=threshold_value,
            scheduled=bool(future),
        )

    def _guard_sent_threshold(
        self,
        *,
        domain: str,
        job_id: Optional[str],
        request: Dict[str, object],
        send_type: str,
        mail_from: str,
        header_from: Optional[str],
        has_anchor: bool,
        context_reason: Optional[str],
        delay_before_check: Optional[float],
        allowed_delay: Optional[int],
        smtp_context: Optional[Dict[str, object]],
        force: bool,
        counter_mode: Optional[str],
        counter_current: Optional[int],
        counter_threshold: Optional[int],
        sent_window_count: Optional[int],
        report_probe_failure: bool = False,
    ) -> Tuple[ImapGuardOutcome, Optional[Dict[str, object]]]:
        normalized = (domain or "").lower()
        pending_entry = self._get_guard_pending(normalized)
        reuse_probe: Optional[SentProbeResult] = None
        if pending_entry:
            stored_probe = pending_entry.get("probe")
            if isinstance(stored_probe, SentProbeResult):
                reuse_probe = stored_probe
        self._pause_sent_workers(normalized)
        try:
            outcome = self._execute_imap_guard_flow(
                domain=normalized,
                job_id=job_id,
                send_type=send_type,
                mail_from=mail_from,
                header_from=header_from,
                has_anchor=has_anchor,
                context_reason=context_reason,
                delay_before_check=delay_before_check,
                allowed_delay=allowed_delay,
                smtp_context=smtp_context,
                force=force,
                counter_mode=counter_mode,
                counter_current=counter_current,
                counter_threshold=counter_threshold,
                sent_window_count=sent_window_count,
                report_probe_failure=report_probe_failure,
                probe_result=reuse_probe,
            )
        except Exception:
            self._resume_sent_workers(normalized)
            raise

        if not outcome.scheduled or outcome.future is None:
            self._resume_sent_workers(normalized)
            return outcome, None

        future = outcome.future
        report_payload: Optional[Dict[str, object]] = None
        try:
            report_payload = future.result()
        except Exception as exc:  # pylint: disable=broad-except
            report_payload = {
                "status": "error",
                "reason": f"IMAP 확인 작업 예외: {exc}",
            }

        status_value = str((report_payload or {}).get("status") or "").lower()
        if status_value == "network_error":
            effective_request = dict(request or {})
            probe_to_store = outcome.probe if outcome.probe and outcome.probe.message_id else reuse_probe
            self._store_guard_pending(
                normalized,
                request=effective_request,
                probe=probe_to_store,
                status=status_value,
            )
        else:
            self._resume_sent_workers(normalized)
        return outcome, report_payload

    def _record_imap_throttle(self, domain: str, marker: Optional[str], detail: Optional[str]) -> None:
        normalized = (domain or "").lower()
        info = {
            "marker": (marker or "").strip(),
            "detail": (detail or "").strip(),
            "recorded_at": utc_now_iso(),
        }
        with self._imap_protection_lock:
            self._imap_throttle_flags[normalized] = info

    def _consume_imap_throttle(self, domain: str) -> Optional[Dict[str, object]]:
        normalized = (domain or "").lower()
        with self._imap_protection_lock:
            return self._imap_throttle_flags.pop(normalized, None)

    def _pause_sent_workers(self, domain: str) -> None:
        normalized = (domain or "").lower()
        if not normalized:
            return
        with self._sent_guard_condition:
            self._sent_guard_paused_domains.add(normalized)
            self._sent_guard_condition.notify_all()

    def _resume_sent_workers(self, domain: str) -> None:
        normalized = (domain or "").lower()
        if not normalized:
            return
        with self._sent_guard_condition:
            if normalized in self._sent_guard_paused_domains:
                self._sent_guard_paused_domains.discard(normalized)
                self._sent_guard_condition.notify_all()
        self._sent_guard_pending.pop(normalized, None)

    def _is_sent_guard_paused(self, domain: str) -> bool:
        normalized = (domain or "").lower()
        if not normalized:
            return False
        with self._sent_guard_condition:
            return normalized in self._sent_guard_paused_domains

    def _store_guard_pending(
        self,
        domain: str,
        *,
        request: Dict[str, object],
        probe: Optional[SentProbeResult],
        status: Optional[str] = None,
    ) -> None:
        normalized = (domain or "").lower()
        if not normalized:
            return
        payload = {
            "request": request,
            "probe": probe,
            "status": status,
            "stored_at": time.monotonic(),
        }
        self._sent_guard_pending[normalized] = payload

    def _get_guard_pending(self, domain: str) -> Optional[Dict[str, object]]:
        normalized = (domain or "").lower()
        if not normalized:
            return None
        return self._sent_guard_pending.get(normalized)

    def _imap_settings_for_domain(self, domain: str) -> Dict[str, object]:
        normalized = (domain or "").lower()
        settings = self.imap_settings.get(normalized)
        if settings is None:
            settings = {
                "enabled": False,
                "username": "",
                "password": "",
                "single_delay_seconds": IMAP_DEFAULT_SINGLE_DELAY_SECONDS,
                "allowed_latency_seconds": IMAP_DEFAULT_ALLOWED_LATENCY_SECONDS,
                "failure_action": "none",
                "notify_before_stop_all": False,
                "sent_threshold": IMAP_DEFAULT_SENT_THRESHOLD,
                "sent_since_last_check": 0,
                "sent_last_reset_at": None,
                "last_threshold_multiple": 0,
            }
            self.imap_settings[normalized] = settings
        return settings

    def _remember_last_mail_from(self, domain: str, value: str) -> None:
        normalized = (domain or "").lower()
        candidate = (value or "").strip()
        if not normalized or not candidate:
            return
        settings = self._imap_settings_for_domain(normalized)
        if settings.get("last_mail_from") == candidate:
            return
        settings["last_mail_from"] = candidate
        self._imap_settings_dirty.add(normalized)

    def _effective_mail_from(
        self,
        domain: str,
        config: Optional[Dict[str, object]],
        fallback: Optional[str] = None,
    ) -> str:
        normalized = (domain or "").lower()
        candidate = ""
        if isinstance(config, dict):
            candidate = str(config.get("mail_from") or "").strip()
        if not candidate:
            settings = self._imap_settings_for_domain(normalized)
            candidate = str(settings.get("last_mail_from") or "").strip()
        if not candidate and fallback is not None:
            candidate = str(fallback or "").strip()
        return candidate

    def _extract_header_from(
        self,
        header_text: Optional[object],
        fallback: Optional[str] = None,
    ) -> Optional[str]:
        if not header_text:
            return fallback
        text = str(header_text)
        try:
            parser = Parser(policy=policy.default)
            message = parser.parsestr(text, headersonly=True)
        except Exception:
            message = None
        if message is not None:
            value = message.get("From")
            if value:
                return value.strip()
        collected: List[str] = []
        in_from_header = False
        for line in text.splitlines():
            if in_from_header:
                if not line or not line[0].isspace():
                    break
                collected.append(line.strip())
                continue
            if line.lower().startswith("from:"):
                in_from_header = True
                collected.append(line.split(":", 1)[1].strip())
        if collected:
            combined = " ".join(part for part in collected if part)
            return combined.strip() or fallback
        return fallback

    def _current_sent_threshold(self, domain: str) -> int:
        settings = self._imap_settings_for_domain(domain)
        threshold = sanitize_imap_sent_threshold(
            settings.get("sent_threshold"),
            default=IMAP_DEFAULT_SENT_THRESHOLD,
        )
        settings["sent_threshold"] = threshold
        return threshold

    def _get_sent_counter(self, domain: str) -> int:
        normalized = (domain or "").lower()
        return max(0, int(self._imap_sent_counters.get(normalized, 0)))

    def _set_sent_counter(self, domain: str, value: int, *, reset_timestamp: Optional[str] = None) -> None:
        normalized = (domain or "").lower()
        counter = max(0, int(value or 0))
        settings = self._imap_settings_for_domain(normalized)
        self._imap_sent_counters[normalized] = counter
        settings["sent_since_last_check"] = counter
        if reset_timestamp is not None:
            settings["sent_last_reset_at"] = reset_timestamp
        self._imap_settings_dirty.add(normalized)

    def _visible_sent_counter(self, domain: str) -> int:
        normalized = (domain or "").lower()
        try:
            sequence_total = max(0, int(self.sent_sequences.get(normalized, 0)))
        except (TypeError, ValueError):
            sequence_total = 0
        base_value = self._sent_log_bases.get(normalized)
        if base_value is None:
            base_value = sequence_total
            self._sent_log_bases[normalized] = base_value
        else:
            try:
                base_value = max(0, int(base_value))
            except (TypeError, ValueError):
                base_value = 0
        if sequence_total < base_value:
            base_value = sequence_total
            self._sent_log_bases[normalized] = base_value
        return max(0, sequence_total - base_value)

    def _get_last_threshold_multiple(self, domain: str) -> int:
        normalized = (domain or "").lower()
        return max(0, int(self._imap_threshold_multiples.get(normalized, 0)))

    def _set_last_threshold_multiple(self, domain: str, multiple: int) -> None:
        normalized = (domain or "").lower()
        value = max(0, int(multiple or 0))
        self._imap_threshold_multiples[normalized] = value
        settings = self._imap_settings_for_domain(normalized)
        if settings.get("last_threshold_multiple") != value:
            settings["last_threshold_multiple"] = value
            self._imap_settings_dirty.add(normalized)

    def _update_imap_settings_from_server(self, domain: str, payload: Dict[str, object]) -> None:
        if not isinstance(payload, dict):
            return
        normalized = (domain or "").lower()
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
        settings["single_delay_seconds"] = sanitize_imap_delay(
            payload.get("imap_single_delay_seconds"),
            default=IMAP_DEFAULT_SINGLE_DELAY_SECONDS,
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
        settings["sent_threshold"] = sanitize_imap_sent_threshold(
            payload.get("imap_sent_threshold"),
            default=settings.get("sent_threshold")
        )

        server_reset_raw = payload.get("imap_sent_last_reset_at")
        local_reset_raw = settings.get("sent_last_reset_at")

        def _parse_reset(value: Optional[object]) -> Optional[datetime]:
            if not value:
                return None
            if isinstance(value, datetime):
                return value
            candidate = str(value).strip()
            if not candidate:
                return None
            try:
                return datetime.fromisoformat(candidate.replace("Z", "+00:00"))
            except ValueError:
                return None

        server_reset_dt = _parse_reset(server_reset_raw)
        local_reset_dt = _parse_reset(local_reset_raw)

        prefer_local_reset = False
        if local_reset_dt and (server_reset_dt is None or server_reset_dt < local_reset_dt):
            prefer_local_reset = True

        try:
            sent_since_value = int(payload.get("imap_sent_since_last_check") or 0)
        except (TypeError, ValueError):
            sent_since_value = 0
        sent_since_value = max(0, sent_since_value)
        if prefer_local_reset:
            effective_sent_since = max(0, int(settings.get("sent_since_last_check") or 0))
            effective_reset_at = local_reset_raw
        else:
            effective_sent_since = sent_since_value
            effective_reset_at = server_reset_raw
            settings["sent_last_reset_at"] = effective_reset_at
        settings["sent_since_last_check"] = effective_sent_since
        self._imap_sent_counters[normalized] = effective_sent_since

        def _extract_last_multiple() -> int:
            source_value: Optional[object]
            if prefer_local_reset:
                source_value = settings.get("last_threshold_multiple")
            else:
                candidate = payload.get("imap_last_threshold_multiple")
                source_value = candidate if candidate is not None else settings.get("last_threshold_multiple")
            try:
                return int(source_value or 0)
            except (TypeError, ValueError):
                return 0

        last_multiple = _extract_last_multiple()
        last_multiple = max(0, last_multiple)
        if prefer_local_reset:
            settings_last_multiple = max(0, int(settings.get("last_threshold_multiple") or 0))
            settings["last_threshold_multiple"] = settings_last_multiple
            self._imap_threshold_multiples[normalized] = settings_last_multiple
        else:
            settings["last_threshold_multiple"] = last_multiple
            self._imap_threshold_multiples[normalized] = last_multiple
        self._imap_settings_dirty.add(normalized)
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

    def _shutdown_offline_probe_executor(self) -> None:
        future: Optional[Future]
        with self._offline_probe_lock:
            future = self._offline_probe_future
            self._offline_probe_future = None
        if future and not future.done():
            future.cancel()
        try:
            self._offline_probe_executor.shutdown(wait=False)
        except Exception:  # pylint: disable=broad-except
            pass

    def _send_imap_probe_mail(
        self,
        *,
        domain: str,
        smtp_context: Optional[Dict[str, object]],
        mail_from: str,
        rcpt_to: str,
        header_text: Optional[str] = None,
        message_id: Optional[str] = None,
    ) -> Tuple[bool, Optional[datetime], str, Optional[str], Optional[str]]:
        normalized = (domain or "").lower()
        if not smtp_context:
            return False, None, "SMTP 설정 없음", "테스트 메일 발송을 위한 SMTP 설정이 비어 있습니다.", message_id
        smtp_host = str(smtp_context.get("smtp_host") or "").strip()
        try:
            smtp_port = int(smtp_context.get("smtp_port") or 25)
        except (TypeError, ValueError):
            smtp_port = 25
        helo_name = str(smtp_context.get("helo") or "")
        header_override = str(smtp_context.get("header") or "").strip()
        if not mail_from:
            return False, None, "MAIL FROM 누락", "메일 발신 주소가 설정되지 않아 테스트 메일을 보낼 수 없습니다.", message_id
        if not rcpt_to:
            return False, None, "RCPT TO 누락", "IMAP 확인용 수신 주소가 비어 있습니다.", message_id
        if "@" not in rcpt_to:
            return False, None, "RCPT TO 형식 오류", "IMAP 계정 ID에 '@'가 없어 테스트 메일을 보낼 수 없습니다.", message_id
        probe_started = utc_now()
        date_header = format_datetime(probe_started.astimezone(timezone.utc))
        subject = f"[IMAP 체크] Sent 누적 확인 {probe_started.astimezone().strftime('%H:%M:%S')}"
        message_id_value = message_id or make_msgid(domain=mail_from.split("@")[-1] if "@" in mail_from else "mailsender")
        if header_text:
            payload_header = header_text
        else:
            default_header = (
                f"From: {mail_from}\n"
                f"To: {rcpt_to}\n"
                f"Subject: {subject}\n"
                f"Date: {date_header}\n"
                f"Message-ID: {message_id_value}\n"
                "MIME-Version: 1.0\n"
                "Content-Type: text/plain; charset=UTF-8\n"
                "Content-Transfer-Encoding: 8bit\n"
                "\n"
                f"Sent 누적 확인용 테스트 메일입니다. 발송 시각(UTC): {probe_started.isoformat()}.\n"
            )
            payload_header = header_override or default_header
        try:
            success, response_text, completed_at, _rcpt_details, _data_response = send_via_telnet(
                smtp_host=smtp_host,
                smtp_port=smtp_port,
                helo=helo_name,
                mail_from=mail_from,
                rcpt_to=rcpt_to,
                header_text=payload_header,
                bcc_emails=None,
                debug=self.telnet_debug_mode,
            )
        except Exception as exc:  # pylint: disable=broad-except
            status_line = "테스트 메일 발송 예외"
            detail_line = str(exc)
            self._log_imap_console(f"IMAP 발송 · 확인 메일 발송 실패 · {detail_line}", domain=normalized)
            return False, None, status_line, detail_line, message_id_value
        status_line, detail_line = self._smtp_status_and_detail(response_text)
        if success:
            self._log_imap_console(
                f"확인용 발송 성공 · 응답 {status_line or '-'}",
                domain=normalized,
            )
            return True, completed_at, status_line, detail_line, message_id_value
        self._log_imap_console(
            f"확인용 발송 실패 · 응답 {status_line or '-'}",
            domain=normalized,
        )
        if detail_line:
            self._log_imap_console(f"  ↳ {detail_line}", domain=normalized)
        return False, completed_at, status_line, detail_line, message_id_value

    def _run_sent_probe_mail(
        self,
        *,
        domain: str,
        mail_from: str,
        smtp_context: Optional[Dict[str, object]],
        rcpt_to: Optional[str],
    ) -> SentProbeResult:
        normalized = (domain or "").lower()
        if not smtp_context:
            self._log_imap_console("IMAP 발송 · SMTP 설정 없음", domain=normalized)
            return SentProbeResult(
                success=False,
                sent_at=None,
                status_line="SMTP 설정 없음",
                detail_line="확인 메일 발송을 위한 SMTP 설정이 비어 있습니다.",
            )
        if not mail_from:
            self._log_imap_console("IMAP 발송 · MAIL FROM 누락", domain=normalized)
            return SentProbeResult(
                success=False,
                sent_at=None,
                status_line="MAIL FROM 누락",
                detail_line="MAIL FROM이 설정되지 않아 확인 메일을 발송할 수 없습니다.",
            )
        rcpt_value = normalize_imap_recipient(domain, rcpt_to)
        if not rcpt_value or "@" not in rcpt_value:
            self._log_imap_console("IMAP 발송 · RCPT TO 형식 오류", domain=normalized)
            return SentProbeResult(
                success=False,
                sent_at=None,
                status_line="RCPT TO 형식 오류",
                detail_line="IMAP 계정 주소가 올바르지 않아 확인 메일을 발송할 수 없습니다.",
            )

        header_template: Optional[str] = None
        newline_hint = "\n"
        raw_header_value = smtp_context.get("header") if smtp_context else None
        if isinstance(raw_header_value, bytes):
            try:
                decoded_header = raw_header_value.decode("utf-8")
            except UnicodeDecodeError:
                decoded_header = raw_header_value.decode("latin-1", errors="ignore")
        elif raw_header_value is not None:
            decoded_header = str(raw_header_value)
        else:
            decoded_header = ""
        if decoded_header and decoded_header.strip():
            header_template = decoded_header
            newline_hint = "\r\n" if "\r\n" in decoded_header else "\n"

        probe_reference = utc_now()
        message_id_value = make_msgid(domain=mail_from.split("@")[-1] if "@" in mail_from else "mailsender")
        message_id_pattern = re.compile(r"(?im)^(?P<indent>[ \t]*)Message-ID\s*:(?P<value>.*(?:\n[ \t].*)*)")

        def build_probe_header(msg_id: str) -> str:
            if header_template:
                normalized_template = header_template.replace("\r\n", "\n").replace("\r", "\n")
                has_terminal_newline = normalized_template.endswith("\n")
                separator_block = ""
                body_section: Optional[str]
                separator_index = normalized_template.find("\n\n")
                if separator_index >= 0:
                    separator_end = separator_index + 2
                    while separator_end < len(normalized_template) and normalized_template[separator_end] == "\n":
                        separator_end += 1
                    header_section = normalized_template[:separator_index]
                    separator_block = normalized_template[separator_index:separator_end]
                    body_section = normalized_template[separator_end:]
                else:
                    header_section = normalized_template
                    body_section = None
                match = message_id_pattern.search(header_section)
                if match:
                    indent = match.group("indent") or ""
                    start, end = match.span()
                    header_section = f"{header_section[:start]}{indent}Message-ID: {msg_id}{header_section[end:]}"
                else:
                    header_lines = header_section.split("\n") if header_section else []
                    header_lines.append(f"Message-ID: {msg_id}")
                    header_section = "\n".join(header_lines)
                if body_section is not None:
                    normalized_result = f"{header_section}{separator_block}{body_section}"
                else:
                    normalized_result = header_section
                final_message = normalized_result.replace("\n", newline_hint)
                if has_terminal_newline and not final_message.endswith(newline_hint):
                    final_message = f"{final_message}{newline_hint}"
                return final_message
            date_header = format_datetime(probe_reference.astimezone(timezone.utc))
            subject = f"[IMAP 체크] Sent 누적 확인 {probe_reference.astimezone().strftime('%H:%M:%S')}"
            return (
                f"From: {mail_from}\n"
                f"To: {rcpt_value}\n"
                f"Subject: {subject}\n"
                f"Date: {date_header}\n"
                f"Message-ID: {msg_id}\n"
                "MIME-Version: 1.0\n"
                "Content-Type: text/plain; charset=UTF-8\n"
                "Content-Transfer-Encoding: 8bit\n"
                "\n"
                f"Sent 누적 확인용 테스트 메일입니다. 발송 시각(UTC): {probe_reference.isoformat()}.\n"
            )

        probe_header = build_probe_header(message_id_value)

        throttle_marker: Optional[str] = None
        throttle_detail: Optional[str] = None
        ip_change_attempted = False
        ip_change_success = False
        ip_change_message: Optional[str] = None
        ip_after_change: Optional[str] = None
        attempts = 0
        last_status_line = "발송 실패"
        last_detail_line: Optional[str] = None
        sent_at_value: Optional[datetime] = None
        max_retry_limit = 2

        while attempts < max_retry_limit:
            attempts += 1
            attempt_label = "" if attempts == 1 else f" (재시도 {attempts})"
            self._log_imap_console(
                f"확인용 발송{attempt_label} · RCPT {rcpt_value}",
                domain=normalized,
            )
            success, sent_at_candidate, status_line, detail_line, returned_message_id = self._send_imap_probe_mail(
                domain=normalized,
                smtp_context=smtp_context,
                mail_from=mail_from,
                rcpt_to=rcpt_value,
                header_text=probe_header,
                message_id=message_id_value,
            )
            if returned_message_id:
                message_id_value = returned_message_id
                probe_header = build_probe_header(message_id_value)
            last_status_line = status_line or "발송 실패"
            last_detail_line = detail_line
            status_lower = (status_line or "").strip().lower()
            smtp_ok = status_lower.startswith("250 2.0.0")
            if success and smtp_ok:
                if sent_at_candidate is not None:
                    sent_at_value = sent_at_candidate
                return SentProbeResult(
                    success=True,
                    sent_at=sent_at_value,
                    status_line=status_line,
                    detail_line=detail_line,
                    throttle_marker=throttle_marker,
                    throttle_detail=throttle_detail,
                    ip_change_attempted=ip_change_attempted,
                    ip_change_success=ip_change_success,
                    ip_change_message=ip_change_message,
                    ip_after_change=ip_after_change,
                    attempts=attempts,
                    message_id=message_id_value,
                    mail_from=mail_from,
                    rcpt_to=rcpt_value,
                )
            should_retry = False
            lower_sources = [status_line.lower(), (detail_line or "").lower()]
            detected_marker: Optional[str] = None
            detected_detail: Optional[str] = None
            for source in lower_sources:
                if not source:
                    continue
                for marker, description in THROTTLE_MARKER_MESSAGES.items():
                    if marker in source:
                        detected_marker = marker
                        detected_detail = description
                        break
                if detected_marker:
                    break

            if detected_marker and not ip_change_attempted:
                throttle_marker = detected_marker
                throttle_detail = detected_detail
                self._record_imap_throttle(normalized, detected_marker, detected_detail)
                ip_change_attempted = True
                self._log_imap_protection("IMAP 발송 · SMTP 제한 응답 감지 → IP 변경 시도")
                success_change, message, new_ip = self._perform_ip_change()
                ip_change_success = success_change
                ip_change_message = message
                if new_ip:
                    ip_after_change = new_ip
                result_label = "성공" if success_change else "실패"
                self._log_imap_protection(f"IP 변경 {result_label} · {message}")
                if success_change:
                    should_retry = True
                    time.sleep(2.0)
                else:
                    break

            if should_retry and attempts < max_retry_limit:
                continue
            break

        if last_status_line:
            self._log_imap_console(f"  ↳ 최종 응답 {last_status_line}", domain=normalized)
        if last_detail_line:
            self._log_imap_console(f"  ↳ 상세 {last_detail_line}", domain=normalized)
        self._log_imap_console("확인용 발송 실패 → IMAP 확인 생략", domain=normalized)
        return SentProbeResult(
            success=False,
            sent_at=sent_at_value,
            status_line=last_status_line,
            detail_line=last_detail_line,
            throttle_marker=throttle_marker,
            throttle_detail=throttle_detail,
            ip_change_attempted=ip_change_attempted,
            ip_change_success=ip_change_success,
            ip_change_message=ip_change_message,
            ip_after_change=ip_after_change,
            attempts=attempts,
        )

    def _submit_imap_check(
        self,
        *,
        domain: str,
        job_id: Optional[str],
        send_type: str,
        mail_from: str,
        header_from: Optional[str],
        sent_at: datetime,
        has_anchor: bool,
        delay_before_check: Optional[float],
        allowed_delay: Optional[int],
        context_reason: Optional[str] = None,
        force: bool = False,
        sent_window_count: Optional[int] = None,
        sent_threshold: Optional[int] = None,
        smtp_context: Optional[Dict[str, object]] = None,
        probe_result: Optional[SentProbeResult] = None,
    ) -> Optional[Future]:
        normalized = (domain or "").lower()
        if normalized != "naver":
            return None
        if not mail_from:
            return None
        if not isinstance(sent_at, datetime):
            return None
        settings = self._imap_settings_for_domain(normalized)
        manual_force = bool(force)
        if not settings.get("enabled") and not manual_force:
            return None
        raw_username = normalize_imap_string(settings.get("username"))
        username = normalize_imap_recipient(normalized, raw_username)
        password = settings.get("password") or ""
        if not username or not password:
            return None
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
        if delay_before_check is None:
            delay_seconds = float(single_delay_setting)
        else:
            try:
                delay_seconds = float(delay_before_check)
            except (TypeError, ValueError):
                delay_seconds = float(single_delay_setting)
            delay_seconds = max(0.0, delay_seconds)
        sent_at_value = sent_at
        sent_at_iso = to_utc_iso(sent_at_value)
        failure_action_value = sanitize_imap_failure_action(settings.get("failure_action"))
        threshold_value = sanitize_imap_sent_threshold(
            sent_threshold if sent_threshold is not None else settings.get("sent_threshold"),
            default=IMAP_DEFAULT_SENT_THRESHOLD,
        )

        passed_probe_result = probe_result
        stop_actions = {"stop_device", "stop_all"}
        should_retry_on_failure = (
            failure_action_value in stop_actions and send_type == "sent-threshold"
        )
        max_check_attempts = 2 if should_retry_on_failure else 1
        retry_wait_seconds = min(5.0, max(0.0, float(delay_seconds)))
        if retry_wait_seconds <= 0:
            retry_wait_seconds = 3.0

        def task() -> Dict[str, object]:
            nonlocal sent_at_value, sent_at_iso
            mode_label = "수동" if manual_force else "자동"
            components = [
                "IMAP 확인",
                f"{mode_label} 확인 시작",
                f"도메인 {normalized}",
                f"대기 {delay_seconds:.1f}s",
                f"허용 {allowed_delay_value}s",
            ]
            if sent_window_count is not None and sent_window_count > 0:
                components.append(f"Sent {sent_window_count}건")
            self._log_imap_console(" · ".join(components), domain=normalized)

            probe_mail_sent = bool(passed_probe_result and passed_probe_result.success)
            probe_status_line = passed_probe_result.status_line if passed_probe_result else None
            probe_detail_line = passed_probe_result.detail_line if passed_probe_result else None
            probe_attempts = passed_probe_result.attempts if passed_probe_result else 0
            probe_mail_error = None if probe_mail_sent else (probe_detail_line or probe_status_line)

            if passed_probe_result and passed_probe_result.sent_at:
                sent_at_value = passed_probe_result.sent_at
                sent_at_iso = to_utc_iso(passed_probe_result.sent_at)

            ip_change_attempted = bool(passed_probe_result and passed_probe_result.ip_change_attempted)
            ip_change_success = bool(passed_probe_result and passed_probe_result.ip_change_success)
            ip_change_message = passed_probe_result.ip_change_message if passed_probe_result else ""
            ip_after_change = passed_probe_result.ip_after_change if passed_probe_result else None
            throttle_marker = passed_probe_result.throttle_marker if passed_probe_result else None
            throttle_detail = passed_probe_result.throttle_detail if passed_probe_result else None

            if delay_seconds > 0:
                time.sleep(delay_seconds)

            checked_at_iso = utc_now_iso()
            status = "error"
            latency = None
            received_at = None
            reason_text: Optional[str] = None
            attempt_history: List[Dict[str, object]] = []

            for attempt_index in range(1, max_check_attempts + 1):
                attempt_reason: Optional[str] = None
                attempt_status: str = "error"
                attempt_latency = None
                attempt_received_at = None

                try:
                    result = verify_delivery(
                        email_id=username,
                        password=password,
                        mail_from=mail_from,
                        sent_at=sent_at_value,
                        allowed_delay=allowed_delay_value,
                        header_from=header_from,
                        max_messages=10,
                        check_delay=delay_seconds,
                        message_id=passed_probe_result.message_id if passed_probe_result else None,
                    )
                    attempt_status = str(result.get("status") or "error")
                    attempt_latency = result.get("latency")
                    attempt_received_at = result.get("received_at")
                    attempt_reason = result.get("reason")
                    sent_display = result.get("sent_display")
                    received_display = result.get("received_display")
                    if sent_display:
                        self._log_imap_console(f"  ↳ 발신 {sent_display}", domain=normalized)
                    if received_display:
                        self._log_imap_console(f"  ↳ 수신 {received_display}", domain=normalized)
                    latency_label = (
                        f"{attempt_latency:.1f}s" if isinstance(attempt_latency, (int, float)) else "-"
                    )
                    attempt_tag = f"({attempt_index}/{max_check_attempts}) " if max_check_attempts > 1 else ""
                    self._log_imap_console(
                        f"{attempt_tag}IMAP 확인 · 상태 {attempt_status} · 지연 {latency_label} · 허용 {allowed_delay_value}s",
                        domain=normalized,
                    )
                    if attempt_status != "success" and attempt_reason:
                        self._log_imap_console(f"  ↳ 사유: {attempt_reason}", domain=normalized)
                except IMAPNetworkError as exc:
                    attempt_status = "network_error"
                    attempt_reason = str(exc)
                    self._log_imap_console("IMAP 확인 · 네트워크 오류", domain=normalized)
                except (socket.timeout, OSError, ssl.SSLError) as exc:
                    attempt_status = "network_error"
                    attempt_reason = f"네트워크 예외: {exc}"
                    self._log_imap_console("IMAP 확인 · 네트워크 예외", domain=normalized)
                except Exception as exc:  # pylint: disable=broad-except
                    attempt_status = "error"
                    attempt_reason = f"IMAP 확인 실패: {exc}"
                    self._log_imap_console("IMAP 확인 · 예외 발생", domain=normalized)

                if attempt_status not in {"success", "failure", "error", "network_error"}:
                    attempt_status = "error"

                attempt_history.append(
                    {
                        "attempt": attempt_index,
                        "status": attempt_status,
                        "reason": attempt_reason or "",
                    }
                )

                status = attempt_status
                latency = attempt_latency
                received_at = attempt_received_at
                reason_text = attempt_reason

                if status in {"success", "network_error"}:
                    break
                if status in {"failure", "error"} and attempt_index < max_check_attempts:
                    retry_components = [f"IMAP 확인 {attempt_index}차 실패 · 재시도 준비"]
                    if attempt_reason:
                        retry_components.append(f"사유 {attempt_reason}")
                    self._log_imap_console(" · ".join(retry_components), domain=normalized)
                    if retry_wait_seconds > 0:
                        time.sleep(retry_wait_seconds)
                    continue
                break

            trigger_stop = bool(
                failure_action_value in stop_actions
                and status in {"failure", "error"}
                and len(attempt_history) >= max_check_attempts
            )
            if status == "network_error":
                trigger_stop = False

            reason_components: List[str] = []
            if ip_change_attempted:
                base_message = "IP 변경 후 IMAP 재확인"
                base_message += " 성공" if ip_change_success else " 시도"
                reason_components.append(base_message)
                if ip_change_message:
                    reason_components.append(ip_change_message)
            if probe_mail_sent and probe_status_line:
                reason_components.append(f"테스트 메일 응답: {probe_status_line}")
            if not probe_mail_sent and probe_mail_error:
                reason_components.append(f"테스트 메일 실패: {probe_mail_error}")
            if reason_text:
                reason_components.append(reason_text)
            elif context_reason:
                reason_components.append(context_reason)
            if len(attempt_history) > 1:
                history_summaries: List[str] = []
                for entry in attempt_history:
                    attempt_no = entry.get("attempt")
                    status_text = str(entry.get("status") or "")
                    summary = f"{attempt_no}차 {status_text}"
                    entry_reason = str(entry.get("reason") or "").strip()
                    if entry_reason:
                        summary = f"{summary} ({entry_reason})"
                    history_summaries.append(summary)
                reason_components.append(f"시도 기록: {', '.join(history_summaries)}")

            combined_reason = " · ".join([component for component in reason_components if component])

            report = {
                "domain": normalized,
                "status": status,
                "latency": latency,
                "received_at": received_at,
                "sent_at": sent_at_iso or sent_at_value.isoformat(),
                "reason": combined_reason or context_reason or "",
                "job_id": job_id,
                "send_type": send_type,
                "mail_from": mail_from,
                "header_from": header_from or "",
                "anchor": has_anchor,
                "trigger_stop": trigger_stop,
                "checked_at": checked_at_iso,
                "delay_seconds": int(delay_seconds),
                "allowed_latency_seconds": allowed_delay_value,
                "failure_action": failure_action_value,
                "sent_window_count": sent_window_count,
                "sent_threshold": threshold_value,
                "ip_change_attempted": ip_change_attempted,
                "ip_change_success": ip_change_success,
                "ip_change_message": ip_change_message,
                "ip_change_marker": throttle_marker,
                "ip_change_reason": throttle_detail,
                "ip_after_change": ip_after_change,
                "probe_mail_sent": probe_mail_sent,
                "probe_mail_error": probe_mail_error,
                "probe_status_line": probe_status_line,
                "probe_detail_line": probe_detail_line,
                "probe_attempts": probe_attempts,
                "check_attempts": len(attempt_history),
                "check_max_attempts": max_check_attempts,
            }
            self._queue_imap_report(report)
            return report

        future = self._imap_executor.submit(task)
        with self._imap_lock:
            self._imap_futures.add(future)
        future.add_done_callback(self._on_imap_future_done)
        return future

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

    def _upgrade_domain_schemas(self) -> None:
        for domain, db_path in self.domain_paths.items():
            self._ensure_emails_table_supports_nouser(domain, db_path)

    def _ensure_emails_table_supports_nouser(self, domain: str, db_path: Path) -> None:
        if not db_path.exists():
            return
        try:
            with sqlite3.connect(db_path) as conn:
                conn.row_factory = sqlite3.Row
                row = conn.execute(
                    "SELECT sql FROM sqlite_master WHERE type='table' AND name='emails'"
                ).fetchone()
                create_sql = ""
                if row is None:
                    return
                if isinstance(row, sqlite3.Row):
                    create_sql = row["sql"]
                elif isinstance(row, (list, tuple)):
                    create_sql = row[0]
                else:
                    create_sql = str(row)
                if create_sql and "nouser" in create_sql:
                    return
                if not create_sql:
                    return
                print(f"[DB] {domain} emails 테이블을 nouser 상태 지원하도록 갱신합니다.")
                self._migrate_emails_table_add_nouser(conn)
        except sqlite3.DatabaseError as exc:
            print(f"[DB] {domain} 스키마 확인 실패: {exc}")

    def _migrate_emails_table_add_nouser(self, conn: sqlite3.Connection) -> None:
        columns = (
            "id, email, source_file, version, status, priority, reserved_by, reserved_at, "
            "next_retry_at, attempts, last_error, meta, created_at, updated_at"
        )
        create_sql = (
            """
            CREATE TABLE emails (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL UNIQUE,
                source_file TEXT,
                version INTEGER,
                status TEXT CHECK(status IN ('pending','reserved','sent','block','failed','nouser','removed')) NOT NULL DEFAULT 'pending',
                priority INTEGER DEFAULT 100,
                reserved_by TEXT,
                reserved_at TEXT,
                next_retry_at TEXT,
                attempts INTEGER DEFAULT 0,
                last_error TEXT,
                meta TEXT DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        try:
            conn.execute("PRAGMA foreign_keys=OFF")
            conn.execute("BEGIN")
            conn.execute("ALTER TABLE emails RENAME TO emails_backup")
            conn.execute(create_sql)
            conn.execute(
                f"INSERT INTO emails ({columns}) SELECT {columns} FROM emails_backup"
            )
            conn.execute("DROP TABLE emails_backup")
            conn.commit()
        except sqlite3.DatabaseError as exc:
            conn.rollback()
            print(f"[DB] emails 테이블 갱신 실패: {exc}")
            raise

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
            self._sent_log_bases[domain] = 0
            self._set_sent_counter(domain, 0, reset_timestamp=utc_now_iso())
            self._set_last_threshold_multiple(domain, 0)
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
    def _sanitize_email_list(entries: Iterable[str], limit: int = 30) -> List[str]:
        sanitized: List[str] = []
        seen: Set[str] = set()
        if not entries:
            return sanitized
        for entry in entries:
            candidate = str(entry or "").strip().lower()
            if not candidate:
                continue
            if not EMAIL_PATTERN.fullmatch(candidate):
                continue
            if candidate in seen:
                continue
            sanitized.append(candidate)
            seen.add(candidate)
            if len(sanitized) >= max(0, limit):
                break
        return sanitized

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
    def _sanitize_reserved_timeout(value: object) -> int:
        try:
            seconds = int(value or 0)
        except (TypeError, ValueError):
            seconds = 300
        return max(0, min(3600, seconds))

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

    @staticmethod
    def _summarize_rcpt_details(entries: Optional[List[Dict[str, object]]]) -> Dict[str, List[str]]:
        summary: Dict[str, List[str]] = {
            "anchor": [],
            "bcc": [],
            "primary": [],
            "failed": [],
        }
        if not entries:
            return summary
        for item in entries:
            address = str(item.get("address") or "").strip() or "-"
            code = str(item.get("code") or "").strip()
            message_text = str(item.get("message") or "").strip()
            success_flag = bool(item.get("success"))
            label = f"{address}→{code or '-'}"
            if not success_flag and message_text and message_text != code:
                trimmed = message_text if len(message_text) <= 60 else f"{message_text[:57]}..."
                label = f"{label} ({trimmed})"
            if item.get("is_anchor"):
                summary["anchor"].append(label)
            elif item.get("is_bcc"):
                summary["bcc"].append(label)
            elif item.get("is_primary"):
                summary["primary"].append(label)
            if not success_flag:
                summary["failed"].append(label)
        return summary

    @staticmethod
    def _normalize_email_key(email: Optional[str]) -> str:
        if email is None:
            return ""
        return str(email).strip().lower()

    @staticmethod
    def _extract_nouser_map(entries: Optional[List[Dict[str, object]]]) -> Dict[str, str]:
        mapping: Dict[str, str] = {}
        if not entries:
            return mapping
        for item in entries:
            address_key = MailClient._normalize_email_key(item.get("address"))
            if not address_key:
                continue
            code = str(item.get("code") or "").strip().lower()
            message_text = str(item.get("message") or "").strip()
            message_lower = message_text.lower()
            if not code.startswith("550"):
                continue
            if "5.1.1" not in message_lower and "5.1.1" not in code:
                continue
            if "no such user" not in message_lower and "user unknown" not in message_lower:
                continue
            mapping[address_key] = message_text or item.get("code") or "550 5.1.1 No such user"
        return mapping

    @staticmethod
    def _is_data_accepted(data_code: Optional[str], data_message: Optional[str]) -> bool:
        code_value = str(data_code or "").strip()
        message_value = str(data_message or "").strip()
        code_upper = code_value.upper()
        message_lower = message_value.lower()
        if code_upper.startswith("250") and "2.0.0" in code_upper:
            return True
        if message_lower.startswith("250") and "2.0.0" in message_lower:
            return True
        if code_upper.startswith("250") and "2.0.0" in message_lower:
            return True
        return False

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
            "nouser": totals.get("nouser", 0),
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
        response = self._request(
            "post",
            "/api/devices/register",
            json=payload,
            log_context="디바이스 등록",
        )
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
        if self.offline_mode:
            raise RuntimeError("오프라인 모드에서는 하트비트를 전송할 수 없습니다.")
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
        data = self._post_heartbeat(payload, log_context="하트비트")
        return data

    def _post_heartbeat(self, payload: Dict[str, object], *, log_context: str) -> Dict[str, object]:
        try:
            response = self._request(
                "post",
                f"/api/devices/{self.device_id}/heartbeat",
                json=payload,
                suppress_log=True,
                log_context=log_context,
            )
        except requests.RequestException as exc:
            self._enter_offline_mode(log_context, exc)
            raise
        response.raise_for_status()
        data = response.json()
        self.active_domain = data.get("active_domain", self.active_domain)
        if data.get("public_ip"):
            self.public_ip = data["public_ip"]
        controls = data.get("job_controls") or []
        if isinstance(controls, Iterable):
            self._update_job_controls(controls)
        return data

    def _process_heartbeat_payload(self, data: Optional[Dict[str, object]]) -> None:
        if not isinstance(data, dict):
            return
        configs = data.get("configs") if isinstance(data, dict) else None
        if isinstance(configs, dict) and configs:
            self.apply_configs(configs)
        new_jobs = data.get("jobs") if isinstance(data, dict) else None
        if isinstance(new_jobs, list):
            self._queue_jobs(new_jobs)
        self._run_priority_jobs()

    def _flush_pending_job_reports(self) -> None:
        if self.offline_mode:
            return
        if self._report_flush_in_progress:
            return
        if not self._pending_job_reports:
            return
        self._report_flush_in_progress = True
        try:
            while self._pending_job_reports and not self.offline_mode:
                batch = list(self._pending_job_reports)
                reports_payload = [item[0] for item in batch]
                latest_states = list(batch[-1][1]) if batch and batch[-1][1] else []
                try:
                    data = self.heartbeat(latest_states, reports_payload)
                except Exception as exc:  # pylint: disable=broad-except
                    print(f"[경고] 작업 보고 실패: {exc}")
                    return
                processed_count = len(batch)
                for _ in range(processed_count):
                    if not self._pending_job_reports:
                        break
                    self._pending_job_reports.popleft()
                self._process_heartbeat_payload(data)
        finally:
            self._report_flush_in_progress = False

    def send_job_report(
        self,
        report: JobResult,
        domain_states: Optional[List[Dict[str, object]]] = None,
    ) -> None:
        states_snapshot: List[Dict[str, object]] = list(domain_states or [])
        self._pending_job_reports.append((report, states_snapshot))
        self._flush_pending_job_reports()

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
        if job_type == "imap_manual_check":
            return self.handle_imap_manual_check(domain, payload, job_id)
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
        response = self._request(
            "get",
            str(download_path),
            timeout=max(self.timeout, 30),
            log_context="파일 다운로드",
        )
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
        force_imap_check = bool(payload.get("force_imap_check"))
        if not rcpt_to:
            return JobResult(job_id=job_id, status="failed", message="RCPT TO 정보가 없습니다.")
        normalized_domain = (domain or "").lower()
        self._initialize_sent_log_base(normalized_domain)
        raw_bcc_entries = payload.get("bcc")
        if isinstance(raw_bcc_entries, str):
            candidate_bcc_entries: Iterable[str] = [raw_bcc_entries]
        elif isinstance(raw_bcc_entries, dict):
            candidate_bcc_entries = raw_bcc_entries.values()
        elif isinstance(raw_bcc_entries, Iterable):
            candidate_bcc_entries = raw_bcc_entries
        else:
            candidate_bcc_entries = []
        bcc_emails = self._sanitize_email_list(candidate_bcc_entries, limit=30)
        bcc_rows: List[sqlite3.Row] = []
        mail_from_value = self._effective_mail_from(normalized_domain, config)
        header_from_value = self._extract_header_from(config.get("header"), mail_from_value)
        success, response_text, completed_at, rcpt_details, data_response = send_via_telnet(
            smtp_host=config.get("smtp_host", ""),
            smtp_port=int(config.get("smtp_port") or 25),
            helo=config.get("helo", ""),
            mail_from=mail_from_value,
            rcpt_to=rcpt_to,
            header_text=config.get("header", ""),
            bcc_emails=bcc_emails,
            debug=self.telnet_debug_mode,
        )
        sent_at = completed_at if isinstance(completed_at, datetime) else utc_now()
        response_text = response_text or ""
        status = "success" if success else "failed"
        status_line, detail_line = self._smtp_status_and_detail(response_text)
        delivery_status = self._classify_delivery(success, response_text)
        if normalized_domain == "naver":
            throttle_label: Optional[str] = None
            marker_code: Optional[str] = None
            combined_sources = [response_text.lower(), (status_line or "").lower(), (detail_line or "").lower()]
            for text in combined_sources:
                if not text:
                    continue
                for marker, message in THROTTLE_MARKER_MESSAGES.items():
                    if marker in text:
                        throttle_label = message
                        marker_code = marker
                        break
                if throttle_label:
                    break
            if not throttle_label:
                for text in combined_sources:
                    if not text:
                        continue
                    for marker, message in FATAL_STOP_MARKER_MESSAGES.items():
                        if marker in text:
                            throttle_label = message
                            marker_code = marker
                            break
                    if throttle_label:
                        break
            if throttle_label and marker_code and self._imap_enabled(normalized_domain):
                self._log_imap_protection(f"SMTP 제한 응답 감지 · {normalized_domain} · {throttle_label}")
                self._record_imap_throttle(normalized_domain, marker_code, throttle_label or detail_line)
        if bcc_rows and normalized_domain:
            self._update_email_rows(normalized_domain, bcc_rows, delivery_status, detail_line or status_line)

        nouser_map = self._extract_nouser_map(rcpt_details)
        nouser_emails_display = sorted(
            {
                str(entry.get("address") or "").strip()
                for entry in (rcpt_details or [])
                if self._normalize_email_key(entry.get("address")) in nouser_map
            }
        )
        data_ok = self._is_data_accepted(
            (data_response or {}).get("code"),
            (data_response or {}).get("message"),
        )
        sequence_domain = (normalized_domain or (self.active_domain or "naver")).lower()
        recipient_keys: List[str] = []
        primary_key = self._normalize_email_key(rcpt_to)
        if primary_key:
            recipient_keys.append(primary_key)
        for email in bcc_emails:
            key = self._normalize_email_key(email)
            if key:
                recipient_keys.append(key)
        nouser_count = sum(1 for key in recipient_keys if key in nouser_map)
        successful_recipient_count = len(recipient_keys) - nouser_count if data_ok else 0
        session_success = successful_recipient_count > 0 and data_ok
        delivery_status = "sent" if session_success else delivery_status
        if session_success:
            accumulated_total = self._next_sent_sequence(normalized_domain or None, successful_recipient_count)
        else:
            accumulated_total = max(0, int(self.sent_sequences.get(sequence_domain, 0)))
        current_batch_success = self._sent_log_progress(sequence_domain, accumulated_total)
        log_line = self._format_dispatch_log_line(
            "Sent" if session_success else "Fail",
            current_batch_success,
            accumulated_total,
            include_anchor=False,
        )
        dispatch_logs: List[Dict[str, object]] = [
            {
                "log": log_line,
                "display": log_line,
                "email": rcpt_to,
                "sequence": accumulated_total,
                "delivery_status": delivery_status,
                "detail": detail_line or status_line,
                "bcc_total": len(bcc_emails),
                "anchor_total": 0,
                "is_primary": True,
                "rcpt_details": rcpt_details,
                "nouser_total": nouser_count,
                "nouser_emails": nouser_emails_display,
                "tags": ["nouser"] if nouser_count else [],
                "bcc_recipients": list(bcc_emails),
            }
        ]
        print(log_line)
        status = "success" if session_success else "failed"
        if delivery_status == "sent" and normalized_domain and self._imap_enabled(normalized_domain):
            if mail_from_value:
                self._remember_last_mail_from(normalized_domain, mail_from_value)
            if force_imap_check:
                settings = self._imap_settings_for_domain(normalized_domain)
                allowed_latency = sanitize_imap_allowed_latency(settings.get("allowed_latency_seconds"))
                single_delay = sanitize_imap_delay(
                    settings.get("single_delay_seconds"),
                    default=IMAP_DEFAULT_SINGLE_DELAY_SECONDS,
                    minimum=0,
                )
                smtp_context_payload = {
                    "smtp_host": config.get("smtp_host"),
                    "smtp_port": config.get("smtp_port"),
                    "helo": config.get("helo"),
                    "header": config.get("header"),
                }
                self._execute_imap_guard_flow(
                    domain=normalized_domain,
                    job_id=job_id,
                    send_type="single",
                    mail_from=mail_from_value,
                    header_from=header_from_value,
                    has_anchor=False,
                    context_reason=detail_line or status_line,
                    delay_before_check=single_delay,
                    allowed_delay=allowed_latency,
                    smtp_context=smtp_context_payload,
                    force=True,
                    report_probe_failure=True,
                )
        result_payload = {
            "rcpt_to": rcpt_to,
            "domain": domain,
            "summary": status_line,
            "detail": detail_line,
            "bcc": bcc_emails,
            "delivery_status": delivery_status,
            "logs": dispatch_logs,
            "nouser_total": nouser_count,
            "nouser_emails": nouser_emails_display,
            "data_response": data_response,
        }
        error_message = None if session_success else (detail_line or status_line or "발송 실패")
        current_sequence_total = max(0, int(self.sent_sequences.get(sequence_domain, 0)))
        primary_log = (
            dispatch_logs[0]["log"]
            if dispatch_logs
            else self._format_dispatch_log_line(
                "Fail",
                self._sent_log_progress(sequence_domain, current_sequence_total),
                current_sequence_total,
                include_anchor=False,
            )
        )
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

        self._initialize_sent_log_base(normalized)

        config = payload.get("config") or {}
        session_count = max(1, self._sanitize_session_count(config.get("session_count")))
        bcc_count = self._sanitize_bcc_count(config.get("bcc_count"))
        group_size = max(1, 1 + bcc_count)
        anchor_interval = self._sanitize_anchor_interval(config.get("anchor_interval"))
        reserved_timeout_seconds = self._sanitize_reserved_timeout(config.get("reserved_timeout_seconds"))
        mail_from_value = self._effective_mail_from(normalized, config)
        header_from_value = self._extract_header_from(config.get("header"), mail_from_value)
        settings = self._imap_settings_for_domain(normalized)
        current_sent_counter = self._visible_sent_counter(normalized)
        self._set_sent_counter(normalized, current_sent_counter)
        if current_sent_counter <= 0 and self._get_last_threshold_multiple(normalized) != 0:
            self._set_last_threshold_multiple(normalized, 0)
        if self._imap_enabled(normalized):
            threshold_snapshot = self._current_sent_threshold(normalized)
            last_multiple_snapshot = self._get_last_threshold_multiple(normalized)
            next_target_snapshot: Optional[int]
            remaining_snapshot: Optional[int]
            if threshold_snapshot > 0:
                next_target_snapshot = threshold_snapshot * (last_multiple_snapshot + 1)
                remaining_snapshot = max(0, next_target_snapshot - current_sent_counter)
            else:
                next_target_snapshot = None
                remaining_snapshot = None
            last_reset_snapshot = str(settings.get("sent_last_reset_at") or "-")
            with self._imap_protection_lock:
                throttle_snapshot = self._imap_throttle_flags.get(normalized)
            throttle_line = "최근 제한 감지: 없음"
            if throttle_snapshot:
                marker = (throttle_snapshot.get("marker") or "-").strip() or "-"
                detail = (throttle_snapshot.get("detail") or "-").strip() or "-"
                recorded = throttle_snapshot.get("recorded_at") or "-"
                throttle_line = f"최근 제한 감지: {marker} / {detail} / {recorded}"
            summary_lines = [
                f"임계 간격: {threshold_snapshot}건" if threshold_snapshot > 0 else "임계 간격: 비활성 (0건)",
                f"현재 누적: {current_sent_counter}건",
                f"마지막 배수: {last_multiple_snapshot}",
                f"다음 배수: {next_target_snapshot}건 (잔여 {remaining_snapshot}건)" if next_target_snapshot is not None else "다음 배수: -",
                f"마지막 리셋: {last_reset_snapshot}",
                throttle_line,
            ]
            self._emit_imap_section("IMAP 시작 상태", summary_lines, domain=normalized)
        threshold_check_request: Optional[Dict[str, object]] = None
        threshold_deferred_notice = False
        anchor_email = self._sanitize_anchor_email(config.get("anchor_email"))
        anchor_enabled = bool(anchor_interval and anchor_email)

        processed = 0
        sent_count = 0
        sent_reset_offset = 0
        block_count = 0
        failed_count = 0
        last_error: Optional[str] = None
        bcc_processed = 0
        anchor_processed = 0
        anchor_retry_pending = 0
        nouser_count = 0
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
            visible_sent = max(0, sent_count - sent_reset_offset)
            return {
                "processed": processed,
                "sent": visible_sent,
                "sent_sequence": absolute_sent,
                "sent_absolute": absolute_sent,
                "failed": failed_count,
                "block": block_count,
                "bcc": bcc_processed,
                "anchor": anchor_processed,
                "nouser": nouser_count,
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
            nouser_total = int(summary_snapshot.get("nouser") or 0)
            remaining_total = int(summary_snapshot.get("remaining") or 0)
            cycles_completed = int(summary_snapshot.get("cycles_completed") or 0)
            message = (
                f"{prefix} 처리={processed_count} 성공={sent_total} "
                f"실패={failed_total} 차단={block_total} BCC={bcc_total} "
                f"알박기={anchor_total} NoUser={nouser_total} 잔여={remaining_total}"
            )
            if absolute_sent:
                message = f"{message} · 누적={absolute_sent}"
            if cycles_completed > 0:
                message = f"{message} · 누적 {cycles_completed}번째 순환 완료"
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
            nonlocal last_poll_at, sent_reset_offset, current_sent_counter
            now_point = time.monotonic()
            if not force and now_point - last_poll_at < poll_interval:
                return
            response: Optional[Dict[str, object]] = None
            try:
                if self.offline_mode:
                    response = self._probe_server_if_due([])
                    if response is None:
                        last_poll_at = now_point
                        return
                else:
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
            inline_jobs: List[Dict[str, object]] = []
            queued_jobs: List[Dict[str, object]] = []
            if isinstance(new_jobs, list):
                for job in new_jobs:
                    if not isinstance(job, dict):
                        continue
                    job_type = str(job.get("job_type") or "")
                    if job_type == "reset_sent_sequence":
                        inline_jobs.append(job)
                    else:
                        queued_jobs.append(job)
            for inline in inline_jobs:
                job_id = str(inline.get("job_id") or "").strip()
                payload = inline.get("payload") or {}
                inline_domain = inline.get("domain")
                if job_id:
                    self._pending_job_ids.discard(job_id)
                    self._active_job_ids.add(job_id)
                    self._active_jobs[job_id] = "reset_sent_sequence"
                    self._cancel_ack_sent.discard(job_id)
                try:
                    self.send_job_report(
                        JobResult(job_id=job_id, status="running", message="작업 시작")
                    )
                except Exception:
                    pass
                try:
                    result = self.handle_reset_sent_sequence(inline_domain, payload, job_id or "")
                    target_domains: Set[str] = set()
                    if isinstance(result.result, dict):
                        domains_value = result.result.get("domains")
                        if isinstance(domains_value, (list, tuple, set)):
                            target_domains = {
                                str(candidate).lower()
                                for candidate in domains_value
                                if candidate
                            }
                    if not target_domains and inline_domain:
                        target_domains = {str(inline_domain).lower()}
                    if normalized in target_domains:
                        sent_reset_offset = sent_count
                        current_sent_counter = self._visible_sent_counter(normalized)
                        self._set_sent_counter(normalized, current_sent_counter)
                        emit_progress(force=True)
                except Exception as exc:  # pylint: disable=broad-except
                    message = f"발송 로그 초기화 처리 중 오류: {exc}"
                    print(f"[경고] {message}")
                    result = JobResult(
                        job_id=job_id or "",
                        status="failed",
                        message=message,
                        error=str(exc),
                    )
                try:
                    domain_states = self.collect_domain_states()
                    self.send_job_report(result, domain_states)
                except Exception:  # pylint: disable=broad-except
                    pass
                finally:
                    if job_id:
                        self._active_job_ids.discard(job_id)
                        self._active_jobs.pop(job_id, None)
                    if job_id:
                        self.job_controls.pop(job_id, None)
                        self._cancel_ack_sent.discard(job_id)
                        self._pending_job_ids.discard(job_id)
            if queued_jobs:
                self._queue_jobs(queued_jobs)
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
                    pending_count = int(summary_row["pending_count"] or 0)
                    reserved_count = int(summary_row["reserved_count"] or 0)
                    block_count = int(summary_row["block_count"] or 0)
                    sent_total = int(summary_row["sent_count"] or 0)
                    failed_total = int(summary_row["failed_count"] or 0)
                    released_reserved = 0
                    reserved_only = pending_count == 0 and block_count == 0 and reserved_count > 0

                    def release_reserved(force: bool = False) -> int:
                        if not force and inflight:
                            return 0
                        now_stamp_local = now_iso()
                        if force:
                            cursor = conn.execute(
                                """
                                UPDATE emails
                                SET status='pending',
                                    reserved_by=NULL,
                                    reserved_at=NULL,
                                    updated_at=?
                                WHERE status='reserved'
                                """,
                                (now_stamp_local,),
                            )
                        else:
                            cutoff_dt = datetime.now() - timedelta(seconds=reserved_timeout_seconds)
                            cutoff_iso = cutoff_dt.strftime("%Y-%m-%dT%H:%M:%S")
                            cursor = conn.execute(
                                """
                                UPDATE emails
                                SET status='pending',
                                    reserved_by=NULL,
                                    reserved_at=NULL,
                                    updated_at=?
                                WHERE status='reserved'
                                  AND (
                                    reserved_by IS NULL
                                    OR reserved_by != ?
                                    OR reserved_at IS NULL
                                    OR reserved_at <= ?
                                  )
                                """,
                                (now_stamp_local, session_token, cutoff_iso),
                            )
                        conn.commit()
                        return cursor.rowcount or 0

                    if reserved_only:
                        released_reserved = release_reserved()
                        if released_reserved == 0 and not inflight:
                            released_reserved = release_reserved(force=True)
                        if released_reserved == 0:
                            return False
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
                        pending_count = int(summary_row["pending_count"] or 0)
                        reserved_count = int(summary_row["reserved_count"] or 0)
                        block_count = int(summary_row["block_count"] or 0)
                        sent_total = int(summary_row["sent_count"] or 0)
                        failed_total = int(summary_row["failed_count"] or 0)
                    elif pending_count + reserved_count + block_count > 0:
                        return False

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
                    refreshed_counts = {status: 0 for status in EMAIL_STATUSES}
                    for row in conn.execute(
                        "SELECT status, COUNT(*) AS cnt FROM emails GROUP BY status"
                    ):
                        key = row["status"]
                        refreshed_counts[key] = row["cnt"] or 0
                    domain_totals.update(refreshed_counts)
                    cycle_entry = self._record_cycle_completion(normalized, reset_total)
                    cycle_count = int(cycle_entry.get("cycles", 0) or 0)
                    cycle_label = f"{cycle_count}번째 순환 완료" if cycle_count > 0 else "순환 완료"
                    base_message = f"{domain_label} 메일 재고 소진 · {cycle_label}"
                    detail_parts = [f"sent {sent_total}건, failed {failed_total}건을 pending으로 전환"]
                    if released_reserved > 0:
                        detail_parts.append(f"reserved {released_reserved}건 재큐잉")
                    restart_message = f"{base_message} · {' · '.join(detail_parts)}"
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
                    injected_emails = [email for email in (group.injected or []) if email]
                    bcc_recipients = [item.email for item in group.bcc if item.email]
                    payload_bcc: List[str] = []
                    if injected_emails:
                        payload_bcc.extend(injected_emails)
                    payload_bcc.extend(bcc_recipients)
                    success, response_text, completed_at, rcpt_details, data_response = send_via_telnet(
                        smtp_host=config.get("smtp_host", ""),
                        smtp_port=int(config.get("smtp_port") or 25),
                        helo=config.get("helo", ""),
                        mail_from=mail_from_value,
                        rcpt_to=rcpt_to,
                        header_text=config.get("header", ""),
                        bcc_emails=payload_bcc,
                        anchor_emails=injected_emails,
                        debug=self.telnet_debug_mode,
                    )
                    response_text = response_text or ""
                    sent_at = completed_at if isinstance(completed_at, datetime) else utc_now()
                    delivery_status = self._classify_delivery(success, response_text)
                    status_line, detail_line = self._smtp_status_and_detail(response_text)
                    return DispatchOutcome(
                        success=success,
                        response_text=response_text,
                        delivery_status=delivery_status,
                        status_line=status_line,
                        detail_line=detail_line,
                        sent_at=sent_at,
                        rcpt_details=rcpt_details,
                        data_response_code=str((data_response or {}).get("code", "")),
                        data_response_message=(data_response or {}).get("message"),
                    )

                def prepare_group_for_dispatch(group: DispatchGroup) -> DispatchGroup:
                    nonlocal dispatched_db_total, anchor_retry_pending, pending_queue
                    if not group:
                        return group
                    group.injected = []
                    max_total = max(0, int(SMTP_RECIPIENT_LIMIT))
                    if max_total > 0:
                        max_bcc_allowed = max(0, max_total - 1)
                        if len(group.bcc) > max_bcc_allowed:
                            overflow = len(group.bcc) - max_bcc_allowed
                            overflow_items: List[DispatchEmail] = []
                            for _ in range(overflow):
                                overflow_items.append(group.bcc.pop())
                            for item in overflow_items:
                                pending_queue.appendleft(item)
                            group.deferred_bcc += overflow
                    start_total = dispatched_db_total
                    group_db_size = 1 + len(group.bcc)
                    end_total = start_total + group_db_size
                    if not anchor_enabled or anchor_interval <= 0 or not anchor_email:
                        anchor_retry_pending = 0
                        dispatched_db_total = end_total
                        return group
                    anchors_from_interval = max(
                        0,
                        (end_total // anchor_interval) - (start_total // anchor_interval),
                    )
                    anchor_required = max(anchor_retry_pending, anchors_from_interval)
                    max_anchor_slots = anchor_required
                    if max_total > 0:
                        max_anchor_slots = max(0, max_total - group_db_size)
                    if max_total > 0 and anchor_required > 0 and max_anchor_slots == 0 and group.bcc:
                        slots_to_free = min(anchor_required, len(group.bcc), max_total - 1)
                        overflow_items: List[DispatchEmail] = []
                        for _ in range(slots_to_free):
                            overflow_items.append(group.bcc.pop())
                        for item in overflow_items:
                            pending_queue.appendleft(item)
                        group.deferred_bcc += len(overflow_items)
                        group_db_size = 1 + len(group.bcc)
                        end_total = start_total + group_db_size
                        anchors_from_interval = max(
                            0,
                            (end_total // anchor_interval) - (start_total // anchor_interval),
                        )
                        anchor_required = max(anchor_retry_pending, anchors_from_interval)
                        max_anchor_slots = max(0, max_total - group_db_size)
                    anchor_to_send = min(anchor_required, max_anchor_slots)
                    if anchor_to_send > 0:
                        group.injected = [anchor_email] * anchor_to_send
                    anchor_retry_pending = max(0, anchor_required - anchor_to_send)
                    dispatched_db_total = end_total
                    return group

                def register_sent_success(sent_at: datetime, detail_text: Optional[str], increment: int) -> None:
                    nonlocal current_sent_counter, threshold_check_request
                    if increment <= 0:
                        return
                    if not self._imap_enabled(normalized):
                        return
                    if mail_from_value:
                        self._remember_last_mail_from(normalized, mail_from_value)
                    visible_sent = max(0, sent_count - sent_reset_offset)
                    visible_from_sequence = self._visible_sent_counter(normalized)
                    if visible_from_sequence > visible_sent:
                        visible_sent = visible_from_sequence
                    current_sent_counter = visible_sent
                    settings_local = self._imap_settings_for_domain(normalized)
                    threshold_value = self._current_sent_threshold(normalized)
                    self._set_sent_counter(normalized, current_sent_counter)
                    if threshold_value <= 0:
                        return
                    last_multiple = max(0, self._get_last_threshold_multiple(normalized))
                    next_multiple = last_multiple + 1
                    target_value = threshold_value * next_multiple
                    if current_sent_counter < target_value:
                        return
                    if threshold_check_request is not None:
                        return
                    raw_multiple = current_sent_counter // max(1, threshold_value)
                    current_multiple = max(next_multiple, raw_multiple if raw_multiple > 0 else 1)
                    session_success = current_sent_counter
                    global_success = max(0, int(self.sent_sequences.get(normalized, 0)))
                    self._emit_imap_section(
                        "IMAP 임계치 도달",
                        [f"배수 {next_multiple} 도달 · 현재 성공 {session_success}건"],
                        domain=normalized,
                    )
                    threshold_check_request = {
                        "sent_at": sent_at,
                        "detail": detail_text,
                        "mail_from": mail_from_value,
                        "allowed_latency": sanitize_imap_allowed_latency(
                            settings_local.get("allowed_latency_seconds"),
                            default=IMAP_DEFAULT_ALLOWED_LATENCY_SECONDS,
                        ),
                        "single_delay": sanitize_imap_delay(
                            settings_local.get("single_delay_seconds"),
                            default=IMAP_DEFAULT_SINGLE_DELAY_SECONDS,
                            minimum=0,
                        ),
                        "threshold": threshold_value,
                        "sent_count": session_success,
                        "sequence_total": global_success,
                        "username": normalize_imap_string(settings_local.get("username")),
                        "header_from": header_from_value,
                        "target_multiple": next_multiple,
                        "current_multiple": current_multiple,
                        "reason": f"threshold reached ({target_value})",
                        "smtp": {
                            "smtp_host": config.get("smtp_host"),
                            "smtp_port": config.get("smtp_port"),
                            "helo": config.get("helo"),
                            "header": config.get("header"),
                        },
                    }

                def ensure_threshold_check() -> None:
                    nonlocal threshold_check_request, current_sent_counter, threshold_deferred_notice, stop_requested, fatal_error, stop_reason
                    if not threshold_check_request:
                        if threshold_deferred_notice:
                            threshold_deferred_notice = False
                        return
                    if inflight:
                        if not threshold_deferred_notice:
                            self._emit_imap_section(
                                "IMAP 임계 대기",
                                [f"진행 중 발송 {len(inflight)}건 정리 중"],
                                domain=normalized,
                            )
                            threshold_deferred_notice = True
                        return
                    if threshold_deferred_notice:
                        threshold_deferred_notice = False
                    if not self._imap_enabled(normalized):
                        threshold_check_request = None
                        return
                    request = threshold_check_request
                    threshold_check_request = None
                    try:
                        threshold_value = int(request.get("threshold") or self._current_sent_threshold(normalized))
                    except (TypeError, ValueError):
                        threshold_value = self._current_sent_threshold(normalized)
                    threshold_value = max(0, threshold_value)
                    if threshold_value <= 0:
                        return
                    try:
                        target_multiple = int(request.get("target_multiple") or 1)
                    except (TypeError, ValueError):
                        target_multiple = 1
                    target_multiple = max(1, target_multiple)
                    try:
                        session_success = int(request.get("sent_count") or current_sent_counter)
                    except (TypeError, ValueError):
                        session_success = current_sent_counter
                    try:
                        global_success = int(request.get("sequence_total") or self.sent_sequences.get(normalized, 0))
                    except (TypeError, ValueError):
                        global_success = int(self.sent_sequences.get(normalized, 0) or 0)
                    reason_text = str(request.get("reason") or f"threshold reached ({threshold_value * target_multiple})")
                    self._emit_imap_section(
                        "Sent 중단",
                        [f"누적 {session_success}건 · 임계 {threshold_value}건"],
                        domain=normalized,
                    )
                    settings_snapshot = self._imap_settings_for_domain(normalized)
                    allowed_latency_value = sanitize_imap_allowed_latency(
                        request.get("allowed_latency"),
                        default=settings_snapshot.get("allowed_latency_seconds"),
                    )
                    delay_value = sanitize_imap_delay(
                        request.get("single_delay"),
                        default=settings_snapshot.get("single_delay_seconds"),
                        minimum=0,
                    )
                    failure_action_value = sanitize_imap_failure_action(settings_snapshot.get("failure_action"))
                    failure_label = failure_action_value
                    if failure_label in {"stop_all", "stop_device"}:
                        failure_label = f"{failure_label} + ip_change"
                    username_candidate = normalize_imap_recipient(
                        normalized,
                        request.get("username") or settings_snapshot.get("username"),
                    )
                    self._emit_imap_section(
                        "IMAP 설정",
                        [
                            f"계정: {username_candidate or '-'}",
                            f"확인 간격: {threshold_value}건",
                            f"확인 대기: {delay_value}초",
                            f"허용 지연: {allowed_latency_value}초",
                            f"실패 대응: {failure_label}",
                        ],
                        domain=normalized,
                    )
                    self._emit_imap_section(
                        "IMAP 확인 대기",
                        [f"현재 성공: {session_success}건 / 누적 성공: {global_success}건"],
                        domain=normalized,
                    )
                    self._emit_imap_section(
                        "IMAP 확인 시도",
                        [f"사유: {reason_text}"],
                        domain=normalized,
                    )
                    mail_from_candidate = str(request.get("mail_from") or mail_from_value)
                    raw_header_from = request.get("header_from")
                    if isinstance(raw_header_from, str) and raw_header_from.strip():
                        header_from_candidate = raw_header_from.strip()
                    elif header_from_value:
                        header_from_candidate = str(header_from_value).strip() or None
                    else:
                        header_from_candidate = None
                    smtp_context = request.get("smtp")
                    guard_outcome, report_payload = self._guard_sent_threshold(
                        domain=normalized,
                        job_id=job_id,
                        request=request,
                        send_type="sent-threshold",
                        mail_from=mail_from_candidate,
                        header_from=header_from_candidate,
                        has_anchor=False,
                        context_reason=str(request.get("detail") or "Sent 기준 확인"),
                        delay_before_check=float(delay_value),
                        allowed_delay=int(allowed_latency_value),
                        smtp_context=smtp_context,
                        force=False,
                        counter_mode="threshold",
                        counter_current=current_sent_counter,
                        counter_threshold=threshold_value,
                        sent_window_count=current_sent_counter,
                        report_probe_failure=True,
                    )
                    if guard_outcome.sent_window_count is not None:
                        current_sent_counter = guard_outcome.sent_window_count
                    if report_payload is None:
                        self._emit_imap_section(
                            "IMAP 확인 실패",
                            ["IMAP 확인 작업을 실행하지 못했습니다."],
                            domain=normalized,
                        )
                        return
                    status_value = str(report_payload.get("status") or "")
                    latency_value = report_payload.get("latency")
                    if status_value == "success":
                        refreshed_counter = self._visible_sent_counter(normalized)
                        if refreshed_counter < current_sent_counter:
                            refreshed_counter = current_sent_counter
                        current_sent_counter = refreshed_counter
                        current_multiple = max(1, current_sent_counter // max(1, threshold_value))
                        self._set_last_threshold_multiple(normalized, current_multiple)
                        self._set_sent_counter(normalized, current_sent_counter, reset_timestamp=utc_now_iso())
                        def _format_latency(value: Optional[object]) -> str:
                            if value is None:
                                return "-"
                            try:
                                latency_float = float(value)
                                if latency_float.is_integer():
                                    return f"{int(latency_float)}초"
                                return f"{latency_float:.1f}초"
                            except (TypeError, ValueError):
                                return str(value)
                        self._emit_imap_section(
                            "IMAP 확인 성공",
                            [f"IMAP latency: {_format_latency(latency_value)} / 재개"],
                            domain=normalized,
                        )
                    elif status_value == "network_error":
                        fatal_error = report_payload.get("reason") or "IMAP 확인 네트워크 오류"
                        stop_reason = fatal_error
                        stop_requested = True
                        self._emit_imap_section(
                            "IMAP 확인 실패",
                            [fatal_error],
                            domain=normalized,
                        )
                        return
                    else:
                        failure_lines: List[str] = []
                        marker = report_payload.get("ip_change_marker")
                        marker_reason = report_payload.get("ip_change_reason")
                        if marker:
                            marker_line = f"제한 응답: {marker}"
                            if marker_reason and marker_reason.lower() not in marker_line.lower():
                                marker_line = f"{marker_line} ({marker_reason})"
                            failure_lines.append(marker_line)
                        reason_line = report_payload.get("reason")
                        if reason_line:
                            failure_lines.append(str(reason_line))
                        probe_status = report_payload.get("probe_status_line")
                        if probe_status:
                            failure_lines.append(str(probe_status))
                        probe_error = report_payload.get("probe_mail_error")
                        if probe_error and probe_error not in failure_lines:
                            failure_lines.append(str(probe_error))
                        if not failure_lines:
                            failure_lines.append(status_value or "IMAP 확인 실패")
                        self._emit_imap_section("IMAP 확인 실패", failure_lines, domain=normalized)
                        refreshed_counter = self._visible_sent_counter(normalized)
                        if refreshed_counter < current_sent_counter:
                            refreshed_counter = current_sent_counter
                        current_sent_counter = refreshed_counter
                        self._set_sent_counter(normalized, current_sent_counter)
                    next_multiple = self._get_last_threshold_multiple(normalized) + 1
                    next_target = threshold_value * next_multiple if threshold_value > 0 else 0
                    remaining = max(0, next_target - current_sent_counter) if next_target else 0
                    if next_target > 0:
                        next_line = f"다음 배수 {next_target}건 (잔여 {remaining}건)"
                    else:
                        next_line = "다음 배수 없음"
                    self._emit_imap_section("IMAP 다음 배수", [next_line], domain=normalized)
                def process_future(future: Future, group: DispatchGroup) -> None:
                    nonlocal processed, sent_count, block_count, failed_count, last_error
                    nonlocal stop_requested, fatal_error, stop_reason, bcc_processed, anchor_processed
                    nonlocal anchor_retry_pending, current_sent_counter, nouser_count
                    try:
                        outcome = future.result()
                    except Exception as exc:  # pylint: disable=broad-except
                        outcome = DispatchOutcome(
                            success=False,
                            response_text=f"ERROR: {exc}",
                            delivery_status="failed",
                            status_line="예외 발생",
                            detail_line=str(exc),
                            rcpt_details=None,
                        )
                    recipients = [group.primary] + group.bcc
                    previous_statuses = [item.previous_status for item in recipients]
                    bcc_processed += len(group.bcc)
                    now_stamp = now_iso()
                    detail_for_log = outcome.detail_line or outcome.status_line
                    lower_response = (outcome.response_text or "").lower()
                    status_lower = (outcome.status_line or "").lower()
                    detail_lower = (detail_for_log or "").lower()
                    text_sources = (lower_response, status_lower, detail_lower)
                    matched_marker: Optional[str] = None
                    matched_message: Optional[str] = None
                    for text in text_sources:
                        if not text:
                            continue
                        for marker, message in THROTTLE_MARKER_MESSAGES.items():
                            if marker in text:
                                matched_marker = marker
                                matched_message = message
                                break
                        if matched_marker:
                            break
                    if matched_marker is None:
                        for text in text_sources:
                            if not text:
                                continue
                            for marker, message in FATAL_STOP_MARKER_MESSAGES.items():
                                if marker in text:
                                    matched_marker = marker
                                    matched_message = message
                                    break
                            if matched_marker:
                                break
                    throttle_detected = matched_marker in THROTTLE_MARKER_MESSAGES if matched_marker else False
                    recipient_limit_detected = False
                    recipient_limit_message: Optional[str] = None
                    for text in text_sources:
                        if text and any(marker in text for marker in RECIPIENT_LIMIT_MARKERS):
                            recipient_limit_detected = True
                            break
                    if not recipient_limit_detected and outcome.rcpt_details:
                        for entry in outcome.rcpt_details:
                            message_text = str(entry.get("message") or "").lower()
                            if message_text and any(marker in message_text for marker in RECIPIENT_LIMIT_MARKERS):
                                recipient_limit_detected = True
                                break
                    if throttle_detected and self._imap_enabled(normalized):
                        throttle_label = matched_message or f"SMTP 제한 응답 {matched_marker}"
                        self._log_imap_protection(f"SMTP 제한 응답 감지 · {normalized} · {throttle_label}")
                        self._record_imap_throttle(normalized, matched_marker, throttle_label or detail_for_log)
                    if recipient_limit_detected:
                        recipient_limit_message = detail_for_log or outcome.status_line or "수신자 수 초과 응답 감지"
                        outcome.delivery_status = "failed"
                        throttle_detected = False
                        matched_message = recipient_limit_message
                    error_text = None if outcome.delivery_status == "sent" else (detail_for_log or outcome.status_line or "")[-500:]
                    group_size_actual = len(recipients)
                    recipient_emails = [record.email for record in recipients]
                    bcc_count = len(group.bcc)
                    anchor_count = len(group.injected)
                    nouser_map = self._extract_nouser_map(outcome.rcpt_details)
                    nouser_emails_display = [
                        record.email
                        for record in recipients
                        if self._normalize_email_key(record.email) in nouser_map
                    ]
                    data_ok = self._is_data_accepted(
                        outcome.data_response_code,
                        outcome.data_response_message,
                    )
                    recipient_keys = [self._normalize_email_key(record.email) for record in recipients]
                    db_nouser_flags = [key in nouser_map for key in recipient_keys]
                    db_nouser_count = sum(1 for flag in db_nouser_flags if flag)
                    nouser_count += db_nouser_count
                    anchor_success_count = anchor_count if data_ok else 0
                    anchor_retry_count = anchor_count - anchor_success_count
                    db_successful_recipient_count = 0

                    for prev_status in previous_statuses:
                        status_key = (prev_status or "pending").lower()
                        domain_totals[status_key] = max(0, domain_totals.get(status_key, 0) - 1)

                    for record, key, is_nouser in zip(recipients, recipient_keys, db_nouser_flags):
                        if is_nouser:
                            persist_status = "nouser"
                            last_error_value = (nouser_map.get(key) or "550 5.1.1 No such user")[:500]
                        elif throttle_detected or outcome.delivery_status == "block":
                            persist_status = "pending"
                            last_error_value = error_text
                        elif data_ok:
                            persist_status = "sent"
                            last_error_value = None
                            db_successful_recipient_count += 1
                        else:
                            persist_status = "failed"
                            last_error_value = error_text
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
                                last_error_value,
                                record.id,
                            ),
                        )
                        domain_totals[persist_status] = domain_totals.get(persist_status, 0) + 1
                    conn.commit()

                    processed += group_size_actual
                    success_increment = db_successful_recipient_count + anchor_success_count
                    session_success = data_ok and success_increment > 0
                    delivery_status_payload = (
                        "sent"
                        if session_success
                        else ("throttle" if throttle_detected else outcome.delivery_status)
                    )
                    failure_detail = detail_for_log or outcome.status_line or "응답 없음"

                    if session_success:
                        sent_count += db_successful_recipient_count
                        sequence_total = self._next_sent_sequence(normalized, success_increment)
                    else:
                        sequence_total = max(0, int(self.sent_sequences.get(normalized, 0)))
                        effective_failed = max(0, group_size_actual - db_nouser_count)
                        if outcome.delivery_status == "block":
                            block_count += effective_failed
                        else:
                            failed_count += effective_failed
                        last_error = failure_detail

                    if session_success and normalized == "naver":
                        register_sent_success(outcome.sent_at, detail_for_log, success_increment)
                    elif not session_success and normalized == "naver":
                        current_sent_counter = max(0, sent_count - sent_reset_offset)
                        self._set_sent_counter(normalized, current_sent_counter)

                    if anchor_count:
                        if session_success:
                            anchor_processed += anchor_success_count
                        elif anchor_retry_count > 0:
                            anchor_retry_pending += anchor_retry_count

                    current_batch_success = self._sent_log_progress(normalized, sequence_total)
                    log_line = self._format_dispatch_log_line(
                        "Sent" if session_success else "Fail",
                        current_batch_success,
                        sequence_total,
                        include_anchor=anchor_count > 0,
                    )
                    dispatch_logs: List[Dict[str, object]] = [
                        {
                            "log": log_line,
                            "display": log_line,
                            "email": recipient_emails[0] if recipient_emails else None,
                            "sequence": sequence_total,
                            "delivery_status": delivery_status_payload,
                            "detail": detail_for_log,
                            "bcc_total": bcc_count,
                            "anchor_total": anchor_count,
                            "is_primary": True,
                            "bcc_recipients": [record.email for record in group.bcc],
                            "anchor": list(group.injected),
                            "anchor_success": anchor_success_count,
                            "anchor_retry": anchor_retry_count,
                            "deferred_bcc": group.deferred_bcc,
                            "failed_recipients": [
                                record.email
                                for record, is_nouser in zip(recipients, db_nouser_flags)
                                if not session_success and not is_nouser
                            ],
                            "rcpt_details": outcome.rcpt_details,
                            "nouser_total": db_nouser_count,
                            "nouser_emails": nouser_emails_display,
                            "tags": (["nouser"] if db_nouser_count else []),
                        }
                    ]
                    print(log_line)
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
                                    ensure_threshold_check()
                                    if threshold_check_request is not None or self._is_sent_guard_paused(normalized):
                                        break
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
                                ensure_threshold_check()
                                if fatal_error and not inflight:
                                    break
                                check_schedule_trigger()
                                maybe_poll_updates()
                                continue
                            check_schedule_trigger()
                            ensure_threshold_check()
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
                            ensure_threshold_check()
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
        if self._imap_settings_dirty:
            self.persist()
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

    def _log_imap_console(self, message: str, domain: Optional[str] = None) -> None:
        if domain and not self._imap_enabled(domain):
            return
        print(f"[IMAP 테스트] {message}", flush=True)

    def _log_imap_protection(self, message: str) -> None:
        # 보호 전용 로그는 콘솔에 노출하지 않습니다.
        return

    def _emit_imap_section(
        self,
        label: str,
        lines: Iterable[str],
        *,
        domain: Optional[str] = None,
    ) -> None:
        if domain and not self._imap_enabled(domain):
            return
        for line in lines:
            if not line:
                continue
            print(f"[{label}] {line}", flush=True)

    def handle_imap_test(self, domain: Optional[str], payload: Dict[str, object], job_id: str) -> JobResult:
        normalized = (domain or "naver").lower()
        if normalized != "naver":
            message = "네이버 도메인에서만 IMAP 테스트를 지원합니다."
            self._log_imap_console(message, domain=normalized)
            return JobResult(job_id=job_id, status="failed", message=message, error=message)
        payload = payload or {}
        username = normalize_imap_string(payload.get("username"))
        settings = self._imap_settings_for_domain(normalized)
        if not bool(settings.get("enabled")):
            message = "IMAP 확인이 비활성화되어 있어 테스트를 실행하지 않습니다."
            return JobResult(job_id=job_id, status="failed", message=message, error=message)
        if not username:
            username = normalize_imap_string(settings.get("username"))
        if not username:
            message = "IMAP 계정 ID가 설정되지 않았습니다."
            self._log_imap_console(message, domain=normalized)
            return JobResult(job_id=job_id, status="failed", message=message, error=message)
        use_saved_password = bool(payload.get("use_saved_password"))
        password = str(payload.get("password") or "")
        used_saved_password = False
        if not password and use_saved_password:
            password = settings.get("password") or ""
            used_saved_password = bool(password)
        if not password:
            message = "IMAP 비밀번호를 확인할 수 없습니다."
            self._log_imap_console(message, domain=normalized)
            return JobResult(job_id=job_id, status="failed", message=message, error=message)
        folder = str(payload.get("folder") or "Junk").strip() or "Junk"
        self._log_imap_console(f"계정 {username} · 폴더 {folder} · 연결 시도", domain=normalized)
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
            self._log_imap_console(f"성공 - {message}", domain=normalized)
            return JobResult(job_id=job_id, status="success", message=message, result=result_payload)
        error_message = reason or "IMAP 연결 실패"
        self._log_imap_console(f"실패 - {error_message}", domain=normalized)
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
            self._log_imap_console(message, domain=normalized)
            return JobResult(job_id=job_id, status="failed", message=message, error=message)
        payload = payload or {}
        settings = self._imap_settings_for_domain(normalized)
        if not bool(settings.get("enabled")):
            message = "IMAP 확인이 비활성화되어 있어 최신 메일 확인을 실행하지 않습니다."
            return JobResult(job_id=job_id, status="failed", message=message, error=message)
        username = normalize_imap_string(payload.get("username")) or normalize_imap_string(
            settings.get("username")
        )
        if not username:
            message = "IMAP 계정 ID가 설정되지 않았습니다."
            self._log_imap_console(message, domain=normalized)
            return JobResult(job_id=job_id, status="failed", message=message, error=message)
        use_saved_password = bool(payload.get("use_saved_password"))
        password = str(payload.get("password") or "")
        used_saved_password = False
        if not password and use_saved_password:
            password = settings.get("password") or ""
            used_saved_password = bool(password)
        if not password:
            message = "IMAP 비밀번호를 확인할 수 없습니다."
            self._log_imap_console(message, domain=normalized)
            return JobResult(job_id=job_id, status="failed", message=message, error=message)
        folder = str(payload.get("folder") or "Junk").strip() or "Junk"
        try:
            limit_candidate = payload.get("limit")
            limit_value = int(limit_candidate) if limit_candidate is not None else 1
        except (TypeError, ValueError):
            limit_value = 1
        limit_value = max(1, min(10, limit_value))
        self._log_imap_console(f"계정 {username} · 폴더 {folder} · 최신 메일 확인", domain=normalized)
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
            f"저장비번 {used_saved}",
            domain=normalized,
        )
        if mail_info:
            self._log_imap_console(
                "  ↳ 헤더 비교 · "
                f"From {mail_info.get('from') or mail_info.get('from_address') or '-'} · "
                f"Subject {mail_info.get('subject') or '-'} · "
                f"수신 {mail_info.get('received_at_iso') or mail_info.get('date_header') or '-'}",
                domain=normalized,
            )
        if reason:
            self._log_imap_console(f"  ↳ 실패 사유: {reason}", domain=normalized)

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
            self._log_imap_console(f"성공 - {message}", domain=normalized)
            return JobResult(job_id=job_id, status="success", message=message, result=result_payload)

        error_message = reason or "최신 메일을 가져오지 못했습니다."
        self._log_imap_console(f"실패 - {error_message}", domain=normalized)
        return JobResult(
            job_id=job_id,
            status="failed",
            message=error_message,
            result=result_payload,
            error=error_message,
        )

    def handle_imap_manual_check(self, domain: Optional[str], payload: Dict[str, object], job_id: str) -> JobResult:
        if not domain:
            message = "도메인 정보가 없어 수동 도착 확인을 실행할 수 없습니다."
            return JobResult(job_id=job_id, status="failed", message=message, error=message)
        normalized = domain.lower()
        config_payload = payload.get("config") or {}
        if not isinstance(config_payload, dict):
            message = "SMTP 설정이 비어 있어 수동 도착 확인을 실행할 수 없습니다."
            return JobResult(job_id=job_id, status="failed", message=message, error=message)
        if not self._imap_enabled(normalized):
            message = "IMAP 확인이 비활성화되어 있어 수동 도착 확인을 실행하지 않습니다."
            return JobResult(job_id=job_id, status="failed", message=message, error=message)
        mail_from_value = self._effective_mail_from(normalized, config_payload)
        if mail_from_value:
            self._remember_last_mail_from(normalized, mail_from_value)
        header_from_value = self._extract_header_from(config_payload.get("header"), mail_from_value)
        context_reason = str(payload.get("context_reason") or "사용자 수동 도착 확인")
        smtp_context = {
            "smtp_host": config_payload.get("smtp_host"),
            "smtp_port": config_payload.get("smtp_port"),
            "helo": config_payload.get("helo"),
            "header": config_payload.get("header"),
        }
        settings_snapshot = self._imap_settings_for_domain(normalized)
        single_delay_seconds = sanitize_imap_delay(
            settings_snapshot.get("single_delay_seconds"),
            default=IMAP_DEFAULT_SINGLE_DELAY_SECONDS,
            minimum=0,
        )
        hold_started_at_iso = utc_now_iso()
        hold_token = f"imap-hold-{uuid.uuid4().hex}"
        hold_message = (
            f"확인 메일 발송 중 · 약 {single_delay_seconds}초 뒤 검사 예정"
            if single_delay_seconds > 0
            else "확인 메일 발송 중"
        )
        running_result: Dict[str, object] = {
            "phase": "waiting_before_imap_check",
            "delay_seconds": single_delay_seconds,
            "context_reason": context_reason,
            "hold": {
                "token": hold_token,
                "kind": "single",
                "estimated_delay_seconds": single_delay_seconds,
                "started_at": hold_started_at_iso,
                "reason": context_reason,
                "message": hold_message,
            },
        }
        if job_id:
            try:
                self.send_job_report(
                    JobResult(
                        job_id=job_id,
                        status="running",
                        message="수동 도착 확인을 준비 중입니다.",
                        result=running_result,
                    )
                )
            except Exception as exc:  # pylint: disable=broad-except
                print(f"[경고] 수동 도착 확인 진행 상태 보고에 실패했습니다: {exc}")
        outcome = self._execute_imap_guard_flow(
            domain=normalized,
            job_id=job_id,
            send_type="manual",
            mail_from=mail_from_value,
            header_from=header_from_value,
            has_anchor=False,
            context_reason=context_reason,
            delay_before_check=None,
            allowed_delay=None,
            smtp_context=smtp_context,
            force=True,
            counter_mode="manual",
            report_probe_failure=True,
        )
        result_payload = {
            "domain": normalized,
            "scheduled": outcome.scheduled,
            "sent_counter": outcome.sent_window_count,
            "sent_threshold": outcome.sent_threshold,
        }
        if outcome.scheduled:
            message = "수동 도착 확인 플로우를 실행했습니다."
            return JobResult(job_id=job_id, status="success", message=message, result=result_payload)
        reason = "확인 메일 발송을 시작하지 못했습니다."
        if outcome.probe:
            reason = outcome.probe.detail_line or outcome.probe.status_line or reason
        return JobResult(job_id=job_id, status="failed", message=reason, error=reason, result=result_payload)

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

    def _initialize_sent_log_base(self, domain: Optional[str]) -> None:
        candidate = (domain or self.active_domain or "naver")
        if not candidate:
            return
        normalized = candidate.lower()
        try:
            current_total = max(0, int(self.sent_sequences.get(normalized, 0)))
        except (TypeError, ValueError):
            current_total = 0
        self._sent_log_bases[normalized] = current_total

    def _sent_log_progress(self, domain: Optional[str], sequence_total: int) -> int:
        normalized = (domain or self.active_domain or "naver").lower()
        base = self._sent_log_bases.get(normalized)
        if base is None:
            try:
                base = max(0, int(self.sent_sequences.get(normalized, 0)))
            except (TypeError, ValueError):
                base = 0
        sequence_value = max(0, int(sequence_total or 0))
        if sequence_value < base:
            base = sequence_value
        self._sent_log_bases[normalized] = base
        return max(0, sequence_value - base)

    def _format_dispatch_log_line(
        self,
        label: str,
        current_batch_success: int,
        accumulated_total: int,
        *,
        include_anchor: bool = False,
    ) -> str:
        safe_label = "Sent" if (label or "").strip().lower() == "sent" else "Fail"
        batch_success = max(0, int(current_batch_success or 0))
        total_value = max(0, int(accumulated_total or 0))
        timestamp = time.strftime("%H:%M:%S", time.localtime())
        device_label = (self.device_name or self.device_id or "-").strip() or "-"
        line = f"{safe_label}({batch_success}/{total_value}) | {timestamp} | {device_label}"
        if include_anchor:
            line += " | 알박기 포함"
        return line

    @staticmethod
    def _build_sqlite_from_emails(db_path: Path, emails: List[str], source_name: str) -> None:
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
                    response: Optional[Dict[str, object]] = None
                    if self.offline_mode:
                        response = self._probe_server_if_due(domain_states)
                    else:
                        response = self.heartbeat(domain_states, [])
                    if not self.offline_mode and response is None:
                        response = {}
                    if not self.offline_mode and response is not None:
                        if not self.connected:
                            print("[연결] 서버와 동기화되었습니다.")
                            self._disconnect_logged = False
                        self.connected = True
                        configs = response.get("configs") or {}
                        if isinstance(configs, dict) and configs:
                            self.apply_configs(configs)
                        controls = response.get("job_controls") or []
                        if isinstance(controls, Iterable):
                            self._update_job_controls(controls)
                    self._evaluate_idle_schedules()
                    self._acknowledge_cancelled_jobs()
                    jobs: List[Dict[str, object]] = []
                    while self._pending_jobs:
                        pending_job = self._pending_jobs.popleft()
                        pending_id = str(pending_job.get("job_id") or "").strip()
                        if pending_id:
                            self._pending_job_ids.discard(pending_id)
                        jobs.append(pending_job)
                    response_jobs = response.get("jobs") if isinstance(response, dict) else None
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
                    if not self.offline_mode:
                        self._flush_pending_job_reports()
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
            self._shutdown_offline_probe_executor()

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
    set_telnet_debug_mode(bool(config.get("telnet_debug_mode")))
    wake_acquired = acquire_wake_lock()
    try:
        while True:
            print("\n========================")
            print(f" 메일 발송 클라이언트 v{APP_VERSION}")
            print("========================")
            print(f"1. 서버 연결 (현재: {config.get('server_url') or '미설정'})")
            print(f"2. 서버 주소 설정")
            print(f"3. 디바이스 이름 설정 (현재: {config.get('device_name') or '미설정'})")
            print(f"4. 설정 (텔넷 디버그 모드: {'ON' if config.get('telnet_debug_mode') else 'OFF'})")
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
                set_telnet_debug_mode(bool(config.get("telnet_debug_mode")))
            elif choice == "2":
                config = configure_server_url(config)
            elif choice == "3":
                config = configure_device_name(config)
            elif choice == "4":
                config = configure_settings_menu(config)
                set_telnet_debug_mode(bool(config.get("telnet_debug_mode")))
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
