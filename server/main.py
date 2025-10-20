# -*- coding: utf-8 -*-
import copy
import hashlib
import logging
import json
import random
import re
import shutil
import sqlite3
import string
import threading
import uuid
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from fastapi import FastAPI, File, HTTPException, Request, Response, UploadFile
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

from encoding_utils import encode_substitution_value, normalize_encoding_name
from email.utils import make_msgid

DOMAINS = ("naver", "daum")
DOMAIN_LABELS = {"naver": "네이버", "daum": "다음"}

IMAP_SENT_THRESHOLD_DEFAULT = 90
MESSAGE_ID_PATTERN_DEFAULT = "<${랜덤:영소:6}.${랜덤:숫자:4}.${랜덤:영소숫자:12}@${HELO}>"
OLD_MESSAGE_ID_PATTERN_DEFAULT = "<${랜덤:영소숫자:22}@auto.local>"

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
    "client_db_version": 0,
    "client_total": 0,
    "client_pending": 0,
    "client_sent": 0,
    "client_failed": 0,
    "client_block": 0,
    "client_removed": 0,
    "client_updated_at": None,
    "client_reserved": 0,
    "client_remaining": 0,
    "client_cycle_completed": False,
    "client_cycle_count": 0,
    "client_last_cycle_at": None,
    "client_last_cycle_processed": 0,
    "stop_schedule_enabled": False,
    "stop_schedule_time": "",
    "stop_schedule_last_run": None,
    "imap_enabled": False,
    "imap_username": "",
    "imap_password": "",
    "imap_single_delay_seconds": 20,
    "imap_allowed_latency_seconds": 20,
    "imap_failure_action": "none",
    "imap_notify_before_stop_all": False,
    "imap_sent_threshold": IMAP_SENT_THRESHOLD_DEFAULT,
    "imap_sent_since_last_check": 0,
    "imap_sent_last_reset_at": None,
    "imap_last_status": "",
    "imap_last_checked_at": None,
    "imap_last_latency": None,
    "imap_last_error": "",
    "imap_last_mail_from": "",
    "imap_last_sent_at": None,
    "imap_last_received_at": None,
    "substitution_lock_mode": "auto",
    "substitution_snapshot": {"fields": {}, "missing_tokens": []},
    "message_id_auto": True,
    "message_id_pattern": MESSAGE_ID_PATTERN_DEFAULT,
}

IMAP_DELAY_MIN_SECONDS = 5
IMAP_DELAY_MAX_SECONDS = 600
IMAP_CHECK_DELAY_MIN_SECONDS = 0
IMAP_SENT_THRESHOLD_MIN = 1
IMAP_SENT_THRESHOLD_MAX = 1000
IMAP_FAILURE_ACTIONS = {"none", "stop_device", "stop_all"}


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
    "substitution_rules": [],
    "telegram_bot_token": "",
    "telegram_chat_id": "",
    "message_id_auto": True,
    "message_id_pattern": MESSAGE_ID_PATTERN_DEFAULT,
}
GLOBAL_CONFIG_DEVICE_FIELDS = ("helo", "mail_from", "header", "bcc_count", "session_count", "message_id_auto", "message_id_pattern")
MAX_DEVICE_LOG_HISTORY = 10
SUBSTITUTION_PATTERN = re.compile(r"\$\{([^{}]+)\}")
FIELD_TOKEN_PATTERN = re.compile(r"^필드:([A-Za-z0-9_]+)$")
SUBSTITUTION_TARGET_FIELDS = ("helo", "mail_from", "header", "rcpt_to", "anchor_email", "message_id_pattern")
RANDOM_TOKEN_PATTERN = re.compile(r"^랜덤:([^:]+):(\d+(?:-\d+)?)$")
LIST_TOKEN_PATTERN = re.compile(r"^목록:(.+)$")
RANDOM_TOKEN_CHARSETS = {
    "영소": string.ascii_lowercase,
    "영대": string.ascii_uppercase,
    "숫자": string.digits,
    "영문": string.ascii_letters,
    "영소숫자": string.ascii_lowercase + string.digits,
    "영대숫자": string.ascii_uppercase + string.digits,
    "영숫자": string.ascii_letters + string.digits,
}
RANDOM_TOKEN_MAX_LENGTH = 128
TELEGRAM_API_BASE = "https://api.telegram.org"
TELEGRAM_TIMEOUT_SECONDS = 5.0
SUBSTITUTION_LOCK_MODES = {"auto", "lock"}
SNAPSHOT_FIELD_KEYS = ("helo", "mail_from", "header", "anchor_email", "rcpt_to", "message_id_pattern")
EMPTY_SUBSTITUTION_SNAPSHOT: Dict[str, Any] = {"fields": {}, "missing_tokens": [], "source_device_id": None}
MESSAGE_ID_HEADER_PATTERN = re.compile(r"(?im)^(?P<indent>[ \t]*)Message-ID\s*:(?P<value>.*(?:\n[ \t].*)*)")


def _extract_mail_domain(mail_from: Optional[str]) -> Optional[str]:
    if not mail_from or "@" not in mail_from:
        return None
    _, domain_part = mail_from.rsplit("@", 1)
    candidate = domain_part.strip().strip(">")
    candidate = candidate.strip().strip(".")
    if not candidate:
        return None
    return candidate.lower()


def _sanitize_hostname_component(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    normalized = re.sub(r"[^a-z0-9.-]+", "-", raw)
    normalized = re.sub(r"-{2,}", "-", normalized)
    normalized = re.sub(r"\.{2,}", ".", normalized)
    normalized = normalized.strip(".-")
    if not normalized:
        return ""
    # 빈 레이블을 제거하며 도메인 규칙을 간단히 보정
    labels = [label.strip("-") for label in normalized.split(".") if label.strip("-")]
    sanitized = ".".join(labels)
    return sanitized


def _build_helo_suffix(helo: Any) -> str:
    sanitized = _sanitize_hostname_component(helo)
    if not sanitized:
        return ""
    return f".{sanitized}"


def _normalize_message_id_pattern(raw: Any) -> str:
    if isinstance(raw, bytes):
        try:
            pattern = raw.decode("utf-8")
        except UnicodeDecodeError:
            pattern = raw.decode("latin-1", errors="ignore")
    else:
        pattern = str(raw or "")
    normalized = pattern.strip()
    if not normalized:
        return MESSAGE_ID_PATTERN_DEFAULT
    if normalized == OLD_MESSAGE_ID_PATTERN_DEFAULT:
        return MESSAGE_ID_PATTERN_DEFAULT
    migrated = normalized
    if "${MAIL_DOMAIN}" in migrated or "${HELO_SUFFIX}" in migrated:
        migrated = migrated.replace("${MAIL_DOMAIN}${HELO_SUFFIX}", "${HELO}")
        migrated = migrated.replace("${MAIL_DOMAIN}", "${HELO}")
        migrated = migrated.replace("${HELO_SUFFIX}", "")
    if migrated != normalized:
        normalized = migrated
    return normalized


def _resolve_reserved_token(name: str, field_ctx: Optional[Dict[str, Any]]) -> Optional[str]:
    context = field_ctx or {}
    token_name = (name or "").strip().upper()
    if token_name == "MAIL_DOMAIN":
        domain = _extract_mail_domain(context.get("mail_from"))
        sanitized_domain = _sanitize_hostname_component(domain)
        return sanitized_domain or "mailsender"
    if token_name == "HELO":
        sanitized_helo = _sanitize_hostname_component(context.get("helo"))
        if sanitized_helo:
            return sanitized_helo
        fallback_domain = _sanitize_hostname_component(_extract_mail_domain(context.get("mail_from")))
        return fallback_domain or "mailsender"
    if token_name == "HELO_SUFFIX":
        return _build_helo_suffix(context.get("helo"))
    return None


def _default_message_id_domain(mail_from: Optional[str], helo: Any) -> str:
    sanitized_helo = _sanitize_hostname_component(helo)
    if sanitized_helo:
        return sanitized_helo
    base = _sanitize_hostname_component(_extract_mail_domain(mail_from))
    if base:
        return base
    return "mailsender"


def _build_message_id_value(
    pattern_value: Optional[str],
    mail_from: Optional[str],
    helo: Any,
) -> str:
    fallback_domain = _default_message_id_domain(mail_from, helo)
    candidate_raw = pattern_value or ""
    candidate_rendered = candidate_raw
    if isinstance(candidate_raw, str) and "${" in candidate_raw:
        substitution_context = {"static": {}, "lists": {}, "fields": {"mail_from": mail_from or "", "helo": helo or ""}}
        rendered, missing_tokens = substitute_tokens(
            candidate_raw,
            [],
            random_generator=random.SystemRandom(),
            context=substitution_context,
        )
        if isinstance(rendered, str) and rendered:
            candidate_rendered = rendered
        if missing_tokens:
            log_substitution_error(
                "Message-ID 패턴 치환 실패(" + ", ".join(sorted(missing_tokens)) + ")"
            )
    candidate = str(candidate_rendered or "").strip()
    if candidate:
        candidate = candidate.replace("\r", "").replace("\n", "")
        if candidate:
            if candidate[0] != "<":
                candidate = f"<{candidate}"
            if candidate[-1] != ">":
                candidate = f"{candidate}>"
            inner = candidate[1:-1]
            if inner and "@" in inner and not any(ch.isspace() for ch in inner):
                local_part, domain_part = inner.rsplit("@", 1)
                sanitized_domain = _sanitize_hostname_component(domain_part) or fallback_domain
                if sanitized_domain != domain_part:
                    candidate = f"<{local_part}@{sanitized_domain}>"
                return candidate
    return make_msgid(domain=fallback_domain)


def ensure_message_id_header(
    header_value: Optional[str],
    *,
    auto_enabled: bool,
    pattern_value: Optional[str],
    mail_from: Optional[str],
    helo: Any,
) -> str:
    header_text = header_value if isinstance(header_value, str) else ""
    if not auto_enabled:
        return header_text
    normalized_newlines = header_text.replace("\r\n", "\n").replace("\r", "\n")
    separator_index = normalized_newlines.find("\n\n")
    if separator_index >= 0:
        separator_end = separator_index + 2
        length = len(normalized_newlines)
        while separator_end < length and normalized_newlines[separator_end] == "\n":
            separator_end += 1
        header_section = normalized_newlines[:separator_index]
        separator_block = normalized_newlines[separator_index:separator_end]
        body_section = normalized_newlines[separator_end:]
    else:
        header_section = normalized_newlines
        separator_block = "\n\n"
        body_section = None
    message_id_value = _build_message_id_value(pattern_value, mail_from, helo)
    match = MESSAGE_ID_HEADER_PATTERN.search(header_section)
    if match:
        indent = match.group("indent") or ""
        start, end = match.span()
        header_section = f"{header_section[:start]}{indent}Message-ID: {message_id_value}{header_section[end:]}"
    else:
        if header_section:
            header_section = f"{header_section}\nMessage-ID: {message_id_value}"
        else:
            header_section = f"Message-ID: {message_id_value}"
    newline_hint = "\r\n" if "\r\n" in header_text else "\n"
    if body_section is not None:
        rebuilt = f"{header_section}{separator_block}{body_section}"
    else:
        rebuilt = header_section
    rebuilt = rebuilt.replace("\n", newline_hint)
    if header_text.endswith(("\r\n", "\n")) and not rebuilt.endswith(newline_hint):
        rebuilt = f"{rebuilt}{newline_hint}"
    return rebuilt


def _strip_message_id_header(header_value: Optional[str]) -> str:
    if not isinstance(header_value, str) or not header_value:
        return header_value if isinstance(header_value, str) else ""
    original = header_value
    normalized = original.replace("\r\n", "\n").replace("\r", "\n")
    lines = normalized.split("\n")
    result_lines: List[str] = []
    skipping_continuation = False
    for line in lines:
        if skipping_continuation:
            if line.startswith((" ", "\t")):
                continue
            skipping_continuation = False
        if line.lstrip().lower().startswith("message-id:"):
            skipping_continuation = True
            continue
        result_lines.append(line)
    normalized_result = "\n".join(result_lines)
    newline_hint = "\r\n" if "\r\n" in original else "\n"
    rebuilt = normalized_result.replace("\n", newline_hint)
    if original.endswith(("\r\n", "\n")) and rebuilt and not rebuilt.endswith(newline_hint):
        rebuilt = f"{rebuilt}{newline_hint}"
    return rebuilt


def resolve_message_id_settings(config: Dict[str, Any]) -> Tuple[bool, str]:
    raw_auto = config.get("message_id_auto")
    if raw_auto is None:
        auto_enabled = bool(DEFAULT_DOMAIN_CONFIG.get("message_id_auto", True))
    else:
        try:
            auto_enabled = bool(int(raw_auto))
        except (TypeError, ValueError):
            auto_enabled = bool(raw_auto)
    pattern_value = _normalize_message_id_pattern(config.get("message_id_pattern"))
    if auto_enabled and pattern_value and "${" not in pattern_value and pattern_value.startswith("<") and pattern_value.endswith(">"):
        inner = pattern_value[1:-1]
        if "@" in inner and not any(ch.isspace() for ch in inner):
            pattern_value = MESSAGE_ID_PATTERN_DEFAULT
    return auto_enabled, pattern_value


def _snapshot_with_preview_header(
    snapshot: Optional[Dict[str, Any]],
    config: Dict[str, Any],
) -> Dict[str, Any]:
    response_snapshot = copy.deepcopy(snapshot or {})
    if not isinstance(response_snapshot, dict):
        response_snapshot = {"fields": {}, "missing_tokens": []}
    fields = response_snapshot.get("fields")
    if not isinstance(fields, dict):
        fields = {}
        response_snapshot["fields"] = fields
    auto_enabled, pattern_value = resolve_message_id_settings(config)
    if auto_enabled:
        header_source = fields.get("header")
        if not isinstance(header_source, str):
            header_source = str(header_source or "")
        fields["header"] = ensure_message_id_header(
            header_source,
            auto_enabled=True,
            pattern_value=pattern_value,
            mail_from=config.get("mail_from"),
            helo=config.get("helo"),
        )
    return response_snapshot


def ensure_storage_root() -> None:
    STORAGE_ROOT.mkdir(parents=True, exist_ok=True)
    (STORAGE_ROOT / "devices").mkdir(exist_ok=True)


ensure_storage_root()


def _format_log_prefix() -> str:
    return datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")


def log_console(message: Any, *, flush: bool = False) -> None:
    """서버 표준 출력에 시간을 접두어로 붙여 기록한다."""
    text = str(message)
    print(f"{_format_log_prefix()} {text}", flush=flush)


class HeartbeatAccessFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            message = record.getMessage()
        except Exception:
            return True
        suppressed_paths = (
            "/api/devices/register",
            "/api/devices",
            "/heartbeat",
            "/health",
            "/favicon.ico",
            '"GET / HTTP/1.1"',
        )
        if any(fragment in message for fragment in suppressed_paths):
            return False
        return True


_uvicorn_access_logger = logging.getLogger("uvicorn.access")
if not any(isinstance(f, HeartbeatAccessFilter) for f in _uvicorn_access_logger.filters):
    _uvicorn_access_logger.addFilter(HeartbeatAccessFilter())


DEVICE_CONNECTION_STATES: Dict[str, str] = {}
DEVICE_LAST_IP: Dict[str, Optional[str]] = {}


def _log_device_connection_event(
    device_id: str,
    device_label: Optional[str],
    status: str,
    public_ip: Optional[str] = None,
) -> None:
    normalized_label = (device_label or "").strip() or device_id
    normalized_status = "connected" if status == "connected" else "disconnected"
    previous_status = DEVICE_CONNECTION_STATES.get(device_id)
    if normalized_status == "connected":
        ip_text = f" (IP: {public_ip})" if public_ip else ""
        if previous_status != "connected":
            log_console(f"[연결] 디바이스 {normalized_label} 온라인{ip_text}")
        DEVICE_CONNECTION_STATES[device_id] = "connected"
        if public_ip is not None:
            DEVICE_LAST_IP[device_id] = public_ip
    else:
        if previous_status != "disconnected":
            log_console(f"[연결] 디바이스 {normalized_label} 오프라인")
        DEVICE_CONNECTION_STATES[device_id] = "disconnected"


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


def sanitize_iso_timestamp(value: Any) -> Optional[str]:
    if not value:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        return value.isoformat()
    candidate = str(value).strip()
    if not candidate:
        return None
    normalized = candidate.replace("Z", "+00:00") if candidate.endswith("Z") else candidate
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return candidate
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return parsed.isoformat()


def sanitize_substitution_lock_mode(value: Any) -> str:
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in SUBSTITUTION_LOCK_MODES:
            return lowered
    return "auto"


def sanitize_substitution_snapshot(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return {"fields": {}, "missing_tokens": []}
    fields_input = raw.get("fields")
    if isinstance(fields_input, dict):
        field_source = fields_input
    else:
        field_source = raw
    fields: Dict[str, str] = {}
    for key in SNAPSHOT_FIELD_KEYS:
        value = field_source.get(key)
        if isinstance(value, str):
            fields[key] = value
    generated_at = sanitize_iso_timestamp(raw.get("generated_at"))
    device_id = raw.get("device_id")
    domain_raw = raw.get("domain")
    domain_value: Optional[str] = None
    if isinstance(domain_raw, str):
        candidate = domain_raw.strip().lower()
        if candidate in DOMAINS:
            domain_value = candidate
        elif candidate:
            domain_value = candidate
    missing_raw = raw.get("missing_tokens")
    if isinstance(missing_raw, list):
        missing_tokens = sorted({str(token).strip() for token in missing_raw if token})
    elif missing_raw:
        missing_tokens = [str(missing_raw).strip()]
    else:
        missing_tokens = []
    snapshot = {
        "fields": fields,
        "missing_tokens": missing_tokens,
    }
    if generated_at:
        snapshot["generated_at"] = generated_at
    if device_id:
        snapshot["device_id"] = str(device_id)
    if domain_value:
        snapshot["domain"] = domain_value
    if "source_device_id" in raw:
        source_device_id = raw.get("source_device_id")
        if source_device_id is None:
            snapshot["source_device_id"] = None
        else:
            candidate = str(source_device_id).strip()
            snapshot["source_device_id"] = candidate or None
    return snapshot


def decode_substitution_snapshot(raw: Any) -> Dict[str, Any]:
    candidate = raw
    if isinstance(candidate, str):
        candidate = candidate.strip()
        if candidate:
            try:
                candidate = json.loads(candidate)
            except json.JSONDecodeError:
                candidate = {}
        else:
            candidate = {}
    if not isinstance(candidate, dict):
        candidate = {}
    snapshot = sanitize_substitution_snapshot(candidate)
    if not snapshot:
        return {"fields": {}, "missing_tokens": []}
    return snapshot


def encode_substitution_snapshot(snapshot: Dict[str, Any]) -> str:
    sanitized = sanitize_substitution_snapshot(snapshot or {})
    return json.dumps(sanitized, ensure_ascii=False)


def field_contains_tokens(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    return bool(SUBSTITUTION_PATTERN.search(value))


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


def sanitize_telegram_bot_token(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def sanitize_telegram_chat_id(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def sanitize_imap_enabled(value: Any) -> bool:
    return sanitize_stop_schedule_enabled(value)


def normalize_imap_username(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_imap_password(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def sanitize_imap_delay(
    value: Any,
    *,
    default: Optional[int] = None,
    minimum: Optional[int] = None,
) -> int:
    effective_default = (
        default
        if default is not None
        else DEFAULT_DOMAIN_CONFIG.get("imap_single_delay_seconds", IMAP_DELAY_MIN_SECONDS)
    )
    effective_min = minimum if minimum is not None else IMAP_DELAY_MIN_SECONDS
    try:
        delay = int(value)
    except (TypeError, ValueError):
        delay = effective_default
    delay = max(effective_min, min(IMAP_DELAY_MAX_SECONDS, delay))
    return delay


def sanitize_imap_allowed_latency(value: Any, *, default: Optional[int] = None) -> int:
    effective_default = (
        default
        if default is not None
        else DEFAULT_DOMAIN_CONFIG.get("imap_allowed_latency_seconds", IMAP_DELAY_MIN_SECONDS)
    )
    try:
        latency = int(value)
    except (TypeError, ValueError):
        latency = effective_default
    latency = max(IMAP_DELAY_MIN_SECONDS, min(IMAP_DELAY_MAX_SECONDS, latency))
    return latency


def sanitize_imap_sent_threshold(value: Any, *, default: Optional[int] = None) -> int:
    effective_default = (
        default if default is not None else IMAP_SENT_THRESHOLD_DEFAULT
    )
    try:
        threshold = int(value)
    except (TypeError, ValueError):
        threshold = effective_default
    threshold = max(IMAP_SENT_THRESHOLD_MIN, min(IMAP_SENT_THRESHOLD_MAX, threshold))
    return threshold


def sanitize_imap_failure_action(value: Any) -> str:
    if not value:
        return "none"
    candidate = str(value).strip().lower()
    if candidate in IMAP_FAILURE_ACTIONS:
        return candidate
    return "none"


def sanitize_imap_notify_before_stop_all(value: Any) -> bool:
    return sanitize_stop_schedule_enabled(value)


def sanitize_imap_status(value: Any) -> str:
    if value is None:
        return ""
    candidate = str(value).strip().lower()
    allowed = {"success", "failure", "error", "network_error", "skipped", "disabled"}
    if candidate in allowed:
        return candidate
    return "error" if candidate else ""


def sanitize_imap_latency(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        latency = float(value)
    except (TypeError, ValueError):
        return None
    if latency < 0:
        return abs(latency)
    return latency


def sanitize_imap_timestamp(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        candidate = value.strip()
        if not candidate:
            return None
    elif isinstance(value, datetime):
        candidate = value.isoformat()
    else:
        candidate = str(value).strip()
        if not candidate:
            return None
    try:
        parsed = datetime.fromisoformat(candidate.replace("Z", "+00:00"))
    except ValueError:
        return None
    normalized = parsed.astimezone(timezone.utc)
    return normalized.isoformat().replace("+00:00", "Z")


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


def normalize_substitution_mode(value: Any) -> str:
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"list", "목록"}:
            return "list"
        if text in {"static", "정적"}:
            return "static"
    return "static"


def _sanitize_description(value: Any, *, limit: int = 300) -> str:
    if not value:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    if len(text) > limit:
        return text[:limit]
    return text


def _extract_list_values(data: Dict[str, Any]) -> List[str]:
    candidates: List[str] = []
    raw_values = data.get("values")
    if isinstance(raw_values, (list, tuple, set)):
        candidates.extend(raw_values)
    elif isinstance(raw_values, str):
        candidates.extend(raw_values.splitlines())
    raw_items = data.get("items")
    if isinstance(raw_items, str):
        candidates.extend(raw_items.splitlines())
    raw_source = data.get("source")
    if isinstance(raw_source, str):
        candidates.extend(raw_source.splitlines())
    raw_value_field = data.get("value")
    if isinstance(raw_value_field, str):
        candidates.extend(raw_value_field.splitlines())
    normalized: List[str] = []
    seen: Set[str] = set()
    for item in candidates:
        if item is None:
            continue
        text = str(item).strip()
        if not text:
            continue
        if text in seen:
            continue
        normalized.append(text)
        seen.add(text)
    return normalized


def canonicalize_substitution_rules(
    raw: Any,
    *,
    strict: bool = False,
    random_generator: Optional[random.Random] = None,
) -> List[Dict[str, Any]]:
    if raw is None:
        return []
    if not isinstance(raw, (list, tuple)):
        if strict:
            raise ValueError("치환 변수 목록 형식이 올바르지 않습니다.")
        return []

    rng = random_generator
    if rng is None:
        system_rng = random.SystemRandom()
        rng = random.Random(system_rng.randrange(1 << 63))

    seen_keys: Set[str] = set()
    ordered_entries: List[Tuple[str, Dict[str, Any]]] = []
    list_entries: List[Dict[str, Any]] = []
    static_entries: Dict[str, Dict[str, Any]] = {}

    for item in raw:
        if hasattr(item, "dict") and callable(getattr(item, "dict")):
            data = item.dict()
        elif isinstance(item, dict):
            data = item
        else:
            if strict:
                raise ValueError("치환 변수 항목 형식이 올바르지 않습니다.")
            continue

        key = str(data.get("key") or "").strip()
        if not key:
            if strict:
                raise ValueError("변수명은 비워둘 수 없습니다.")
            continue
        key_token = key.lower()
        if key_token in seen_keys:
            if strict:
                raise ValueError(f"'{key}' 변수명이 중복되었습니다.")
            continue

        mode = normalize_substitution_mode(data.get("mode"))
        description = _sanitize_description(data.get("description"))

        if mode == "list":
            values = _extract_list_values(data)
            if not values:
                if strict:
                    raise ValueError(f"'{key}' 목록 항목을 한 개 이상 입력하세요.")
                continue
            entry = {
                "key": key,
                "mode": "list",
                "values": values,
                "description": description,
                "source": "",
                "encoding": "none",
                "value": "",
            }
            ordered_entries.append(("list", entry))
            list_entries.append(entry)
            seen_keys.add(key_token)
            continue

        source = str(data.get("source") or "")
        raw_value = data.get("value")
        if not source and raw_value is not None:
            source = str(raw_value)
        encoding = normalize_encoding_name(data.get("encoding"))
        if not source:
            if strict:
                raise ValueError(f"'{key}' 원본을 입력하세요.")
            continue
        entry = {
            "key": key,
            "source": source,
            "encoding": encoding,
            "value": "",
            "mode": "static",
            "values": [],
            "description": description,
        }
        ordered_entries.append(("static", entry))
        static_entries[key] = entry
        seen_keys.add(key_token)

    list_map: Dict[str, Tuple[str, ...]] = {
        entry["key"]: tuple(entry["values"])
        for entry in list_entries
    }
    static_results: Dict[str, str] = {}
    pending = {key: entry for key, entry in static_entries.items()}
    max_iterations = max(1, len(pending) * 2)

    for _ in range(max_iterations):
        if not pending:
            break
        progressed = False
        removal_queue: List[str] = []
        for key, entry in list(pending.items()):
            raw_source = entry.get("source", "")
            context = {
                "static": static_results,
                "lists": list_map,
            }
            substituted, missing = substitute_tokens(
                raw_source,
                [],
                random_generator=rng,
                context=context,
            )

            unresolved_dependency = False
            blocking_tokens: List[str] = []
            for token in missing:
                stripped = token.strip()
                if not stripped:
                    continue
                if stripped in pending:
                    unresolved_dependency = True
                    continue
                if stripped in static_results:
                    continue
                list_match = LIST_TOKEN_PATTERN.match(stripped)
                if list_match:
                    list_name = list_match.group(1).strip()
                    blocking_tokens.append(f"목록:{list_name}")
                    continue
                if RANDOM_TOKEN_PATTERN.match(stripped):
                    blocking_tokens.append(stripped)
                    continue
                blocking_tokens.append(stripped)

            if blocking_tokens:
                if strict:
                    raise ValueError(
                        f"'{key}' 치환에서 {', '.join(sorted(set(blocking_tokens)))} 패턴을 처리하지 못했습니다."
                    )
                continue
            if unresolved_dependency:
                continue

            try:
                encoded_value = encode_substitution_value(
                    substituted,
                    entry.get("encoding"),
                    random_choice=rng.choice if hasattr(rng, "choice") else None,
                    random_generator=rng,
                )
            except UnicodeEncodeError as exc:
                if strict and entry.get("encoding") == "quoted_printable_euckr":
                    raise ValueError(f"'{key}' 원본을 EUC-KR로 변환할 수 없습니다.") from exc
                raise ValueError(f"'{key}' 치환 값을 생성하는 중 오류가 발생했습니다.") from exc

            if not encoded_value:
                if strict:
                    raise ValueError(f"'{key}' 치환 값을 입력하세요.")
                continue

            entry["value"] = encoded_value
            static_results[key] = encoded_value
            removal_queue.append(key)
            progressed = True

        for key in removal_queue:
            pending.pop(key, None)

        if not progressed:
            break

    if pending:
        if strict:
            unresolved = ", ".join(sorted(pending.keys()))
            raise ValueError(f"치환 변수 {unresolved} 값을 계산하지 못했습니다.")
        unresolved_keys = set(pending.keys())
        ordered_entries = [
            (kind, entry)
            for kind, entry in ordered_entries
            if not (kind == "static" and entry.get("key") in unresolved_keys)
        ]

    sanitized: List[Dict[str, Any]] = []
    for kind, entry in ordered_entries:
        if kind == "list":
            sanitized.append(entry)
        else:
            if entry.get("key") in static_results:
                sanitized.append(entry)
            elif strict:
                raise ValueError(f"'{entry.get('key')}' 치환 값을 계산하지 못했습니다.")
    return sanitized


def sanitize_substitution_rules(raw: Any) -> List[Dict[str, Any]]:
    try:
        return canonicalize_substitution_rules(raw, strict=False)
    except ValueError:
        return []


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
    sanitized["substitution_rules"] = sanitize_substitution_rules(raw.get("substitution_rules"))
    sanitized["telegram_bot_token"] = sanitize_telegram_bot_token(raw.get("telegram_bot_token"))
    sanitized["telegram_chat_id"] = sanitize_telegram_chat_id(raw.get("telegram_chat_id"))
    raw_message_auto = raw.get("message_id_auto")
    if isinstance(raw_message_auto, str):
        lowered = raw_message_auto.strip().lower()
        sanitized["message_id_auto"] = lowered in {"1", "true", "yes", "on"}
    elif raw_message_auto is None:
        sanitized["message_id_auto"] = bool(DEFAULT_DOMAIN_CONFIG.get("message_id_auto", True))
    else:
        sanitized["message_id_auto"] = bool(raw_message_auto)
    sanitized["message_id_pattern"] = _normalize_message_id_pattern(raw.get("message_id_pattern"))
    return sanitized


def send_telegram_message(bot_token: str, chat_id: str, text: str, *, timeout: float = TELEGRAM_TIMEOUT_SECONDS) -> Dict[str, Any]:
    token = sanitize_telegram_bot_token(bot_token)
    target_chat_id = sanitize_telegram_chat_id(chat_id)
    if not token or not target_chat_id:
        raise ValueError("텔레그램 봇 토큰과 챗 ID가 필요합니다.")
    message = text or ""
    url = f"{TELEGRAM_API_BASE}/bot{token}/sendMessage"
    encoded = urllib.parse.urlencode(
        {
            "chat_id": target_chat_id,
            "text": message,
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=encoded,
        headers={
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw_body = response.read()
            try:
                payload = json.loads(raw_body.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise RuntimeError("텔레그램 API 응답을 해석하지 못했습니다.") from exc
    except urllib.error.HTTPError as exc:
        try:
            detail_body = exc.read().decode("utf-8")
        except Exception:
            detail_body = ""
        detail_payload: Optional[Dict[str, Any]] = None
        detail_description = ""
        detail_error_code: Optional[int] = None
        if detail_body:
            try:
                parsed = json.loads(detail_body)
                if isinstance(parsed, dict):
                    detail_payload = parsed
            except (json.JSONDecodeError, UnicodeDecodeError):
                detail_payload = None
        if detail_payload:
            raw_description = detail_payload.get("description") or detail_payload.get("error")
            if isinstance(raw_description, str):
                detail_description = raw_description.strip()
            try:
                detail_error_code = int(detail_payload.get("error_code"))  # type: ignore[arg-type]
            except (TypeError, ValueError):
                detail_error_code = None
        normalized_description = detail_description.lower() if detail_description else ""
        status_code = getattr(exc, "code", None)
        if status_code in (401, 403) or (status_code == 404 and (detail_error_code == 404 or "not found" in normalized_description)):
            raise ValueError("텔레그램 봇 토큰이 올바르지 않습니다. BotFather에서 발급한 토큰을 사용하세요.") from exc
        if status_code == 400 and ("chat not found" in normalized_description or "wrong chat id" in normalized_description):
            raise ValueError("텔레그램 챗 ID를 찾을 수 없습니다. 챗 ID를 확인하세요.") from exc
        message_text = detail_description or detail_body or getattr(exc, "reason", "") or f"HTTP {status_code}"
        raise RuntimeError(f"HTTP {status_code}: {message_text}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(str(exc.reason or exc)) from exc
    except OSError as exc:
        raise RuntimeError(str(exc)) from exc
    if not isinstance(payload, dict):
        raise RuntimeError("텔레그램 API 응답 형식이 올바르지 않습니다.")
    if not payload.get("ok"):
        description = payload.get("description") or payload.get("error") or "알 수 없는 오류가 발생했습니다."
        raise RuntimeError(description)
    return payload


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


def _remove_anchor_imap_columns(conn: sqlite3.Connection) -> None:
    """Rebuild device_configs table to align with current IMAP schema."""
    conn.execute("PRAGMA foreign_keys=OFF")
    try:
        conn.execute("ALTER TABLE device_configs RENAME TO device_configs_legacy")
        conn.execute(
            """
            CREATE TABLE device_configs (
                device_id TEXT NOT NULL,
                domain TEXT NOT NULL,
                helo TEXT DEFAULT '',
                smtp_host TEXT DEFAULT '',
                smtp_port INTEGER DEFAULT 25,
                mail_from TEXT DEFAULT '',
                header TEXT DEFAULT '',
                session_count INTEGER DEFAULT 1,
                bcc_count INTEGER DEFAULT 0,
                anchor_interval INTEGER DEFAULT 0,
                anchor_email TEXT DEFAULT '',
                rcpt_to TEXT DEFAULT '',
                client_db_version INTEGER DEFAULT 0,
                client_total INTEGER DEFAULT 0,
                client_pending INTEGER DEFAULT 0,
                client_sent INTEGER DEFAULT 0,
                client_failed INTEGER DEFAULT 0,
                client_block INTEGER DEFAULT 0,
                client_removed INTEGER DEFAULT 0,
                client_updated_at TEXT,
                client_reserved INTEGER DEFAULT 0,
                client_remaining INTEGER DEFAULT 0,
                client_cycle_completed INTEGER DEFAULT 0,
                client_cycle_count INTEGER DEFAULT 0,
                client_last_cycle_at TEXT,
                client_last_cycle_processed INTEGER DEFAULT 0,
                stop_schedule_enabled INTEGER NOT NULL DEFAULT 0,
                stop_schedule_time TEXT DEFAULT '',
                stop_schedule_last_run TEXT,
                imap_enabled INTEGER NOT NULL DEFAULT 0,
                imap_username TEXT DEFAULT '',
                imap_password TEXT DEFAULT '',
                imap_delay_seconds INTEGER NOT NULL DEFAULT 20,
                imap_single_delay_seconds INTEGER NOT NULL DEFAULT 20,
                imap_allowed_latency_seconds INTEGER NOT NULL DEFAULT 20,
                imap_failure_action TEXT NOT NULL DEFAULT 'none',
                imap_notify_before_stop_all INTEGER NOT NULL DEFAULT 0,
                imap_sent_threshold INTEGER NOT NULL DEFAULT 90,
                imap_sent_since_last_check INTEGER NOT NULL DEFAULT 0,
                imap_sent_last_reset_at TEXT,
                imap_last_status TEXT DEFAULT '',
                imap_last_checked_at TEXT,
                imap_last_latency REAL,
                imap_last_error TEXT,
                imap_last_mail_from TEXT,
                imap_last_sent_at TEXT,
                imap_last_received_at TEXT,
                substitution_lock_mode TEXT NOT NULL DEFAULT 'auto',
                substitution_snapshot TEXT DEFAULT '{}',
                updated_at TEXT NOT NULL,
                PRIMARY KEY (device_id, domain),
                FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE
            )
            """
        )
        conn.execute(
            """
            INSERT INTO device_configs (
                device_id, domain, helo, smtp_host, smtp_port,
                mail_from, header, session_count, bcc_count,
                anchor_interval, anchor_email, rcpt_to,
                client_db_version, client_total, client_pending,
                client_sent, client_failed, client_block, client_removed,
                client_updated_at, client_reserved, client_remaining,
                client_cycle_completed, client_cycle_count,
                client_last_cycle_at, client_last_cycle_processed,
                stop_schedule_enabled, stop_schedule_time, stop_schedule_last_run,
                imap_enabled, imap_username, imap_password, imap_delay_seconds, imap_single_delay_seconds,
                imap_allowed_latency_seconds,
                imap_failure_action,
                imap_notify_before_stop_all,
                imap_sent_threshold,
                imap_sent_since_last_check,
                imap_sent_last_reset_at,
                imap_last_status, imap_last_checked_at, imap_last_latency,
                imap_last_error, imap_last_mail_from, imap_last_sent_at,
                imap_last_received_at, 'auto' AS substitution_lock_mode,
                '{{}}' AS substitution_snapshot, updated_at
            )
            SELECT
                device_id, domain, helo, smtp_host, smtp_port,
                mail_from, header, session_count, bcc_count,
                anchor_interval, anchor_email, rcpt_to,
                client_db_version, client_total, client_pending,
                client_sent, client_failed, client_block, client_removed,
                client_updated_at, client_reserved, client_remaining,
                client_cycle_completed, client_cycle_count,
                client_last_cycle_at, client_last_cycle_processed,
                stop_schedule_enabled, stop_schedule_time, stop_schedule_last_run,
                imap_enabled, imap_username, imap_password, imap_delay_seconds, imap_delay_seconds AS imap_single_delay_seconds,
                imap_delay_seconds AS imap_allowed_latency_seconds,
                'none' AS imap_failure_action,
                0 AS imap_notify_before_stop_all,
                {threshold_default} AS imap_sent_threshold,
                0 AS imap_sent_since_last_check,
                NULL AS imap_sent_last_reset_at,
                imap_last_status, imap_last_checked_at, imap_last_latency,
                imap_last_error, imap_last_mail_from, imap_last_sent_at,
                imap_last_received_at, 'auto' AS substitution_lock_mode,
                '{{}}' AS substitution_snapshot, 1 AS message_id_auto,
                '{pattern_default}' AS message_id_pattern, updated_at
            FROM device_configs_legacy
            """
        ).format(
            threshold_default=IMAP_SENT_THRESHOLD_DEFAULT,
            pattern_default=MESSAGE_ID_PATTERN_DEFAULT.replace("'", "''"),
        )
        conn.execute("DROP TABLE device_configs_legacy")
    finally:
        conn.execute("PRAGMA foreign_keys=ON")


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
        if {"imap_anchor_enabled", "imap_anchor_delay_seconds"} & config_columns:
            _remove_anchor_imap_columns(conn)
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
        if "imap_enabled" not in config_columns:
            conn.execute(
                "ALTER TABLE device_configs ADD COLUMN imap_enabled INTEGER NOT NULL DEFAULT 0"
            )
        if "imap_username" not in config_columns:
            conn.execute(
                "ALTER TABLE device_configs ADD COLUMN imap_username TEXT DEFAULT ''"
            )
        if "imap_password" not in config_columns:
            conn.execute(
                "ALTER TABLE device_configs ADD COLUMN imap_password TEXT DEFAULT ''"
            )
        if "imap_delay_seconds" not in config_columns:
            conn.execute(
                "ALTER TABLE device_configs ADD COLUMN imap_delay_seconds INTEGER NOT NULL DEFAULT 20"
            )
        if "imap_single_delay_seconds" not in config_columns:
            conn.execute(
                "ALTER TABLE device_configs ADD COLUMN imap_single_delay_seconds INTEGER NOT NULL DEFAULT 20"
            )
        if "imap_allowed_latency_seconds" not in config_columns:
            conn.execute(
                "ALTER TABLE device_configs ADD COLUMN imap_allowed_latency_seconds INTEGER NOT NULL DEFAULT 20"
            )
            conn.execute(
                "UPDATE device_configs SET imap_allowed_latency_seconds=imap_delay_seconds WHERE imap_delay_seconds IS NOT NULL"
            )
        if "imap_failure_action" not in config_columns:
            conn.execute(
                "ALTER TABLE device_configs ADD COLUMN imap_failure_action TEXT NOT NULL DEFAULT 'none'"
            )
        if "imap_notify_before_stop_all" not in config_columns:
            conn.execute(
                "ALTER TABLE device_configs ADD COLUMN imap_notify_before_stop_all INTEGER NOT NULL DEFAULT 0"
            )
        if "imap_sent_threshold" not in config_columns:
            conn.execute(
                f"ALTER TABLE device_configs ADD COLUMN imap_sent_threshold INTEGER NOT NULL DEFAULT {IMAP_SENT_THRESHOLD_DEFAULT}"
            )
        if "imap_sent_since_last_check" not in config_columns:
            conn.execute(
                "ALTER TABLE device_configs ADD COLUMN imap_sent_since_last_check INTEGER NOT NULL DEFAULT 0"
            )
        if "imap_sent_last_reset_at" not in config_columns:
            conn.execute(
                "ALTER TABLE device_configs ADD COLUMN imap_sent_last_reset_at TEXT"
            )
        if "imap_last_status" not in config_columns:
            conn.execute(
                "ALTER TABLE device_configs ADD COLUMN imap_last_status TEXT DEFAULT ''"
            )
        if "imap_last_checked_at" not in config_columns:
            conn.execute(
                "ALTER TABLE device_configs ADD COLUMN imap_last_checked_at TEXT"
            )
        if "imap_last_latency" not in config_columns:
            conn.execute(
                "ALTER TABLE device_configs ADD COLUMN imap_last_latency REAL"
            )
        if "imap_last_error" not in config_columns:
            conn.execute(
                "ALTER TABLE device_configs ADD COLUMN imap_last_error TEXT"
            )
        if "imap_last_mail_from" not in config_columns:
            conn.execute(
                "ALTER TABLE device_configs ADD COLUMN imap_last_mail_from TEXT"
            )
        if "imap_last_sent_at" not in config_columns:
            conn.execute(
                "ALTER TABLE device_configs ADD COLUMN imap_last_sent_at TEXT"
            )
        if "imap_last_received_at" not in config_columns:
            conn.execute(
                "ALTER TABLE device_configs ADD COLUMN imap_last_received_at TEXT"
            )
        if "substitution_lock_mode" not in config_columns:
            conn.execute(
                "ALTER TABLE device_configs ADD COLUMN substitution_lock_mode TEXT NOT NULL DEFAULT 'auto'"
            )
        if "substitution_snapshot" not in config_columns:
            conn.execute(
                "ALTER TABLE device_configs ADD COLUMN substitution_snapshot TEXT DEFAULT '{}'"
            )
            conn.execute(
                "UPDATE device_configs SET substitution_snapshot='{}' WHERE substitution_snapshot IS NULL OR substitution_snapshot=''"
            )
        else:
            conn.execute(
                "UPDATE device_configs SET substitution_snapshot='{}' WHERE substitution_snapshot IS NULL OR substitution_snapshot=''"
            )
        if "message_id_auto" not in config_columns:
            conn.execute(
                "ALTER TABLE device_configs ADD COLUMN message_id_auto INTEGER NOT NULL DEFAULT 1"
            )
        if "message_id_pattern" not in config_columns:
            conn.execute(
                "ALTER TABLE device_configs ADD COLUMN message_id_pattern TEXT DEFAULT ''"
            )
            conn.execute(
                "UPDATE device_configs SET message_id_pattern=? WHERE message_id_pattern IS NULL OR message_id_pattern=''",
                (MESSAGE_ID_PATTERN_DEFAULT,),
            )
        else:
            conn.execute(
                "UPDATE device_configs SET message_id_pattern=? WHERE message_id_pattern IS NULL OR message_id_pattern=''",
                (MESSAGE_ID_PATTERN_DEFAULT,),
            )
        conn.execute(
            "UPDATE device_configs SET message_id_pattern=? WHERE message_id_pattern=?",
            (MESSAGE_ID_PATTERN_DEFAULT, OLD_MESSAGE_ID_PATTERN_DEFAULT),
        )
        conn.execute(
            """
            UPDATE device_configs
            SET message_id_pattern = REPLACE(message_id_pattern, '${MAIL_DOMAIN}${HELO_SUFFIX}', '${HELO}')
            WHERE message_id_pattern LIKE '%${MAIL_DOMAIN}${HELO_SUFFIX}%'
            """
        )
        conn.execute(
            """
            UPDATE device_configs
            SET message_id_pattern = REPLACE(message_id_pattern, '${MAIL_DOMAIN}', '${HELO}')
            WHERE message_id_pattern LIKE '%${MAIL_DOMAIN}%'
            """
        )
        conn.execute(
            """
            UPDATE device_configs
            SET message_id_pattern = REPLACE(message_id_pattern, '${HELO_SUFFIX}', '')
            WHERE message_id_pattern LIKE '%${HELO_SUFFIX}%'
            """
        )
        conn.execute(
            "UPDATE device_configs SET substitution_lock_mode='auto' WHERE substitution_lock_mode IS NULL OR substitution_lock_mode=''"
        )
        global_row = conn.execute(
            "SELECT value FROM global_settings WHERE key=?",
            (GLOBAL_CONFIG_KEY,),
        ).fetchone()
        if global_row:
            raw_value = global_row["value"] or "{}"
            try:
                payload = json.loads(raw_value)
            except json.JSONDecodeError:
                payload = {}
            normalized_pattern = _normalize_message_id_pattern(payload.get("message_id_pattern"))
            if payload.get("message_id_pattern") != normalized_pattern:
                payload["message_id_pattern"] = normalized_pattern
                conn.execute(
                    "UPDATE global_settings SET value=?, updated_at=? WHERE key=?",
                    (json.dumps(payload, ensure_ascii=False), now_ts(), GLOBAL_CONFIG_KEY),
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
                    client_db_version, client_total, client_pending,
                    client_sent, client_failed, client_block, client_removed,
                    client_updated_at, client_reserved, client_remaining,
                    client_cycle_completed, client_cycle_count,
                    client_last_cycle_at, client_last_cycle_processed,
                    stop_schedule_enabled, stop_schedule_time, stop_schedule_last_run,
                    imap_enabled, imap_username, imap_password, imap_delay_seconds,
                    imap_single_delay_seconds, imap_allowed_latency_seconds,
                    imap_failure_action, imap_notify_before_stop_all,
                    imap_sent_threshold, imap_sent_since_last_check, imap_sent_last_reset_at,
                    imap_last_status, imap_last_checked_at, imap_last_latency, imap_last_error, imap_last_mail_from,
                    imap_last_sent_at, imap_last_received_at,
                    substitution_lock_mode, substitution_snapshot,
                    message_id_auto, message_id_pattern,
                    updated_at
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
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
                    DEFAULT_DOMAIN_CONFIG["client_db_version"],
                    DEFAULT_DOMAIN_CONFIG["client_total"],
                    DEFAULT_DOMAIN_CONFIG["client_pending"],
                    DEFAULT_DOMAIN_CONFIG["client_sent"],
                    DEFAULT_DOMAIN_CONFIG["client_failed"],
                    DEFAULT_DOMAIN_CONFIG["client_block"],
                    DEFAULT_DOMAIN_CONFIG["client_removed"],
                    DEFAULT_DOMAIN_CONFIG["client_updated_at"],
                    DEFAULT_DOMAIN_CONFIG["client_reserved"],
                    DEFAULT_DOMAIN_CONFIG["client_remaining"],
                    1 if DEFAULT_DOMAIN_CONFIG["client_cycle_completed"] else 0,
                    DEFAULT_DOMAIN_CONFIG["client_cycle_count"],
                    DEFAULT_DOMAIN_CONFIG["client_last_cycle_at"],
                    DEFAULT_DOMAIN_CONFIG["client_last_cycle_processed"],
                    1 if DEFAULT_DOMAIN_CONFIG["stop_schedule_enabled"] else 0,
                    DEFAULT_DOMAIN_CONFIG["stop_schedule_time"],
                    DEFAULT_DOMAIN_CONFIG["stop_schedule_last_run"],
                    1 if DEFAULT_DOMAIN_CONFIG["imap_enabled"] else 0,
                    DEFAULT_DOMAIN_CONFIG["imap_username"],
                    DEFAULT_DOMAIN_CONFIG["imap_password"],
                    DEFAULT_DOMAIN_CONFIG.get("imap_delay_seconds", DEFAULT_DOMAIN_CONFIG["imap_single_delay_seconds"]),
                    DEFAULT_DOMAIN_CONFIG["imap_single_delay_seconds"],
                    DEFAULT_DOMAIN_CONFIG["imap_allowed_latency_seconds"],
                    DEFAULT_DOMAIN_CONFIG["imap_failure_action"],
                    1 if DEFAULT_DOMAIN_CONFIG["imap_notify_before_stop_all"] else 0,
                    DEFAULT_DOMAIN_CONFIG["imap_sent_threshold"],
                    DEFAULT_DOMAIN_CONFIG["imap_sent_since_last_check"],
                    DEFAULT_DOMAIN_CONFIG["imap_sent_last_reset_at"],
                    DEFAULT_DOMAIN_CONFIG["imap_last_status"],
                    DEFAULT_DOMAIN_CONFIG["imap_last_checked_at"],
                    DEFAULT_DOMAIN_CONFIG["imap_last_latency"],
                    DEFAULT_DOMAIN_CONFIG["imap_last_error"],
                    DEFAULT_DOMAIN_CONFIG["imap_last_mail_from"],
                    DEFAULT_DOMAIN_CONFIG["imap_last_sent_at"],
                    DEFAULT_DOMAIN_CONFIG["imap_last_received_at"],
                    DEFAULT_DOMAIN_CONFIG["substitution_lock_mode"],
                    json.dumps(DEFAULT_DOMAIN_CONFIG["substitution_snapshot"], ensure_ascii=False),
                    1 if DEFAULT_DOMAIN_CONFIG["message_id_auto"] else 0,
                    DEFAULT_DOMAIN_CONFIG["message_id_pattern"],
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


def serialize_config(row: Dict[str, Any], *, include_secret: bool = True) -> Dict[str, Any]:
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
    imap_enabled = sanitize_imap_enabled(row.get("imap_enabled"))
    imap_username = normalize_imap_username(row.get("imap_username"))
    raw_allowed_latency = row.get("imap_allowed_latency_seconds")
    if raw_allowed_latency is None:
        raw_allowed_latency = row.get("imap_delay_seconds")
    imap_allowed_latency = sanitize_imap_allowed_latency(
        raw_allowed_latency,
        default=DEFAULT_DOMAIN_CONFIG.get("imap_allowed_latency_seconds"),
    )
    imap_single_delay = sanitize_imap_delay(
        row.get("imap_single_delay_seconds"),
        default=DEFAULT_DOMAIN_CONFIG.get("imap_single_delay_seconds"),
        minimum=IMAP_CHECK_DELAY_MIN_SECONDS,
    )
    imap_sent_threshold = sanitize_imap_sent_threshold(
        row.get("imap_sent_threshold"),
        default=DEFAULT_DOMAIN_CONFIG.get("imap_sent_threshold"),
    )
    try:
        sent_since_last_check = int(row.get("imap_sent_since_last_check") or 0)
    except (TypeError, ValueError):
        sent_since_last_check = 0
    sent_since_last_check = max(0, sent_since_last_check)
    imap_sent_last_reset = row.get("imap_sent_last_reset_at")
    imap_failure_action = sanitize_imap_failure_action(row.get("imap_failure_action"))
    imap_notify_before_stop_all = sanitize_imap_notify_before_stop_all(
        row.get("imap_notify_before_stop_all")
    )
    imap_status = sanitize_imap_status(row.get("imap_last_status"))
    imap_checked_at = row.get("imap_last_checked_at")
    imap_latency = sanitize_imap_latency(row.get("imap_last_latency"))
    imap_error = str(row.get("imap_last_error") or "")
    imap_mail_from = str(row.get("imap_last_mail_from") or "")
    imap_last_sent = row.get("imap_last_sent_at")
    imap_last_received = row.get("imap_last_received_at")
    lock_mode = sanitize_substitution_lock_mode(row.get("substitution_lock_mode"))
    snapshot_payload = decode_substitution_snapshot(row.get("substitution_snapshot"))
    password_raw = row.get("imap_password") or ""
    message_id_auto_value = row.get("message_id_auto")
    if message_id_auto_value is None:
        message_id_enabled = bool(DEFAULT_DOMAIN_CONFIG.get("message_id_auto", True))
    else:
        try:
            message_id_enabled = bool(int(message_id_auto_value))
        except (TypeError, ValueError):
            message_id_enabled = bool(message_id_auto_value)
    message_id_pattern = _normalize_message_id_pattern(row.get("message_id_pattern"))
    if include_secret:
        imap_password = password_raw
    else:
        imap_password = "********" if password_raw else ""
    return {
        "domain": row["domain"],
        "helo": row.get("helo", ""),
        "smtp_host": row.get("smtp_host", ""),
        "smtp_port": row.get("smtp_port", 25),
        "mail_from": row.get("mail_from", ""),
        "header": row.get("header", ""),
        "message_id_auto": message_id_enabled,
        "message_id_pattern": message_id_pattern,
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
        "imap_enabled": imap_enabled,
        "imap_username": imap_username,
        "imap_password": imap_password,
        "imap_password_saved": bool(password_raw),
        "imap_single_delay_seconds": imap_single_delay,
        "imap_allowed_latency_seconds": imap_allowed_latency,
        "imap_failure_action": imap_failure_action,
        "imap_notify_before_stop_all": imap_notify_before_stop_all,
        "imap_sent_threshold": imap_sent_threshold,
        "imap_sent_since_last_check": sent_since_last_check,
        "imap_sent_last_reset_at": imap_sent_last_reset,
        "imap_delay_seconds": imap_allowed_latency,
        "imap_last_status": imap_status,
        "imap_last_checked_at": imap_checked_at,
        "imap_last_latency": imap_latency,
        "imap_last_error": imap_error,
        "imap_last_mail_from": imap_mail_from,
        "imap_last_sent_at": imap_last_sent,
        "imap_last_received_at": imap_last_received,
        "substitution_lock_mode": lock_mode,
        "substitution_lock_active": lock_mode == "lock",
        "substitution_snapshot": snapshot_payload,
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
        for device_id in stale_updates:
            details = device_lookup.get(device_id)
            device_name = details.get("name") if details else None
            _log_device_connection_event(device_id, device_name or device_id, "disconnected")
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
                if updated_row["job_type"] in {"single_send", "imap_manual_check", "batch_send"}:
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
        config_map.setdefault(row["device_id"], {})[row["domain"]] = serialize_config(
            to_dict(row), include_secret=False
        )
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
        payload_public = sanitize_job_payload_for_output(row["job_type"], payload)
        row_keys = row.keys()
        cancel_requested_flag = bool(row["cancel_requested"]) if "cancel_requested" in row_keys else False
        job_map.setdefault(row["device_id"], []).append(
            {
                "id": row["id"],
                "job_type": row["job_type"],
                "domain": row["domain"],
                "status": row["status"],
                 "payload": payload_public,
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
            "SELECT id, name, public_ip FROM devices WHERE id=?",
            (device_id,),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="디바이스를 찾을 수 없습니다.")
        conn.execute("DELETE FROM devices WHERE id=?", (device_id,))
        conn.commit()
    if device_root.exists():
        shutil.rmtree(device_root, ignore_errors=True)
    device_label = ""
    public_ip: Optional[str] = None
    if row:
        raw_name = row["name"] if "name" in row.keys() else ""
        raw_ip = row["public_ip"] if "public_ip" in row.keys() else None
        device_label = (raw_name or "").strip()
        public_ip = (raw_ip or "").strip() or None
    DEVICE_CONNECTION_STATES.pop(device_id, None)
    DEVICE_LAST_IP.pop(device_id, None)
    _log_device_connection_event(device_id, device_label or device_id, "disconnected", public_ip)
    log_console(f"[연결] 디바이스 {(device_label or device_id)} 등록 해제")


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


def extract_telegram_credentials(config: Dict[str, Any]) -> Tuple[str, str]:
    return (
        sanitize_telegram_bot_token(config.get("telegram_bot_token")),
        sanitize_telegram_chat_id(config.get("telegram_chat_id")),
    )


def attach_telegram_credentials(
    payload: Dict[str, Any],
    *,
    bot_token: str,
    chat_id: str,
) -> Dict[str, Any]:
    if not bot_token and not chat_id:
        return dict(payload)
    enriched = dict(payload)
    if bot_token:
        enriched["telegram_bot_token"] = bot_token
    if chat_id:
        enriched["telegram_chat_id"] = chat_id
    return enriched


def build_substitution_context(rules: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    static_map: Dict[str, str] = {}
    list_map: Dict[str, List[str]] = {}
    for rule in rules or []:
        if not isinstance(rule, dict):
            continue
        key = str(rule.get("key") or "").strip()
        if not key:
            continue
        mode = normalize_substitution_mode(rule.get("mode"))
        if mode == "list":
            values_field = rule.get("values")
            values: List[str] = []
            if isinstance(values_field, list):
                seen_values: Set[str] = set()
                for item in values_field:
                    if item is None:
                        continue
                    text = str(item).strip()
                    if not text or text in seen_values:
                        continue
                    values.append(text)
                    seen_values.add(text)
            else:
                values = _extract_list_values(rule)
            if values:
                list_map[key] = tuple(values)
            continue
        value_field = rule.get("value")
        if isinstance(value_field, str) and value_field:
            static_map[key] = value_field
    return {"static": static_map, "lists": list_map}


def log_substitution_error(message: str) -> None:
    log_console(f"[SUBSTITUTION] {message}")


def _parse_random_length(spec: str) -> Tuple[int, int]:
    cleaned = (spec or "").strip()
    if not cleaned:
        raise ValueError("길이 정보가 비어 있습니다.")
    if "-" in cleaned:
        start_str, end_str = cleaned.split("-", 1)
    else:
        start_str = cleaned
        end_str = cleaned
    try:
        min_len = int(start_str)
        max_len = int(end_str)
    except (TypeError, ValueError) as exc:
        raise ValueError("길이는 정수로 입력해야 합니다.") from exc
    if min_len <= 0 or max_len <= 0:
        raise ValueError("길이는 1 이상이어야 합니다.")
    if min_len > max_len:
        raise ValueError("최소 길이가 최대 길이보다 큽니다.")
    if max_len > RANDOM_TOKEN_MAX_LENGTH:
        raise ValueError(f"최대 길이는 {RANDOM_TOKEN_MAX_LENGTH} 이하로 입력하세요.")
    return min_len, max_len


def _resolve_random_token(kind: str, length_spec: str, rng: random.Random) -> str:
    normalized_kind = (kind or "").strip()
    charset = RANDOM_TOKEN_CHARSETS.get(normalized_kind)
    if charset is None:
        charset = RANDOM_TOKEN_CHARSETS.get(normalized_kind.lower())
    if charset is None:
        raise ValueError(f"지원하지 않는 랜덤 조합입니다: {normalized_kind or '?'}")
    min_len, max_len = _parse_random_length(length_spec)
    length = rng.randint(min_len, max_len)
    if length <= 0:
        return ""
    return "".join(rng.choice(charset) for _ in range(length))


def _choose_list_value(values: Sequence[str], rng: random.Random) -> str:
    if isinstance(values, (list, tuple)):
        sequence: Sequence[str] = values
    else:
        sequence = tuple(values)
    if not sequence:
        raise ValueError("목록 패턴 값이 비어 있습니다.")
    if hasattr(rng, "randrange"):
        index = rng.randrange(len(sequence))
    else:
        random_func = getattr(rng, "random", None)
        if callable(random_func):
            raw = float(random_func())
            index = int(raw * len(sequence))
        else:
            index = 0
    if index >= len(sequence):
        index = len(sequence) - 1
    if index < 0:
        index = 0
    return sequence[index]


def substitute_tokens(
    value: Any,
    rules: List[Dict[str, Any]],
    *,
    random_generator: Optional[random.Random] = None,
    context: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Tuple[Any, Set[str]]:
    if not isinstance(value, str) or not value or "${" not in value:
        return value, set()
    ctx = context or build_substitution_context(rules or [])
    static_map = ctx.get("static") or {}
    list_map = ctx.get("lists") or {}
    field_ctx: Dict[str, Any] = {}
    raw_field_ctx = ctx.get("fields") if isinstance(ctx, dict) else None
    if isinstance(raw_field_ctx, dict):
        field_ctx = raw_field_ctx
    rng = random_generator or random.SystemRandom()
    result = value
    missing: Set[str] = set()
    max_iterations = max(1, len(static_map) + len(list_map) + 5)

    for _ in range(max_iterations):
        changed = False

        def replace(match: re.Match) -> str:
            nonlocal changed
            token_raw = match.group(1)
            if token_raw in static_map:
                changed = True
                return static_map[token_raw]
            stripped = token_raw.strip()
            random_match = RANDOM_TOKEN_PATTERN.match(stripped)
            if random_match:
                kind_raw = random_match.group(1)
                length_spec = random_match.group(2)
                try:
                    replacement = _resolve_random_token(kind_raw, length_spec, rng)
                except ValueError as exc:
                    log_substitution_error(f"랜덤 패턴 처리 실패({stripped}): {exc}")
                    missing.add(stripped)
                    return ""
                changed = True
                return replacement
            list_match = LIST_TOKEN_PATTERN.match(stripped)
            if list_match:
                list_name = list_match.group(1).strip()
                if not list_name:
                    log_substitution_error("목록 패턴 이름이 비어 있습니다.")
                    missing.add(stripped)
                    return ""
                values = list_map.get(list_name)
                if values:
                    try:
                        replacement = _choose_list_value(values, rng)
                    except ValueError:
                        log_substitution_error(f"'{list_name}' 목록이 정의되지 않았습니다.")
                        missing.add(f"목록:{list_name}")
                        return ""
                    changed = True
                    return replacement
                missing.add(f"목록:{list_name}")
                log_substitution_error(f"'{list_name}' 목록이 비어 있거나 정의되지 않았습니다.")
                return ""
            reserved_value = _resolve_reserved_token(stripped, field_ctx)
            if reserved_value is not None:
                changed = True
                return reserved_value
            field_match = FIELD_TOKEN_PATTERN.match(stripped)
            if field_match:
                key_name = field_match.group(1)
                replacement = str(field_ctx.get(key_name, ""))
                changed = True
                return replacement
            if token_raw in static_map:
                changed = True
                return static_map[token_raw]
            return match.group(0)

        new_result = SUBSTITUTION_PATTERN.sub(replace, result)
        if new_result == result:
            break
        result = new_result

    leftovers = SUBSTITUTION_PATTERN.findall(result)
    static_keys = set(static_map.keys())
    for token in leftovers:
        if token in static_keys:
            missing.add(token)
            continue
        stripped = token.strip()
        if RANDOM_TOKEN_PATTERN.match(stripped) or LIST_TOKEN_PATTERN.match(stripped):
            missing.add(stripped)
            continue
        missing.add(token)
    return result, missing


def apply_substitutions_to_config(
    config: Dict[str, Any],
    rules: List[Dict[str, Any]],
    *,
    context: Optional[Dict[str, Dict[str, Any]]] = None,
    random_generator: Optional[random.Random] = None,
) -> Set[str]:
    if not config or not rules:
        return set()
    ctx_base = context or build_substitution_context(rules)
    ctx = dict(ctx_base)
    rng = random_generator or random.SystemRandom()
    missing: Set[str] = set()
    for field in SUBSTITUTION_TARGET_FIELDS:
        raw_value = config.get(field)
        ctx["fields"] = config
        substituted, unresolved = substitute_tokens(
            raw_value,
            rules,
            random_generator=rng,
            context=ctx,
        )
        if isinstance(substituted, str):
            config[field] = substituted
        missing.update(unresolved)
    return missing


def resolve_substitution_outputs(
    config_snapshot: Dict[str, Any],
    substitution_rules: List[Dict[str, Any]],
    lock_mode: Any,
    lock_snapshot: Any,
    *,
    context: Optional[Dict[str, Dict[str, Any]]] = None,
    random_generator: Optional[random.Random] = None,
    rcpt_source: Optional[str] = None,
    rcpt_override: bool = False,
    override_fields: Optional[Set[str]] = None,
) -> Tuple[Dict[str, Any], Optional[str], Set[str], Optional[Dict[str, Any]]]:
    working = dict(config_snapshot or {})
    overrides = set(override_fields or set())
    mode = sanitize_substitution_lock_mode(lock_mode)
    snapshot_payload = sanitize_substitution_snapshot(lock_snapshot)
    if mode == "lock":
        locked_fields = snapshot_payload.get("fields") or {}
        if not locked_fields:
            raise HTTPException(
                status_code=409,
                detail="고정 모드를 사용 중이지만 저장된 고정 값이 없습니다. '다시 뽑기'로 값을 만들거나 고정을 해제하세요.",
            )
        for field in SUBSTITUTION_TARGET_FIELDS:
            if field == "message_id_pattern":
                continue
            if field in overrides and field not in {"rcpt_to", "message_id_pattern"}:
                if field_contains_tokens(working.get(field)):
                    raise HTTPException(
                        status_code=409,
                        detail=f"고정 모드에서는 {field.upper()} 필드에 랜덤 패턴을 사용할 수 없습니다. 고정을 해제하거나 고정값을 갱신하세요.",
                    )
                continue
            locked_value = locked_fields.get(field)
            if locked_value is not None and field != "rcpt_to":
                working[field] = locked_value
            elif field not in {"rcpt_to", "message_id_pattern"} and field_contains_tokens(working.get(field)):
                raise HTTPException(
                    status_code=409,
                    detail=f"고정 모드에서 {field.upper()} 값을 계산할 수 없습니다. '다시 뽑기'로 고정값을 만든 뒤 잠그거나 고정을 해제하세요.",
                )
        if rcpt_override:
            if rcpt_source and field_contains_tokens(rcpt_source):
                raise HTTPException(
                    status_code=409,
                    detail="고정 모드에서는 RCPT TO에 랜덤 패턴을 사용할 수 없습니다. 고정을 해제하거나 고정값을 갱신하세요.",
                )
            rcpt_result = rcpt_source
        else:
            locked_rcpt = locked_fields.get("rcpt_to")
            if locked_rcpt is not None:
                rcpt_result = locked_rcpt
            else:
                if rcpt_source and field_contains_tokens(rcpt_source):
                    raise HTTPException(
                        status_code=409,
                        detail="고정 모드에서 RCPT TO 랜덤 값을 계산할 수 없습니다. '다시 뽑기' 후 고정을 유지하거나 고정을 해제하세요.",
                    )
                rcpt_result = rcpt_source
        auto_enabled, pattern_value = resolve_message_id_settings(working)
        working["message_id_auto"] = auto_enabled
        working["message_id_pattern"] = pattern_value
        if auto_enabled:
            locked_header = working.get("header")
            if isinstance(locked_header, str) and locked_header:
                working["header"] = _strip_message_id_header(locked_header)
        missing_tokens = set(snapshot_payload.get("missing_tokens") or [])
        return working, rcpt_result, missing_tokens, snapshot_payload
    rng = random_generator or random.SystemRandom()
    effective_context = context or build_substitution_context(substitution_rules)
    missing_tokens = apply_substitutions_to_config(
        working,
        substitution_rules,
        context=effective_context,
        random_generator=rng,
    )
    rcpt_result: Optional[str] = None
    if rcpt_source is not None:
        rcpt_result, rcpt_missing = substitute_tokens(
            rcpt_source,
            substitution_rules,
            random_generator=rng,
            context=effective_context,
        )
        missing_tokens.update(rcpt_missing)
    return working, rcpt_result, missing_tokens, None


def log_missing_substitutions(job: Dict[str, Any], missing: Iterable[str]) -> None:
    deduped = sorted({token for token in missing if token})
    if not deduped:
        return
    job_id = job.get("id")
    job_type = job.get("job_type")
    domain = job.get("domain")
    device_id = job.get("device_id")
    missing_str = ", ".join(deduped)
    log_console(
        f"[SUBSTITUTION] 미정의 변수({missing_str}) · job={job_id} type={job_type} device={device_id} domain={domain}"
    )


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


def sanitize_job_payload_for_output(job_type: str, payload: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(payload, dict):
        return None
    sanitized = dict(payload)
    if job_type in {"imap_test", "imap_fetch_latest", "imap_purge_spam"}:
        sanitized.pop("password", None)
        if not sanitized.get("use_saved_password"):
            sanitized.pop("use_saved_password", None)
    return sanitized


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
    trackable_job_types = {"batch_send", "imap_manual_check", "inject_file"}
    if job_row["job_type"] not in trackable_job_types:
        return
    data_json: Optional[str] = None
    if result is not None:
        try:
            if isinstance(result, dict):
                filtered = {
                    key: value
                    for key, value in result.items()
                    if key != "logs"
                }
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
    elif job_row["job_type"] in {"imap_test", "imap_fetch_latest", "imap_purge_spam"}:
        sanitized_payload = sanitize_job_payload_for_output(job_row["job_type"], payload)
        conn.execute(
            "UPDATE jobs SET payload=? WHERE id=?",
            (
                json.dumps(sanitized_payload, ensure_ascii=False) if sanitized_payload else None,
                job_row["id"],
            ),
        )


def cancel_active_sends(
    conn: sqlite3.Connection,
    cancel_message: str,
    device_id: Optional[str] = None,
    job_types: Optional[Iterable[str]] = None,
) -> Dict[str, int]:
    allowed_types = tuple({job_type for job_type in (job_types or ("batch_send", "single_send")) if job_type})
    if not allowed_types:
        return {"total_jobs": 0, "cancelled": 0, "cancel_requested": 0}
    query = """
        SELECT jobs.*, devices.status AS device_status
        FROM jobs
        JOIN devices ON devices.id = jobs.device_id
        WHERE jobs.job_type IN ({job_type_placeholders})
          AND jobs.status IN ('pending', 'dispatched', 'running')
    """
    job_type_placeholders = ",".join(["?"] * len(allowed_types))
    query = query.format(job_type_placeholders=job_type_placeholders)
    params: List[str] = list(allowed_types)
    if device_id:
        query = f"{query} AND jobs.device_id=?"
        params.append(device_id)
    job_rows = conn.execute(query, tuple(params)).fetchall()
    cancelled = 0
    cancel_requested = 0
    for job_row in job_rows:
        status = job_row["status"]
        job_id = job_row["id"]
        job_device_id = job_row["device_id"]
        device_status = job_row["device_status"]
        if status == "pending" or device_status != "connected":
            updated_row = update_job_status(
                conn,
                job_device_id,
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
    return {
        "total_jobs": len(job_rows),
        "cancelled": cancelled,
        "cancel_requested": cancel_requested,
    }


def format_local_timestamp() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


def _coerce_non_negative_int(value: Any) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def _collect_target_domains(raw: Any) -> List[str]:
    domains: List[str] = []
    if isinstance(raw, str):
        candidate = raw.strip().lower()
        if candidate:
            domains.append(candidate)
    elif isinstance(raw, Iterable):
        for item in raw:
            if not isinstance(item, str):
                continue
            candidate = item.strip().lower()
            if candidate:
                domains.append(candidate)
    return sorted({domain for domain in domains if domain})


def enrich_stop_metadata_with_sent_stats(
    conn: sqlite3.Connection,
    metadata: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    if metadata is None:
        metadata_obj: Dict[str, Any] = {}
    else:
        metadata_obj = dict(metadata)
    device_id = str(metadata_obj.get("device_id") or "").strip()
    if not device_id:
        return metadata_obj
    domain_key = metadata_obj.get("domain")
    domains_key = metadata_obj.get("domains")
    target_domains = _collect_target_domains(domain_key if domain_key is not None else domains_key)
    query_params: List[Any] = [device_id]
    if target_domains:
        placeholders = ",".join(["?"] * len(target_domains))
        domain_clause = f"AND domain IN ({placeholders})"
        query_params.extend(target_domains)
    else:
        domain_clause = ""
    rows = conn.execute(
        f"""
        SELECT domain, client_sent, imap_sent_since_last_check
        FROM device_configs
        WHERE device_id=?
        {domain_clause}
        """,
        tuple(query_params),
    ).fetchall()
    if not rows:
        return metadata_obj
    if target_domains and len(rows) == 1:
        row = rows[0]
        current_value = _coerce_non_negative_int(row["imap_sent_since_last_check"])
        total_value = _coerce_non_negative_int(row["client_sent"])
    else:
        current_value = sum(_coerce_non_negative_int(row["imap_sent_since_last_check"]) for row in rows)
        total_value = sum(_coerce_non_negative_int(row["client_sent"]) for row in rows)
    existing_current = max(
        _coerce_non_negative_int(metadata_obj.get("sent_window_count")),
        _coerce_non_negative_int(metadata_obj.get("sent_since_last_check")),
        _coerce_non_negative_int(metadata_obj.get("imap_sent_since_last_check")),
    )
    existing_total = max(
        _coerce_non_negative_int(metadata_obj.get("client_sent")),
        _coerce_non_negative_int(metadata_obj.get("sent_total_success")),
        _coerce_non_negative_int(metadata_obj.get("sent_sequence_total")),
    )
    if current_value > 0 or existing_current == 0:
        metadata_obj["sent_window_count"] = current_value
        metadata_obj["sent_since_last_check"] = current_value
        metadata_obj["imap_sent_since_last_check"] = current_value
    if total_value > 0 or existing_total == 0:
        metadata_obj["client_sent"] = total_value
        metadata_obj["sent_total_success"] = total_value
        metadata_obj["sent_sequence_total"] = total_value
    if target_domains and not metadata_obj.get("domains"):
        metadata_obj["domains"] = target_domains
    if not metadata_obj.get("stop_event_marker"):
        metadata_obj["stop_event_marker"] = f"{device_id}:{uuid.uuid4().hex}"
    return metadata_obj


def handle_auto_stop(
    conn: sqlite3.Connection,
    reason: str,
    *,
    origin: str = "auto",
    metadata: Optional[Dict[str, Any]] = None,
    device_id: Optional[str] = None,
    job_types: Optional[Iterable[str]] = None,
    suppress_notification: bool = False,
) -> Dict[str, Any]:
    enriched_metadata = enrich_stop_metadata_with_sent_stats(conn, metadata)
    result = cancel_active_sends(
        conn,
        reason,
        device_id=device_id,
        job_types=job_types,
    )
    payload = {
        "reason": reason,
        "origin": origin,
        "result": result,
        "metadata": enriched_metadata or {},
    }
    if device_id:
        payload["device_id"] = device_id
    if job_types:
        payload["job_types"] = list(job_types)
    return payload


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
    message_id_auto: Optional[bool] = True
    message_id_pattern: Optional[str] = MESSAGE_ID_PATTERN_DEFAULT
    session_count: Optional[int] = 1
    bcc_count: Optional[int] = 0
    anchor_interval: Optional[int] = 0
    anchor_email: Optional[str] = ""
    rcpt_to: Optional[str] = ""


class UpdateConfigRequest(DeviceConfigPayload):
    pass


class ImapSettingsPayload(BaseModel):
    enabled: Optional[bool] = None
    username: Optional[str] = None
    password: Optional[str] = None
    single_delay_seconds: Optional[int] = None
    sent_threshold: Optional[int] = None
    allowed_latency_seconds: Optional[int] = None
    failure_action: Optional[str] = None
    notify_before_stop_all: Optional[bool] = None
    delay_seconds: Optional[int] = None  # legacy alias for allowed latency


class ImapTestRequest(BaseModel):
    username: Optional[str] = None
    password: Optional[str] = None
    use_saved_password: Optional[bool] = False
    folder: Optional[str] = Field(
        default="Junk",
        description="연결 테스트 시 선택할 메일함 (기본값 Junk)",
    )


class ImapFetchLatestRequest(ImapTestRequest):
    limit: Optional[int] = Field(
        default=1,
        ge=1,
        le=10,
        description="가져올 최신 메일 개수 (현재 1개만 사용)",
    )


class ImapPurgeSpamRequest(ImapTestRequest):
    """스팸함 비우기 작업 요청"""

    pass


class ImapManualCheckRequest(BaseModel):
    reason: Optional[str] = Field(
        default=None,
        description="대시보드에서 수동 도착 확인을 요청한 사유",
    )


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


class ImapReportPayload(BaseModel):
    domain: str
    status: str
    sent_at: Optional[str] = None
    received_at: Optional[str] = None
    latency: Optional[float] = None
    reason: Optional[str] = None
    job_id: Optional[str] = None
    send_type: Optional[str] = None
    mail_from: Optional[str] = None
    trigger_stop: Optional[bool] = False
    anchor: Optional[bool] = False
    checked_at: Optional[str] = None
    delay_seconds: Optional[int] = None
    allowed_latency_seconds: Optional[int] = None
    failure_action: Optional[str] = None
    sent_window_count: Optional[int] = None
    sent_threshold: Optional[int] = None
    ip_change_attempted: Optional[bool] = None
    ip_change_success: Optional[bool] = None
    ip_change_message: Optional[str] = None
    ip_change_marker: Optional[str] = None
    ip_change_reason: Optional[str] = None
    ip_after_change: Optional[str] = None
    probe_mail_sent: Optional[bool] = None
    probe_mail_error: Optional[str] = None
    probe_status_line: Optional[str] = None
    probe_detail_line: Optional[str] = None


class HeartbeatRequest(BaseModel):
    device_name: str
    active_domain: Optional[str] = "naver"
    domain_states: List[DomainStatePayload] = Field(default_factory=list)
    job_reports: List[JobReportPayload] = Field(default_factory=list)
    public_ip: Optional[str] = Field(default=None, description="현재 공인 IP 주소")
    imap_reports: List[ImapReportPayload] = Field(default_factory=list)


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
    force_imap_check: Optional[bool] = False


class BatchSendRequest(BaseModel):
    domain: str


class HeaderPreviewRequest(BaseModel):
    rcpt_to: Optional[str] = None
    config_override: Dict[str, Any] = Field(default_factory=dict)


class HeaderPreviewResponse(BaseModel):
    helo: str
    mail_from: str
    header: str
    rcpt_to: str
    anchor_email: str
    missing_tokens: List[str] = Field(default_factory=list)
    generated_at: str


@app.post("/api/devices/{device_id}/domains/{domain}/preview-header", response_model=HeaderPreviewResponse)
def preview_single_header(device_id: str, domain: str, payload: HeaderPreviewRequest) -> HeaderPreviewResponse:
    normalized = normalize_domain(domain)
    with db_lock, get_conn() as conn:
        device = get_device(device_id, conn=conn)
        if not device:
            raise HTTPException(status_code=404, detail="디바이스를 찾을 수 없습니다.")
        configs = load_device_configs(device_id, conn=conn)
        global_config = load_global_config(conn=conn)
    base_config = configs.get(normalized) or {}
    substitution_rules = sanitize_substitution_rules(global_config.get("substitution_rules"))
    context = build_substitution_context(substitution_rules)
    config_snapshot = build_config_snapshot(configs, normalized)
    overrides = sanitize_preview_override(payload.config_override)
    if overrides:
        config_snapshot.update(overrides)
    rcpt_override = payload.rcpt_to is not None
    rcpt_source = str(payload.rcpt_to or "").strip() if rcpt_override else config_snapshot.get("rcpt_to")
    rng = random.SystemRandom()
    lock_mode = sanitize_substitution_lock_mode(base_config.get("substitution_lock_mode"))
    lock_snapshot = decode_substitution_snapshot(base_config.get("substitution_snapshot"))
    resolved_config, resolved_rcpt, missing_tokens, snapshot_meta = resolve_substitution_outputs(
        config_snapshot,
        substitution_rules,
        lock_mode,
        lock_snapshot,
        context=context,
        random_generator=rng,
        rcpt_source=rcpt_source,
        rcpt_override=rcpt_override,
        override_fields=set(overrides.keys()) if overrides else None,
    )
    auto_enabled, pattern_value = resolve_message_id_settings(resolved_config)
    resolved_config["message_id_auto"] = auto_enabled
    resolved_config["message_id_pattern"] = pattern_value
    raw_header_value = resolved_config.get("header")
    if auto_enabled:
        resolved_config["header"] = ensure_message_id_header(
            raw_header_value,
            auto_enabled=True,
            pattern_value=pattern_value,
            mail_from=resolved_config.get("mail_from"),
            helo=resolved_config.get("helo"),
        )
    elif not isinstance(raw_header_value, str):
        resolved_config["header"] = str(raw_header_value or "")
    rcpt_value = resolved_rcpt if resolved_rcpt is not None else (rcpt_source or "")
    generated_marker = snapshot_meta.get("generated_at") if snapshot_meta else None
    return HeaderPreviewResponse(
        helo=resolved_config.get("helo", ""),
        mail_from=resolved_config.get("mail_from", ""),
        header=resolved_config.get("header", ""),
        rcpt_to=rcpt_value,
        anchor_email=resolved_config.get("anchor_email", ""),
        missing_tokens=sorted(missing_tokens),
        generated_at=generated_marker or now_ts(),
    )


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


class SubstitutionRule(BaseModel):
    key: str
    value: str = ""
    source: str = ""
    encoding: str = "none"
    mode: str = "static"
    values: List[str] = Field(default_factory=list)
    description: str = ""


class SubstitutionPreviewItem(BaseModel):
    key: Optional[str] = None
    source: Optional[str] = ""
    encoding: Optional[str] = None


class SubstitutionSnapshotResponse(BaseModel):
    fields: Dict[str, str] = Field(default_factory=dict)
    generated_at: Optional[str] = None
    device_id: Optional[str] = None
    domain: Optional[str] = None
    missing_tokens: List[str] = Field(default_factory=list)


class DeviceLockRequest(BaseModel):
    helo: Optional[str] = None
    mail_from: Optional[str] = None
    header: Optional[str] = None
    anchor_email: Optional[str] = None
    rcpt_to: Optional[str] = None
    missing_tokens: Optional[List[str]] = None


class DeviceLockRefreshRequest(BaseModel):
    rcpt_to: Optional[str] = None
    header_override: Optional[str] = None


class SubstitutionPreviewRequest(BaseModel):
    items: List[SubstitutionPreviewItem] = Field(default_factory=list)
    rules: Optional[List[SubstitutionRule]] = None


class SubstitutionPreviewResponse(BaseModel):
    results: List[str] = Field(default_factory=list)


class DeviceLockCopyRequest(BaseModel):
    source_device_id: str
    domain: str
    target_device_ids: List[str] = Field(default_factory=list)


class GlobalConfigPayload(BaseModel):
    helo: Optional[str] = None
    mail_from: Optional[str] = None
    header: Optional[str] = None
    bcc_count: Optional[int] = None
    session_count: Optional[int] = None
    active_domain: Optional[str] = None
    stop_schedule_enabled: Optional[bool] = None
    stop_schedule_time: Optional[str] = None
    substitution_rules: Optional[List[SubstitutionRule]] = None
    telegram_bot_token: Optional[str] = None
    telegram_chat_id: Optional[str] = None
    message_id_auto: Optional[bool] = None
    message_id_pattern: Optional[str] = None


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
    substitution_rules: List[SubstitutionRule] = Field(default_factory=list)
    telegram_bot_token: str
    telegram_chat_id: str
    message_id_auto: bool
    message_id_pattern: str


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


class TelegramTestRequest(BaseModel):
    bot_token: Optional[str] = None
    chat_id: Optional[str] = None
    message: Optional[str] = None


@app.get("/health")
def health_check() -> Dict[str, str]:
    return {
        "status": "ok",
        "timestamp": now_ts(),
    }


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request) -> HTMLResponse:
    return templates.TemplateResponse("dashboard.html", {"request": request})


@app.get("/api/devices")
def list_devices(response: Response) -> Dict[str, Any]:
    response.headers["Cache-Control"] = "no-store"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return load_device_summary()


@app.post("/api/substitution/encode", response_model=SubstitutionPreviewResponse)
def preview_substitution_endpoint(payload: SubstitutionPreviewRequest) -> SubstitutionPreviewResponse:
    items = payload.items or []
    base_rules: List[Dict[str, Any]]
    if payload.rules:
        base_rules = payload.rules
    else:
        config = load_global_config()
        base_rules = config.get("substitution_rules") or []

    rng = random.Random(random.SystemRandom().randrange(1 << 63))
    sanitized_rules = canonicalize_substitution_rules(base_rules, random_generator=rng)
    context = build_substitution_context(sanitized_rules)
    static_base = context.get("static") or {}
    list_base = context.get("lists") or {}

    results: List[str] = []
    for index, item in enumerate(items, start=1):
        source = item.source or ""
        encoding = normalize_encoding_name(item.encoding)
        key = (item.key or "").strip()
        if not source:
            results.append("")
            continue

        preview_rng = random.Random(random.SystemRandom().randrange(1 << 63))
        static_map = dict(static_base)
        if key:
            static_map.pop(key, None)
        substitution_context = {
            "static": static_map,
            "lists": list_base,
        }

        substituted, missing = substitute_tokens(
            source,
            sanitized_rules,
            random_generator=preview_rng,
            context=substitution_context,
        )
        if missing:
            unresolved = ", ".join(sorted(missing))
            raise HTTPException(
                status_code=400,
                detail=f"{index}번 행: {unresolved} 패턴을 치환하지 못했습니다.",
            )

        try:
            encoded = encode_substitution_value(
                substituted,
                encoding,
                random_choice=preview_rng.choice if hasattr(preview_rng, "choice") else None,
                random_generator=preview_rng,
            )
        except UnicodeEncodeError as exc:
            if encoding == "quoted_printable_euckr":
                raise HTTPException(
                    status_code=400,
                    detail=f"{index}번 행: EUC-KR로 변환할 수 없는 문자가 포함되어 있습니다.",
                ) from exc
            raise HTTPException(
                status_code=400,
                detail=f"{index}번 행: 치환 값을 생성하지 못했습니다.",
            ) from exc

        results.append(encoded)

    return SubstitutionPreviewResponse(results=results)


def build_global_config_response(config: Dict[str, Any]) -> GlobalConfigResponse:
    schedule_time = sanitize_stop_schedule_time(config.get("stop_schedule_time"))
    schedule_enabled = sanitize_stop_schedule_enabled(config.get("stop_schedule_enabled"))
    if schedule_enabled and not schedule_time:
        schedule_enabled = False
    raw_message_auto = config.get("message_id_auto")
    if isinstance(raw_message_auto, str):
        lowered = raw_message_auto.strip().lower()
        message_id_auto = lowered in {"1", "true", "yes", "on"}
    elif raw_message_auto is None:
        message_id_auto = bool(DEFAULT_DOMAIN_CONFIG.get("message_id_auto", True))
    else:
        message_id_auto = bool(raw_message_auto)
    message_id_pattern = _normalize_message_id_pattern(config.get("message_id_pattern"))
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
        substitution_rules=[
            SubstitutionRule(**rule)
            for rule in sanitize_substitution_rules(config.get("substitution_rules"))
        ],
        telegram_bot_token=sanitize_telegram_bot_token(config.get("telegram_bot_token")),
        telegram_chat_id=sanitize_telegram_chat_id(config.get("telegram_chat_id")),
        message_id_auto=message_id_auto,
        message_id_pattern=message_id_pattern,
    )


@app.get("/api/global/config", response_model=GlobalConfigResponse)
def get_global_config_endpoint() -> GlobalConfigResponse:
    config = load_global_config()
    return build_global_config_response(config)


@app.post("/api/global/config/apply")
def apply_global_config_endpoint(payload: GlobalConfigPayload) -> Dict[str, Any]:
    fields_set = getattr(payload, "__fields_set__", set())
    domain_update_requested = "active_domain" in fields_set
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
            if field not in fields_set:
                continue
            raw_value = getattr(payload, field)
            if field == "header":
                value_to_apply = raw_value or ""
                should_apply = isinstance(value_to_apply, str) and bool(value_to_apply.strip())
            elif field in ("helo", "mail_from"):
                value_to_apply = (raw_value or "").strip()
                should_apply = bool(value_to_apply)
            elif field == "bcc_count":
                value_to_apply = clamp_bcc_count(raw_value)
                should_apply = raw_value is not None
            elif field == "session_count":
                value_to_apply = sanitize_session_count(raw_value)
                should_apply = raw_value is not None
            elif field == "message_id_auto":
                if raw_value is None:
                    continue
                value_to_apply = 1 if raw_value else 0
                should_apply = True
            elif field == "message_id_pattern":
                if raw_value is None:
                    continue
                candidate_pattern = str(raw_value or "").strip() or MESSAGE_ID_PATTERN_DEFAULT
                value_to_apply = candidate_pattern
                should_apply = True
            else:
                continue
            if should_apply:
                apply_values[field] = value_to_apply
                if field == "message_id_auto":
                    stored_config[field] = bool(value_to_apply)
                else:
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
        if "substitution_rules" in fields_set:
            try:
                normalized_rules = canonicalize_substitution_rules(payload.substitution_rules or [], strict=True)
            except ValueError as exc:
                raise HTTPException(status_code=400, detail=str(exc))
            if normalized_rules != stored_config.get("substitution_rules", []):
                stored_config["substitution_rules"] = normalized_rules
                applied_fields.append("substitution_rules")
        else:
            stored_config["substitution_rules"] = sanitize_substitution_rules(stored_config.get("substitution_rules"))
        if "telegram_bot_token" in fields_set:
            sanitized_token = sanitize_telegram_bot_token(payload.telegram_bot_token)
            if sanitized_token != stored_config.get("telegram_bot_token", ""):
                stored_config["telegram_bot_token"] = sanitized_token
                applied_fields.append("telegram_bot_token")
            else:
                stored_config["telegram_bot_token"] = sanitized_token
        else:
            stored_config["telegram_bot_token"] = sanitize_telegram_bot_token(stored_config.get("telegram_bot_token"))
        if "telegram_chat_id" in fields_set:
            sanitized_chat_id = sanitize_telegram_chat_id(payload.telegram_chat_id)
            if sanitized_chat_id != stored_config.get("telegram_chat_id", ""):
                stored_config["telegram_chat_id"] = sanitized_chat_id
                applied_fields.append("telegram_chat_id")
            else:
                stored_config["telegram_chat_id"] = sanitized_chat_id
        else:
            stored_config["telegram_chat_id"] = sanitize_telegram_chat_id(stored_config.get("telegram_chat_id"))
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
    log_console(
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


def _sanitize_lock_fields(payload: DeviceLockRequest) -> Dict[str, str]:
    fields: Dict[str, str] = {}
    for key in SNAPSHOT_FIELD_KEYS:
        if key == "message_id_pattern":
            continue
        value = getattr(payload, key, None)
        if value is None:
            continue
        text = str(value)
        if field_contains_tokens(text):
            raise HTTPException(
                status_code=400,
                detail=f"{key.upper()} 값에 치환 토큰이 포함되어 있습니다. 미리보기에서 생성된 실제 값을 전달하세요.",
            )
        fields[key] = text
    return fields


@app.post("/api/devices/{device_id}/domains/{domain}/lock")
def set_device_lock(device_id: str, domain: str, payload: DeviceLockRequest) -> Dict[str, Any]:
    normalized = normalize_domain(domain)
    lock_fields = _sanitize_lock_fields(payload)
    if not lock_fields:
        raise HTTPException(status_code=400, detail="고정할 필드 값을 최소 1개 이상 전달하세요.")
    missing_tokens = sorted({str(token).strip() for token in (payload.missing_tokens or []) if token})
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
        auto_enabled, _ = resolve_message_id_settings(config_data)
        if auto_enabled and "header" in lock_fields:
            lock_fields["header"] = _strip_message_id_header(lock_fields["header"])
        snapshot_payload: Dict[str, Any] = {
            "fields": lock_fields,
            "missing_tokens": missing_tokens,
            "generated_at": now_ts(),
            "device_id": device_id,
            "domain": normalized,
            "source_device_id": device_id,
        }
        snapshot_json = encode_substitution_snapshot(snapshot_payload)
        now = now_ts()
        conn.execute(
            """
            UPDATE device_configs
            SET substitution_lock_mode='lock',
                substitution_snapshot=?,
                updated_at=?
            WHERE device_id=? AND domain=?
            """,
            (snapshot_json, now, device_id, normalized),
        )
        conn.commit()
        updated_row = conn.execute(
            "SELECT * FROM device_configs WHERE device_id=? AND domain=?",
            (device_id, normalized),
        ).fetchone()
    if not updated_row:
        raise HTTPException(status_code=500, detail="고정 값을 저장하지 못했습니다.")
    config_response = serialize_config(to_dict(updated_row), include_secret=False)
    response_snapshot = _snapshot_with_preview_header(
        config_response.get("substitution_snapshot"),
        config_response,
    )
    config_response["substitution_snapshot"] = response_snapshot
    return {
        "device_id": device_id,
        "domain": normalized,
        "snapshot": response_snapshot,
        "config": config_response,
    }


@app.post("/api/devices/{device_id}/domains/{domain}/lock/refresh")
def refresh_device_lock(device_id: str, domain: str, payload: DeviceLockRefreshRequest) -> Dict[str, Any]:
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
        global_config = load_global_config(conn=conn)
        substitution_rules = sanitize_substitution_rules(global_config.get("substitution_rules"))
        context = build_substitution_context(substitution_rules)
        configs = load_device_configs(device_id, conn=conn)
        config_snapshot = build_config_snapshot(configs, normalized)
        override_fields: Set[str] = set()
        if payload.header_override is not None:
            config_snapshot["header"] = payload.header_override
            override_fields.add("header")
        rcpt_source = (
            (payload.rcpt_to or "").strip()
            if payload.rcpt_to is not None
            else config_snapshot.get("rcpt_to")
        )
        rng = random.SystemRandom()
        resolved_config, resolved_rcpt, missing_tokens, _ = resolve_substitution_outputs(
            config_snapshot,
            substitution_rules,
            "auto",
            EMPTY_SUBSTITUTION_SNAPSHOT,
            context=context,
            random_generator=rng,
            rcpt_source=rcpt_source,
            rcpt_override=payload.rcpt_to is not None,
            override_fields=override_fields,
        )
        auto_enabled, pattern_value = resolve_message_id_settings(resolved_config)
        resolved_config["message_id_auto"] = auto_enabled
        resolved_config["message_id_pattern"] = pattern_value
        if auto_enabled:
            header_candidate = resolved_config.get("header")
            if isinstance(header_candidate, str) and header_candidate:
                resolved_config["header"] = _strip_message_id_header(header_candidate)
        if missing_tokens:
            unresolved = ", ".join(sorted(missing_tokens))
            raise HTTPException(
                status_code=400,
                detail=f"치환되지 않은 토큰({unresolved})이 있어 고정 값을 생성할 수 없습니다.",
            )
        lock_fields: Dict[str, str] = {}
        for key in SNAPSHOT_FIELD_KEYS:
            value = resolved_config.get(key)
            if isinstance(value, str):
                lock_fields[key] = value
        if resolved_rcpt is not None:
            lock_fields["rcpt_to"] = resolved_rcpt
        if not lock_fields:
            raise HTTPException(status_code=400, detail="고정할 필드 값을 생성하지 못했습니다.")
        snapshot_payload: Dict[str, Any] = {
            "fields": lock_fields,
            "missing_tokens": [],
            "generated_at": now_ts(),
            "device_id": device_id,
            "domain": normalized,
            "source_device_id": device_id,
        }
        snapshot_json = encode_substitution_snapshot(snapshot_payload)
        now = now_ts()
        conn.execute(
            """
            UPDATE device_configs
            SET substitution_lock_mode='lock',
                substitution_snapshot=?,
                updated_at=?
            WHERE device_id=? AND domain=?
            """,
            (snapshot_json, now, device_id, normalized),
        )
        conn.commit()
        updated_row = conn.execute(
            "SELECT * FROM device_configs WHERE device_id=? AND domain=?",
            (device_id, normalized),
        ).fetchone()
    if not updated_row:
        raise HTTPException(status_code=500, detail="고정 값을 저장하지 못했습니다.")
    config_response = serialize_config(to_dict(updated_row), include_secret=False)
    response_snapshot = _snapshot_with_preview_header(
        config_response.get("substitution_snapshot"),
        config_response,
    )
    config_response["substitution_snapshot"] = response_snapshot
    return {
        "device_id": device_id,
        "domain": normalized,
        "snapshot": response_snapshot,
        "config": config_response,
    }


@app.post("/api/devices/{device_id}/domains/{domain}/lock/reset")
def reset_device_lock(device_id: str, domain: str) -> Dict[str, Any]:
    normalized = normalize_domain(domain)
    with db_lock, get_conn() as conn:
        device = get_device(device_id, conn=conn)
        if not device:
            raise HTTPException(status_code=404, detail="디바이스를 찾을 수 없습니다.")
        conn.execute(
            """
            UPDATE device_configs
            SET substitution_lock_mode='auto',
                substitution_snapshot='{}',
                updated_at=?
            WHERE device_id=? AND domain=?
            """,
            (now_ts(), device_id, normalized),
        )
        conn.commit()
        updated_row = conn.execute(
            "SELECT * FROM device_configs WHERE device_id=? AND domain=?",
            (device_id, normalized),
        ).fetchone()
    if not updated_row:
        raise HTTPException(status_code=500, detail="고정을 해제하지 못했습니다.")
    snapshot_payload = {
        "fields": {},
        "missing_tokens": [],
        "generated_at": None,
        "device_id": device_id,
        "domain": normalized,
        "source_device_id": None,
    }
    return {
        "device_id": device_id,
        "domain": normalized,
        "snapshot": snapshot_payload,
        "config": serialize_config(to_dict(updated_row), include_secret=False),
    }


@app.post("/api/substitution-lock/copy")
def copy_device_lock(payload: DeviceLockCopyRequest) -> Dict[str, Any]:
    source_device_id = (payload.source_device_id or "").strip()
    if not source_device_id:
        raise HTTPException(status_code=400, detail="원본 디바이스 ID를 입력하세요.")
    normalized_domain = normalize_domain(payload.domain or "naver")
    target_ids = [device_id.strip() for device_id in payload.target_device_ids or [] if device_id and device_id.strip()]
    target_ids = [device_id for device_id in target_ids if device_id != source_device_id]
    if not target_ids:
        raise HTTPException(status_code=400, detail="복사할 대상 디바이스를 선택하세요.")
    with db_lock, get_conn() as conn:
        source_row = conn.execute(
            "SELECT substitution_lock_mode, substitution_snapshot, message_id_auto, message_id_pattern FROM device_configs WHERE device_id=? AND domain=?",
            (source_device_id, normalized_domain),
        ).fetchone()
        if not source_row:
            raise HTTPException(status_code=404, detail="원본 디바이스 도메인 설정을 찾을 수 없습니다.")
        source_data = to_dict(source_row)
        source_mode = sanitize_substitution_lock_mode(source_data.get("substitution_lock_mode"))
        if source_mode != "lock":
            raise HTTPException(status_code=409, detail="원본 카드가 고정 모드가 아닙니다.")
        source_snapshot = decode_substitution_snapshot(source_data.get("substitution_snapshot"))
        locked_fields = source_snapshot.get("fields") or {}
        if not locked_fields:
            raise HTTPException(status_code=409, detail="원본 카드에 고정된 값이 없습니다.")
        source_message_auto_raw = source_data.get("message_id_auto")
        if source_message_auto_raw is None:
            source_message_auto = 1
        else:
            try:
                source_message_auto = 1 if int(source_message_auto_raw) else 0
            except (TypeError, ValueError):
                source_message_auto = 1 if source_message_auto_raw else 0
        source_message_pattern = _normalize_message_id_pattern(source_data.get("message_id_pattern"))
        updated_entries: List[Dict[str, Any]] = []
        now = now_ts()
        for target_id in target_ids:
            target = get_device(target_id, conn=conn)
            if not target:
                raise HTTPException(status_code=404, detail=f"대상 디바이스({target_id})를 찾을 수 없습니다.")
            target_row = conn.execute(
                "SELECT * FROM device_configs WHERE device_id=? AND domain=?",
                (target_id, normalized_domain),
            ).fetchone()
            if not target_row:
                raise HTTPException(status_code=404, detail=f"대상 디바이스({target_id})의 도메인 설정을 찾을 수 없습니다.")
            snapshot_payload = {
                "fields": dict(locked_fields),
                "missing_tokens": source_snapshot.get("missing_tokens", []),
                "generated_at": now,
                "device_id": target_id,
                "domain": normalized_domain,
                "source_device_id": source_device_id,
            }
            snapshot_json = encode_substitution_snapshot(snapshot_payload)
            conn.execute(
                """
                UPDATE device_configs
                SET substitution_lock_mode='lock',
                    substitution_snapshot=?,
                    message_id_auto=?,
                    message_id_pattern=?,
                    updated_at=?
                WHERE device_id=? AND domain=?
                """,
                (snapshot_json, source_message_auto, source_message_pattern, now, target_id, normalized_domain),
            )
            updated_row = conn.execute(
                "SELECT * FROM device_configs WHERE device_id=? AND domain=?",
                (target_id, normalized_domain),
            ).fetchone()
            updated_entries.append(
                {
                    "device_id": target_id,
                    "domain": normalized_domain,
                    "snapshot": snapshot_payload,
                    "config": serialize_config(to_dict(updated_row), include_secret=False) if updated_row else {},
                }
            )
        conn.commit()
    return {
        "updated": updated_entries,
        "count": len(updated_entries),
        "domain": normalized_domain,
        "source_device_id": source_device_id,
    }


@app.post("/api/substitution-lock/reset-all")
def reset_all_device_locks() -> Dict[str, Any]:
    with db_lock, get_conn() as conn:
        cursor = conn.execute(
            """
            UPDATE device_configs
            SET substitution_lock_mode='auto',
                substitution_snapshot='{}',
                updated_at=?
            WHERE substitution_lock_mode!='auto'
               OR substitution_snapshot NOT IN ('{}', '')
            """,
            (now_ts(),),
        )
        updated_count = cursor.rowcount
        conn.commit()
    return {"updated": updated_count}


@app.post("/api/global/telegram/test")
def send_global_telegram_test(payload: TelegramTestRequest) -> Dict[str, Any]:
    with db_lock, get_conn() as conn:
        config = load_global_config(conn=conn)
    candidate_token = payload.bot_token if payload.bot_token is not None else config.get("telegram_bot_token")
    candidate_chat_id = payload.chat_id if payload.chat_id is not None else config.get("telegram_chat_id")
    bot_token = sanitize_telegram_bot_token(candidate_token)
    chat_id = sanitize_telegram_chat_id(candidate_chat_id)
    if not bot_token or not chat_id:
        raise HTTPException(status_code=400, detail="텔레그램 봇 토큰과 챗 ID를 모두 입력하세요.")
    message_text = payload.message or f"[MailSender] 텔레그램 테스트 ({format_local_timestamp()})"
    try:
        response = send_telegram_message(bot_token, chat_id, message_text)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=f"텔레그램 발송 실패: {exc}")
    log_console("[TELEGRAM] 테스트 메시지를 발송했습니다.")
    return {
        "ok": True,
        "message": "텔레그램 메시지를 발송했습니다.",
        "response": response,
    }


@app.post("/api/devices/register", response_model=RegisterResponse)
def register_device(payload: RegisterRequest) -> RegisterResponse:
    device = ensure_device(payload.device_id or uuid.uuid4().hex, payload.device_name, payload.public_ip)
    raw_configs = load_device_configs(device["id"])
    configs = {domain: serialize_config(row, include_secret=True) for domain, row in raw_configs.items()}
    _log_device_connection_event(device["id"], device.get("name"), "connected", device.get("public_ip"))
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
        if payload.message_id_auto is None:
            message_id_auto_flag = 1 if config_data.get("message_id_auto", 1) else 0
        else:
            message_id_auto_flag = 1 if payload.message_id_auto else 0
        pattern_candidate = payload.message_id_pattern if payload.message_id_pattern is not None else config_data.get("message_id_pattern")
        sanitized_pattern = _normalize_message_id_pattern(pattern_candidate)
        conn.execute(
            """
            UPDATE device_configs
            SET helo=?, smtp_host=?, smtp_port=?, mail_from=?, header=?, message_id_auto=?, message_id_pattern=?, session_count=?, bcc_count=?, anchor_interval=?, anchor_email=?, rcpt_to=?,
                updated_at=?
            WHERE device_id=? AND domain=?
            """,
            (
                payload.helo or "",
                payload.smtp_host or "",
                int(payload.smtp_port or 25),
                payload.mail_from or "",
                payload.header or "",
                message_id_auto_flag,
                sanitized_pattern,
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
    return serialize_config(to_dict(config_row), include_secret=False)


@app.post("/api/devices/{device_id}/domains/{domain}/imap")
def update_device_imap_settings(device_id: str, domain: str, payload: ImapSettingsPayload) -> Dict[str, Any]:
    normalized = normalize_domain(domain)
    if normalized != "naver":
        raise HTTPException(status_code=400, detail="네이버 도메인에서만 IMAP 확인을 지원합니다.")
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
        config = to_dict(config_row)
        desired_enabled = sanitize_imap_enabled(payload.enabled if payload.enabled is not None else config.get("imap_enabled"))
        username = normalize_imap_username(payload.username if payload.username is not None else config.get("imap_username"))
        if payload.password is not None:
            password = normalize_imap_password(payload.password)
        else:
            password = config.get("imap_password", "")
        allowed_latency_source = (
            payload.allowed_latency_seconds
            if payload.allowed_latency_seconds is not None
            else payload.delay_seconds
        )
        allowed_latency_value = sanitize_imap_allowed_latency(
            allowed_latency_source
            if allowed_latency_source is not None
            else config.get("imap_allowed_latency_seconds")
        )
        single_delay_value = (
            sanitize_imap_delay(
                payload.single_delay_seconds,
                default=DEFAULT_DOMAIN_CONFIG.get("imap_single_delay_seconds"),
                minimum=IMAP_CHECK_DELAY_MIN_SECONDS,
            )
            if payload.single_delay_seconds is not None
            else sanitize_imap_delay(
                config.get("imap_single_delay_seconds"),
                default=DEFAULT_DOMAIN_CONFIG.get("imap_single_delay_seconds"),
                minimum=IMAP_CHECK_DELAY_MIN_SECONDS,
            )
        )
        threshold_source = payload.sent_threshold if payload.sent_threshold is not None else config.get("imap_sent_threshold")
        sent_threshold_value = sanitize_imap_sent_threshold(
            threshold_source,
            default=DEFAULT_DOMAIN_CONFIG.get("imap_sent_threshold"),
        )
        failure_action_value = sanitize_imap_failure_action(
            payload.failure_action if payload.failure_action is not None else config.get("imap_failure_action")
        )
        notify_before_stop_value = sanitize_imap_notify_before_stop_all(
            payload.notify_before_stop_all
            if payload.notify_before_stop_all is not None
            else config.get("imap_notify_before_stop_all")
        )
        if failure_action_value == "none":
            notify_before_stop_value = False
        if desired_enabled and (not username or not password):
            raise HTTPException(status_code=400, detail="IMAP을 활성화하려면 계정 ID와 비밀번호가 필요합니다.")
        now = now_ts()
        if desired_enabled:
            status_value = ""
            error_text = ""
            checked_at = None
        else:
            status_value = "disabled"
            error_text = ""
            checked_at = now
        latency_reset = None
        sent_reset = None
        received_reset = None
        mail_from_value = config.get("mail_from") or config.get("imap_last_mail_from") or ""
        conn.execute(
            """
            UPDATE device_configs
            SET imap_enabled=?,
                imap_username=?,
                imap_password=?,
                imap_delay_seconds=?,
                imap_single_delay_seconds=?,
                imap_allowed_latency_seconds=?,
                imap_failure_action=?,
                imap_notify_before_stop_all=?,
                imap_sent_threshold=?,
                imap_last_status=?,
                imap_last_checked_at=?,
                imap_last_error=?,
                imap_last_latency=?,
                imap_last_sent_at=?,
                imap_last_received_at=?,
                imap_last_mail_from=?,
                updated_at=?
            WHERE device_id=? AND domain=?
            """,
            (
                1 if desired_enabled else 0,
                username,
                password,
                allowed_latency_value,
                single_delay_value,
                allowed_latency_value,
                failure_action_value,
                1 if notify_before_stop_value else 0,
                sent_threshold_value,
                status_value,
                checked_at,
                error_text,
                latency_reset,
                sent_reset,
                received_reset,
                mail_from_value,
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
    return {"config": serialize_config(to_dict(refreshed), include_secret=False)}


@app.post("/api/devices/{device_id}/domains/{domain}/imap/test")
def enqueue_imap_test(device_id: str, domain: str, payload: ImapTestRequest) -> Dict[str, Any]:
    normalized = normalize_domain(domain)
    if normalized != "naver":
        raise HTTPException(status_code=400, detail="네이버 도메인에서만 IMAP 확인을 지원합니다.")
    username = normalize_imap_username(payload.username)
    if not username:
        raise HTTPException(status_code=400, detail="IMAP 계정 ID를 입력하세요.")
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
        global_config = load_global_config(conn=conn)
        bot_token, chat_id = extract_telegram_credentials(global_config)
        saved_password = config_data.get("imap_password") or ""
        use_saved = bool(payload.use_saved_password)
        password_value = (
            normalize_imap_password(payload.password)
            if payload.password is not None
            else ""
        )
        if not password_value and not use_saved:
            raise HTTPException(status_code=400, detail="비밀번호를 입력하거나 저장된 비밀번호 사용을 선택하세요.")
        if use_saved and not saved_password and not password_value:
            raise HTTPException(status_code=400, detail="저장된 비밀번호가 없어 사용할 수 없습니다.")
        job_payload: Dict[str, Any] = {
            "username": username,
            "folder": (payload.folder or "Junk").strip() or "Junk",
        }
        if password_value:
            job_payload["password"] = password_value
        else:
            job_payload["use_saved_password"] = True
        job_payload = attach_telegram_credentials(
            job_payload,
            bot_token=bot_token,
            chat_id=chat_id,
        )
        job = create_job(conn, device_id, normalized, "imap_test", job_payload)
        conn.commit()
    public_job = dict(job)
    public_job["payload"] = sanitize_job_payload_for_output(job["job_type"], job.get("payload"))
    return {"job": public_job}


@app.post("/api/devices/{device_id}/domains/{domain}/imap/latest")
def enqueue_imap_fetch_latest(device_id: str, domain: str, payload: ImapFetchLatestRequest) -> Dict[str, Any]:
    normalized = normalize_domain(domain)
    if normalized != "naver":
        raise HTTPException(status_code=400, detail="네이버 도메인에서만 IMAP 확인을 지원합니다.")
    username = normalize_imap_username(payload.username)
    if not username:
        raise HTTPException(status_code=400, detail="IMAP 계정 ID를 입력하세요.")
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
        global_config = load_global_config(conn=conn)
        bot_token, chat_id = extract_telegram_credentials(global_config)
        saved_password = config_data.get("imap_password") or ""
        use_saved = bool(payload.use_saved_password)
        password_value = (
            normalize_imap_password(payload.password)
            if payload.password is not None
            else ""
        )
        if not password_value and not use_saved:
            raise HTTPException(status_code=400, detail="비밀번호를 입력하거나 저장된 비밀번호 사용을 선택하세요.")
        if use_saved and not saved_password and not password_value:
            raise HTTPException(status_code=400, detail="저장된 비밀번호가 없어 사용할 수 없습니다.")
        try:
            limit_candidate = int(payload.limit or 1)
        except (TypeError, ValueError):
            limit_candidate = 1
        limit_value = max(1, min(10, limit_candidate))
        job_payload: Dict[str, Any] = {
            "username": username,
            "folder": (payload.folder or "Junk").strip() or "Junk",
            "limit": limit_value,
        }
        if password_value:
            job_payload["password"] = password_value
        else:
            job_payload["use_saved_password"] = True
        job_payload = attach_telegram_credentials(
            job_payload,
            bot_token=bot_token,
            chat_id=chat_id,
        )
        job = create_job(conn, device_id, normalized, "imap_fetch_latest", job_payload)
        conn.commit()
    public_job = dict(job)
    public_job["payload"] = sanitize_job_payload_for_output(job["job_type"], job.get("payload"))
    return {"job": public_job}


@app.post("/api/devices/{device_id}/domains/{domain}/imap/purge-spam")
def enqueue_imap_purge_spam(device_id: str, domain: str, payload: ImapPurgeSpamRequest) -> Dict[str, Any]:
    normalized = normalize_domain(domain)
    if normalized != "naver":
        raise HTTPException(status_code=400, detail="네이버 도메인에서만 스팸함 비우기를 지원합니다.")
    username = normalize_imap_username(payload.username)
    if not username:
        raise HTTPException(status_code=400, detail="IMAP 계정 ID를 입력하세요.")
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
        global_config = load_global_config(conn=conn)
        bot_token, chat_id = extract_telegram_credentials(global_config)
        saved_password = config_data.get("imap_password") or ""
        use_saved = bool(payload.use_saved_password)
        password_value = (
            normalize_imap_password(payload.password)
            if payload.password is not None
            else ""
        )
        if not password_value and not use_saved:
            raise HTTPException(status_code=400, detail="비밀번호를 입력하거나 저장된 비밀번호 사용을 선택하세요.")
        if use_saved and not saved_password and not password_value:
            raise HTTPException(status_code=400, detail="저장된 비밀번호가 없어 사용할 수 없습니다.")
        folder_value = (payload.folder or "Junk").strip() or "Junk"
        job_payload: Dict[str, Any] = {
            "username": username,
            "folder": folder_value,
        }
        if password_value:
            job_payload["password"] = password_value
        else:
            job_payload["use_saved_password"] = True
        job_payload = attach_telegram_credentials(
            job_payload,
            bot_token=bot_token,
            chat_id=chat_id,
        )
        job = create_job(conn, device_id, normalized, "imap_purge_spam", job_payload)
        conn.commit()
    public_job = dict(job)
    public_job["payload"] = sanitize_job_payload_for_output(job["job_type"], job.get("payload"))
    return {"job": public_job}


@app.post("/api/devices/{device_id}/domains/{domain}/imap/manual-check")
def enqueue_imap_manual_check(device_id: str, domain: str, payload: ImapManualCheckRequest) -> Dict[str, Any]:
    normalized = normalize_domain(domain)
    if normalized != "naver":
        raise HTTPException(status_code=400, detail="네이버 도메인에서만 IMAP 확인을 지원합니다.")
    reason_text = (payload.reason or "").strip()
    context_reason = reason_text or "사용자 수동 도착 확인"
    missing_tokens: Set[str] = set()
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
        username_value = normalize_imap_username(config_data.get("imap_username"))
        if not username_value:
            raise HTTPException(status_code=400, detail="IMAP 계정 ID가 설정되어 있지 않습니다.")
        global_config = load_global_config(conn=conn)
        bot_token, chat_id = extract_telegram_credentials(global_config)
        substitution_rules = sanitize_substitution_rules(global_config.get("substitution_rules"))
        context = build_substitution_context(substitution_rules)
        configs = load_device_configs(device_id, conn=conn)
        config_snapshot = build_config_snapshot(configs, normalized)
        config_snapshot["bcc_count"] = 0
        config_snapshot["anchor_interval"] = 0
        rng = random.SystemRandom()
        lock_mode = sanitize_substitution_lock_mode(config_data.get("substitution_lock_mode"))
        lock_snapshot = decode_substitution_snapshot(config_data.get("substitution_snapshot"))
        resolved_config, _, missing_tokens, _ = resolve_substitution_outputs(
            config_snapshot,
            substitution_rules,
            lock_mode,
            lock_snapshot,
            context=context,
            random_generator=rng,
        )
        auto_enabled, pattern_value = resolve_message_id_settings(resolved_config)
        resolved_config["message_id_auto"] = auto_enabled
        resolved_config["message_id_pattern"] = pattern_value
        raw_header_value = resolved_config.get("header")
        if auto_enabled:
            resolved_config["header"] = ensure_message_id_header(
                raw_header_value,
                auto_enabled=True,
                pattern_value=pattern_value,
                mail_from=resolved_config.get("mail_from"),
                helo=resolved_config.get("helo"),
            )
        elif not isinstance(raw_header_value, str):
            resolved_config["header"] = str(raw_header_value or "")
        minimal_config = {
            "smtp_host": resolved_config.get("smtp_host"),
            "smtp_port": resolved_config.get("smtp_port"),
            "helo": resolved_config.get("helo"),
            "header": resolved_config.get("header"),
            "mail_from": resolved_config.get("mail_from"),
        }
        existing_job = conn.execute(
            """
            SELECT id
            FROM jobs
            WHERE device_id=? AND domain=? AND job_type='imap_manual_check'
              AND status IN ('pending', 'dispatched', 'running')
              AND cancel_requested=0
            """,
            (device_id, normalized),
        ).fetchone()
        if existing_job:
            raise HTTPException(status_code=409, detail="이미 도착 확인 작업이 진행 중입니다.")
        job_payload: Dict[str, Any] = {
            "trigger": "manual",
            "context_reason": context_reason,
            "config": minimal_config,
            "username": username_value,
        }
        job_payload = attach_telegram_credentials(
            job_payload,
            bot_token=bot_token,
            chat_id=chat_id,
        )
        job = create_job(conn, device_id, normalized, "imap_manual_check", job_payload)
        conn.commit()
    public_job = dict(job)
    public_job["payload"] = sanitize_job_payload_for_output(job["job_type"], job.get("payload"))
    log_missing_substitutions(job, missing_tokens)
    return {"job": public_job}


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
    return {"config": serialize_config(to_dict(refreshed), include_secret=False)}


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
    message_id_auto_raw = base.get("message_id_auto", DEFAULT_DOMAIN_CONFIG.get("message_id_auto"))
    if message_id_auto_raw is None:
        message_id_auto = bool(DEFAULT_DOMAIN_CONFIG.get("message_id_auto", True))
    else:
        try:
            message_id_auto = bool(int(message_id_auto_raw))
        except (TypeError, ValueError):
            message_id_auto = bool(message_id_auto_raw)
    message_id_pattern = _normalize_message_id_pattern(base.get("message_id_pattern"))
    return {
        "helo": base.get("helo", ""),
        "smtp_host": base.get("smtp_host", ""),
        "smtp_port": base.get("smtp_port", 25),
        "mail_from": base.get("mail_from", ""),
        "header": base.get("header", ""),
        "message_id_auto": message_id_auto,
        "message_id_pattern": message_id_pattern,
        "session_count": session_count,
        "bcc_count": bcc_count,
        "anchor_interval": anchor_interval,
        "anchor_email": anchor_email,
        "rcpt_to": base.get("rcpt_to", ""),
    }


PREVIEW_OVERRIDE_FIELDS = ("helo", "mail_from", "header", "anchor_email", "message_id_pattern", "message_id_auto")


def sanitize_preview_override(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    sanitized: Dict[str, Any] = {}
    for field in PREVIEW_OVERRIDE_FIELDS:
        if field not in raw:
            continue
        value = raw.get(field)
        if field == "anchor_email":
            sanitized[field] = normalize_anchor_email(value)
        elif field == "message_id_auto":
            if isinstance(value, str):
                lowered = value.strip().lower()
                sanitized[field] = lowered in {"1", "true", "yes", "on"}
            else:
                sanitized[field] = bool(value)
        else:
            sanitized[field] = str(value or "")
    return sanitized


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
        global_config = load_global_config(conn=conn)
        bot_token, chat_id = extract_telegram_credentials(global_config)
        substitution_rules = sanitize_substitution_rules(global_config.get("substitution_rules"))
        context = build_substitution_context(substitution_rules)
        for device_row in device_rows:
            device_id = device_row["id"]
            active = device_row["active_domain"] or "naver"
            domain = forced_domain or normalize_domain(active)
            configs = load_device_configs(device_id, conn=conn)
            config_snapshot = build_config_snapshot(configs, domain)
            rng = random.SystemRandom()
            base_config = configs.get(domain) or {}
            lock_mode = sanitize_substitution_lock_mode(base_config.get("substitution_lock_mode"))
            lock_snapshot = decode_substitution_snapshot(base_config.get("substitution_snapshot"))
            resolved_config, _, missing_tokens, _ = resolve_substitution_outputs(
                config_snapshot,
                substitution_rules,
                lock_mode,
                lock_snapshot,
                context=context,
                random_generator=rng,
            )
            auto_enabled, pattern_value = resolve_message_id_settings(resolved_config)
            resolved_config["message_id_auto"] = auto_enabled
            resolved_config["message_id_pattern"] = pattern_value
            raw_header_value = resolved_config.get("header")
            if auto_enabled:
                resolved_config["header"] = ensure_message_id_header(
                    raw_header_value,
                    auto_enabled=True,
                    pattern_value=pattern_value,
                    mail_from=resolved_config.get("mail_from"),
                    helo=resolved_config.get("helo"),
                )
            elif not isinstance(raw_header_value, str):
                resolved_config["header"] = str(raw_header_value or "")
            job_payload = attach_telegram_credentials(
                {"config": resolved_config},
                bot_token=bot_token,
                chat_id=chat_id,
            )
            job = create_job(
                conn,
                device_id,
                domain,
                "batch_send",
                job_payload,
            )
            log_missing_substitutions(job, missing_tokens)
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
        global_config = load_global_config(conn=conn)
        bot_token, chat_id = extract_telegram_credentials(global_config)
        substitution_rules = sanitize_substitution_rules(global_config.get("substitution_rules"))
        context = build_substitution_context(substitution_rules)
        configs = load_device_configs(device_id, conn=conn)
        config_snapshot = build_config_snapshot(configs, domain)
        config_snapshot["bcc_count"] = 0
        config_snapshot["anchor_interval"] = 0
        rcpt_to = (payload.rcpt_to or "").strip() or config_snapshot.get("rcpt_to")
        if not rcpt_to:
            raise HTTPException(status_code=400, detail="RCPT TO 주소가 필요합니다.")
        if payload.header_override:
            config_snapshot["header"] = payload.header_override
        override_fields: Set[str] = set()
        if payload.header_override:
            override_fields.add("header")
        rng = random.SystemRandom()
        base_config = configs.get(domain) or {}
        lock_mode = sanitize_substitution_lock_mode(base_config.get("substitution_lock_mode"))
        lock_snapshot = decode_substitution_snapshot(base_config.get("substitution_snapshot"))
        resolved_config, resolved_rcpt, missing_tokens, snapshot_meta = resolve_substitution_outputs(
            config_snapshot,
            substitution_rules,
            lock_mode,
            lock_snapshot,
            context=context,
            random_generator=rng,
            rcpt_source=rcpt_to,
            rcpt_override=bool(payload.rcpt_to),
            override_fields=override_fields,
        )
        auto_enabled, pattern_value = resolve_message_id_settings(resolved_config)
        resolved_config["message_id_auto"] = auto_enabled
        resolved_config["message_id_pattern"] = pattern_value
        raw_header_value = resolved_config.get("header")
        if auto_enabled:
            resolved_config["header"] = ensure_message_id_header(
                raw_header_value,
                auto_enabled=True,
                pattern_value=pattern_value,
                mail_from=resolved_config.get("mail_from"),
                helo=resolved_config.get("helo"),
            )
        elif not isinstance(raw_header_value, str):
            resolved_config["header"] = str(raw_header_value or "")
        substituted_rcpt = resolved_rcpt or rcpt_to
        force_imap_check = bool(payload.force_imap_check)
        base_payload = {
            "rcpt_to": substituted_rcpt,
            "config": resolved_config,
            "force_imap_check": force_imap_check,
        }
        job_payload = attach_telegram_credentials(
            base_payload,
            bot_token=bot_token,
            chat_id=chat_id,
        )
        job = create_job(
            conn,
            device_id,
            domain,
            "single_send",
            job_payload,
        )
        conn.commit()
    log_missing_substitutions(job, missing_tokens)
    return {"job": job}


@app.post("/api/devices/{device_id}/actions/send-batch")
def enqueue_batch_send(device_id: str, payload: BatchSendRequest) -> Dict[str, Any]:
    domain = normalize_domain(payload.domain)
    with db_lock, get_conn() as conn:
        device = get_device(device_id, conn=conn)
        if not device:
            raise HTTPException(status_code=404, detail="디바이스를 찾을 수 없습니다.")
        global_config = load_global_config(conn=conn)
        bot_token, chat_id = extract_telegram_credentials(global_config)
        substitution_rules = sanitize_substitution_rules(global_config.get("substitution_rules"))
        context = build_substitution_context(substitution_rules)
        configs = load_device_configs(device_id, conn=conn)
        config_snapshot = build_config_snapshot(configs, domain)
        rng = random.SystemRandom()
        base_config = configs.get(domain) or {}
        lock_mode = sanitize_substitution_lock_mode(base_config.get("substitution_lock_mode"))
        lock_snapshot = decode_substitution_snapshot(base_config.get("substitution_snapshot"))
        resolved_config, _, missing_tokens, _ = resolve_substitution_outputs(
            config_snapshot,
            substitution_rules,
            lock_mode,
            lock_snapshot,
            context=context,
            random_generator=rng,
        )
        auto_enabled, pattern_value = resolve_message_id_settings(resolved_config)
        resolved_config["message_id_auto"] = auto_enabled
        resolved_config["message_id_pattern"] = pattern_value
        raw_header_value = resolved_config.get("header")
        if auto_enabled:
            resolved_config["header"] = ensure_message_id_header(
                raw_header_value,
                auto_enabled=True,
                pattern_value=pattern_value,
                mail_from=resolved_config.get("mail_from"),
                helo=resolved_config.get("helo"),
            )
        elif not isinstance(raw_header_value, str):
            resolved_config["header"] = str(raw_header_value or "")
        job_payload = attach_telegram_credentials(
            {"config": resolved_config},
            bot_token=bot_token,
            chat_id=chat_id,
        )
        job = create_job(
            conn,
            device_id,
            domain,
            "batch_send",
            job_payload,
        )
        conn.commit()
    log_missing_substitutions(job, missing_tokens)
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


@app.get("/api/jobs/{job_id}")
def get_job_detail(job_id: str) -> Dict[str, Any]:
    with db_lock, get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM jobs WHERE id=?",
            (job_id,),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="작업을 찾을 수 없습니다.")
        payload = sanitize_job_payload_for_output(row["job_type"], load_job_payload(row))
        result_payload: Optional[Dict[str, Any]] = None
        if row["result"]:
            try:
                result_payload = json.loads(row["result"])
            except json.JSONDecodeError:
                result_payload = None
        progress_rows = conn.execute(
            """
            SELECT status, message, data, created_at
            FROM job_progress_logs
            WHERE job_id=?
            ORDER BY created_at ASC
            """,
            (job_id,),
        ).fetchall()
        progress: List[Dict[str, Any]] = []
        for progress_row in progress_rows:
            data_payload: Optional[Dict[str, Any]] = None
            if progress_row["data"]:
                try:
                    data_payload = json.loads(progress_row["data"])
                except json.JSONDecodeError:
                    data_payload = None
            progress.append(
                {
                    "status": progress_row["status"],
                    "message": progress_row["message"],
                    "data": data_payload,
                    "created_at": progress_row["created_at"],
                }
            )
    return {
        "id": row["id"],
        "device_id": row["device_id"],
        "domain": row["domain"],
        "job_type": row["job_type"],
        "status": row["status"],
        "payload": payload,
        "result": result_payload,
        "created_at": row["created_at"],
        "queued_at": row["queued_at"],
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
        "cancel_requested": bool(row["cancel_requested"]),
        "error": row["error"],
        "progress": progress,
    }


@app.post("/api/global/actions/stop")
def request_global_stop(payload: GlobalStopRequest) -> Dict[str, Any]:
    cancel_message = (payload.reason or "").strip() or "사용자가 전체 중지를 요청했습니다."
    with db_lock, get_conn() as conn:
        result = cancel_active_sends(conn, cancel_message)
        conn.commit()
    return result


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
                """
                DELETE FROM send_logs
                WHERE device_id=?
                  AND (domain=? OR domain IS NULL)
                """,
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
                "file_size": row["size"],
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
        existing_device_name = (device["name"] or "").strip()
        payload_device_name = (payload.device_name or "").strip()
        device_label = payload_device_name or existing_device_name or device_id
        now = now_ts()
        conn.execute(
            """
            UPDATE devices
            SET name=?,
                status='connected',
                last_seen=?,
                public_ip=COALESCE(?, public_ip),
                updated_at=?
            WHERE id=?
            """,
            (payload.device_name, now, public_ip, now, device_id),
        )
        schedule_triggers: List[str] = []
        for state in payload.domain_states:
            domain = normalize_domain(state.domain)
            cycle_completed_value: Optional[int]
            if state.cycle_completed is None:
                cycle_completed_value = None
            else:
                cycle_completed_value = 1 if state.cycle_completed else 0
            config_row = conn.execute(
                """
                SELECT stop_schedule_last_run
                FROM device_configs
                WHERE device_id=? AND domain=?
                """,
                (device_id, domain),
            ).fetchone()
            previous_last_run = (
                sanitize_stop_schedule_last_run(config_row["stop_schedule_last_run"])
                if config_row
                else None
            )
            state_last_run = sanitize_stop_schedule_last_run(state.stop_schedule_last_run)
            if state_last_run and state_last_run != previous_last_run:
                schedule_triggers.append(domain)
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
        schedule_auto_stop_result: Optional[Dict[str, int]] = None
        if schedule_triggers:
            reason_parts = [f"{DOMAIN_LABELS.get(domain, domain)} 예약된 자동 정지 실행" for domain in schedule_triggers]
            schedule_reason = " / ".join(reason_parts) if reason_parts else "예약된 자동 정지 실행"
            schedule_metadata = {
                "device_id": device_id,
                "device_name": device_label,
                "domains": schedule_triggers,
            }
            schedule_metadata = enrich_stop_metadata_with_sent_stats(conn, schedule_metadata)
            schedule_auto_stop_payload = handle_auto_stop(
                conn,
                schedule_reason,
                origin="schedule",
                metadata=schedule_metadata,
                device_id=device_id,
                job_types=("batch_send",),
            )
            schedule_auto_stop_result = (
                schedule_auto_stop_payload.get("result", {}) if schedule_auto_stop_payload else {}
            )
            if schedule_auto_stop_result.get("cancelled") or schedule_auto_stop_result.get("cancel_requested"):
                log_console(f"[SCHEDULE] 디바이스 {device_id} 자동 중지: {schedule_reason}")
        auto_stop_context: Optional[Dict[str, Any]] = None
        device_stop_context: Optional[Dict[str, Any]] = None
        job_type_cache: Dict[str, str] = {}
        for report in payload.imap_reports or []:
            try:
                normalized_domain = normalize_domain(report.domain)
            except HTTPException:
                continue
            if normalized_domain != "naver":
                continue
            config_row = conn.execute(
                "SELECT * FROM device_configs WHERE device_id=? AND domain=?",
                (device_id, normalized_domain),
            ).fetchone()
            if not config_row:
                continue
            config_snapshot = to_dict(config_row)
            status_value = sanitize_imap_status(report.status)
            checked_at_raw = report.checked_at or now
            checked_at_value = sanitize_imap_timestamp(checked_at_raw) or checked_at_raw
            latency_value = sanitize_imap_latency(report.latency)
            reason_text = (report.reason or "").strip()
            ip_change_attempted = bool(getattr(report, "ip_change_attempted", False))
            ip_change_success = bool(getattr(report, "ip_change_success", False))
            ip_change_message = (getattr(report, "ip_change_message", "") or "").strip()
            ip_change_marker = (getattr(report, "ip_change_marker", "") or "").strip()
            ip_change_reason = (getattr(report, "ip_change_reason", "") or "").strip()
            ip_after_change = (getattr(report, "ip_after_change", "") or "").strip()
            probe_mail_sent = bool(getattr(report, "probe_mail_sent", False))
            probe_mail_error = (getattr(report, "probe_mail_error", "") or "").strip()
            probe_status_line = (getattr(report, "probe_status_line", "") or "").strip()
            probe_detail_line = (getattr(report, "probe_detail_line", "") or "").strip()
            sent_at_value = (
                sanitize_imap_timestamp(report.sent_at)
                or sanitize_imap_timestamp(config_snapshot.get("imap_last_sent_at"))
            )
            received_at_value = (
                sanitize_imap_timestamp(report.received_at)
                or sanitize_imap_timestamp(config_snapshot.get("imap_last_received_at"))
            )
            if latency_value is None and sent_at_value and received_at_value:
                try:
                    sent_dt = datetime.fromisoformat(sent_at_value.replace("Z", "+00:00"))
                    received_dt = datetime.fromisoformat(received_at_value.replace("Z", "+00:00"))
                    delta_seconds = (received_dt - sent_dt).total_seconds()
                    latency_value = abs(delta_seconds)
                except ValueError:
                    pass
            anchor_flag = bool(getattr(report, "anchor", False))
            raw_allowed_latency = (
                getattr(report, "allowed_latency_seconds", None)
                if hasattr(report, "allowed_latency_seconds")
                else None
            )
            if raw_allowed_latency is None:
                raw_allowed_latency = report.delay_seconds
            allowed_latency_value = sanitize_imap_allowed_latency(
                raw_allowed_latency,
                default=config_snapshot.get("imap_allowed_latency_seconds"),
            )
            allowed_storage_value = allowed_latency_value
            mail_from_value = (report.mail_from or config_snapshot.get("imap_last_mail_from")
                               or config_snapshot.get("mail_from") or "")
            error_value = "" if status_value == "success" else reason_text
            reason_parts: List[str] = []
            if ip_change_attempted:
                base_part = "IP 변경 후 IMAP 재확인"
                base_part += " 성공" if ip_change_success else " 시도"
                reason_parts.append(base_part)
                if ip_change_message:
                    reason_parts.append(ip_change_message)
                elif ip_change_reason:
                    reason_parts.append(ip_change_reason)
                if ip_after_change:
                    reason_parts.append(f"새 IP {ip_after_change}")
            if probe_mail_sent:
                if probe_status_line:
                    reason_parts.append(f"테스트 메일 응답: {probe_status_line}")
            elif probe_mail_error:
                reason_parts.append(f"테스트 메일 실패: {probe_mail_error}")
            if reason_text:
                reason_parts.append(reason_text)
            reason_text = " · ".join(part for part in reason_parts if part)
            if status_value == "network_error":
                log_console(
                    "[IMAP] 네트워크 오류 보고 · 디바이스 {} · 도메인 {} · 사유 {}".format(
                        device_id,
                        normalized_domain,
                        reason_text or "사유 없음",
                    ),
                    flush=True,
                )
            sent_window_raw = getattr(report, "sent_window_count", None)
            if sent_window_raw is None:
                sent_window_count = None
            else:
                try:
                    sent_window_count = max(0, int(sent_window_raw))
                except (TypeError, ValueError):
                    sent_window_count = None
            stored_threshold = sanitize_imap_sent_threshold(
                config_snapshot.get("imap_sent_threshold"),
                default=DEFAULT_DOMAIN_CONFIG.get("imap_sent_threshold"),
            )
            report_threshold_raw = getattr(report, "sent_threshold", None)
            if report_threshold_raw is not None:
                new_threshold_value = sanitize_imap_sent_threshold(
                    report_threshold_raw,
                    default=stored_threshold,
                )
            else:
                new_threshold_value = stored_threshold
            try:
                existing_sent_since = int(config_snapshot.get("imap_sent_since_last_check") or 0)
            except (TypeError, ValueError):
                existing_sent_since = 0
            existing_sent_since = max(0, existing_sent_since)
            if status_value == "success":
                new_sent_since = 0
                sent_reset_at_value = checked_at_value
            elif sent_window_count is not None:
                new_sent_since = sent_window_count
                sent_reset_at_value = config_snapshot.get("imap_sent_last_reset_at")
            else:
                new_sent_since = existing_sent_since
                sent_reset_at_value = config_snapshot.get("imap_sent_last_reset_at")
            conn.execute(
                """
                UPDATE device_configs
                SET imap_last_status=?,
                    imap_last_checked_at=?,
                    imap_last_latency=?,
                    imap_last_error=?,
                    imap_last_mail_from=?,
                    imap_last_sent_at=?,
                    imap_last_received_at=?,
                    imap_delay_seconds=?,
                    imap_allowed_latency_seconds=?,
                    imap_sent_threshold=?,
                    imap_sent_since_last_check=?,
                    imap_sent_last_reset_at=?,
                    updated_at=?
                WHERE device_id=? AND domain=?
                """,
                (
                    status_value,
                    checked_at_value,
                    latency_value,
                    error_value,
                    mail_from_value,
                    sent_at_value,
                    received_at_value,
                    allowed_storage_value,
                    allowed_storage_value,
                    new_threshold_value,
                    new_sent_since,
                    sent_reset_at_value,
                    now,
                    device_id,
                    normalized_domain,
                ),
            )
            failure_action = sanitize_imap_failure_action(config_snapshot.get("imap_failure_action"))
            notify_before_stop = sanitize_imap_notify_before_stop_all(
                config_snapshot.get("imap_notify_before_stop_all")
            )
            send_type_value = (report.send_type or "").strip().lower()
            is_batch_context = send_type_value.startswith("batch") or send_type_value in {"sent-threshold", "threshold"}
            job_id_value = (report.job_id or "").strip()
            if not is_batch_context and job_id_value:
                cached_job_type = job_type_cache.get(job_id_value)
                if cached_job_type is None:
                    job_row = conn.execute(
                        "SELECT job_type FROM jobs WHERE id=?",
                        (job_id_value,),
                    ).fetchone()
                    cached_job_type = job_row["job_type"] if job_row else ""
                    job_type_cache[job_id_value] = cached_job_type
                if cached_job_type == "batch_send":
                    is_batch_context = True
            suppress_auto_notification = not is_batch_context
            if suppress_auto_notification:
                notify_before_stop = False
            should_stop = False
            if (
                status_value in {"failure", "error"}
                and failure_action != "none"
                and bool(report.trigger_stop)
            ):
                should_stop = True
            if should_stop:
                delay_seconds_value = getattr(report, "delay_seconds", None)
                detail_parts: List[str] = []
                if reason_text:
                    detail_parts.append(reason_text)
                if delay_seconds_value is not None:
                    detail_parts.append(f"체크대기 {delay_seconds_value}s")
                if latency_value is not None:
                    detail_parts.append(f"지연 {latency_value:.1f}s")
                if allowed_latency_value is not None:
                    detail_parts.append(f"허용 {allowed_latency_value}s")
                if sent_window_count is not None:
                    detail_parts.append(f"Sent 누적 {sent_window_count}건")
                detail_text = " · ".join(detail_parts) if detail_parts else "허용 지연 초과"
                stop_reason = f"IMAP 체크 실패 - 디바이스 {device_id} ({report.send_type or 'unknown'})"
                meta = {
                    "device_id": device_id,
                    "device_name": device_label,
                    "domain": normalized_domain,
                    "send_type": report.send_type or "unknown",
                    "job_id": report.job_id,
                    "latency": latency_value,
                    "allowed_latency": allowed_latency_value,
                    "check_delay": delay_seconds_value,
                    "mail_from": mail_from_value,
                    "detail": detail_text,
                    "anchor": anchor_flag,
                    "failure_action": failure_action,
                    "notify_before_stop": notify_before_stop,
                    "sent_window_count": sent_window_count,
                    "sent_since_last_check": new_sent_since,
                    "client_sent": config_snapshot.get("client_sent"),
                    "sent_threshold": new_threshold_value,
                    "ip_change_attempted": ip_change_attempted,
                    "ip_change_success": ip_change_success,
                    "ip_after_change": ip_after_change,
                    "probe_mail_sent": probe_mail_sent,
                    "ip_change_message": ip_change_message,
                    "probe_mail_error": probe_mail_error,
                    "probe_status_line": probe_status_line,
                }
                meta = enrich_stop_metadata_with_sent_stats(conn, meta)
                if detail_text:
                    stop_reason = f"{stop_reason}: {detail_text}"
                if failure_action == "stop_device" and device_stop_context is None:
                    device_stop_context = {
                        "reason": stop_reason,
                        "metadata": meta,
                        "notify_before": notify_before_stop,
                    }
                elif failure_action == "stop_all" and auto_stop_context is None:
                    auto_stop_context = {
                        "reason": stop_reason,
                        "metadata": meta,
                        "notify_before": notify_before_stop,
                        "suppress_notification": suppress_auto_notification,
                    }
        device_stop_result: Optional[Dict[str, Any]] = None
        if device_stop_context is not None:
            device_stop_result = cancel_active_sends(
                conn,
                device_stop_context["reason"],
                device_id=device_id,
            )
            log_console(f"[IMAP] 디바이스 중지: {device_stop_context['reason']}")
        auto_stop_result: Optional[Dict[str, Any]] = None
        if auto_stop_context is not None:
            auto_stop_result = handle_auto_stop(
                conn,
                auto_stop_context["reason"],
                origin="imap",
                metadata=auto_stop_context.get("metadata"),
                suppress_notification=bool(auto_stop_context.get("suppress_notification")),
            )
            log_console(f"[IMAP] 자동 전체 중지: {auto_stop_context['reason']}")
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
                    WHEN 'reset_sent_sequence' THEN -1
                    WHEN 'imap_manual_check' THEN 0
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
    configs = {row["domain"]: serialize_config(to_dict(row), include_secret=True) for row in config_rows}
    job_controls = [
        JobControlPayload(job_id=row["id"], cancel_requested=bool(row["cancel_requested"]))
        for row in control_rows
        if row["cancel_requested"]
    ]
    _log_device_connection_event(device_id, device_label, "connected", public_ip_value)
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
