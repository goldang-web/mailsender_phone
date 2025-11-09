# -*- coding: utf-8 -*-
import socket
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from lib import test as telnet_mailer

TELNET_READ_TIMEOUT_SECONDS = 5
TELNET_DEBUG_MODE = False


def set_telnet_debug_mode(enabled: bool) -> None:
    """텔넷 요청/응답 전체 출력 여부를 설정합니다."""
    global TELNET_DEBUG_MODE
    TELNET_DEBUG_MODE = bool(enabled)


def resolve_smtp_host(host: str, port: int) -> str:
    candidate = (host or "").strip()
    if not candidate:
        return ""
    try:
        socket.getaddrinfo(candidate, port)
    except socket.gaierror:
        print(f"지정한 SMTP 호스트 {candidate} 를 찾지 못했습니다. MX 레코드로 대체합니다.")
        return ""
    return candidate


def normalize_newlines(text: str) -> str:
    return text.replace("\r\n", "\n").replace("\r", "\n")


def send_via_telnet(
    smtp_host: str,
    smtp_port: int,
    helo: str,
    mail_from: str,
    rcpt_to: str,
    header_text: str,
    bcc_emails: Optional[List[str]] = None,
    anchor_emails: Optional[List[str]] = None,
    *,
    debug: Optional[bool] = None,
) -> Tuple[bool, str, datetime, List[Dict[str, object]], Dict[str, str]]:
    try:
        telnet_mailer.READ_LINE_TIMEOUT = int(TELNET_READ_TIMEOUT_SECONDS)
    except Exception:
        telnet_mailer.READ_LINE_TIMEOUT = TELNET_READ_TIMEOUT_SECONDS
    normalized = normalize_newlines(header_text or "")
    payload = normalized.replace("\n", "\r\n")
    attempt_hosts = []
    target_host = resolve_smtp_host(smtp_host, smtp_port)
    if target_host:
        attempt_hosts.append(target_host)
    attempt_hosts.append(None)  # MX fallback

    bcc_targets = [email.strip() for email in (bcc_emails or []) if email and email.strip()]
    anchor_payload = [email.strip() for email in (anchor_emails or []) if email and email.strip()]
    anchor_targets = {email.lower() for email in anchor_payload}
    primary_lower = (rcpt_to or "").strip().lower()
    bcc_lower = {email.lower() for email in bcc_targets}

    response_text = ""
    success = False
    completed_at = datetime.now(timezone.utc)
    rcpt_details: List[Dict[str, object]] = []

    debug_enabled = TELNET_DEBUG_MODE if debug is None else bool(debug)

    def _normalize_address(value: Optional[str]) -> str:
        return (value or "").strip()

    def _address_key(value: Optional[str]) -> str:
        return _normalize_address(value).lower()

    rcpt_sequence: List[Dict[str, object]] = []
    rcpt_index_template: Dict[str, List[int]] = {}

    def _register_rcpt(
        address: Optional[str],
        *,
        is_primary: bool,
        is_bcc: bool,
    ) -> None:
        normalized = _normalize_address(address)
        if not normalized:
            return
        lowered = normalized.lower()
        is_anchor = lowered in anchor_targets
        index = len(rcpt_sequence)
        sequence_role = "anchor" if is_anchor else ("primary" if is_primary else "bcc")
        entry = {
            "address": normalized,
            "index": index,
            "is_primary": is_primary,
            "is_bcc": is_bcc,
            "is_anchor": is_anchor,
            "sequence_role": sequence_role,
        }
        rcpt_sequence.append(entry)
        rcpt_index_template.setdefault(lowered, []).append(index)

    _register_rcpt(rcpt_to, is_primary=True, is_bcc=False)
    for bcc_email in bcc_targets:
        _register_rcpt(bcc_email, is_primary=False, is_bcc=True)

    for index, host_candidate in enumerate(attempt_hosts):
        if index > 0:
            print("지정 호스트 실패. MX 레코드로 대체 시도합니다.")
        index_queue_map = {key: list(values) for key, values in rcpt_index_template.items()}
        raw_response = telnet_mailer.send_mail_telnet(
            smtp_server=host_candidate or None,
            sender_email=mail_from,
            recipient_email=rcpt_to,
            helo_name=helo or "localhost",
            header=payload,
            bcc_emails=bcc_targets or None,
            anchor_emails=anchor_payload or None,
            smtp_port_override=smtp_port or None,
            debug=debug_enabled,
        )
        completed_at = datetime.now(timezone.utc)
        if isinstance(raw_response, tuple) and len(raw_response) == 2:
            response_text, response_entries = raw_response
        else:
            response_text = str(raw_response)
            response_entries = [(None, part.strip()) for part in response_text.split("\n") if part.strip()]

        rcpt_details = []
        data_end_code = ""
        data_end_message = ""
        quit_code = ""
        quit_message = ""
        rcpt_success = True
        for label, message in response_entries:
            if not label:
                continue
            if label.startswith("RCPT:"):
                address = label.split(":", 1)[1].strip()
                lowered = address.lower()
                code = ""
                detail_text = message or ""
                if detail_text:
                    code = detail_text.split()[0]
                entry_success = bool(code.startswith("2"))
                sequence_index = None
                sequence_role = None
                queue = index_queue_map.get(_address_key(address))
                sequence_entry = None
                if queue:
                    sequence_index = queue.pop(0)
                    if 0 <= sequence_index < len(rcpt_sequence):
                        sequence_entry = rcpt_sequence[sequence_index]
                if sequence_entry:
                    is_primary_entry = bool(sequence_entry.get("is_primary"))
                    is_bcc_entry = bool(sequence_entry.get("is_bcc"))
                    is_anchor_entry = bool(sequence_entry.get("is_anchor"))
                    sequence_role = sequence_entry.get("sequence_role")
                else:
                    is_primary_entry = lowered == primary_lower
                    is_bcc_entry = lowered in bcc_lower
                    is_anchor_entry = lowered in anchor_targets
                detail_entry = {
                    "address": address,
                    "code": code,
                    "message": detail_text,
                    "is_primary": is_primary_entry,
                    "is_bcc": is_bcc_entry,
                    "is_anchor": is_anchor_entry,
                    "success": entry_success,
                }
                if sequence_index is not None:
                    detail_entry["sequence_index"] = sequence_index
                if sequence_role:
                    detail_entry["sequence_role"] = sequence_role
                rcpt_details.append(detail_entry)
                if not entry_success:
                    rcpt_success = False
            elif label == "DATA END":
                detail_text = message or ""
                data_end_code = detail_text.split()[0] if detail_text else ""
                data_end_message = detail_text
            elif label == "QUIT":
                detail_text = message or ""
                quit_code = detail_text.split()[0] if detail_text else ""
                quit_message = detail_text
        if not rcpt_details:
            # 구형 응답 포맷 대비: RCPT 라인 없이 250만 있는 경우
            rcpt_success = True
        final_ack_code = ""
        final_ack_message = ""
        final_ack_source = ""
        if data_end_code or data_end_message:
            final_ack_code = data_end_code
            final_ack_message = data_end_message
            final_ack_source = "DATA END"
        elif quit_code or quit_message:
            final_ack_code = quit_code
            final_ack_message = quit_message
            final_ack_source = "QUIT"
        final_ack_success = final_ack_code.startswith("2") if final_ack_code else False
        success = rcpt_success and final_ack_success
        if success or host_candidate is None:
            break
    data_response = {
        "code": final_ack_code,
        "message": final_ack_message,
    }
    if final_ack_source:
        data_response["source"] = final_ack_source
    return success, response_text, completed_at, rcpt_details, data_response
