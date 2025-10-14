# -*- coding: utf-8 -*-
import socket
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from lib import test as telnet_mailer


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
) -> Tuple[bool, str, datetime, List[Dict[str, object]]]:
    normalized = normalize_newlines(header_text or "")
    payload = normalized.replace("\n", "\r\n")
    attempt_hosts = []
    target_host = resolve_smtp_host(smtp_host, smtp_port)
    if target_host:
        attempt_hosts.append(target_host)
    attempt_hosts.append(None)  # MX fallback

    bcc_targets = [email.strip() for email in (bcc_emails or []) if email and email.strip()]
    anchor_targets = {email.strip().lower() for email in (anchor_emails or []) if email and email.strip()}
    primary_lower = (rcpt_to or "").strip().lower()
    bcc_lower = {email.lower() for email in bcc_targets}

    response_text = ""
    success = False
    completed_at = datetime.now(timezone.utc)
    rcpt_details: List[Dict[str, object]] = []

    for index, host_candidate in enumerate(attempt_hosts):
        if index > 0:
            print("지정 호스트 실패. MX 레코드로 대체 시도합니다.")
        raw_response = telnet_mailer.send_mail_telnet(
            smtp_server=host_candidate or None,
            sender_email=mail_from,
            recipient_email=rcpt_to,
            helo_name=helo or "localhost",
            header=payload,
            bcc_emails=bcc_targets or None,
            smtp_port_override=smtp_port or None,
        )
        completed_at = datetime.now(timezone.utc)
        if isinstance(raw_response, tuple) and len(raw_response) == 2:
            response_text, response_entries = raw_response
        else:
            response_text = str(raw_response)
            response_entries = [(None, part.strip()) for part in response_text.split("\n") if part.strip()]

        rcpt_details = []
        data_end_code = ""
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
                rcpt_details.append(
                    {
                        "address": address,
                        "code": code,
                        "message": detail_text,
                        "is_primary": lowered == primary_lower,
                        "is_bcc": lowered in bcc_lower,
                        "is_anchor": lowered in anchor_targets,
                        "success": entry_success,
                    }
                )
                if not entry_success:
                    rcpt_success = False
            elif label == "DATA END":
                detail_text = message or ""
                data_end_code = detail_text.split()[0] if detail_text else ""
        if not rcpt_details:
            # 구형 응답 포맷 대비: RCPT 라인 없이 250만 있는 경우
            rcpt_success = True
        final_ack_success = data_end_code.startswith("2") if data_end_code else False
        success = rcpt_success and final_ack_success
        if success or host_candidate is None:
            break

    return success, response_text, completed_at, rcpt_details
