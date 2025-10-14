# -*- coding: utf-8 -*-
import socket
from datetime import datetime, timezone
from typing import List, Optional, Tuple

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
) -> Tuple[bool, str, datetime]:
    normalized = normalize_newlines(header_text or "")
    payload = normalized.replace("\n", "\r\n")
    attempt_hosts = []
    target_host = resolve_smtp_host(smtp_host, smtp_port)
    if target_host:
        attempt_hosts.append(target_host)
    attempt_hosts.append(None)  # MX fallback

    bcc_targets = [email.strip() for email in (bcc_emails or []) if email and email.strip()]

    response_text = ""
    success = False
    completed_at = datetime.now(timezone.utc)

    for index, host_candidate in enumerate(attempt_hosts):
        if index > 0:
            print("지정 호스트 실패. MX 레코드로 대체 시도합니다.")
        response_text = telnet_mailer.send_mail_telnet(
            smtp_server=host_candidate or None,
            sender_email=mail_from,
            recipient_email=rcpt_to,
            helo_name=helo or "localhost",
            header=payload,
            bcc_emails=bcc_targets or None,
            smtp_port_override=smtp_port or None,
        )
        completed_at = datetime.now(timezone.utc)
        lines = [line.strip() for line in response_text.split("\n") if line.strip()]
        for line in lines:
            raw = line.split(":", 1)[-1].strip() if ":" in line else line
            if raw.startswith("250"):
                success = True
                break
        if success or host_candidate is None:
            break

    return success, response_text, completed_at
