# -*- coding: utf-8 -*-
import base64
import random
import smtplib
import telnetlib
import time
from email.header import Header
from typing import List, Optional, Tuple

READ_LINE_TIMEOUT = 5

# SMTP 기본 포트
smtp_port = 25  # SMTP 기본 포트

def encode_mime_header(text, charset='euc-kr', force=True):
    try:
        if force or not text.isascii():
            encoded_bytes = text.encode(charset)
            b64_encoded = base64.b64encode(encoded_bytes).decode('ascii')
            return f"=?{charset.upper()}?B?{b64_encoded}?="
        else:
            return text
    except Exception as e:  # pylint: disable=broad-except
        print(f"MIME 인코딩 실패: {e}")
        return text

def get_mx_server_for_email(email):
    if '@' in email:
        domain = email.split('@')[1].lower()
    else:
        return random.choice([
            "mx1.naver.com",
            "mx2.naver.com",
            "mx3.naver.com",
            "mx4.naver.com",
            "mx5.naver.com",
            "mx6.naver.com",
        ])

    if domain in ['naver.com', 'navercorp.com']:
        return random.choice([
            "mx1.naver.com",
            "mx2.naver.com",
            "mx3.naver.com",
            "mx4.naver.com",
            "mx5.naver.com",
            "mx6.naver.com",
        ])
    if domain in ['daum.net', 'hanmail.net', 'kakao.com']:
        return random.choice([
            "mx1.hanmail.net",
            "mx2.hanmail.net",
            "mx3.hanmail.net",
            "mx4.hanmail.net",
        ])
    return random.choice([
        "mx1.naver.com",
        "mx2.naver.com",
        "mx3.naver.com",
        "mx4.naver.com",
        "mx5.naver.com",
        "mx6.naver.com",
    ])

def send_mail_telnet(
    smtp_server,
    sender_email,
    recipient_email,
    helo_name,
    header,
    bcc_emails=None,
    smtp_port_override=None,
):
    if smtp_server is None:
        smtp_server = get_mx_server_for_email(recipient_email)

    response_lines: List[str] = []
    response_entries: List[Tuple[str, str]] = []

    try:
        port_value = smtp_port_override or smtp_port
        with telnetlib.Telnet(smtp_server, port_value) as tn:
            def record(label: str, text: str) -> None:
                trimmed = (text or "").strip()
                response_entries.append((label, trimmed))
                if label and trimmed:
                    response_lines.append(f"{label}: {trimmed}")
                elif label:
                    response_lines.append(label)
                elif trimmed:
                    response_lines.append(trimmed)

            def read_line(label: str, address: Optional[str] = None) -> str:
                raw = tn.read_until(b"\r\n", timeout=READ_LINE_TIMEOUT)
                text = raw.decode('utf-8', errors='ignore')
                entry_label = label if address is None else f"{label}:{address}"
                record(entry_label, text)
                return text

            read_line("CONNECT")
            tn.write(f"HELO {helo_name}\r\n".encode('utf-8'))
            read_line("HELO")

            tn.write(f"MAIL FROM:<{sender_email}>\r\n".encode('utf-8'))
            read_line("MAIL FROM")

            tn.write(f"RCPT TO:<{recipient_email}>\r\n".encode('utf-8'))
            read_line("RCPT", recipient_email)

            if bcc_emails:
                for bcc_email in bcc_emails:
                    tn.write(f"RCPT TO:<{bcc_email}>\r\n".encode('utf-8'))
                    read_line("RCPT", bcc_email)

            tn.write(b"DATA\r\n")
            read_line("DATA")

            payload = f"{header}\r\n.\r\n"
            try:
                tn.write(payload.encode('euc-kr'))
            except UnicodeEncodeError as exc:
                fallback_lines = []
                for char in payload:
                    try:
                        char.encode('euc-kr')
                        fallback_lines.append(char)
                    except UnicodeEncodeError:
                        fallback_lines.append(f"&#{ord(char)};")
                alt_payload = ''.join(fallback_lines)
                record("EUC-KR 변환 실패", str(exc))
                tn.write(alt_payload.encode('euc-kr', errors='ignore'))

            read_line("DATA END")
            tn.write(b"QUIT\r\n")
            read_line("QUIT")

        return "\n".join(response_lines), response_entries
    except Exception as exc:  # pylint: disable=broad-except
        message = f"ERROR: {exc}"
        response_lines.append(message)
        response_entries.append(("ERROR", str(exc)))
        return "\n".join(response_lines), response_entries
