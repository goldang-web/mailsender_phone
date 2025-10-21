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
    *,
    debug: bool = False,
):
    if smtp_server is None:
        smtp_server = get_mx_server_for_email(recipient_email)

    response_lines: List[str] = []
    response_entries: List[Tuple[str, str]] = []

    debug_enabled = bool(debug)

    def debug_log(tag: str, message: str) -> None:
        if debug_enabled:
            print(f"[텔넷-{tag}] {message}")

    try:
        port_value = smtp_port_override or smtp_port
        debug_log("CONNECT", f"연결 시도 → {smtp_server}:{port_value}")
        with telnetlib.Telnet(smtp_server, port_value) as tn:
            debug_log("CONNECT", "연결 성공")

            def record(label: str, text: str) -> None:
                trimmed = (text or "").strip()
                response_entries.append((label, trimmed))
                if label and trimmed:
                    response_lines.append(f"{label}: {trimmed}")
                elif label:
                    response_lines.append(label)
                elif trimmed:
                    response_lines.append(trimmed)
                if debug_enabled:
                    entry_label = label if label else "RESP"
                    tag = "Resp"
                    base_label = label or ""
                    upper_label = base_label.upper()
                    if upper_label.startswith("RCPT"):
                        tag = "Rcpt"
                    elif upper_label == "MAIL FROM":
                        tag = "MailFrom"
                    elif upper_label == "HELO":
                        tag = "HELO"
                    elif upper_label == "DATA":
                        tag = "DATA"
                    elif upper_label == "DATA END":
                        tag = "DataEnd"
                    elif upper_label == "QUIT":
                        tag = "QUIT"
                    elif upper_label == "CONNECT":
                        tag = "CONNECT"
                    message_text = f"<< {entry_label}: {trimmed}" if trimmed else f"<< {entry_label}"
                    debug_log(tag, message_text)

            def read_line(label: str, address: Optional[str] = None) -> str:
                raw = tn.read_until(b"\r\n", timeout=READ_LINE_TIMEOUT)
                text = raw.decode('utf-8', errors='ignore')
                entry_label = label if address is None else f"{label}:{address}"
                record(entry_label, text)
                return text

            def write_line(text: str) -> None:
                upper_text = text.strip().upper()
                if upper_text.startswith("HELO") or upper_text.startswith("EHLO"):
                    tag = "HELO"
                elif upper_text.startswith("MAIL FROM"):
                    tag = "MailFrom"
                elif upper_text.startswith("RCPT TO"):
                    tag = "Rcpt"
                elif upper_text == "DATA":
                    tag = "DATA"
                elif upper_text == "QUIT":
                    tag = "QUIT"
                else:
                    tag = "Command"
                debug_log(tag, f">> {text}")
                tn.write(f"{text}\r\n".encode('utf-8'))

            read_line("CONNECT")
            write_line(f"HELO {helo_name}")
            read_line("HELO")

            write_line(f"MAIL FROM:<{sender_email}>")
            read_line("MAIL FROM")

            write_line(f"RCPT TO:<{recipient_email}>")
            read_line("RCPT", recipient_email)

            if bcc_emails:
                for bcc_email in bcc_emails:
                    write_line(f"RCPT TO:<{bcc_email}>")
                    read_line("RCPT", bcc_email)

            write_line("DATA")
            read_line("DATA")

            payload = f"{header}\r\n.\r\n"
            try:
                if debug_enabled:
                    for line in payload.split("\r\n"):
                        if line == "":
                            debug_log("Body", ">> ")
                        else:
                            debug_log("Body", f">> {line}")
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
                if debug_enabled:
                    debug_log("Body", f">> [EUC-KR 변환 실패, 대체 전송] {alt_payload}")
                tn.write(alt_payload.encode('euc-kr', errors='ignore'))

            read_line("DATA END")
            write_line("QUIT")
            read_line("QUIT")

        return "\n".join(response_lines), response_entries
    except Exception as exc:  # pylint: disable=broad-except
        message = f"ERROR: {exc}"
        response_lines.append(message)
        response_entries.append(("ERROR", str(exc)))
        debug_log("ERROR", f"<< ERROR: {exc}")
        return "\n".join(response_lines), response_entries
