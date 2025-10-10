# -*- coding: utf-8 -*-
import base64
import random
import smtplib
import telnetlib
import time
from email.header import Header

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

def send_mail_telnet(smtp_server, sender_email, recipient_email, helo_name, header, bcc_emails=None, smtp_port_override=None):
    if smtp_server is None:
        smtp_server = get_mx_server_for_email(recipient_email)

    responses = []

    try:
        port_value = smtp_port_override or smtp_port
        tn = telnetlib.Telnet(smtp_server, port_value)

        def read_line(label):
            raw = tn.read_until(b"\r\n", timeout=5)
            text = raw.decode('utf-8', errors='ignore')
            if label in {"CONNECT", "DATA END", "QUIT"}:
                responses.append(f"{label}: {text.strip()}")
            return text

        read_line("CONNECT")
        tn.write(f"HELO {helo_name}\r\n".encode('utf-8'))
        read_line("HELO")

        tn.write(f"MAIL FROM:<{sender_email}>\r\n".encode('utf-8'))
        read_line("MAIL FROM")

        tn.write(f"RCPT TO:<{recipient_email}>\r\n".encode('utf-8'))
        read_line("RCPT TO")

        if bcc_emails:
            for index, bcc_email in enumerate(bcc_emails, start=1):
                tn.write(f"RCPT TO:<{bcc_email}>\r\n".encode('utf-8'))
                read_line(f"RCPT TO BCC {index}")

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
            responses.append(f"EUC-KR 변환 실패: {exc}")
            tn.write(alt_payload.encode('euc-kr', errors='ignore'))

        read_line("DATA END")
        tn.write(b"QUIT\r\n")
        read_line("QUIT")

        tn.close()

        return "\n".join(responses)
    except Exception as exc:  # pylint: disable=broad-except
        responses.append(f"ERROR: {exc}")
        return "\n".join(responses)
