# -*- coding: utf-8 -*-
import random
import smtplib
import telnetlib
import time
from typing import Iterable, List, Optional, Tuple

SMTP_PORT = 25

NAVER_MX = [
    "mx1.naver.com",
    "mx2.naver.com",
    "mx3.naver.com",
    "mx4.naver.com",
    "mx5.naver.com",
    "mx6.naver.com",
]

DAUM_MX = [
    "mx1.hanmail.net",
    "mx2.hanmail.net",
    "mx3.hanmail.net",
    "mx4.hanmail.net",
]


def get_mx_server_for_email(email: str) -> str:
    if "@" in email:
        domain = email.split("@", 1)[1].lower()
    else:
        return random.choice(NAVER_MX)
    if domain in {"naver.com", "navercorp.com"}:
        return random.choice(NAVER_MX)
    if domain in {"daum.net", "hanmail.net", "kakao.com"}:
        return random.choice(DAUM_MX)
    return random.choice(NAVER_MX)


def _encode_euc_kr(text: str) -> bytes:
    try:
        return text.encode("euc-kr")
    except UnicodeEncodeError:
        parts: List[str] = []
        for char in text:
            try:
                char.encode("euc-kr")
            except UnicodeEncodeError:
                parts.append(f"&#{ord(char)};")
            else:
                parts.append(char)
        return "".join(parts).encode("euc-kr", errors="ignore")


def send_mail(
    smtp_server: Optional[str],
    sender_email: str,
    recipient_email: str,
    subject: str,
    body: str,
    port: int = SMTP_PORT,
) -> Tuple[bool, str]:
    target_server = smtp_server or get_mx_server_for_email(recipient_email)
    header = (
        f"From: {sender_email}\r\n"
        f"To: {recipient_email}\r\n"
        f"Subject: {subject}\r\n"
        "MIME-Version: 1.0\r\n"
        "Content-Type: text/plain; charset=euc-kr\r\n"
        "Content-Transfer-Encoding: 8bit\r\n"
        "\r\n"
    )
    message = header + body + "\r\n"
    try:
        with smtplib.SMTP(target_server, port, timeout=15) as server:
            server.ehlo()
            server.sendmail(
                sender_email,
                [recipient_email],
                _encode_euc_kr(message),
            )
        return True, "이메일 전송 성공"
    except Exception as exc:  # pylint: disable=broad-except
        return False, f"이메일 전송 실패: {exc}"


def send_mail_telnet(
    smtp_server: Optional[str],
    sender_email: str,
    recipient_email: str,
    helo_name: str,
    header: str,
    timeout: int = 10,
    smtp_port_value: int = SMTP_PORT,
    bcc_emails: Optional[Iterable[str]] = None,
) -> str:
    target_server = smtp_server or get_mx_server_for_email(recipient_email)
    responses: List[str] = []

    def _read_line(tn: telnetlib.Telnet) -> str:
        data = tn.read_until(b"\n", timeout=timeout)
        if not data:
            return ""
        return data.decode("utf-8", errors="ignore").strip()

    def _write_cmd(tn: telnetlib.Telnet, cmd: str) -> None:
        tn.write((cmd + "\r\n").encode("utf-8"))
        time.sleep(0.2)

    try:
        with telnetlib.Telnet(target_server, smtp_port_value, timeout=timeout) as tn:
            responses.append(_read_line(tn))

            greet = helo_name or "localhost"
            _write_cmd(tn, f"EHLO {greet}")
            reply = _read_line(tn)
            responses.append(reply)
            if not reply.startswith("250"):
                _write_cmd(tn, f"HELO {greet}")
                responses.append(_read_line(tn))

            _write_cmd(tn, f"MAIL FROM:<{sender_email}>")
            responses.append(_read_line(tn))

            _write_cmd(tn, f"RCPT TO:<{recipient_email}>")
            responses.append(_read_line(tn))

            if bcc_emails:
                for bcc in bcc_emails:
                    _write_cmd(tn, f"RCPT TO:<{bcc}>")
                    responses.append(_read_line(tn))

            _write_cmd(tn, "DATA")
            responses.append(_read_line(tn))

            payload = header.replace("\r\n", "\n").replace("\r", "\n")
            if not payload.endswith("\n"):
                payload += "\n"
            payload = payload.replace("\n", "\r\n")
            tn.write(_encode_euc_kr(payload))
            tn.write(b".\r\n")
            time.sleep(0.2)
            responses.append(_read_line(tn))

            _write_cmd(tn, "QUIT")
            responses.append(_read_line(tn))

        return "\n".join(filter(None, responses))
    except Exception as exc:  # pylint: disable=broad-except
        return f"텔넷을 통한 이메일 전송 실패: {exc}"


if __name__ == "__main__":
    sender = "mobile2@elite-watch.com"
    recipient = "nanagame2@naver.com"
    subject = "텔넷 샘플"
    header_text = (
        f"From: {sender}\r\n"
        f"To: {recipient}\r\n"
        f"Subject: {subject}\r\n"
        "MIME-Version: 1.0\r\n"
        "Content-Type: text/plain; charset=euc-kr\r\n"
        "Content-Transfer-Encoding: 8bit\r\n"
        "\r\n"
        "텔넷 전송 테스트 본문입니다.\r\n"
    )
    print("메일 전송을 시작합니다...")
    server_host = get_mx_server_for_email(recipient)
    print(f"선택된 SMTP 서버: {server_host}")
    result = send_mail_telnet(
        smtp_server=server_host,
        sender_email=sender,
        recipient_email=recipient,
        helo_name="sample-helo",
        header=header_text,
        smtp_port_value=SMTP_PORT,
    )
    print(result)
