# -*- coding: euc-kr -*-
import argparse
import json
import time
import socket
from typing import Dict, Tuple

import requests

from lib import test as telnet_mailer



def resolve_smtp_host(host: str, port: int) -> str:
    candidate = (host or "").strip()
    if not candidate:
        return ""
    try:
        socket.getaddrinfo(candidate, port)
    except socket.gaierror:
        print(f"지정한 SMTP 호스트 {candidate} 를 찾을 수 없어 MX 자동 선택으로 전환합니다.")
        return ""
    return candidate

def euc_kr_json_dump(data: Dict) -> bytes:
    return json.dumps(data, ensure_ascii=False).encode("euc-kr", errors="ignore")


def register(session: requests.Session, base_url: str, device_key: str, name: str) -> None:
    payload = {"device_id": device_key, "name": name}
    res = session.post(
        f"{base_url}/api/register",
        data=euc_kr_json_dump(payload),
        headers={"Content-Type": "application/json; charset=euc-kr"},
        timeout=10,
    )
    res.encoding = "euc-kr"
    res.raise_for_status()


def poll_command(session: requests.Session, base_url: str, device_key: str) -> Dict:
    res = session.get(
        f"{base_url}/api/send",
        params={"device_id": device_key},
        timeout=15,
    )
    res.encoding = "euc-kr"
    res.raise_for_status()
    data = res.json()
    return data.get("task")


def report_result(
    session: requests.Session,
    base_url: str,
    device_key: str,
    task_id: str,
    success: bool,
    response_text: str,
) -> None:
    payload = {
        "device_id": device_key,
        "task_id": task_id,
        "success": success,
        "response": response_text,
    }
    res = session.post(
        f"{base_url}/api/report",
        data=euc_kr_json_dump(payload),
        headers={"Content-Type": "application/json; charset=euc-kr"},
        timeout=10,
    )
    res.encoding = "euc-kr"
    res.raise_for_status()


def normalize_newlines(text: str) -> str:
    return text.replace("\r\n", "\n").replace("\r", "\n")


def smtp_exchange(
    smtp_host: str,
    smtp_port: int,
    helo: str,
    mail_from: str,
    rcpt_to: str,
    header_text: str,
    timeout: int,
) -> Tuple[bool, str]:
    normalized = normalize_newlines(header_text)
    payload = normalized.replace("\n", "\r\n")
    attempt_hosts = []
    target_host = resolve_smtp_host(smtp_host, smtp_port)
    if target_host:
        attempt_hosts.append(target_host)
    attempt_hosts.append(None)

    previous_port = getattr(telnet_mailer, "smtp_port", None)
    if smtp_port:
        telnet_mailer.smtp_port = smtp_port

    response_text = ""
    success = False

    try:
        for index, host_candidate in enumerate(attempt_hosts):
            if index > 0:
                print("지정한 SMTP 호스트로 연결하지 못해 MX 자동 선택으로 재시도합니다.")
            response_text = telnet_mailer.send_mail_telnet(
                smtp_server=host_candidate or None,
                sender_email=mail_from,
                recipient_email=rcpt_to,
                helo_name=helo or "localhost",
                header=payload,
            )
            lines = [line.strip() for line in response_text.split("\n") if line.strip()]
            success = any(line.startswith("250") for line in lines)
            if success or host_candidate is None:
                break
    finally:
        if previous_port is not None:
            telnet_mailer.smtp_port = previous_port

    return success, response_text


def run_agent(
    server_url: str,
    device_key: str,
    device_name: str,
    interval: int = 5,
    timeout: int = 15,
) -> None:
    base_url = server_url.rstrip("/")
    device_key = (device_key or "").strip()
    device_name = (device_name or "").strip()
    if not device_key:
        device_key = device_name or "device"
    if not device_name:
        device_name = device_key

    session = requests.Session()
    register(session, base_url, device_key, device_name)

    print("등록 완료. 명령 대기 중...")

    while True:
        try:
            task = poll_command(session, base_url, device_key)
            if task:
                print(f"명령 수신: {task['task_id']}")
                success, response_text = smtp_exchange(
                    "",
                    25,
                    task.get("helo", ""),
                    task.get("mail_from", ""),
                    task.get("rcpt_to", ""),
                    task.get("header", ""),
                    timeout,
                )
                print(response_text)
                report_result(
                    session,
                    base_url,
                    device_key,
                    task.get("task_id", ""),
                    success,
                    response_text,
                )
            time.sleep(interval)
        except KeyboardInterrupt:
            print("중단 요청 감지. 종료합니다.")
            break
        except Exception as exc:  # pylint: disable=broad-except
            print(f"폴링 중 오류: {exc}")
            time.sleep(interval)


def main() -> None:
    parser = argparse.ArgumentParser(description="Telnet 이메일 발송 에이전트")
    parser.add_argument("--server", required=True, help="FastAPI 서버 기본 URL")
    parser.add_argument("--device-name", required=True, help="디바이스 표시 이름")
    parser.add_argument("--device-key", default="", help="디바이스 고유 식별자 (미입력 시 자동 생성)")
    parser.add_argument("--interval", type=int, default=5, help="폴링 간격 (초)")
    parser.add_argument("--timeout", type=int, default=15, help="SMTP 타임아웃 (초)")
    args = parser.parse_args()

    run_agent(
        server_url=args.server,
        device_key=args.device_key or args.device_name,
        device_name=args.device_name,
        interval=args.interval,
        timeout=args.timeout,
    )


if __name__ == "__main__":
    main()
