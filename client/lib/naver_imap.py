import email
import imaplib
import socket
import ssl
import time
from datetime import datetime, timezone
from email.header import decode_header, make_header
from email.utils import parseaddr, parsedate_to_datetime


class IMAPNetworkError(Exception):
    """IMAP 네트워크 오류를 명확하게 표현하기 위한 예외."""

    def __init__(self, message: str, *, original: Exception = None) -> None:
        super().__init__(message)
        self.original = original


def decode_mime_header_value(value):
    if not value:
        return ""
    try:
        return str(make_header(decode_header(value))).strip()
    except Exception:
        if isinstance(value, bytes):
            try:
                return value.decode("utf-8", errors="replace").strip()
            except Exception:
                return value.decode("latin-1", errors="replace").strip()
        return str(value).strip()


def fetch_latest_message_summary(email_id: str, password: str, *, folder: str = "Junk", limit: int = 1) -> dict:
    if not email_id or not password:
        raise ValueError("IMAP 계정 정보가 필요합니다.")
    try:
        limit_value = int(limit or 1)
    except (TypeError, ValueError):
        limit_value = 1
    limit_value = max(1, min(10, limit_value))
    target_folder = (folder or "Junk").strip() or "Junk"
    mail = None
    try:
        mail = imaplib.IMAP4_SSL("imap.naver.com", 993, timeout=30)
        mail.login(email_id, password)
        status, _ = mail.select(target_folder, readonly=True)
        if status != "OK":
            return {
                "success": False,
                "reason": f"{target_folder} 메일함을 열 수 없습니다.",
                "folder": target_folder,
            }
        status, messages = mail.search(None, "ALL")
        if status != "OK":
            return {
                "success": False,
                "reason": "메일 검색에 실패했습니다.",
                "folder": target_folder,
            }
        mail_ids = messages[0].split()
        if not mail_ids:
            return {
                "success": False,
                "reason": "메일함이 비어 있습니다.",
                "folder": target_folder,
                "total": 0,
            }
        latest_ids = mail_ids[-limit_value:]
        latest_id = latest_ids[-1]
        header_status, header_data = mail.fetch(latest_id, "(BODY.PEEK[HEADER])")
        if header_status != "OK" or not header_data or header_data[0] is None:
            header_status, header_data = mail.fetch(latest_id, "(RFC822.HEADER)")
        if header_status != "OK" or not header_data or header_data[0] is None:
            return {
                "success": False,
                "reason": "헤더를 가져오지 못했습니다.",
                "folder": target_folder,
            }
        raw_header = header_data[0][1] if isinstance(header_data[0], tuple) else header_data[0]
        if not raw_header:
            return {
                "success": False,
                "reason": "헤더 데이터가 비어 있습니다.",
                "folder": target_folder,
            }
        msg = email.message_from_bytes(raw_header)
        from_header = msg.get("From", "")
        subject_header = msg.get("Subject", "")
        date_header = msg.get("Date")
        message_id = msg.get("Message-ID") or msg.get("Message-Id") or ""
        decoded_from = decode_mime_header_value(from_header)
        decoded_subject = decode_mime_header_value(subject_header)
        name_part, address_part = parseaddr(from_header)
        decoded_name = decode_mime_header_value(name_part)
        received_at_iso = None
        received_at_local = None
        if date_header:
            try:
                parsed_dt = parsedate_to_datetime(date_header)
                if parsed_dt.tzinfo is None:
                    parsed_dt = parsed_dt.replace(tzinfo=timezone.utc)
                received_at_iso = parsed_dt.astimezone(timezone.utc).isoformat()
                received_at_local = parsed_dt.astimezone().isoformat()
            except Exception:
                received_at_iso = None
                received_at_local = None
        try:
            sequence_id = latest_id.decode()
        except Exception:
            sequence_id = str(latest_id)
        return {
            "success": True,
            "folder": target_folder,
            "total": len(mail_ids),
            "limit": limit_value,
            "mail": {
                "sequence": sequence_id,
                "from": decoded_from,
                "from_name": decoded_name,
                "from_address": address_part.strip(),
                "subject": decoded_subject,
                "date_header": date_header or "",
                "received_at_iso": received_at_iso,
                "received_at_local": received_at_local,
                "message_id": message_id.strip(),
            },
        }
    except imaplib.IMAP4.error as exc:
        return {
            "success": False,
            "reason": f"IMAP 인증 실패: {exc}",
            "folder": target_folder,
        }
    except Exception as exc:
        return {
            "success": False,
            "reason": f"IMAP 확인 중 오류: {exc}",
            "folder": target_folder,
        }
    finally:
        if mail is not None:
            try:
                mail.logout()
            except Exception:  # pylint: disable=broad-except
                pass


def purge_imap_folder(email_id: str, password: str, *, folder: str = "Junk", chunk_size: int = 100) -> dict:
    """지정한 IMAP 폴더의 모든 메일을 삭제하고 정리합니다."""
    if not email_id or not password:
        return {"success": False, "reason": "IMAP 계정 정보가 필요합니다.", "folder": (folder or "Junk") or "Junk"}
    try:
        chunk_value = int(chunk_size or 100)
    except (TypeError, ValueError):
        chunk_value = 100
    chunk_value = max(1, min(500, chunk_value))
    target_folder = (folder or "Junk").strip() or "Junk"
    mail = None
    started = time.monotonic()
    deleted_total = 0
    total_messages = 0
    try:
        mail = imaplib.IMAP4_SSL("imap.naver.com", 993, timeout=30)
        mail.login(email_id, password)
        status, _ = mail.select(target_folder, readonly=False)
        if status != "OK":
            return {
                "success": False,
                "reason": f"{target_folder} 메일함을 열 수 없습니다.",
                "folder": target_folder,
            }
        status, messages = mail.search(None, "ALL")
        if status != "OK":
            return {
                "success": False,
                "reason": "메일 목록을 가져오지 못했습니다.",
                "folder": target_folder,
            }
        ids_raw = messages[0].split() if messages and messages[0] else []
        total_messages = len(ids_raw)
        if total_messages == 0:
            elapsed_empty = time.monotonic() - started
            return {
                "success": True,
                "folder": target_folder,
                "total_count": 0,
                "deleted_count": 0,
                "remaining_count": 0,
                "elapsed_seconds": elapsed_empty,
            }
        for index in range(0, total_messages, chunk_value):
            chunk = ids_raw[index : index + chunk_value]
            message_set_parts = []
            for item in chunk:
                if not item:
                    continue
                if isinstance(item, bytes):
                    try:
                        message_set_parts.append(item.decode())
                    except Exception:
                        message_set_parts.append(item.decode("latin-1", errors="ignore"))
                else:
                    message_set_parts.append(str(item))
            if not message_set_parts:
                continue
            message_set = ",".join(message_set_parts)
            status, _ = mail.store(message_set, "+FLAGS", "(\\Deleted)")
            if status != "OK":
                return {
                    "success": False,
                    "reason": "메일에 삭제 플래그를 지정하지 못했습니다.",
                    "folder": target_folder,
                    "total_count": total_messages,
                    "deleted_count": deleted_total,
                }
            deleted_total += len(message_set_parts)
        status, _ = mail.expunge()
        if status != "OK":
            return {
                "success": False,
                "reason": "메일 영구 삭제(EXPUNGE)에 실패했습니다.",
                "folder": target_folder,
                "total_count": total_messages,
                "deleted_count": deleted_total,
            }
        status, remaining_messages = mail.search(None, "ALL")
        if status == "OK":
            remaining_ids = remaining_messages[0].split() if remaining_messages and remaining_messages[0] else []
            remaining_count = len(remaining_ids)
        else:
            remaining_count = None
        elapsed = time.monotonic() - started
        return {
            "success": True,
            "folder": target_folder,
            "total_count": total_messages,
            "deleted_count": deleted_total,
            "remaining_count": remaining_count if remaining_count is not None else max(0, total_messages - deleted_total),
            "elapsed_seconds": elapsed,
        }
    except imaplib.IMAP4.error as exc:
        return {
            "success": False,
            "reason": f"IMAP 인증 실패: {exc}",
            "folder": target_folder,
        }
    except (socket.timeout, ssl.SSLError) as exc:
        return {
            "success": False,
            "reason": f"IMAP 네트워크 오류: {exc}",
            "folder": target_folder,
        }
    except Exception as exc:  # pylint: disable=broad-except
        return {
            "success": False,
            "reason": f"스팸함 비우기 중 오류: {exc}",
            "folder": target_folder,
        }
    finally:
        if mail is not None:
            try:
                mail.close()
            except Exception:
                pass
            try:
                mail.logout()
            except Exception:
                pass


def probe_imap_connection(email_id: str, password: str, *, folder: str = "Junk", timeout: int = 30) -> dict:
    """네이버 IMAP 서버 연결 및 폴더 선택을 검사합니다."""
    if not email_id or not password:
        return {"success": False, "reason": "IMAP 계정 정보가 필요합니다."}
    mail = None
    started = time.monotonic()
    try:
        print(f"[IMAP-연결디버그] 로그인 시도 · 계정 {email_id} · 폴더 {folder}", flush=True)
        mail = imaplib.IMAP4_SSL("imap.naver.com", 993, timeout=timeout)
        mail.login(email_id, password)
        status, _ = mail.select(folder, readonly=True)
        if status != "OK":
            print(f"[IMAP-연결디버그] 폴더 선택 실패 · 상태 {status}", flush=True)
            return {"success": False, "reason": f"{folder} 메일함을 열 수 없습니다."}
        latency = time.monotonic() - started
        print(f"[IMAP-연결디버그] 로그인 및 폴더 선택 성공 · 지연 {latency:.2f}s", flush=True)
        return {
            "success": True,
            "latency": latency,
            "checked_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
    except imaplib.IMAP4.error as exc:
        latency = time.monotonic() - started
        message = f"IMAP 인증 실패: {exc}"
        print(f"[IMAP-연결디버그] {message} · 소요 {latency:.2f}s", flush=True)
        return {"success": False, "reason": message}
    except Exception as exc:  # pylint: disable=broad-except
        latency = time.monotonic() - started
        message = f"IMAP 연결 오류: {exc}"
        print(f"[IMAP-연결디버그] {message} · 소요 {latency:.2f}s", flush=True)
        return {"success": False, "reason": message}
    finally:
        if mail is not None:
            try:
                mail.logout()
            except Exception:  # pylint: disable=broad-except
                pass


def test_imap_connection(email_id, password):
    """
    IMAP 서버 연결을 테스트하는 함수

    Args:
        email_id (str): 이메일 주소
        password (str): 이메일 비밀번호

    Returns:
        bool: 연결 성공 여부
    """
    result = probe_imap_connection(email_id, password)
    if not result.get("success"):
        print(f"IMAP 연결 테스트 실패: {result.get('reason')}")
    return bool(result.get("success"))


def verify_delivery(
    email_id,
    password,
    mail_from,
    sent_at,
    allowed_delay,
    header_from=None,
    *,
    max_messages=15,
    check_delay=None,
    message_id=None,
):
    """네이버 스팸메일함에서 발신자 메일 도착 여부를 확인합니다."""

    if not email_id or not password:
        raise ValueError("IMAP 계정 정보가 필요합니다.")
    if isinstance(sent_at, datetime):
        sent_dt = sent_at
    else:
        try:
            sent_dt = datetime.fromisoformat(str(sent_at).replace("Z", "+00:00"))
        except Exception as exc:  # pylint: disable=broad-except
            raise ValueError(f"유효하지 않은 발송 시각: {sent_at}") from exc
    if sent_dt.tzinfo is None:
        sent_dt = sent_dt.replace(tzinfo=timezone.utc)
    else:
        sent_dt = sent_dt.astimezone(timezone.utc)

    def _normalize_message_id(raw_value) -> str:
        if raw_value is None:
            return ""
        if isinstance(raw_value, bytes):
            try:
                raw_text = raw_value.decode("utf-8", errors="ignore")
            except Exception:  # pylint: disable=broad-except
                raw_text = raw_value.decode("latin-1", errors="ignore")
        else:
            raw_text = str(raw_value)
        flattened = "".join(part.strip() for part in raw_text.replace("\r", "\n").split("\n"))
        return flattened.strip().lower()

    expected_message_id = str(message_id or "").strip()
    expected_message_id_lower = _normalize_message_id(expected_message_id)

    def _normalize_from_compare(raw_value: str) -> str:
        if not raw_value:
            return ""
        decoded_value = decode_mime_header_value(raw_value)
        candidate = decoded_value or str(raw_value)
        return " ".join(candidate.split()).lower().strip()

    def _parse_address_lower(raw_value: str) -> str:
        _, address = parseaddr(raw_value or "")
        return (address or "").strip().lower()

    expected_header_source = header_from or mail_from
    expected_header_display = decode_mime_header_value(expected_header_source) if expected_header_source else ""
    if not expected_header_display and mail_from:
        expected_header_display = decode_mime_header_value(mail_from)

    expected_header_compare = _normalize_from_compare(expected_header_source)
    if not expected_header_compare and mail_from:
        expected_header_compare = _normalize_from_compare(mail_from)

    expected_address_lower = _parse_address_lower(header_from) or _parse_address_lower(mail_from)
    try:
        allowed = int(allowed_delay)
    except (TypeError, ValueError):
        allowed = 20
    allowed = max(0, min(600, allowed))
    try:
        search_limit = int(max_messages or 15)
    except (TypeError, ValueError):
        search_limit = 15
    search_limit = max(1, min(50, search_limit))
    try:
        delay_before_check = float(check_delay) if check_delay is not None else 0.0
    except (TypeError, ValueError):
        delay_before_check = 0.0
    delay_before_check = max(0.0, min(600.0, delay_before_check))

    def _fmt_local(dt: datetime) -> str:
        return dt.astimezone().strftime("%H:%M:%S%z")

    sent_address_display = expected_header_display or header_from or mail_from or "-"
    sent_line = f"{sent_address_display or '-'} {_fmt_local(sent_dt)}"
    default_received_line = "- -"

    def _log_sent_received(received_line: str) -> None:
        print(f"[IMAP 확인] 발신 {sent_line}", flush=True)
        print(f"[IMAP 확인] 수신 {received_line}", flush=True)

    mail = None
    try:
        mail = imaplib.IMAP4_SSL("imap.naver.com", 993, timeout=30)
        mail.login(email_id, password)
        mail.select("Junk", readonly=True)
        status, messages = mail.search(None, "ALL")
        if status != "OK":
            _log_sent_received(default_received_line)
            return {
                "status": "error",
                "latency": None,
                "received_at": None,
                "reason": "메일 검색에 실패했습니다.",
                "allowed_latency": allowed,
                "sent_at": sent_dt.isoformat(),
                "delay_before_check": delay_before_check,
                "sent_display": sent_line,
                "received_display": default_received_line,
            }
        mail_ids = messages[0].split()
        if not mail_ids:
            _log_sent_received(default_received_line)
            print("[IMAP 확인] 지연: 측정 불가", flush=True)
            print(f"[IMAP 확인] 허용지연: {allowed}초", flush=True)
            print("[IMAP 확인] 판정: 메일함이 비어 있습니다.", flush=True)
            return {
                "status": "failure",
                "latency": None,
                "received_at": None,
                "reason": "메일함이 비어 있습니다.",
                "allowed_latency": allowed,
                "sent_at": sent_dt.isoformat(),
                "delay_before_check": delay_before_check,
                "sent_display": sent_line,
                "received_display": default_received_line,
            }

        candidates = list(reversed(mail_ids[-search_limit:]))
        sender_mismatch_found = False
        matched = False
        for num in candidates:
            status, data = mail.fetch(num, "(RFC822.HEADER)")
            if status != "OK" or not data or data[0] is None:
                continue
            header_bytes = data[0][1]
            if not header_bytes:
                continue
            msg = email.message_from_bytes(header_bytes)
            from_header = msg.get("From") or ""
            decoded_from = decode_mime_header_value(from_header)
            normalized_header = _normalize_from_compare(from_header)
            _, sender_address = parseaddr(from_header)
            normalized_sender = (sender_address or "").strip().lower()
            message_id_header = (msg.get("Message-ID") or msg.get("Message-Id") or "").strip()
            normalized_message_id = _normalize_message_id(message_id_header)

            if expected_message_id_lower:
                if normalized_message_id != expected_message_id_lower:
                    print(
                        f"[IMAP 확인] Message-ID 불일치 · 수신 {message_id_header or '-'} · 기대 {expected_message_id}",
                        flush=True,
                    )
                    continue
                if expected_address_lower and normalized_sender and normalized_sender != expected_address_lower:
                    sender_mismatch_found = True
                    print(
                        "[IMAP 확인] 발신자 불일치:" f" 수신 {normalized_sender} · 기대 {expected_address_lower}",
                        flush=True,
                    )
                    continue
            else:
                if expected_address_lower:
                    if not normalized_sender:
                        sender_mismatch_found = True
                        print(
                            f"[IMAP 확인] 발신자 주소를 해석하지 못했습니다. 헤더={decoded_from or '-'}",
                            flush=True,
                        )
                        continue
                    if normalized_sender != expected_address_lower:
                        sender_mismatch_found = True
                        print(
                            "[IMAP 확인] 발신자 불일치:" f" 수신 {normalized_sender} · 기대 {expected_address_lower}",
                            flush=True,
                        )
                        continue
                elif expected_header_compare:
                    if not normalized_header:
                        sender_mismatch_found = True
                        print(
                            f"[IMAP 확인] 발신자 헤더를 해석하지 못했습니다. 헤더={decoded_from or '-'}",
                            flush=True,
                        )
                        continue
                    if normalized_header != expected_header_compare:
                        sender_mismatch_found = True
                        expected_label = expected_header_display or header_from or mail_from or "-"
                        print(
                            "[IMAP 확인] 발신자 헤더 불일치:" f" 수신 {decoded_from or '-'} · 기대 {expected_label}",
                            flush=True,
                        )
                        continue

            date_header = msg.get("Date")
            if not date_header:
                continue
            try:
                received_at = parsedate_to_datetime(date_header)
            except Exception:  # pylint: disable=broad-except
                continue
            if received_at.tzinfo is None:
                received_at = received_at.replace(tzinfo=timezone.utc)
            else:
                received_at = received_at.astimezone(timezone.utc)
            latency_raw = (received_at - sent_dt).total_seconds()
            received_address_display = decoded_from or sender_address or "-"
            received_line = f"{received_address_display or '-'} {_fmt_local(received_at)}"
            _log_sent_received(received_line)
            print(f"[IMAP 확인] 지연: {latency_raw:.1f}초", flush=True)
            latency = abs(latency_raw)
            print(f"[IMAP 확인] 지연(절대값): {latency:.1f}초", flush=True)
            print(f"[IMAP 확인] 허용지연: {allowed}초", flush=True)
            result_payload = {
                "latency": latency,
                "received_at": received_at.isoformat(),
                "allowed_latency": allowed,
                "delay_before_check": delay_before_check,
                "sent_at": sent_dt.isoformat(),
                "sent_display": sent_line,
                "received_display": received_line,
            }
            if latency <= allowed:
                print("[IMAP 확인] 판정: 성공", flush=True)
                result_payload["reason"] = None
                result_payload["status"] = "success"
                mail.logout()
                return result_payload
            print("[IMAP 확인] 판정: 허용 지연 초과", flush=True)
            result_payload["reason"] = f"허용 지연 {allowed}s 초과 (절대값 {latency:.1f}s)"
            result_payload["status"] = "failure"
            mail.logout()
            return result_payload

        _log_sent_received(default_received_line)
        print("[IMAP 확인] 지연: 측정 불가", flush=True)
        print(f"[IMAP 확인] 허용지연: {allowed}초", flush=True)
        if sender_mismatch_found:
            reason_text = "발신자 주소가 일치하는 메일을 찾지 못했습니다."
            print("[IMAP 확인] 판정: 발신자 주소가 일치하는 메일 없음", flush=True)
        else:
            reason_text = "유효한 메일을 찾지 못했습니다."
            print("[IMAP 확인] 판정: 유효한 메일을 찾지 못했습니다.", flush=True)
        return {
            "status": "failure",
            "latency": None,
            "received_at": None,
            "reason": reason_text,
            "allowed_latency": allowed,
            "sent_at": sent_dt.isoformat(),
            "delay_before_check": delay_before_check,
            "sent_display": sent_line,
            "received_display": default_received_line,
        }
    except imaplib.IMAP4.abort as exc:
        raise IMAPNetworkError(f"IMAP 세션이 예기치 않게 종료되었습니다: {exc}", original=exc) from exc
    except (socket.timeout, ssl.SSLError, OSError) as exc:
        raise IMAPNetworkError(f"IMAP 네트워크 오류: {exc}", original=exc) from exc
    finally:
        try:
            if mail is not None:
                mail.logout()
        except Exception:
            pass
