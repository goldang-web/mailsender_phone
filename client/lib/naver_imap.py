import email
import imaplib
import time
from datetime import datetime, timezone
from email.header import decode_header, make_header
from email.utils import parseaddr, parsedate_to_datetime


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


def probe_imap_connection(email_id: str, password: str, *, folder: str = "Junk", timeout: int = 30) -> dict:
    """네이버 IMAP 서버 연결 및 폴더 선택을 검사합니다."""
    if not email_id or not password:
        return {"success": False, "reason": "IMAP 계정 정보가 필요합니다."}
    mail = None
    started = time.monotonic()
    try:
        print(f"[IMAP 테스트][디버그] 로그인 시도 · 계정 {email_id} · 폴더 {folder}", flush=True)
        mail = imaplib.IMAP4_SSL("imap.naver.com", 993, timeout=timeout)
        mail.login(email_id, password)
        status, _ = mail.select(folder, readonly=True)
        if status != "OK":
            print(f"[IMAP 테스트][디버그] 폴더 선택 실패 · 상태 {status}", flush=True)
            return {"success": False, "reason": f"{folder} 메일함을 열 수 없습니다."}
        latency = time.monotonic() - started
        print(f"[IMAP 테스트][디버그] 로그인 및 폴더 선택 성공 · 지연 {latency:.2f}s", flush=True)
        return {
            "success": True,
            "latency": latency,
            "checked_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
    except imaplib.IMAP4.error as exc:
        latency = time.monotonic() - started
        message = f"IMAP 인증 실패: {exc}"
        print(f"[IMAP 테스트][디버그] {message} · 소요 {latency:.2f}s", flush=True)
        return {"success": False, "reason": message}
    except Exception as exc:  # pylint: disable=broad-except
        latency = time.monotonic() - started
        message = f"IMAP 연결 오류: {exc}"
        print(f"[IMAP 테스트][디버그] {message} · 소요 {latency:.2f}s", flush=True)
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

def check_latest_emails(email_id, password, check_time, num_emails=5, sender_name=None, from_email=None):
    """
    IMAP을 통해 최신 메일의 날짜와 발신자를 확인하는 함수
    
    Args:
        email_id (str): 이메일 주소
        password (str): 이메일 비밀번호
        check_time (datetime): 확인할 시간
        num_emails (int): 확인할 메일 개수 (기본값: 5)
        sender_name (str): 확인할 발신자 이름 (옵션)
        from_email (str): 확인할 From 이메일 주소 (옵션)
        
    Returns:
        bool: 지정된 시간 이후의 메일이 있으면 True, 없으면 False
    """
    try:
        # IMAP 서버 설정
        IMAP_SERVER = 'imap.naver.com'
        IMAP_PORT = 993
        
        # IMAP 접속 (타임아웃 30초 설정)
        mail = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT, timeout=30)
        mail.login(email_id, password)
        
        # 스팸메일함 (Junk) 선택
        mail.select("Junk", readonly=True)
        
        # 메일 검색
        status, messages = mail.search(None, 'ALL')
        if status != 'OK':
            print("[디버그] 메일 검색 실패")
            mail.logout()
            return False
            
        mail_ids = messages[0].split()
        if not mail_ids:
            print("[디버그] 메일함이 비어있음")
            mail.logout()
            return False
        
        print(f"\n[디버그] IMAP에서 최근 {num_emails}개의 메일 확인 중...")
        print(f"[디버그] 확인 기준 시간: {check_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 최신 메일부터 num_emails개 확인
        mail_check_list = list(reversed(mail_ids[-num_emails:]))
        for idx, num in enumerate(mail_check_list, 1):
            print(f"\n[디버그] {idx}/{len(mail_check_list)}번째 메일 확인 중...")
            try:
                status, data = mail.fetch(num, '(RFC822)')
                if status != 'OK':
                    print(f"[디버그] 메일 {num} 가져오기 실패")
                    continue
                    
                msg = email.message_from_bytes(data[0][1])
                
                # 발신자 확인 (from_email 또는 sender_name이 제공된 경우)
                if from_email or sender_name:
                    from_header = msg.get("From", "")
                    
                    # Header 객체를 문자열로 변환
                    if hasattr(from_header, '__str__'):
                        from_header = str(from_header)
                    
                    print(f"[디버그] From 헤더 원본: {from_header}")
                    
                    # from_email이 제공된 경우 이메일 주소 비교
                    if from_email:
                        # < > 안의 이메일 주소 추출
                        header_email = ""
                        if '<' in from_header and '>' in from_header:
                            start = from_header.index('<')
                            end = from_header.index('>')
                            header_email = from_header[start+1:end].strip()
                        else:
                            # < > 없이 이메일 주소만 있는 경우
                            # From: 이후의 내용에서 공백 전까지를 이메일로 간주
                            parts = from_header.split()
                            for part in parts:
                                if '@' in part:
                                    header_email = part.strip()
                                    break
                        
                        print(f"[디버그] 추출된 이메일 주소: {header_email}")
                        print(f"[디버그] 확인할 이메일 주소: {from_email}")
                        
                        # 이메일 주소 비교
                        if header_email.lower() != from_email.lower():
                            print(f"[디버그] 이메일 주소 불일치, 다음 메일 확인")
                            continue
                        else:
                            print(f"[디버그] ✅ 이메일 주소 일치!")
                    
                    # sender_name이 제공된 경우 발신자 이름 확인 (기존 로직)
                    elif sender_name:
                        # From 헤더 디코딩
                        decoded_from = ""
                        try:
                            # decode_header는 문자열을 요구함
                            if isinstance(from_header, str):
                                decoded_parts = decode_header(from_header)
                            else:
                                decoded_parts = [(from_header, None)]
                                
                            for part, charset in decoded_parts:
                                if isinstance(part, bytes):
                                    # 네이버 메일은 EUC-KR 사용 - 단순하게 EUC-KR로만 디코딩
                                    try:
                                        # EUC-KR로 디코딩 (네이버 메일 표준)
                                        decoded_text = part.decode('euc-kr', errors='replace')
                                    except:
                                        # EUC-KR 실패 시 UTF-8 시도
                                        try:
                                            decoded_text = part.decode('utf-8', errors='replace')
                                        except:
                                            # 모두 실패 시 latin-1
                                            decoded_text = part.decode('latin-1', errors='replace')
                                    
                                    decoded_from += decoded_text
                                else:
                                    # bytes가 아닌 경우 문자열로 변환
                                    decoded_from += str(part)
                                    
                        except Exception as e:
                            print(f"[디버그] From 헤더 디코딩 에러: {e}")
                            # 디코딩 완전 실패 시 원본 문자열 사용
                            decoded_from = str(from_header)
                        
                        print(f"[디버그] 메일 발신자: {decoded_from}")
                        print(f"[디버그] 확인할 발신자 이름: {sender_name}")
                        
                        # 발신자 이름이 포함되어 있는지 확인
                        try:
                            if sender_name not in decoded_from:
                                print(f"[디버그] 발신자 이름 불일치, 다음 메일 확인")
                                continue
                        except TypeError as e:
                            print(f"[디버그] 발신자 이름 비교 에러: {e}")
                            # 비교 실패 시 다음 메일로
                            continue
                
                # 날짜 가져오기
                date_str = msg["Date"]
                if not date_str:
                    print(f"[디버그] 메일 {num}의 날짜 정보 없음")
                    continue
                    
                # 날짜 문자열을 datetime 객체로 변환
                try:
                    email_date = parsedate_to_datetime(date_str)
                    print(f"[디버그] 메일 수신 시간: {email_date.strftime('%Y-%m-%d %H:%M:%S')}")
                    
                    # 이메일 날짜가 체크 시간 이후인지 확인 (15초 이내의 메일을 확인)
                    if email_date.tzinfo is not None:
                        check_time = check_time.replace(tzinfo=email_date.tzinfo)
                    time_diff = (email_date - check_time).total_seconds()
                    print(f"[디버그] 시간 차이: {time_diff}초")
                    
                    # 시간 차이의 절대값이 15초 이내인 경우 확인
                    if abs(time_diff) <= 15:  # 15초 이내의 메일
                        if sender_name:
                            print(f"[디버그] ✅ 발신자 이름과 시간 모두 일치!")
                        mail.logout()
                        return True
                except Exception as e:
                    print(f"[디버그] 날짜 파싱 오류: {e}")
                    continue
            except Exception as e:
                print(f"[디버그] 메일 {num} 처리 중 오류: {e}")
                continue
        
        mail.logout()
        return False
        
    except Exception as e:
        print(f"[디버그] IMAP 확인 중 오류 발생: {e}")
        try:
            mail.logout()
        except:
            pass
        return False


def verify_delivery(
    email_id,
    password,
    mail_from,
    sent_at,
    allowed_delay,
    *,
    max_messages=15,
    check_delay=None,
):
    """네이버 스팸메일함에서 발신자 메일 도착 여부를 확인합니다."""

    if not email_id or not password:
        raise ValueError("IMAP 계정 정보가 필요합니다.")
    if not mail_from:
        return {
            "status": "error",
            "latency": None,
            "received_at": None,
            "reason": "MAIL FROM이 설정되지 않았습니다.",
        }
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
    try:
        allowed = int(allowed_delay)
    except (TypeError, ValueError):
        allowed = 20
    allowed = max(0, min(600, allowed))
    try:
        search_limit = int(max_messages or 15)
    except (TypeError, ValueError):
        search_limit = 15
    search_limit = max(5, min(50, search_limit))
    try:
        delay_before_check = float(check_delay) if check_delay is not None else 0.0
    except (TypeError, ValueError):
        delay_before_check = 0.0
    delay_before_check = max(0.0, min(600.0, delay_before_check))
    normalized_from = mail_from.strip().lower()
    mail = None
    observed_senders = []
    try:
        mail = imaplib.IMAP4_SSL("imap.naver.com", 993, timeout=30)
        mail.login(email_id, password)
        mail.select("Junk", readonly=True)
        status, messages = mail.search(None, "ALL")
        if status != "OK":
            return {
                "status": "error",
                "latency": None,
                "received_at": None,
                "reason": "메일 검색에 실패했습니다.",
                "allowed_latency": allowed,
            }
        mail_ids = messages[0].split()
        if not mail_ids:
            return {
                "status": "failure",
                "latency": None,
                "received_at": None,
                "reason": "스팸메일함이 비어 있습니다.",
                "allowed_latency": allowed,
            }
        candidates = list(reversed(mail_ids[-search_limit:]))
        for num in candidates:
            status, data = mail.fetch(num, "(RFC822.HEADER)")
            if status != "OK" or not data or data[0] is None:
                continue
            header_bytes = data[0][1]
            if not header_bytes:
                continue
            msg = email.message_from_bytes(header_bytes)
            header_addresses = {}
            for header_name in ("Return-Path", "From", "Sender", "Reply-To", "X-Original-From"):
                for header_value in msg.get_all(header_name, []):
                    _, addr = parseaddr(header_value)
                    if not addr:
                        continue
                    candidate = addr.strip().lower()
                    if not candidate:
                        continue
                    header_addresses.setdefault(candidate, header_name)
                    if len(observed_senders) < 10:
                        observed_senders.append(candidate)
            if normalized_from not in header_addresses:
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
            latency_seconds = (received_at - sent_dt).total_seconds()
            if latency_seconds < 0:
                if abs(latency_seconds) <= 300:
                    latency_seconds = 0.0
                else:
                    continue
            latency = max(0.0, latency_seconds)
            matched_header = header_addresses.get(normalized_from)
            message_id = (msg.get("Message-ID") or msg.get("Message-Id") or "").strip()
            payload_common = {
                "latency": latency,
                "received_at": received_at.isoformat(),
                "allowed_latency": allowed,
                "matched_header": matched_header,
                "message_id": message_id or None,
                "candidate_addresses": sorted(set(observed_senders or [normalized_from])),
                "delay_before_check": delay_before_check,
            }
            if latency <= allowed:
                return {
                    "status": "success",
                    "reason": None,
                    **payload_common,
                }
            return {
                "status": "failure",
                "reason": f"허용 지연 {allowed}s 초과",
                **payload_common,
            }
        return {
            "status": "failure",
            "latency": None,
            "received_at": None,
            "reason": "발신자 메일을 찾지 못했습니다.",
            "allowed_latency": allowed,
            "candidate_addresses": sorted(set(observed_senders)),
            "delay_before_check": delay_before_check,
        }
    finally:
        try:
            if mail is not None:
                mail.logout()
        except Exception:
            pass


def get_latest_email_from(email_id, password):
    """
    IMAP을 통해 최신 메일 1개의 From 헤더를 가져와서 디코딩 테스트
    
    Args:
        email_id (str): 이메일 주소
        password (str): 이메일 비밀번호
        
    Returns:
        str: 디코딩된 From 헤더 내용
    """
    try:
        # IMAP 서버 설정
        IMAP_SERVER = 'imap.naver.com'
        IMAP_PORT = 993
        
        # IMAP 접속
        mail = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT, timeout=30)
        mail.login(email_id, password)
        
        # 스팸메일함 (Junk) 선택
        mail.select("Junk", readonly=True)
        
        # 메일 검색
        status, messages = mail.search(None, 'ALL')
        if status != 'OK':
            print("[테스트] 메일 검색 실패")
            mail.logout()
            return None
            
        mail_ids = messages[0].split()
        if not mail_ids:
            print("[테스트] 메일함이 비어있음")
            mail.logout()
            return None
        
        # 가장 최신 메일 1개 가져오기
        latest_mail_id = mail_ids[-1]
        
        print(f"\n[테스트] 최신 메일 ID: {latest_mail_id}")
        
        # 먼저 ENVELOPE 정보를 가져와서 네이버가 해석한 발신자 정보 확인
        print("\n[테스트] ENVELOPE 정보 가져오기...")
        status, envelope_data = mail.fetch(latest_mail_id, '(ENVELOPE)')
        if status == 'OK':
            print(f"[테스트] ENVELOPE 원본: {envelope_data}")
            # ENVELOPE 파싱 시도
            try:
                envelope_str = str(envelope_data[0])
                print(f"[테스트] ENVELOPE 문자열: {envelope_str}")
            except Exception as e:
                print(f"[테스트] ENVELOPE 파싱 실패: {e}")
        
        # RFC822.HEADER로 헤더만 가져오기
        print("\n[테스트] RFC822.HEADER로 헤더만 가져오기...")
        status, header_data = mail.fetch(latest_mail_id, '(RFC822.HEADER)')
        if status == 'OK':
            header_bytes = header_data[0][1]
            # 원본 바이트 일부 출력
            print(f"[테스트] 헤더 원본 바이트 (처음 500바이트): {header_bytes[:500]}")
            
            # EUC-KR로 직접 디코딩 시도
            try:
                header_text_euckr = header_bytes.decode('euc-kr', errors='replace')
                print(f"\n[테스트] EUC-KR로 디코딩한 전체 헤더:")
                for line in header_text_euckr.split('\n')[:10]:  # 처음 10줄만
                    if line.strip():
                        print(f"  {line}")
            except Exception as e:
                print(f"[테스트] EUC-KR 디코딩 실패: {e}")
        
        # 기존 방식으로도 가져오기
        status, data = mail.fetch(latest_mail_id, '(RFC822)')
        if status != 'OK':
            print(f"[테스트] 메일 가져오기 실패")
            mail.logout()
            return None
            
        msg = email.message_from_bytes(data[0][1])
        
        # From 헤더 가져오기
        from_header = msg.get("From", "")
        
        print(f"\n[테스트] email.message_from_bytes로 파싱한 From 헤더: {from_header}")
        print(f"[테스트] From 헤더 타입: {type(from_header)}")
        
        # raw From 헤더 가져오기
        raw_from = msg.get_all("From")
        print(f"[테스트] get_all('From') 결과: {raw_from}")
        
        # Header 객체를 문자열로 변환
        if hasattr(from_header, '__str__'):
            from_header = str(from_header)
        
        # From 헤더 디코딩
        decoded_from = ""
        try:
            # decode_header는 문자열을 요구함
            if isinstance(from_header, str):
                decoded_parts = decode_header(from_header)
            else:
                decoded_parts = [(from_header, None)]
                
            print(f"[테스트] decode_header 결과: {decoded_parts}")
            
            for part, charset in decoded_parts:
                print(f"[테스트] Part: {part}, Charset: {charset}, Part 타입: {type(part)}")
                
                if isinstance(part, bytes):
                    # 여러 인코딩 시도
                    encodings = ['euc-kr', 'utf-8', 'cp949', 'iso-2022-kr', 'latin-1']
                    for enc in encodings:
                        try:
                            decoded_text = part.decode(enc, errors='replace')
                            print(f"[테스트] {enc} 디코딩: {decoded_text}")
                            if enc == 'euc-kr':  # 기본적으로 EUC-KR 사용
                                decoded_from += decoded_text
                        except Exception as e:
                            print(f"[테스트] {enc} 디코딩 실패: {e}")
                else:
                    # bytes가 아닌 경우 문자열로 변환
                    decoded_from += str(part)
                    print(f"[테스트] 문자열 부분: {str(part)}")
                    
        except Exception as e:
            print(f"[테스트] From 헤더 디코딩 에러: {e}")
            decoded_from = str(from_header)
        
        print(f"\n[테스트] 최종 디코딩된 From: {decoded_from}")
        
        # 날짜 정보도 출력
        date_str = msg.get("Date", "")
        print(f"[테스트] 메일 날짜: {date_str}")
        
        # Subject도 테스트
        subject = msg.get("Subject", "")
        print(f"[테스트] 원본 Subject: {subject}")
        
        # Subject 디코딩도 시도
        try:
            decoded_subject_parts = decode_header(subject)
            decoded_subject = ""
            for part, charset in decoded_subject_parts:
                if isinstance(part, bytes):
                    try:
                        decoded_subject += part.decode('euc-kr' if charset is None else charset, errors='replace')
                    except:
                        decoded_subject += str(part)
                else:
                    decoded_subject += str(part)
            print(f"[테스트] 디코딩된 Subject: {decoded_subject}")
        except Exception as e:
            print(f"[테스트] Subject 디코딩 실패: {e}")
        
        mail.logout()
        return decoded_from
        
    except Exception as e:
        print(f"[테스트] IMAP 테스트 중 오류 발생: {e}")
        try:
            mail.logout()
        except:
            pass
        return None 
