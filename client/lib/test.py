import smtplib
import telnetlib
import time
import random
from email.header import Header
import base64

# SMTP 기본 포트
smtp_port = 25  # SMTP 기본 포트

def encode_mime_header(text, charset='euc-kr', force=True):
    """
    텍스트를 MIME 헤더 형식으로 인코딩
    예: "안심정품" -> "=?EUC-KR?B?...?="
    
    Args:
        text: 인코딩할 텍스트
        charset: 사용할 문자셋 (기본값: euc-kr)
        force: ASCII 문자도 강제로 인코딩할지 여부
    """
    try:
        if force or not text.isascii():
            # 강제로 Base64 인코딩
            encoded_bytes = text.encode(charset)
            b64_encoded = base64.b64encode(encoded_bytes).decode('ascii')
            return f"=?{charset.upper()}?B?{b64_encoded}?="
        else:
            # ASCII만 있으면 그대로 반환
            return text
    except Exception as e:
        print(f"MIME 인코딩 실패: {e}")
        # 실패하면 원본 반환
        return text

def get_mx_server_for_email(email):
    """
    이메일 주소의 도메인에 따라 적절한 MX 서버를 랜덤하게 선택하여 반환
    """
    # 이메일에서 도메인 추출
    if '@' in email:
        domain = email.split('@')[1].lower()
    else:
        # 기본값으로 네이버 MX 서버 반환
        return random.choice([
            "mx1.naver.com",
            "mx2.naver.com",
            "mx3.naver.com",
            "mx4.naver.com",
            "mx5.naver.com",
            "mx6.naver.com"
        ])
    
    # 네이버 도메인
    if domain in ['naver.com', 'navercorp.com']:
        return random.choice([
            "mx1.naver.com",
            "mx2.naver.com",
            "mx3.naver.com",
            "mx4.naver.com",
            "mx5.naver.com",
            "mx6.naver.com"
        ])
    
    # 다음/한메일 도메인
    elif domain in ['daum.net', 'hanmail.net', 'kakao.com']:
        return random.choice([
            "mx1.hanmail.net",
            "mx2.hanmail.net",
            "mx3.hanmail.net",
            "mx4.hanmail.net"
        ])
    
    # 기타 도메인의 경우 네이버 MX 서버를 기본으로 사용
    else:
        return random.choice([
            "mx1.naver.com",
            "mx2.naver.com",
            "mx3.naver.com",
            "mx4.naver.com",
            "mx5.naver.com",
            "mx6.naver.com"
        ])

# 테스트용 기본 SMTP 서버 (실제 사용 시 동적으로 선택됨)
smtp_server = "mx1.naver.com"

# 보낼 이메일 정보
sender_email = "mobile2@elite-watch.com"  # 보내는 사람 이메일 (네이버가 아닌 다른 이메일도 가능)
recipient_email = "vkdlxm145@naver.com"  # 수신자 이메일
subject = "하이루"  # 이메일 제목
body = "at1515.ＳＰace"


def send_mail(smtp_server, sender_email, recipient_email, subject, body):
    # smtp_server가 None이면 수신자 도메인에 따라 자동 선택
    if smtp_server is None:
        smtp_server = get_mx_server_for_email(recipient_email)
    
    # 이메일 본문 작성
    email_message = f"From: {sender_email}\r\nTo: {recipient_email}\r\nSubject: {subject}\r\n\r\n{body}\r\n"

    try:
        # SMTP 서버에 연결
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.ehlo()

        # 이메일 전송
         # 🔥 여기서 EUC-KR로 인코딩하여 바이트로 전송 (에러 처리 포함)
        try:
            server.sendmail(sender_email, recipient_email, email_message.encode("euc-kr")) #여기가 핵심이다 여기를utf8로하면 전각문자, 한글이 그대로 노출됨됨
        except UnicodeEncodeError as e:
            # EUC-KR로 인코딩할 수 없는 문자가 있을 경우, HTML 엔티티로 변환
            print(f"⚠️ EUC-KR 인코딩 에러: {e}")
            print(f"문제 문자 위치: {e.start}-{e.end}, 문자: '{email_message[e.start:e.end]}'")

            # 인코딩 불가능한 문자를 HTML 엔티티로 변환
            encoded_message = []
            for char in email_message:
                try:
                    char.encode('euc-kr')
                    encoded_message.append(char)
                except UnicodeEncodeError:
                    # HTML 엔티티로 변환
                    encoded_message.append(f'&#{ord(char)};')

            # 변환된 문자열을 다시 인코딩
            converted_message = ''.join(encoded_message)
            server.sendmail(sender_email, recipient_email, converted_message.encode("euc-kr"))
        print("이메일 전송 성공 ✅")

        # SMTP 서버 연결 종료
        server.quit()
        return True, "이메일 전송 성공"
    except Exception as e:
        error_msg = f"이메일 전송 실패: {e}"
        print(f"{error_msg} ❌")
        return False, error_msg


def send_mail_telnet(smtp_server, sender_email, recipient_email, helo_name, header, bcc_emails=None):
    """텔넷을 사용하여 SMTP 서버에 직접 연결하여 이메일을 전송합니다."""
    # smtp_server가 None이면 수신자 도메인에 따라 자동 선택
    if smtp_server is None:
        smtp_server = get_mx_server_for_email(recipient_email)
    
    try:
        # 텔넷으로 SMTP 서버에 연결
        tn = telnetlib.Telnet(smtp_server, smtp_port)
        
         # 서버 응답 대기
        response = tn.read_until(b"\r\n", timeout=5).decode('utf-8')
        # print(f"서버 응답: {response}")
        if "block" in response.lower():
            return "smtp block"
        
        # HELO 명령 전송
        tn.write(f"HELO {helo_name}\r\n".encode('utf-8'))
        response = tn.read_until(b"\r\n", timeout=5).decode('utf-8')
        # print(f"HELO 응답: {response}")
        
        # MAIL FROM 명령 전송
        tn.write(f"MAIL FROM:<{sender_email}>\r\n".encode('utf-8'))
        response = tn.read_until(b"\r\n", timeout=5).decode('utf-8')
        # print(f"MAIL FROM 응답: {response}")
        
        # RCPT TO 명령 전송
        tn.write(f"RCPT TO:<{recipient_email}>\r\n".encode('utf-8'))
        response = tn.read_until(b"\r\n", timeout=5).decode('utf-8')
        # print(f"RCPT TO 응답: {response}")
        
        # BCC 이메일 처리
        if bcc_emails:
            for bcc_email in bcc_emails:
                tn.write(f"RCPT TO:<{bcc_email}>\r\n".encode('utf-8'))
                response = tn.read_until(b"\r\n", timeout=5).decode('utf-8')
                # print(f"BCC RCPT TO 응답 ({bcc_email}): {response}")
        
        # DATA 명령 전송
        tn.write(b"DATA\r\n")
        response = tn.read_until(b"\r\n", timeout=5).decode('utf-8')
        # print(f"DATA 응답: {response}")
        
        # 이메일 헤더와 본문 전송 - EUC-KR 인코딩 적용 (에러 처리 포함)
        email_content = f"{header}\r\n.\r\n"
        try:
            tn.write(email_content.encode('euc-kr')) #여기가 핵심이다 여기를utf8로하면 전각문자, 한글이 그대로 노출됨
        except UnicodeEncodeError as e:
            # EUC-KR로 인코딩할 수 없는 문자가 있을 경우, 해당 문자를 HTML 엔티티로 변환
            print(f"⚠️ EUC-KR 인코딩 에러: {e}")
            print(f"문제 문자 위치: {e.start}-{e.end}, 문자: '{email_content[e.start:e.end]}'")

            # 인코딩 불가능한 문자를 HTML 엔티티로 변환
            encoded_content = []
            for char in email_content:
                try:
                    char.encode('euc-kr')
                    encoded_content.append(char)
                except UnicodeEncodeError:
                    # HTML 엔티티로 변환
                    encoded_content.append(f'&#{ord(char)};')

            # 변환된 문자열을 다시 인코딩
            converted_content = ''.join(encoded_content)
            tn.write(converted_content.encode('euc-kr'))
        response = tn.read_until(b"\r\n", timeout=5).decode('utf-8')
        # print(f"{response}")
        
        # QUIT 명령 전송
        tn.write(b"QUIT\r\n")
        tn.close()
        
        return response
        
    except Exception as e:
        error_msg = f"텔넷을 통한 이메일 전송 실패: {e}"
        print(f"{error_msg} ❌")
        return error_msg


def test_mime_encoding():
    """MIME 인코딩 테스트 함수"""
    test_texts = [
        "안심정품",
        "안63심정품",
        "테스트123",
        "ａｂｃ전각",
        "ⓐⓑⓒ원문자"
    ]
    
    print("=== MIME 인코딩 테스트 ===")
    for text in test_texts:
        encoded = encode_mime_header(text)
        print(f"원본: {text}")
        print(f"인코딩: {encoded}")
        print(f"From 헤더 예시: {encoded} <noreply@example.com>")
        print("-" * 50)

# 파일이 직접 실행될 때 메일 전송 수행
if __name__ == "__main__":
    print("메일 전송을 시작합니다...")
    # 수신자 도메인에 따라 적절한 MX 서버 자동 선택
    selected_smtp_server = get_mx_server_for_email(recipient_email)
    print(f"선택된 SMTP 서버: {selected_smtp_server}")
    result, message = send_mail(selected_smtp_server, sender_email, recipient_email, subject, body)
    if result:
        print(f"결과: {message}")
    else:
        print(f"오류: {message}")