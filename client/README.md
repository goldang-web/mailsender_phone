# Termux 에이전트 개요

이 디렉터리는 각 휴대폰(또는 Termux 환경)에서 실행되는 이메일 발송 클라이언트 코드를 포함합니다.

## 주요 기능
- FastAPI 서버에 장치를 등록하고 주기적으로 명령을 폴링
- 지정된 SMTP 서버에 telnet 방식으로 연결하여 메일 발송
- 발송 결과를 서버에 즉시 보고
- 간단한 터미널 메뉴(`main.py`)로 서버 주소·디바이스 정보·SMTP 설정을 저장/불러오기
- `lib/mailer.py`를 통해 EUC-KR 인코딩 텔넷 발송 로직을 재사용 (단독 실행 시 샘플 메일 전송 테스트 가능)

## 기본 실행 흐름 (uv 가상환경)
1. `uv venv .venv`
2. `source .venv/bin/activate`
3. `uv pip install requests`
4. `uv run main.py`
5. 메뉴에서 `1`번을 선택하여 연결을 시작 (최초 실행 시 필요한 정보는 즉시 입력하라는 안내가 나옵니다.)

## 수동 실행 (기존 방식)
- `uv run python agent.py --server http://서버주소:8000 --device-id phone01 --device-name "휴대폰1" --smtp-host smtp.example.com --smtp-port 25`

## 파일 설명
- `main.py` : 설정 저장/불러오기를 지원하는 메뉴 기반 클라이언트 런처
- `agent.py` : 명령행 인자를 직접 받아 동작하는 이메일 발송 에이전트
- `lib/mailer.py` : 텔넷/SMTP 전송 함수와 샘플 실행 엔트리포인트
- `settings.json` : `main.py` 실행 시 자동 생성되는 설정 저장 파일
