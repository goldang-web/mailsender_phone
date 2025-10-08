# FastAPI 서버 개요

이 디렉터리는 다중 디바이스 이메일 발송 시스템의 서버(FastAPI) 코드를 포함합니다.

## 주요 기능
- 디바이스 등록, 목록 조회, 발송 명령 큐, 발송 결과 보고 API 제공
- SQLite `email_logs.db` 파일에 발송 이력을 저장
- 순수 HTML/JavaScript 기반의 대시보드 제공 (EUCKR 인코딩)

## 실행 방법 (uv 가상환경)
1. `uv venv .venv`
2. `source .venv/bin/activate`
3. `uv pip install fastapi uvicorn`
4. `uvicorn server.main:app --host 0.0.0.0 --port 8000`

## 디렉터리 구조
- `main.py` : FastAPI 애플리케이션 단일 파일
- `email_logs.db` : 실행 중 생성되는 SQLite 로그 파일
