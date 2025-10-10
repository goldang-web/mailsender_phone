# 메일 발송 제어 서버

FastAPI 기반의 대시보드와 REST API를 제공하여 다중 디바이스 메일 발송을 중앙에서 제어합니다.

## 제공 기능
- 웹 대시보드: 디바이스별 설정 편집, 단일/전체 발송, 워커 상태 모니터링
- 파일 관리: 도메인별 DB 파일 업로드·미리보기·삭제·Inject 트리거
- 디바이스 API: 등록, 하트비트, 작업 큐, 작업 결과 보고, 파일 다운로드
- 발송 및 Inject 기록을 위한 SQLite 스토리지 관리

## 실행 방법
```bash
uv venv .venv
source .venv/bin/activate
uv pip install fastapi uvicorn
uvicorn server.main:app --host 0.0.0.0 --port 8000
```

## 주요 경로
- `main.py`: FastAPI 엔트리포인트 및 모든 엔드포인트 구현
- `templates/dashboard.html`: 대시보드 UI (Vanilla JS)
- `control.db`: 디바이스·작업·파일 메타데이터가 저장되는 SQLite DB
- `storage/devices/<device>/<domain>/`: 업로드된 DB 파일 저장소

## 브라우저 접속
서버가 실행 중이면 `http://<서버주소>:8000/` 에 접속하여 대시보드를 이용할 수 있습니다.
