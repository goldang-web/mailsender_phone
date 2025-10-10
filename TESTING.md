# 기능 검증 플로우

요구사항별로 수동으로 확인할 수 있는 절차입니다.

## 1. 서버 기동 및 대시보드 확인
1. `uv venv .venv && source .venv/bin/activate`
2. `uv pip install fastapi uvicorn`
3. `uvicorn server.main:app --host 0.0.0.0 --port 8000`
4. 브라우저에서 `http://localhost:8000` 접속
5. 대시보드가 로드되고 "총 디바이스" 카드가 0으로 표시되는지 확인

## 2. 클라이언트 등록 및 하트비트
1. 다른 터미널에서 `cd client`
2. `uv venv .venv && source .venv/bin/activate`
3. `uv pip install requests`
4. `uv run python main.py` 실행 후 서버 주소/디바이스 이름 입력
5. 서버 로그에 `/api/devices/register` → `/heartbeat` 요청이 도착하는지 확인
6. 대시보드에 새 디바이스 카드가 생성되는지 확인

## 3. 설정 편집 및 단일 발송
1. 대시보드 카드에서 도메인 라디오 버튼을 전환하면 저장 안내가 뜨는지 확인
2. HELO/SMTP 호스트/MAIL FROM/HEADER를 수정하고 "설정 저장" 클릭
3. 클라이언트 로그에 설정이 저장되었다는 메시지가 뜨는지 확인(`settings.json` 업데이트)
4. RCPT TO 입력 후 "단일 발송" → 서버 Job 상태가 대기→진행 중→완료로 바뀌는지 확인
5. 서버 DB `send_logs`에 해당 작업이 기록되는지 확인 (sqlite3 `SELECT * FROM send_logs ORDER BY id DESC LIMIT 1;`)

## 4. DB 파일 업로드 및 Inject
1. 카드에서 "네이버 DB 추가" 선택 → 모달이 열리고 비어 있음 표시 확인
2. 샘플 DB 파일 업로드 (예: `sample_naver.db`)
3. 업로드 목록에 파일이 등장하고 버전이 1로 표시되는지 확인
4. Inject 버튼 클릭 → 클라이언트 로그에 다운로드 완료 메시지가 찍히는지 확인
5. `client/data/naver/naver.db`가 갱신되고 `settings.json`의 `local_versions.naver` 값이 업데이트되는지 확인

## 5. 전체 발송 워커
1. 업로드한 DB에 `emails` 레코드를 삽입한 뒤 "전체 발송" 버튼 클릭
2. 클라이언트 로그에 처리 건수/성공/실패가 표기되는지 확인
3. 대시보드 카드의 도메인 통계가 하트비트 이후 갱신되는지 확인
4. 실패가 발생한 경우 Job 상태가 실패로 표시되고 응답 메시지가 출력되는지 확인

## 6. 로그 & 상태 모니터링
1. 대시보드 Worker 상태 영역에서 최근 5개의 작업이 순서대로 보이는지 확인
2. 파일 모달에서 미리보기/다운로드/삭제가 즉시 반영되는지 확인

## 7. 기타 확인사항
- 클라이언트 종료 시 Ctrl+C → "[정지]" 메시지 출력 여부
- 서버를 종료한 뒤 클라이언트가 네트워크 오류를 감지하고 재시도하는지 확인

위 절차가 이상 없이 수행되면 요구된 새로운 구조가 정상 동작하는 것으로 판단할 수 있습니다.
