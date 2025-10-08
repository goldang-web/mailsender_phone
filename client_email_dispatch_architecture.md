# 클라이언트 메일 발송 시스템 아키텍처 계획서

## 1. 시스템 개요
- 대상: 단일 운영자가 여러 휴대폰(최대 20대)에서 Termux 기반 클라이언트 실행.
- 목적: 네이버(`naver.com`)와 다음(`hanmail.net`) 대상 대량 메일 전송을 안정적·고속으로 수행.
- 구조: 각 도메인별 독립 SQLite DB(`naver.db`, `daum.db`)를 다운로드 받아 로컬에서 발송 루프를 무한 실행.
- 중앙 웹 대시보드: 주소 파일 관리, Inject 명령 수행, 설정 배포, 상태 모니터링.

## 2. 데이터 모델
### 2.1 도메인별 SQLite 파일
- 파일 분리: `naver.db`, `daum.db` (향후 채널 추가 시 파일 추가).
- 공통 테이블 `emails`
  - `id INTEGER PRIMARY KEY`
  - `email TEXT NOT NULL`
  - `source_file TEXT` (대시보드 폴더 기준 파일명)
  - `version INTEGER` (Inject 버전)
  - `status TEXT CHECK(status IN ('pending','reserved','sent','block','failed','removed'))`
  - `priority INTEGER DEFAULT 100` (값이 작을수록 우선순위 높음)
  - `reserved_by TEXT` (세션 UUID)
  - `reserved_at DATETIME`
  - `next_retry_at DATETIME`
  - `attempts INTEGER DEFAULT 0`
  - `last_error TEXT`
  - `meta JSON` (도메인별 추가 정보)
  - `created_at DATETIME`
  - `updated_at DATETIME`
- 보조 테이블
  - `injection_meta(version INTEGER PRIMARY KEY, created_at, total_count, files JSON, notes TEXT)`
  - `domain_config(key TEXT PRIMARY KEY, value TEXT)` (SMTP 호스트, 파싱 규칙 등)

### 2.2 상태 정의
- `pending`: 최초 또는 다시 큐에 포함되어 대기.
- `reserved`: 워커가 가져가 발송 중. 타임아웃 시 자동 롤백.
- `sent`: 원하는 응답 수신. 반복 발송 필요 시 `priority` 낮춰 재순환.
- `block`: 차단 응답. `priority` 최상위, `next_retry_at` 기반으로 재시도.
- `failed`: 주소 없음, 영구 실패. Inject로 파일에서 제거하지 않는 한 기록 유지.
- `removed`: 대시보드에서 파일 삭제/제거된 주소. 발송 대상에서 제외.

## 3. 동작 시나리오
### 3.1 초기 세팅
1. 대시보드에서 도메인별 주소 파일 업로드(예: `naver/1.txt`, `naver/2.txt`).
2. Inject 실행 → 서버가 파일 목록과 기존 `emails`를 비교.
   - 신규 파일/주소: `pending` 상태로 INSERT.
   - 삭제/수정된 파일: 차집합 계산 → 해당 주소 `removed` 처리.
   - 유지 주소: 상태 그대로.
3. `injection_meta`에 새 버전 기록, 해당 DB 파일을 패키징(압축) 후 최신 버전으로 보관.

### 3.2 클라이언트 시작
1. Termux 클라이언트가 대시보드 API에서 `domain`, `db_version`, 세션 설정, 워커 옵션(JSON) 수신.
2. 로컬 버전과 다르면 최신 `naver.db` 또는 `daum.db` 다운로드 후 교체.
3. 세션 프로세스/스레드 수만큼 워커 생성. 각 워커는 고유 `session_id`와 `worker_id` 부여.
4. 워커는 무한 루프로 실행하며 아래 로직 반복.

### 3.3 워커 발송 루프
1. 예약 단계: `UPDATE ... SET status='reserved', reserved_by=?, reserved_at=NOW() WHERE id IN (SELECT id FROM emails WHERE status IN ('block','pending','sent') AND next_retry_at<=NOW() ORDER BY priority ASC, id ASC LIMIT batch_size)` 형태의 트랜잭션 실행.
2. 선택된 레코드를 메모리에 로드하고 SMTP 발송 시도. 도메인별 설정(`domain_config`)으로 파서/헤더/재시도 정책 적용.
3. 응답 분기
   - 성공(원하는 응답): `status='sent'`, `priority` 기본값보다 높게(예: 200) 설정, `next_retry_at` 재발송 주기에 맞춰 갱신.
   - 차단: `status='block'`, `priority` 최상위(예: 10), `attempts++`, `next_retry_at` 짧게 설정. 필요 시 IP 교체 훅 발동.
   - 영구 실패: `status='failed'`, `priority` 고정, `last_error` 기록.
   - 예외로 인해 재시도 필요(네트워크 오류 등): `status`를 `block`이나 `pending`으로 되돌리고 `next_retry_at` 설정.
4. 트랜잭션 커밋. 발송 결과 로그는 로컬 파일(`sent.log`, `block.log`, `failed.log`)에 append.
5. 워커는 즉시 다음 예약으로 이동해 무한 반복.

### 3.4 발송 중단 / 재개
- 중단 명령 수신 시 워커는 현재 예약 건만 마무리하고 종료.
- 재개 시 기존 `reserved` 레코드 중 `reserved_at`이 타임아웃을 넘은 항목을 `pending`으로 자동 복귀시키는 백그라운드 작업 수행.

### 3.5 대시보드와 통신
- 설정 폴링: 30~60초마다 HTTP GET → 세션 수, 레이트리밋, 발송 도메인, 재발송 규칙 등을 갱신.
- 상태 보고: `sent_count`, `block_count`, `failed_count`, `pending_count`, 최신 `db_version`, 현재 IP, 최근 차단 이벤트 등을 POST.
- Inject 트리거 감지: `db_version`이 변경되면 워커를 순차적으로 재시작하여 새 DB 적용.

## 4. 파일/DB 동기화 전략
- 파일 관리
  - 대시보드에서 클라이언트별 도메인 폴더 제공: 업로드·삭제·미리보기·주소 수 통계.
  - 파일 추가 시 자동 해시 계산, 삭제 시 목록에서 제거.
- Inject 알고리즘
  1. 현재 파일 세트와 `injection_meta`의 최신 버전 비교.
  2. 각 파일의 주소 목록을 빠르게 읽기 위해 파이프라인 구성(멀티프로세스 파싱 or 스트림 처리).
  3. `emails`에서 `source_file` + `version` 조합으로 기존 레코드 조회.
  4. 추가/삭제/유지 그룹을 분기하여 `INSERT`/`UPDATE status='removed'`/유지 처리.
  5. `priority` 기본값은 `pending=100`, `block=10`, `sent=200`, `failed=900`, `removed=1000` 등으로 통일.
  6. 새로운 버전 번호 부여 후 DB 파일을 최종 저장, 압축.

## 5. 장애 및 성능 고려
- **고성능**: `PRAGMA journal_mode=WAL`, `synchronous=NORMAL`, 배치 크기 200~1000, 인덱스 구성(`status`, `next_retry_at`, `priority`).
- **무한 루프**: `sent`와 `block` 상태는 계속 재예약되지만 `priority` 조절로 `block`이 우선 처리되도록 보장.
- **IP 차단 대응**: `block_count` 급증 시 워커가 IP 교체 스크립트 실행, 완료 후 `block` 상태 레코드부터 다시 예약.
- **데이터 보호**: Inject로 생성된 DB 파일은 메인 장비에만 저장, 필요 시 수동 백업.
- **비정상 종료 복구**: `reserved` 타임아웃 스캐너가 주기적으로 `pending`으로 되돌림.

## 6. 구현 단계 로드맵
1. **데이터 계층**
   - `naver.db`, `daum.db` 스키마 정의.
   - Inject 스크립트(파일 diff → DB 업데이트) 작성.
2. **클라이언트 워커**
   - Termux Python 서비스: 설정 폴링, DB 다운로드, 워커 풀, SMTP 파서.
   - 응답 분기 로직과 로컬 로그 시스템 구축.
3. **웹 대시보드**
   - 파일 업로드/삭제 UI + API.
   - Inject 버튼 → 서버에서 diff 수행 → 버전 기록 → DB 배포.
   - 설정 라디오/세션 수 조절, 상태 모니터링 차트 구현.
4. **운영 자동화**
   - IP 교체 스크립트 통합.
   - 주기적 백업/로그 수집 파이프라인.
   - 경보 시스템(예: block 급증 시 알림).
5. **고도화(선택)**
   - 추가 도메인/채널 확장 시 DB 파일 추가.
   - 도메인별 커스텀 파서 내장 구조 정리.
   - 통합 레포팅(기간별 발송량, 성공률 등) 개발.

## 7. 결론
- 도메인별 SQLite 분리, Inject 기반 버전 관리, 우선순위/재시도 정책으로 구성하면 무한 발송 루프를 안전하게 유지하면서도 빠른 처리와 쉬운 운영이 가능하다.
- 대시보드는 파일 관리·버전 제어·상태 모니터링에 집중하고, 클라이언트는 로컬 DB만으로 고속 발송을 수행하므로 병목 없이 세션 확장에 대응할 수 있다.
