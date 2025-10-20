import json
import sqlite3
import threading
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple


EMAIL_STATUSES = (
    "pending",
    "reserved",
    "sent",
    "block",
    "failed",
    "removed",
)

DEFAULT_PRIORITIES = {
    "block": 10,
    "pending": 100,
    "sent": 200,
    "failed": 900,
    "removed": 1000,
}

DEFAULT_DOMAIN_CONFIG = {
    "batch_size": "200",
    "reserved_timeout_seconds": "300",
    "block_retry_seconds": "60",
    "pending_retry_seconds": "120",
    "sent_retry_seconds": "3600",
    "smtp_port": "25",
    "smtp_host": "",
    "helo": "",
    "max_workers": "3",
    "report_interval_seconds": "60",
    "mail_from": "",
    "header_template": "",
}


@dataclass
class DomainFileInfo:
    name: str
    size: int
    line_count: int
    modified_at: str


@dataclass
class DomainState:
    domain: str
    db_version: int
    last_injected_at: Optional[str]
    total_count: int
    status_counts: Dict[str, int]
    files: List[DomainFileInfo]
    notes: Optional[str]


class DomainManager:
    def __init__(self, root: Path, domains: Optional[Iterable[str]] = None) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self._locks: Dict[str, threading.RLock] = {}
        for domain in domains or []:
            self.ensure_domain(domain)

    # ------------------------------------------------------------------ #
    # 경로/락 관리
    # ------------------------------------------------------------------ #
    def ensure_domain(self, domain: str) -> Path:
        domain = domain.lower()
        domain_dir = self.root / domain
        domain_dir.mkdir(parents=True, exist_ok=True)
        (domain_dir / "files").mkdir(exist_ok=True)
        self._ensure_db(domain)
        if domain not in self._locks:
            self._locks[domain] = threading.RLock()
        return domain_dir

    def files_dir(self, domain: str) -> Path:
        return self.ensure_domain(domain) / "files"

    def db_path(self, domain: str) -> Path:
        domain_dir = self.ensure_domain(domain)
        return domain_dir / f"{domain}.db"

    def _lock(self, domain: str) -> threading.RLock:
        self.ensure_domain(domain)
        return self._locks[domain]

    # ------------------------------------------------------------------ #
    # DB 초기화/접근
    # ------------------------------------------------------------------ #
    def _ensure_db(self, domain: str) -> None:
        path = self.root / domain / f"{domain}.db"
        path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(path) as conn:
            self._apply_schema(conn)
            self._ensure_emails_table_allows_duplicates(conn, domain)
            self._ensure_config_defaults(conn)

    @staticmethod
    def _ensure_config_defaults(conn: sqlite3.Connection) -> None:
        payload = [(key, value) for key, value in DEFAULT_DOMAIN_CONFIG.items()]
        conn.executemany(
            """
            INSERT INTO domain_config (key, value)
            VALUES (?, ?)
            ON CONFLICT(key) DO NOTHING
            """,
            payload,
        )
        conn.commit()

    @staticmethod
    def _apply_schema(conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS emails (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL,
                source_file TEXT,
                version INTEGER,
                status TEXT CHECK(status IN ('pending','reserved','sent','block','failed','removed')) NOT NULL DEFAULT 'pending',
                priority INTEGER DEFAULT 100,
                reserved_by TEXT,
                reserved_at TEXT,
                next_retry_at TEXT,
                attempts INTEGER DEFAULT 0,
                last_error TEXT,
                meta TEXT DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS injection_meta (
                version INTEGER PRIMARY KEY,
                created_at TEXT NOT NULL,
                total_count INTEGER NOT NULL,
                files TEXT NOT NULL,
                notes TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS domain_config (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_emails_status_priority
                ON emails(status, priority, id)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_emails_retry
                ON emails(next_retry_at)
            """
        )
        conn.commit()

    @staticmethod
    def _fetch_table_definition(conn: sqlite3.Connection, table: str) -> str:
        row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
            (table,),
        ).fetchone()
        if row is None:
            return ""
        if isinstance(row, sqlite3.Row):
            return str(row["sql"] or "")
        if isinstance(row, (list, tuple)):
            return str(row[0] or "")
        return str(row or "")

    def _ensure_emails_table_allows_duplicates(self, conn: sqlite3.Connection, domain: str) -> None:
        create_sql = self._fetch_table_definition(conn, "emails")
        if not create_sql:
            return
        compact = "".join(create_sql.lower().split())
        if "emailtextnotnullunique" not in compact and "unique(email)" not in compact:
            return
        print(f"[DomainManager] {domain} emails 테이블에서 UNIQUE 제약을 제거합니다.")
        self._rebuild_emails_table(conn)
        self._apply_schema(conn)

    @staticmethod
    def _rebuild_emails_table(conn: sqlite3.Connection) -> None:
        columns = (
            "id, email, source_file, version, status, priority, reserved_by, reserved_at, "
            "next_retry_at, attempts, last_error, meta, created_at, updated_at"
        )
        create_sql = (
            """
            CREATE TABLE emails (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL,
                source_file TEXT,
                version INTEGER,
                status TEXT CHECK(status IN ('pending','reserved','sent','block','failed','removed')) NOT NULL DEFAULT 'pending',
                priority INTEGER DEFAULT 100,
                reserved_by TEXT,
                reserved_at TEXT,
                next_retry_at TEXT,
                attempts INTEGER DEFAULT 0,
                last_error TEXT,
                meta TEXT DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        try:
            conn.execute("PRAGMA foreign_keys=OFF")
            conn.execute("BEGIN")
            conn.execute("ALTER TABLE emails RENAME TO emails_backup")
            conn.execute(create_sql)
            conn.execute(
                f"INSERT INTO emails ({columns}) SELECT {columns} FROM emails_backup"
            )
            conn.execute("DROP TABLE emails_backup")
            conn.commit()
        except sqlite3.DatabaseError:
            conn.rollback()
            raise

    @staticmethod
    def _connect(path: Path) -> sqlite3.Connection:
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA temp_store=MEMORY")
        return conn

    # ------------------------------------------------------------------ #
    # 파일 관리
    # ------------------------------------------------------------------ #
    def list_files(self, domain: str) -> List[DomainFileInfo]:
        result: List[DomainFileInfo] = []
        for file_path in sorted(self.files_dir(domain).glob("*.txt")):
            line_count = 0
            try:
                with file_path.open("r", encoding="utf-8") as fp:
                    for line in fp:
                        if line.strip():
                            line_count += 1
            except UnicodeDecodeError:
                with file_path.open("r", encoding="euc-kr", errors="ignore") as fp:
                    for line in fp:
                        if line.strip():
                            line_count += 1
            stat = file_path.stat()
            result.append(
                DomainFileInfo(
                    name=file_path.name,
                    size=stat.st_size,
                    line_count=line_count,
                    modified_at=datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
                )
            )
        return result

    def save_file(self, domain: str, filename: str, content: str, *, encoding: str = "utf-8") -> None:
        filename = filename.strip()
        if not filename:
            raise ValueError("파일 이름이 필요합니다.")
        if "/" in filename or "\\" in filename:
            raise ValueError("파일 이름에 경로 구분자를 사용할 수 없습니다.")
        if not filename.endswith(".txt"):
            filename = f"{filename}.txt"
        path = self.files_dir(domain) / filename
        path.write_text(content, encoding=encoding)

    def delete_file(self, domain: str, filename: str) -> None:
        path = self.files_dir(domain) / filename
        if path.exists():
            path.unlink()

    # ------------------------------------------------------------------ #
    # 도메인 구성 관리
    # ------------------------------------------------------------------ #
    def get_config(self, domain: str) -> Dict[str, str]:
        db_path = self.db_path(domain)
        with self._lock(domain):
            with self._connect(db_path) as conn:
                rows = conn.execute("SELECT key, value FROM domain_config").fetchall()
        return {row["key"]: row["value"] for row in rows}

    def set_config(self, domain: str, updates: Dict[str, str]) -> None:
        if not updates:
            return
        db_path = self.db_path(domain)
        payload = [(key, str(value)) for key, value in updates.items()]
        with self._lock(domain):
            with self._connect(db_path) as conn:
                conn.executemany(
                    """
                    INSERT INTO domain_config (key, value)
                    VALUES (?, ?)
                    ON CONFLICT(key) DO UPDATE SET value = excluded.value
                    """,
                    payload,
                )
                conn.commit()

    # ------------------------------------------------------------------ #
    # 인젝션
    # ------------------------------------------------------------------ #
    def inject(self, domain: str, *, notes: str = "") -> DomainState:
        files = self.list_files(domain)
        if not files:
            raise ValueError("업로드된 주소 파일이 없습니다.")
        entries: List[Tuple[str, str]] = []
        per_file_counts: Dict[str, int] = defaultdict(int)
        for info in files:
            path = self.files_dir(domain) / info.name
            lines = self._read_lines(path)
            for email in lines:
                per_file_counts[info.name] += 1
                entries.append((email, info.name))

        now = datetime.now(timezone.utc).isoformat()
        db_path = self.db_path(domain)
        with self._lock(domain):
            with self._connect(db_path) as conn:
                self._apply_schema(conn)
                self._ensure_config_defaults(conn)
                self._ensure_emails_table_allows_duplicates(conn, domain)
                current_version = conn.execute("SELECT COALESCE(MAX(version), 0) FROM injection_meta").fetchone()[0] or 0
                new_version = current_version + 1
                existing_rows = conn.execute(
                    "SELECT id, email, status, source_file FROM emails ORDER BY id ASC"
                ).fetchall()

                inserts: List[Tuple] = []
                refresh_rows: List[Tuple] = []
                replacement_rows: List[Tuple] = []
                revive_ids: List[int] = []
                remove_ids: List[int] = []

                existing_count = len(existing_rows)
                new_count = len(entries)
                min_count = min(existing_count, new_count)

                for idx in range(min_count):
                    row = existing_rows[idx]
                    email, source_file = entries[idx]
                    row_email = row["email"]
                    row_source = row["source_file"]
                    row_status = row["status"]

                    if row_email == email and row_source == source_file:
                        refresh_rows.append((source_file, new_version, now, row["id"]))
                        if row_status == "removed":
                            revive_ids.append(row["id"])
                        continue

                    replacement_rows.append(
                        (
                            email,
                            source_file,
                            new_version,
                            DEFAULT_PRIORITIES["pending"],
                            now,
                            row["id"],
                        )
                    )

                if new_count > existing_count:
                    for email, source_file in entries[min_count:]:
                        inserts.append(
                            (
                                email,
                                source_file,
                                new_version,
                                "pending",
                                DEFAULT_PRIORITIES["pending"],
                                None,
                                None,
                                None,
                                0,
                                None,
                                "{}",
                                now,
                                now,
                            )
                        )

                if existing_count > new_count:
                    for row in existing_rows[min_count:]:
                        remove_ids.append(row["id"])

                if inserts:
                    conn.executemany(
                        """
                        INSERT INTO emails (
                            email,
                            source_file,
                            version,
                            status,
                            priority,
                            reserved_by,
                            reserved_at,
                            next_retry_at,
                            attempts,
                            last_error,
                            meta,
                            created_at,
                            updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        inserts,
                    )

                if refresh_rows:
                    conn.executemany(
                        """
                        UPDATE emails
                        SET source_file = ?,
                            version = ?,
                            updated_at = ?
                        WHERE id = ?
                        """,
                        refresh_rows,
                    )

                if replacement_rows:
                    conn.executemany(
                        """
                        UPDATE emails
                        SET email = ?,
                            source_file = ?,
                            version = ?,
                            status = 'pending',
                            priority = ?,
                            reserved_by = NULL,
                            reserved_at = NULL,
                            next_retry_at = NULL,
                            attempts = 0,
                            last_error = NULL,
                            meta = '{}',
                            updated_at = ?
                        WHERE id = ?
                        """,
                        replacement_rows,
                    )

                if revive_ids:
                    conn.executemany(
                        """
                        UPDATE emails
                        SET status = 'pending',
                            priority = ?,
                            reserved_by = NULL,
                            reserved_at = NULL,
                            next_retry_at = NULL,
                            attempts = 0,
                            last_error = NULL,
                            updated_at = ?
                        WHERE id = ?
                        """,
                        [
                            (
                                DEFAULT_PRIORITIES["pending"],
                                now,
                                row_id,
                            )
                            for row_id in revive_ids
                        ],
                    )

                if remove_ids:
                    conn.executemany(
                        """
                        UPDATE emails
                        SET status = 'removed',
                            priority = ?,
                            reserved_by = NULL,
                            reserved_at = NULL,
                            next_retry_at = NULL,
                            updated_at = ?
                        WHERE id = ?
                        """,
                        [
                            (
                                DEFAULT_PRIORITIES["removed"],
                                now,
                                row_id,
                            )
                            for row_id in remove_ids
                        ],
                    )

                files_payload = [
                    {"name": info.name, "lines": per_file_counts.get(info.name, 0)}
                    for info in files
                ]
                conn.execute(
                    """
                    INSERT INTO injection_meta (version, created_at, total_count, files, notes)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        new_version,
                        now,
                        sum(per_file_counts.values()),
                        json.dumps(files_payload, ensure_ascii=False),
                        notes,
                    ),
                )
                conn.commit()

        return self.get_state(domain)

    # ------------------------------------------------------------------ #
    # 상태 조회 & 내보내기
    # ------------------------------------------------------------------ #
    def get_state(self, domain: str) -> DomainState:
        domain = domain.lower()
        files = self.list_files(domain)
        db_path = self.db_path(domain)
        with self._lock(domain):
            with self._connect(db_path) as conn:
                latest_meta = conn.execute(
                    """
                    SELECT version, created_at, total_count, files, notes
                    FROM injection_meta
                    ORDER BY version DESC
                    LIMIT 1
                    """
                ).fetchone()
                counts = conn.execute(
                    """
                    SELECT status, COUNT(*)
                    FROM emails
                    GROUP BY status
                    """
                ).fetchall()
        status_counts = {row["status"]: row[1] for row in counts} if counts else {}
        if latest_meta:
            version = latest_meta["version"]
            injected_at = latest_meta["created_at"]
            total_count = latest_meta["total_count"]
            notes = latest_meta["notes"]
        else:
            version = 0
            injected_at = None
            total_count = 0
            notes = None
        return DomainState(
            domain=domain,
            db_version=version,
            last_injected_at=injected_at,
            total_count=total_count,
            status_counts=status_counts,
            files=files,
            notes=notes,
        )

    def list_states(self) -> List[DomainState]:
        domains = [path.name for path in self.root.iterdir() if path.is_dir()]
        return [self.get_state(domain) for domain in sorted(domains)]

    def export_db_zip(self, domain: str) -> BytesIO:
        db_path = self.db_path(domain)
        if not db_path.exists():
            raise FileNotFoundError(f"{domain} DB가 존재하지 않습니다.")
        buffer = BytesIO()
        with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
            zf.write(db_path, arcname=db_path.name)
        buffer.seek(0)
        return buffer

    # ------------------------------------------------------------------ #
    # 유틸
    # ------------------------------------------------------------------ #
    @staticmethod
    def _read_lines(path: Path) -> Iterable[str]:
        def _iter(fp):
            for raw in fp:
                email = raw.strip()
                if not email or email.startswith("#"):
                    continue
                yield email

        try:
            with path.open("r", encoding="utf-8") as fp:
                yield from _iter(fp)
        except UnicodeDecodeError:
            with path.open("r", encoding="euc-kr", errors="ignore") as fp:
                yield from _iter(fp)
