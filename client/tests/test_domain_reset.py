import importlib.util
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict
from unittest import TestCase
from unittest.mock import patch


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CLIENT_DIR = PROJECT_ROOT / "client"

if str(CLIENT_DIR) not in sys.path:
    sys.path.insert(0, str(CLIENT_DIR))

spec = importlib.util.spec_from_file_location("client_main", CLIENT_DIR / "main.py")
client_main = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(client_main)


class DomainResetTests(TestCase):
    def test_reset_domain_status_sets_all_rows_pending(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            data_dir = base_path / "data"
            log_dir = base_path / "logs"
            config_path = base_path / "settings.json"
            naver_dir = data_dir / "naver"
            naver_dir.mkdir(parents=True, exist_ok=True)
            log_dir.mkdir(parents=True, exist_ok=True)

            db_path = naver_dir / "naver.db"
            with sqlite3.connect(db_path) as conn:
                conn.execute(
                    """
                    CREATE TABLE emails (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        email TEXT NOT NULL,
                        source_file TEXT,
                        version INTEGER,
                        status TEXT CHECK(status IN ('pending','reserved','sent','block','failed','nouser','removed')) NOT NULL DEFAULT 'pending',
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
                now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

                def insert_row(email: str, status: str, reserved: Dict[str, str]) -> None:
                    conn.execute(
                        """
                        INSERT INTO emails (
                            email, status, priority, reserved_by, reserved_at, next_retry_at,
                            attempts, last_error, meta, created_at, updated_at
                        )
                        VALUES (?, ?, 100, ?, ?, ?, 0, NULL, '{}', ?, ?)
                        """,
                        (
                            email,
                            status,
                            reserved.get("reserved_by"),
                            reserved.get("reserved_at"),
                            reserved.get("next_retry_at"),
                            now_iso,
                            now_iso,
                        ),
                    )

                insert_row("pending@example.com", "pending", {})
                insert_row(
                    "sent@example.com",
                    "sent",
                    {
                        "reserved_by": "session-a",
                        "reserved_at": now_iso,
                        "next_retry_at": now_iso,
                    },
                )
                insert_row(
                    "block@example.com",
                    "block",
                    {
                        "reserved_by": "session-b",
                        "reserved_at": now_iso,
                        "next_retry_at": now_iso,
                    },
                )
                insert_row(
                    "reserved@example.com",
                    "reserved",
                    {
                        "reserved_by": "session-c",
                        "reserved_at": now_iso,
                        "next_retry_at": now_iso,
                    },
                )
                conn.commit()

            with patch.object(client_main, "DATA_DIR", data_dir), \
                patch.object(client_main, "LOG_DIR", log_dir), \
                patch.object(client_main, "CONFIG_PATH", config_path), \
                patch.object(client_main, "save_config", lambda cfg: None), \
                patch.object(client_main.MailClient, "send_job_report", lambda self, *args, **kwargs: None):

                client = client_main.MailClient(
                    {
                        "server_url": "",
                        "device_name": "TestDevice",
                        "device_id": "dev-1",
                        "interval": 1,
                        "timeout": 1,
                        "imap_settings": {},
                        "local_versions": {},
                        "domain_cycles": {},
                        "stop_schedule": {},
                        "sent_sequences": {},
                        "active_domain": "naver",
                    }
                )

                result = client.handle_reset_domain_status("naver", {}, "job-reset")
                client._shutdown_imap_executor()
                client._shutdown_offline_probe_executor()

            self.assertEqual(result.status, "success")
            self.assertIsNotNone(result.result)
            assert result.result is not None
            self.assertEqual(result.result.get("updated_rows"), 3)
            with sqlite3.connect(db_path) as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute("SELECT status, reserved_by, reserved_at, next_retry_at FROM emails").fetchall()
                self.assertEqual(len(rows), 4)
                for row in rows:
                    self.assertEqual(row["status"], "pending")
                    self.assertIsNone(row["reserved_by"])
                    self.assertIsNone(row["reserved_at"])
                    self.assertIsNone(row["next_retry_at"])
