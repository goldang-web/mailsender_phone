import importlib.util
import sqlite3
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
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


class AnchorDispatchTests(TestCase):
    def test_anchor_separate_send_when_bcc_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            base_path = Path(tmpdir)
            data_dir = base_path / "data"
            log_dir = base_path / "logs"
            config_path = base_path / "settings.json"
            naver_dir = data_dir / "naver"
            daum_dir = data_dir / "daum"
            naver_dir.mkdir(parents=True, exist_ok=True)
            daum_dir.mkdir(parents=True, exist_ok=True)
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
                conn.execute(
                    """
                    INSERT INTO emails (email, status, priority, meta, created_at, updated_at)
                    VALUES (?, 'pending', 100, '{}', ?, ?)
                    """,
                    ("primary@example.com", now_iso, now_iso),
                )
                conn.commit()

            call_log: List[Dict[str, Any]] = []

            def fake_send_via_telnet(
                *,
                smtp_host: str,
                smtp_port: int,
                helo: str,
                mail_from: str,
                rcpt_to: str,
                header_text: str,
                bcc_emails: Optional[List[str]] = None,
                anchor_emails: Optional[List[str]] = None,
                debug: object = None,
            ):
                bcc_list = [email for email in (bcc_emails or []) if email]
                anchor_list = [email for email in (anchor_emails or []) if email]
                call_log.append(
                    {
                        "rcpt_to": rcpt_to,
                        "bcc": list(bcc_list),
                        "anchors": list(anchor_list),
                    }
                )
                anchor_set = {email.lower() for email in anchor_list}
                rcpt_details = [
                    {
                        "address": rcpt_to,
                        "code": "250",
                        "message": "250 2.1.5 OK",
                        "is_primary": True,
                        "is_bcc": False,
                        "is_anchor": rcpt_to.lower() in anchor_set,
                        "success": True,
                    }
                ]
                for email in bcc_list:
                    rcpt_details.append(
                        {
                            "address": email,
                            "code": "250",
                            "message": "250 2.1.5 OK",
                            "is_primary": False,
                            "is_bcc": True,
                            "is_anchor": email.lower() in anchor_set,
                            "success": True,
                        }
                    )
                data_response = {"code": "250", "message": "250 2.0.0 OK"}
                return True, "250 2.0.0 OK", datetime.now(timezone.utc), rcpt_details, data_response

            def raise_stop_iteration(self, domain: str, total: int) -> None:
                raise StopIteration

            with patch.object(client_main, "DATA_DIR", data_dir), \
                patch.object(client_main, "LOG_DIR", log_dir), \
                patch.object(client_main, "CONFIG_PATH", config_path), \
                patch.object(client_main, "save_config", lambda cfg: None), \
                patch.object(client_main.MailClient, "_imap_enabled", lambda self, domain: False), \
                patch.object(client_main.MailClient, "send_job_report", lambda self, *args, **kwargs: None), \
                patch.object(client_main.MailClient, "_record_cycle_completion", raise_stop_iteration), \
                patch.object(client_main, "send_via_telnet", fake_send_via_telnet):

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

                payload = {
                    "config": {
                        "smtp_host": "",
                        "smtp_port": 25,
                        "helo": "localhost",
                        "mail_from": "sender@test.com",
                        "session_count": 1,
                        "bcc_count": 0,
                        "anchor_interval": 1,
                        "anchor_email": "anchor@example.com",
                    }
                }

                with self.assertRaises(StopIteration):
                    client.handle_batch_send("naver", payload, "job-anchor")
                client._shutdown_imap_executor()
                client._shutdown_offline_probe_executor()

            self.assertEqual(len(call_log), 2)
            self.assertEqual(call_log[0]["rcpt_to"], "primary@example.com")
            self.assertEqual(call_log[0]["bcc"], [])
            self.assertEqual(call_log[0]["anchors"], [])
            self.assertEqual(call_log[1]["rcpt_to"], "anchor@example.com")
            self.assertEqual(call_log[1]["bcc"], [])
            self.assertEqual(call_log[1]["anchors"], ["anchor@example.com"])
