import importlib.util
import re
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CLIENT_DIR = PROJECT_ROOT / "client"

if str(CLIENT_DIR) not in sys.path:
    sys.path.insert(0, str(CLIENT_DIR))

spec = importlib.util.spec_from_file_location("client_main", CLIENT_DIR / "main.py")
client_main = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(client_main)


def _sent_log_pattern(label: str) -> re.Pattern[str]:
    return re.compile(rf"^{label}\(\d+/\d+\) \| \d{{2}}:\d{{2}}:\d{{2}} \| TestDevice(?: \| 알박기 포함)?$")


class DispatchLoggingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.save_config_patch = patch.object(client_main, "save_config", lambda cfg: None)
        self.save_config_patch.start()
        self.imap_patch = patch.object(client_main.MailClient, "_imap_enabled", lambda self, domain: False)
        self.imap_patch.start()
        config = {
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
        }
        self.client = client_main.MailClient(config)

    def tearDown(self) -> None:
        self.client._shutdown_imap_executor()
        self.imap_patch.stop()
        self.save_config_patch.stop()

    def test_single_send_success_logs(self) -> None:
        def fake_send_success(*, smtp_host, smtp_port, helo, mail_from, rcpt_to, header_text, bcc_emails=None, anchor_emails=None, debug=None):
            rcpt_details = [
                {
                    "address": rcpt_to,
                    "code": "250",
                    "message": "250 2.1.5 OK",
                    "is_primary": True,
                    "is_bcc": False,
                    "is_anchor": False,
                    "success": True,
                }
            ]
            data_response = {"code": "250", "message": "250 2.0.0 OK"}
            return True, "250 2.0.0 OK", datetime.now(timezone.utc), rcpt_details, data_response

        with patch.object(client_main, "send_via_telnet", fake_send_success):
            payload = {
                "config": {"smtp_host": "", "smtp_port": 25, "helo": "", "mail_from": "sender@test"},
                "rcpt_to": "user@example.com",
            }
            result = self.client.handle_single_send("naver", payload, "job-1")

        self.assertEqual(result.status, "success")
        self.assertEqual(self.client.sent_sequences["naver"], 1)

        log_entry = result.result["logs"][0]["log"]
        self.assertRegex(log_entry, _sent_log_pattern("Sent"))
        self.assertEqual(result.result["logs"][0]["tags"], [])

    def test_single_send_with_nouser_excludes_from_counts(self) -> None:
        def fake_send_partial(*, smtp_host, smtp_port, helo, mail_from, rcpt_to, header_text, bcc_emails=None, anchor_emails=None, debug=None):
            primary_detail = {
                "address": rcpt_to,
                "code": "250",
                "message": "250 2.1.5 OK",
                "is_primary": True,
                "is_bcc": False,
                "is_anchor": False,
                "success": True,
            }
            bcc_address = (bcc_emails or ["bcc@test.com"])[0]
            nouser_detail = {
                "address": bcc_address,
                "code": "550",
                "message": "550 5.1.1 No such user",
                "is_primary": False,
                "is_bcc": True,
                "is_anchor": False,
                "success": False,
            }
            data_response = {"code": "250", "message": "250 2.0.0 OK"}
            return False, "550 5.1.1", datetime.now(timezone.utc), [primary_detail, nouser_detail], data_response

        with patch.object(client_main, "send_via_telnet", fake_send_partial):
            payload = {
                "config": {"smtp_host": "", "smtp_port": 25, "helo": "", "mail_from": "sender@test"},
                "rcpt_to": "user@example.com",
                "bcc": ["bcc@test.com"],
            }
            result = self.client.handle_single_send("naver", payload, "job-2")

        self.assertEqual(result.status, "success")
        self.assertEqual(self.client.sent_sequences["naver"], 1)

        log_entry = result.result["logs"][0]
        self.assertRegex(log_entry["log"], _sent_log_pattern("Sent"))
        self.assertEqual(log_entry["nouser_total"], 1)
        self.assertEqual(log_entry["tags"], ["nouser"])

    def test_single_send_data_failure_marks_fail(self) -> None:
        def fake_send_fail(*, smtp_host, smtp_port, helo, mail_from, rcpt_to, header_text, bcc_emails=None, anchor_emails=None, debug=None):
            rcpt_details = [
                {
                    "address": rcpt_to,
                    "code": "250",
                    "message": "250 2.1.5 OK",
                    "is_primary": True,
                    "is_bcc": False,
                    "is_anchor": False,
                    "success": True,
                }
            ]
            data_response = {"code": "550", "message": "550 5.1.0 Failure"}
            return False, "550 5.1.0 Failure", datetime.now(timezone.utc), rcpt_details, data_response

        with patch.object(client_main, "send_via_telnet", fake_send_fail):
            payload = {
                "config": {"smtp_host": "", "smtp_port": 25, "helo": "", "mail_from": "sender@test"},
                "rcpt_to": "user@example.com",
            }
            result = self.client.handle_single_send("naver", payload, "job-3")

        self.assertEqual(result.status, "failed")
        self.assertEqual(self.client.sent_sequences["naver"], 0)
        self.assertRegex(result.result["logs"][0]["log"], _sent_log_pattern("Fail"))

    def test_log_line_appends_anchor_tag(self) -> None:
        line = self.client._format_dispatch_log_line("Sent", batch_index=3, accumulated_total=15, include_anchor=True)
        self.assertIn("알박기 포함", line)


if __name__ == "__main__":
    unittest.main()
