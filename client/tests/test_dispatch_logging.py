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
    return re.compile(
        rf"^{label}\(\d+/\d+\) \| \d{{2}}:\d{{2}}:\d{{2}} \| TestDevice"
        r"(?: \| 알박기 포함)?"
        r"(?: \| .+)?$"
    )

def _extract_counts(entry: str) -> tuple[int, int]:
    match = re.match(r"^(Sent|Fail)\((\d+)/(\d+)\)", entry)
    if not match:
        raise AssertionError(f"로그 포맷이 예상과 다릅니다: {entry}")
    return int(match.group(2)), int(match.group(3))


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
        self.client._shutdown_offline_probe_executor()
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
        current_batch, cumulative = _extract_counts(log_entry)
        self.assertEqual(current_batch, 1)
        self.assertEqual(cumulative, 1)
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
        current_batch, cumulative = _extract_counts(log_entry["log"])
        self.assertEqual(current_batch, 1)
        self.assertEqual(cumulative, 1)
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
        fail_entry = result.result["logs"][0]["log"]
        self.assertRegex(fail_entry, _sent_log_pattern("Fail"))
        current_batch, cumulative = _extract_counts(fail_entry)
        self.assertEqual(current_batch, 0)
        self.assertEqual(cumulative, 0)

    def test_log_line_appends_anchor_tag(self) -> None:
        line = self.client._format_dispatch_log_line("Sent", current_batch_success=3, accumulated_total=15, include_anchor=True)
        self.assertIn("알박기 포함", line)

    def test_log_line_includes_recipient_summary(self) -> None:
        multiple = self.client._format_dispatch_log_line(
            "Sent",
            current_batch_success=2,
            accumulated_total=10,
            recipient="primary@example.com",
            extra_recipient_count=24,
        )
        self.assertIn("primary@example.com 외 24개", multiple)
        single = self.client._format_dispatch_log_line(
            "Sent",
            current_batch_success=1,
            accumulated_total=5,
            recipient="solo@example.com",
        )
        self.assertTrue(single.endswith(" | solo@example.com"))

    def test_single_send_passes_effective_mail_from_to_imap(self) -> None:
        captured_mail_from: list[str] = []

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

        called_submit = False

        def fake_submit(self, *, domain, job_id, send_type, mail_from, **kwargs):  # type: ignore[override]
            nonlocal called_submit
            called_submit = True
            return None

        payload = {
            "config": {"smtp_host": "", "smtp_port": 25, "helo": "", "mail_from": "resolved@example.com"},
            "rcpt_to": "user@example.com",
        }

        with patch.object(client_main, "send_via_telnet", fake_send_success), \
             patch.object(client_main.MailClient, "_imap_enabled", lambda self, domain: True), \
             patch.object(client_main.MailClient, "_submit_imap_check", fake_submit):
            result = self.client.handle_single_send("naver", payload, "job-mailfrom")

        self.assertEqual(result.status, "success")
        self.assertFalse(called_submit)
        settings = self.client._imap_settings_for_domain("naver")
        self.assertEqual(settings.get("last_mail_from"), "resolved@example.com")

    def test_effective_mail_from_falls_back_to_last_value(self) -> None:
        settings = self.client._imap_settings_for_domain("naver")
        settings["last_mail_from"] = "stored@example.com"
        value = self.client._effective_mail_from("naver", {}, fallback=None)
        self.assertEqual(value, "stored@example.com")


if __name__ == "__main__":
    unittest.main()
