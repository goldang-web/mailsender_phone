import importlib.util
import sys
import unittest
from copy import deepcopy
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


class ToPlaceholderTests(unittest.TestCase):
    def setUp(self) -> None:
        config = deepcopy(client_main.DEFAULT_CONFIG)
        self.client = client_main.MailClient(config)

    def tearDown(self) -> None:
        self.client._shutdown_imap_executor()
        self.client._shutdown_offline_probe_executor()

    def test_compose_to_header_value_formats_and_deduplicates(self) -> None:
        rendered = client_main._compose_to_header_value(
            "pxy528@daum.net",
            [
                "nasaman@daum.net",
                "rcy72@daum.net",
                "pxy528@daum.net",
                "이름 <na4542@daum.net>",
            ],
        )
        self.assertEqual(
            rendered,
            "pxy528 <pxy528@daum.net>, nasaman <nasaman@daum.net>, rcy72 <rcy72@daum.net>, 이름 <na4542@daum.net>",
        )

    @patch.object(client_main, "send_via_telnet")
    def test_single_send_expands_placeholder_before_dispatch(self, mock_send) -> None:
        now = datetime.now(timezone.utc)
        mock_send.return_value = (
            True,
            "250 2.0.0 OK",
            now,
            [
                {
                    "address": "pxy528@daum.net",
                    "code": "250",
                    "message": "250 2.1.5 OK",
                    "is_primary": True,
                    "is_bcc": False,
                    "is_anchor": False,
                    "success": True,
                },
                {
                    "address": "nasaman@daum.net",
                    "code": "250",
                    "message": "250 2.1.5 OK",
                    "is_primary": False,
                    "is_bcc": True,
                    "is_anchor": False,
                    "success": True,
                },
            ],
            {"code": "250", "message": "250 2.0.0 OK"},
        )
        header_template = (
            "From: Sender <sender@example.com>\n"
            "To: {$TO}\n"
            "Subject: 테스트\n"
            "\n"
            "본문"
        )
        payload = {
            "config": {
                "smtp_host": "",
                "smtp_port": 25,
                "helo": "",
                "mail_from": "sender@example.com",
                "header": header_template,
                "bcc_count": 1,
            },
            "rcpt_to": "pxy528@daum.net",
            "bcc": ["nasaman@daum.net"],
            "bcc_enabled": True,
        }

        result = self.client.handle_single_send("daum", payload, "job-placeholder")
        self.assertEqual(result.status, "success")

        header_text = mock_send.call_args.kwargs["header_text"]
        normalized = header_text.replace("\r\n", "\n")
        self.assertIn(
            "To: pxy528 <pxy528@daum.net>, nasaman <nasaman@daum.net>",
            normalized,
        )
        self.assertNotIn("{$TO}", normalized)

    @patch.object(client_main, "send_via_telnet")
    def test_single_send_expands_from_placeholder_before_dispatch(self, mock_send) -> None:
        now = datetime.now(timezone.utc)
        mock_send.return_value = (
            True,
            "250 2.0.0 OK",
            now,
            [
                {
                    "address": "pxy528@daum.net",
                    "code": "250",
                    "message": "250 2.1.5 OK",
                    "is_primary": True,
                    "is_bcc": False,
                    "is_anchor": False,
                    "success": True,
                },
            ],
            {"code": "250", "message": "250 2.0.0 OK"},
        )
        header_template = (
            "From: {$FROM}\n"
            "To: {$TO}\n"
            "Subject: 테스트\n"
            "\n"
            "본문"
        )
        payload = {
            "config": {
                "smtp_host": "",
                "smtp_port": 25,
                "helo": "",
                "mail_from": "sender@example.com",
                "header": header_template,
                "bcc_count": 0,
            },
            "rcpt_to": "pxy528@daum.net",
            "bcc": [],
            "bcc_enabled": False,
        }

        result = self.client.handle_single_send("daum", payload, "job-from-placeholder")
        self.assertEqual(result.status, "success")

        header_text = mock_send.call_args.kwargs["header_text"]
        normalized = header_text.replace("\r\n", "\n")
        self.assertIn("From: sender@example.com", normalized)
        self.assertNotIn("{$FROM}", normalized)


if __name__ == "__main__":
    unittest.main()
