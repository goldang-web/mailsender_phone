import sys
import unittest
from datetime import datetime, timezone
from copy import deepcopy
from unittest.mock import patch
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
CLIENT_DIR = ROOT_DIR / "client"
if str(CLIENT_DIR) not in sys.path:
    sys.path.insert(0, str(CLIENT_DIR))

from client import main as client_main


class ImapProbeMailHeaderTests(unittest.TestCase):
    def setUp(self) -> None:
        config = deepcopy(client_main.DEFAULT_CONFIG)
        self.client = client_main.MailClient(config)
        self.client._log_imap_console = lambda *args, **kwargs: None
        self.client._log_imap_protection = lambda *args, **kwargs: None
        self.client._record_imap_throttle = lambda *args, **kwargs: None

    @patch("client.main.send_via_telnet")
    def test_probe_mail_uses_custom_header(self, mock_send) -> None:
        now = datetime.now(timezone.utc)
        mock_send.return_value = (
            True,
            "250 2.0.0 OK",
            now,
            [],
            {"code": "250", "message": "250 2.0.0 OK"},
        )
        custom_header = (
            "From: Display Name <sender@example.com>\n"
            "To: Recipient Name <recipient@example.com>\n"
            "Subject: 커스텀 제목\n"
            "Message-ID: <original@example.com>\n"
            "X-Test: Header\n"
            "\n"
            "본문 본문\n"
        )
        smtp_context = {
            "smtp_host": "smtp.example.com",
            "smtp_port": 25,
            "helo": "hello.example.com",
            "header": custom_header,
        }

        result = self.client._run_sent_probe_mail(
            domain="naver",
            mail_from="sender@example.com",
            smtp_context=smtp_context,
            rcpt_to="imap-user@naver.com",
        )

        self.assertTrue(result.success)
        mock_send.assert_called_once()
        header_text = mock_send.call_args.kwargs["header_text"]
        normalized_header = header_text.replace("\r\n", "\n")
        expected_header = custom_header.replace("<original@example.com>", result.message_id).replace("\r\n", "\n")
        self.assertEqual(normalized_header, expected_header)
        self.assertNotIn("Sent 누적 확인용 테스트 메일입니다", normalized_header)


if __name__ == "__main__":
    unittest.main()
