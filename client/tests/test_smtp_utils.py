import importlib.util
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CLIENT_DIR = PROJECT_ROOT / "client"

if str(CLIENT_DIR) not in sys.path:
    sys.path.insert(0, str(CLIENT_DIR))

import smtp_utils  # pylint: disable=wrong-import-position

spec = importlib.util.spec_from_file_location("client_main", CLIENT_DIR / "main.py")
client_main = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(client_main)


class SmtpUtilsTests(unittest.TestCase):
    def test_quit_response_used_when_data_end_missing(self) -> None:
        response_entries = [
            ("MAIL FROM", "250 2.1.0 OK"),
            ("RCPT:primary@test.com", "250 2.1.5 OK"),
            ("DATA", "354 End data with <CR><LF>.<CR><LF>"),
            ("DATA END", ""),
            ("QUIT", "250 2.0.0 Message accepted for delivery"),
        ]
        response_text = "\n".join(f"{label}: {text}" for label, text in response_entries)
        with patch.object(
            smtp_utils.telnet_mailer,
            "send_mail_telnet",
            return_value=(response_text, response_entries),
        ):
            success, _text, _finished_at, rcpt_details, data_response = smtp_utils.send_via_telnet(
                smtp_host="",
                smtp_port=25,
                helo="test-helo",
                mail_from="sender@test.com",
                rcpt_to="primary@test.com",
                header_text="From: sender@test.com\r\n\r\nbody",
            )
        self.assertTrue(success)
        self.assertEqual(len(rcpt_details), 1)
        self.assertTrue(rcpt_details[0]["success"])
        self.assertEqual(data_response.get("code"), "250")
        self.assertEqual(data_response.get("message"), "250 2.0.0 Message accepted for delivery")
        self.assertEqual(data_response.get("source"), "QUIT")

    def test_format_uses_source_label(self) -> None:
        detail = {
            "code": "250",
            "message": "250 2.0.0 Message accepted for delivery",
            "source": "QUIT",
        }
        formatted = client_main.MailClient._format_data_end_detail(detail)
        self.assertEqual(formatted, "QUIT: 250 2.0.0 Message accepted for delivery")


if __name__ == "__main__":
    unittest.main()
