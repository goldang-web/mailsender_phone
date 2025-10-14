# -*- coding: utf-8 -*-
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

CLIENT_ROOT = Path(__file__).resolve().parents[1]
if str(CLIENT_ROOT) not in sys.path:
    sys.path.insert(0, str(CLIENT_ROOT))

from client.smtp_utils import send_via_telnet


class SendViaTelnetTests(unittest.TestCase):
    @patch("client.smtp_utils.telnet_mailer.send_mail_telnet")
    def test_success_with_anchor_and_bcc(self, mock_send_mail):
        entries = [
            ("CONNECT", "220 Ready"),
            ("HELO", "250 Hello"),
            ("MAIL FROM", "250 OK"),
            ("RCPT:primary@example.com", "250 2.1.5 OK"),
            ("RCPT:bcc@example.com", "250 2.1.5 OK"),
            ("RCPT:anchor@example.com", "250 2.1.5 OK"),
            ("DATA", "354 Start"),
            ("DATA END", "250 2.0.0 Accepted"),
            ("QUIT", "221 Bye"),
        ]
        mock_send_mail.return_value = ("\n".join(f"{label}: {text}" for label, text in entries), entries)

        success, response_text, completed_at, rcpt_details = send_via_telnet(
            smtp_host="",
            smtp_port=25,
            helo="test-client",
            mail_from="sender@example.com",
            rcpt_to="primary@example.com",
            header_text="From: sender@example.com\n\nbody",
            bcc_emails=["bcc@example.com", "anchor@example.com"],
            anchor_emails=["anchor@example.com"],
        )

        self.assertTrue(success)
        self.assertIn("DATA END: 250 2.0.0 Accepted", response_text)
        self.assertIsInstance(completed_at, datetime)
        self.assertEqual(completed_at.tzinfo, timezone.utc)
        self.assertEqual(len(rcpt_details), 3)
        anchor_entries = [item for item in rcpt_details if item.get("is_anchor")]
        self.assertEqual(len(anchor_entries), 1)
        self.assertTrue(anchor_entries[0]["success"])
        self.assertEqual(anchor_entries[0]["code"], "250")

    @patch("client.smtp_utils.telnet_mailer.send_mail_telnet")
    def test_failure_when_bcc_rejected(self, mock_send_mail):
        entries = [
            ("CONNECT", "220 Ready"),
            ("HELO", "250 Hello"),
            ("MAIL FROM", "250 OK"),
            ("RCPT:primary@example.com", "250 2.1.5 OK"),
            ("RCPT:bcc@example.com", "550 5.7.1 Blocked"),
            ("DATA", "354 Start"),
            ("DATA END", "554 Transaction failed"),
            ("QUIT", "221 Bye"),
        ]
        mock_send_mail.return_value = ("\n".join(f"{label}: {text}" for label, text in entries), entries)

        success, _response_text, _completed_at, rcpt_details = send_via_telnet(
            smtp_host="",
            smtp_port=25,
            helo="test-client",
            mail_from="sender@example.com",
            rcpt_to="primary@example.com",
            header_text="From: sender@example.com\n\nbody",
            bcc_emails=["bcc@example.com"],
            anchor_emails=[],
        )

        self.assertFalse(success)
        rejected = [item for item in rcpt_details if not item.get("success")]
        self.assertEqual(len(rejected), 1)
        self.assertEqual(rejected[0]["address"], "bcc@example.com")
        self.assertEqual(rejected[0]["code"], "550")


if __name__ == "__main__":
    unittest.main()
