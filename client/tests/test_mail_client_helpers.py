# -*- coding: utf-8 -*-
import sys
import types
import unittest
from pathlib import Path

CLIENT_ROOT = Path(__file__).resolve().parents[1]
if str(CLIENT_ROOT) not in sys.path:
    sys.path.insert(0, str(CLIENT_ROOT))

if "smtp_utils" not in sys.modules:
    smtp_stub = types.ModuleType("smtp_utils")

    def _stub_send_via_telnet(*_args, **_kwargs):
        raise RuntimeError("smtp_utils stub")

    smtp_stub.send_via_telnet = _stub_send_via_telnet
    sys.modules["smtp_utils"] = smtp_stub

from client.main import MailClient


class MailClientHelperTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = MailClient.__new__(MailClient)
        self.client.imap_settings = {}
        self.client._imap_sent_counters = {}
        self.client._imap_settings_dirty = set()

    def test_summarize_rcpt_details(self) -> None:
        entries = [
            {
                "address": "primary@example.com",
                "code": "250",
                "message": "250 2.1.5 OK",
                "is_primary": True,
                "is_bcc": False,
                "is_anchor": False,
                "success": True,
            },
            {
                "address": "bcc@example.com",
                "code": "250",
                "message": "250 2.1.5 OK",
                "is_primary": False,
                "is_bcc": True,
                "is_anchor": False,
                "success": True,
            },
            {
                "address": "anchor@example.com",
                "code": "550",
                "message": "550 5.7.1 Blocked",
                "is_primary": False,
                "is_bcc": True,
                "is_anchor": True,
                "success": False,
            },
        ]
        summary = MailClient._summarize_rcpt_details(entries)
        self.assertEqual(summary["primary"], ["primary@example.com→250"])
        self.assertIn("bcc@example.com→250", summary["bcc"][0])
        self.assertTrue(summary["anchor"][0].startswith("anchor@example.com→550"))
        self.assertGreater(len(summary["failed"]), 0)

    def test_rollback_sent_counter(self) -> None:
        self.client._set_sent_counter("naver", 12)
        # floor 9, decrement 3 => new counter 9
        new_value = self.client._rollback_sent_counter("naver", 3, 9)
        self.assertEqual(new_value, 9)
        self.assertEqual(self.client._get_sent_counter("naver"), 9)

    def test_rollback_sent_counter_respects_floor(self) -> None:
        self.client._set_sent_counter("naver", 5)
        # stored counter below floor -> unchanged
        new_value = self.client._rollback_sent_counter("naver", 3, 6)
        self.assertEqual(new_value, 5)
        self.assertEqual(self.client._get_sent_counter("naver"), 5)


if __name__ == "__main__":
    unittest.main()
