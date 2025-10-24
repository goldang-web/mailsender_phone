import unittest
from datetime import datetime, timezone
from email.utils import format_datetime
from unittest.mock import patch

from client.lib import naver_imap


class _FakeIMAP:
    header_bytes: bytes = b""

    def __init__(self, host, port, timeout=30):  # pylint: disable=unused-argument
        self._selected = False

    def login(self, email_id, password):  # pylint: disable=unused-argument
        return "OK", []

    def select(self, mailbox, readonly=True):  # pylint: disable=unused-argument
        self._selected = True
        return "OK", [b"Junk"]

    def search(self, charset, criterion):  # pylint: disable=unused-argument
        if not self._selected:
            return "NO", [b""]
        return "OK", [b"1"]

    def fetch(self, num, query):  # pylint: disable=unused-argument
        return "OK", [(b"1", self.header_bytes)]

    def logout(self):
        return "BYE", []


class VerifyDeliveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.patch_imap = patch("client.lib.naver_imap.imaplib.IMAP4_SSL", _FakeIMAP)
        self.patch_imap.start()

    def tearDown(self) -> None:
        self.patch_imap.stop()

    def test_verify_delivery_rejects_sender_mismatch(self) -> None:
        now = datetime.now(timezone.utc)
        mismatch_header = (
            f"From: other@example.com\r\nDate: {format_datetime(now)}\r\n\r\n"
        ).encode()
        _FakeIMAP.header_bytes = mismatch_header

        result = naver_imap.verify_delivery(
            email_id="user@naver.com",
            password="pass",
            mail_from="expected@example.com",
            sent_at=now,
            allowed_delay=30,
            max_messages=3,
        )

        self.assertEqual(result["status"], "failure")
        self.assertEqual(result["reason"], "발신자 주소가 일치하는 메일을 찾지 못했습니다.")

    def test_verify_delivery_accepts_matching_sender(self) -> None:
        now = datetime.now(timezone.utc)
        matching_header = (
            f"From: Expected Name <expected@example.com>\r\n"
            f"Date: {format_datetime(now)}\r\n\r\n"
        ).encode()
        _FakeIMAP.header_bytes = matching_header

        result = naver_imap.verify_delivery(
            email_id="user@naver.com",
            password="pass",
            mail_from="expected@example.com",
            sent_at=now,
            allowed_delay=30,
            max_messages=3,
        )

        self.assertEqual(result["status"], "success")
        self.assertIsNotNone(result["latency"])
        self.assertIsNone(result["reason"])

    def test_verify_delivery_accepts_sender_with_garbled_display(self) -> None:
        now = datetime.now(timezone.utc)
        garbled_header = (
            f"From: ????? <expected@example.com>\r\n"
            f"Date: {format_datetime(now)}\r\n\r\n"
        ).encode()
        _FakeIMAP.header_bytes = garbled_header

        result = naver_imap.verify_delivery(
            email_id="user@naver.com",
            password="pass",
            mail_from="expected@example.com",
            sent_at=now,
            allowed_delay=30,
            max_messages=3,
        )

        self.assertEqual(result["status"], "success")
        self.assertIsNone(result["reason"])

    def test_verify_delivery_accepts_sender_with_split_angle_brackets(self) -> None:
        now = datetime.now(timezone.utc)
        split_header = (
            f"From: <성유리>,<expected@example.com>\r\n"
            f"Date: {format_datetime(now)}\r\n\r\n"
        ).encode()
        _FakeIMAP.header_bytes = split_header

        result = naver_imap.verify_delivery(
            email_id="user@naver.com",
            password="pass",
            mail_from="expected@example.com",
            sent_at=now,
            allowed_delay=30,
            max_messages=3,
        )

        self.assertEqual(result["status"], "success")
        self.assertIsNone(result["reason"])


if __name__ == "__main__":
    unittest.main()
