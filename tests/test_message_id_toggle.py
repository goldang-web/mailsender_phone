import sys
import unittest
from pathlib import Path
from unittest.mock import patch

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SERVER_DIR = PROJECT_ROOT / "server"
CLIENT_DIR = PROJECT_ROOT / "client"

for path in (str(SERVER_DIR), str(CLIENT_DIR)):
    if path not in sys.path:
        sys.path.insert(0, path)

from server.main import ensure_message_id_header  # noqa: E402
from client.main import _ensure_message_id_header  # noqa: E402


class MessageIdAutoToggleTests(unittest.TestCase):
    def test_server_auto_enabled_generates_unique_message_ids(self) -> None:
        base_header = "Subject: Hello"
        with patch("server.main._build_message_id_value", side_effect=["<first@local>", "<second@local>"]):
            first = ensure_message_id_header(
                base_header,
                auto_enabled=True,
                pattern_value=None,
                mail_from="sender@example.com",
                helo="helo.test",
            )
            second = ensure_message_id_header(
                base_header,
                auto_enabled=True,
                pattern_value=None,
                mail_from="sender@example.com",
                helo="helo.test",
            )
        self.assertIn("Message-ID: <first@local>", first)
        self.assertIn("Message-ID: <second@local>", second)
        self.assertNotEqual(first, second)

    def test_server_auto_disabled_preserves_user_header(self) -> None:
        manual_header = "Message-ID: <manual@example.com>\nSubject: Static"
        with patch("server.main._build_message_id_value", side_effect=AssertionError("should not generate")):
            result = ensure_message_id_header(
                manual_header,
                auto_enabled=False,
                pattern_value=None,
                mail_from="sender@example.com",
                helo="helo.test",
            )
        self.assertEqual(result, manual_header)

    def test_client_auto_enabled_generates_unique_message_ids(self) -> None:
        base_header = "Subject: Client"
        with patch("client.main._build_message_id_value", side_effect=["<first@local>", "<second@local>"]):
            first = _ensure_message_id_header(
                base_header,
                auto_enabled=True,
                pattern_value=None,
                mail_from="sender@example.com",
                helo="helo.test",
            )
            second = _ensure_message_id_header(
                base_header,
                auto_enabled=True,
                pattern_value=None,
                mail_from="sender@example.com",
                helo="helo.test",
            )
        self.assertIn("Message-ID: <first@local>", first)
        self.assertIn("Message-ID: <second@local>", second)
        self.assertNotEqual(first, second)

    def test_client_auto_disabled_preserves_user_header(self) -> None:
        manual_header = "Message-ID: <manual@example.com>\nSubject: Static"
        with patch("client.main._build_message_id_value", side_effect=AssertionError("should not generate")):
            result = _ensure_message_id_header(
                manual_header,
                auto_enabled=False,
                pattern_value=None,
                mail_from="sender@example.com",
                helo="helo.test",
            )
        self.assertEqual(result, manual_header)


if __name__ == "__main__":
    unittest.main()
