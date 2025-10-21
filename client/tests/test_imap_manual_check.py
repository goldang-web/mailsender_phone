import sys
import types
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
CLIENT_DIR = ROOT_DIR / "client"
if str(CLIENT_DIR) not in sys.path:
    sys.path.insert(0, str(CLIENT_DIR))

from client import main as client_main  # type: ignore  # noqa: E402


def _build_client() -> client_main.MailClient:
    client = client_main.MailClient(dict(client_main.DEFAULT_CONFIG))
    settings = client._imap_settings_for_domain("naver")
    settings.update(
        {
            "enabled": True,
            "username": "imap-user",
            "password": "secret",
            "single_delay_seconds": 0,
            "allowed_latency_seconds": 10,
            "failure_action": "none",
            "purge_before_check": False,
            "recheck_attempts": 1,
            "sent_threshold": 1,
        }
    )
    client.send_job_report = types.MethodType(lambda self, *args, **kwargs: None, client)
    client._log_substitution_missing = lambda *args, **kwargs: None
    return client


def _capture_guard(client: client_main.MailClient):
    captured = {}

    def fake_guard(self, **kwargs):
        captured.update(kwargs)
        return client_main.ImapGuardOutcome(
            probe=None,
            future=None,
            sent_window_count=None,
            sent_threshold=None,
            scheduled=False,
            failure_report_enqueued=False,
        )

    client._execute_imap_guard_flow = types.MethodType(fake_guard, client)
    return captured


def test_manual_check_passes_header_message_id():
    client = _build_client()
    captured = _capture_guard(client)

    header_value = "From: Test <test@example.com>\nMessage-ID: <HEADER-ID@example.com>\n\nBody"
    payload = {
        "config": {
            "smtp_host": "smtp.example.com",
            "smtp_port": 25,
            "helo": "helo.example.com",
            "header": header_value,
            "mail_from": "test@example.com",
            "all_headers_unique": False,
            "message_id_auto": False,
        }
    }

    client.handle_imap_manual_check("naver", payload, "job-1")

    assert captured["message_id"] == "<HEADER-ID@example.com>"


def test_manual_check_prefers_payload_message_id():
    client = _build_client()
    captured = _capture_guard(client)

    header_value = "From: Test <test@example.com>\nMessage-ID: <HEADER-ID@example.com>\n\nBody"
    payload = {
        "config": {
            "smtp_host": "smtp.example.com",
            "smtp_port": 25,
            "helo": "helo.example.com",
            "header": header_value,
            "mail_from": "test@example.com",
            "all_headers_unique": False,
            "message_id_auto": False,
        },
        "message_id": "PAYLOAD-ID",
    }

    client.handle_imap_manual_check("naver", payload, "job-2")

    assert captured["message_id"] == "PAYLOAD-ID"
