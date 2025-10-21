import sys
from concurrent.futures import Future
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT_DIR = Path(__file__).resolve().parents[2]
CLIENT_DIR = ROOT_DIR / "client"
if str(CLIENT_DIR) not in sys.path:
    sys.path.insert(0, str(CLIENT_DIR))

from client import main as client_main  # type: ignore  # noqa: E402


class ImmediateExecutor:
    def submit(self, fn, *args, **kwargs):
        future = Future()
        try:
            result = fn(*args, **kwargs)
            future.set_result(result)
        except Exception as exc:  # pylint: disable=broad-except
            future.set_exception(exc)
        return future


def _build_client() -> client_main.MailClient:
    client = client_main.MailClient(dict(client_main.DEFAULT_CONFIG))
    client._imap_executor = ImmediateExecutor()
    client._log_imap_console = lambda *args, **kwargs: None
    client._emit_imap_section = lambda *args, **kwargs: None
    client._record_imap_throttle = lambda *args, **kwargs: None
    client._notify_stop_event = lambda *args, **kwargs: None
    client._run_imap_precheck_purge = lambda **kwargs: {"attempted": False}
    settings = client._imap_settings_for_domain("naver")
    settings.update(
        {
            "enabled": True,
            "username": "imap-user",
            "password": "secret",
            "single_delay_seconds": 0,
            "allowed_latency_seconds": 15,
            "failure_action": "none",
            "notify_before_stop_all": False,
            "purge_before_check": False,
            "recheck_attempts": 2,
            "sent_threshold": 1,
        }
    )
    return client


def _probe_result(message_id: str = "PROBE-ID") -> client_main.SentProbeResult:
    now = datetime.now(timezone.utc)
    return client_main.SentProbeResult(
        success=True,
        sent_at=now,
        status_line="250 2.0.0 OK",
        detail_line=None,
        message_id=message_id,
        mail_from="mail@example.com",
        header_from="Mail Sender <mail@example.com>",
        rcpt_to="imap-user@naver.com",
    )


def _success_result() -> dict:
    now = datetime.now(timezone.utc).isoformat()
    return {
        "status": "success",
        "latency": 1.2,
        "received_at": now,
        "reason": None,
        "allowed_latency": 15,
        "delay_before_check": 0,
        "sent_at": now,
        "sent_display": "sent",
        "received_display": "received",
    }


@pytest.mark.parametrize("snapshot_id", ["SNAP-ID", None])
def test_submit_imap_check_prefers_snapshot_message_id(monkeypatch: pytest.MonkeyPatch, snapshot_id: str) -> None:
    client = _build_client()
    probe = _probe_result()
    client._run_sent_probe_mail = lambda **kwargs: probe

    captured_calls = []

    def fake_verify_delivery(*, message_id=None, **kwargs):
        captured_calls.append(message_id)
        return _success_result()

    monkeypatch.setattr(client_main, "verify_delivery", fake_verify_delivery)

    future = client._submit_imap_check(
        domain="naver",
        job_id="job-1",
        send_type="sent-threshold",
        mail_from="mail@example.com",
        header_from="Mail Sender <mail@example.com>",
        sent_at=datetime.now(timezone.utc),
        has_anchor=False,
        delay_before_check=0,
        allowed_delay=15,
        context_reason="threshold reached (1)",
        force=False,
        sent_window_count=1,
        sent_threshold=1,
        smtp_context={"smtp_host": "smtp.example.com", "smtp_port": 25, "helo": "helo.example", "header": "Header"},
        probe_result=probe,
        precheck_purge=None,
        message_id_snapshot=snapshot_id,
        recheck_attempts=1,
    )

    assert future is not None, "IMAP 확인 작업이 예약되어야 합니다."
    report = future.result()
    expected_id = snapshot_id or probe.message_id
    assert captured_calls == [expected_id]
    assert report["message_id"] == expected_id


def test_submit_imap_check_retries_only_on_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _build_client()
    probe = _probe_result("FIRST-PROBE")
    client._run_sent_probe_mail = lambda **kwargs: probe

    responses = [
        {
            "status": "failure",
            "latency": None,
            "received_at": None,
            "reason": "delay exceeded",
            "allowed_latency": 15,
            "delay_before_check": 0,
            "sent_at": datetime.now(timezone.utc).isoformat(),
            "sent_display": "sent",
            "received_display": "-",
        },
        _success_result(),
    ]

    captured = []

    def fake_verify_delivery(*, message_id=None, **kwargs):
        captured.append(message_id)
        return responses[min(len(captured) - 1, len(responses) - 1)]

    monkeypatch.setattr(client_main, "verify_delivery", fake_verify_delivery)

    future = client._submit_imap_check(
        domain="naver",
        job_id="job-2",
        send_type="sent-threshold",
        mail_from="mail@example.com",
        header_from="Mail Sender <mail@example.com>",
        sent_at=datetime.now(timezone.utc),
        has_anchor=False,
        delay_before_check=0,
        allowed_delay=15,
        context_reason="threshold reached (1)",
        force=False,
        sent_window_count=1,
        sent_threshold=1,
        smtp_context={"smtp_host": "smtp.example.com", "smtp_port": 25, "helo": "helo.example", "header": "Header"},
        probe_result=probe,
        precheck_purge=None,
        message_id_snapshot=None,
        recheck_attempts=2,
    )

    assert future is not None
    report = future.result()
    assert len(captured) == 2, "첫 번째 실패 후에만 한 번 더 재확인을 시도해야 합니다."
    assert captured[0] == "FIRST-PROBE"
    assert report["status"] == "success"


def test_guard_sent_threshold_stores_failure_snapshot(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _build_client()
    client._pause_sent_workers = lambda domain: None
    resumed = []
    client._resume_sent_workers = lambda domain: resumed.append(domain)
    probe = _probe_result("STORED-ID")
    client._sent_guard_pending.clear()

    future = Future()
    future.set_result(
        {
            "status": "failure",
            "reason": "latency exceeded",
            "message_id": "STORED-ID",
        }
    )

    outcome = client_main.ImapGuardOutcome(
        probe=probe,
        future=future,
        sent_window_count=3,
        sent_threshold=1,
        scheduled=True,
        failure_report_enqueued=False,
    )

    monkeypatch.setattr(client, "_execute_imap_guard_flow", lambda **kwargs: outcome)

    request_payload = {
        "threshold": 1,
        "sent_count": 3,
        "sequence_total": 3,
        "target_multiple": 1,
        "current_multiple": 1,
        "reason": "threshold reached (1)",
        "message_id": "STORED-ID",
        "smtp": {},
    }

    client._guard_sent_threshold(
        domain="naver",
        job_id="job-3",
        request=request_payload,
        send_type="sent-threshold",
        mail_from="mail@example.com",
        message_id="STORED-ID",
        header_from="Mail Sender <mail@example.com>",
        has_anchor=False,
        context_reason="threshold reached (1)",
        delay_before_check=0,
        allowed_delay=15,
        smtp_context={},
        force=False,
        counter_mode="threshold",
        counter_current=3,
        counter_threshold=1,
        sent_window_count=3,
        report_probe_failure=False,
    )

    pending = client._sent_guard_pending.get("naver")
    assert pending is not None
    assert pending["request"]["message_id"] == "STORED-ID"
    assert resumed, "실패 후에는 Sent 작업이 재개되어야 합니다."
