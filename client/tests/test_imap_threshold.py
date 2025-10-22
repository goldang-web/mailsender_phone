import copy
import random
from collections import deque
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


def test_submit_imap_check_reroll_updates_context(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _build_client()
    settings = client._imap_settings_for_domain("naver")
    settings.update(
        {
            "reroll_on_retry": True,
            "enabled": True,
        }
    )
    client._substitution_lock_active["naver"] = False
    client._substitution_lock_modes["naver"] = "auto"

    base_header = (
        "From: Old Sender <old@example.com>\n"
        "Subject: Test\n"
        "Message-ID: <OLD-ID@example.com>\n"
    )
    config_snapshot = {
        "smtp_host": "smtp.example.com",
        "smtp_port": 25,
        "helo": "helo.example.com",
        "mail_from": "old@example.com",
        "header": base_header,
        "message_id_auto": True,
        "message_id_pattern": "<OLD-ID@example.com>",
    }
    substitution_payload = {
        "template": copy.deepcopy(config_snapshot),
        "rules": [],
    }
    smtp_context = {
        "smtp_host": "smtp.example.com",
        "smtp_port": 25,
        "helo": "helo.example.com",
        "mail_from": "old@example.com",
        "header": base_header,
        "config_snapshot": copy.deepcopy(config_snapshot),
        "substitution": copy.deepcopy(substitution_payload),
    }

    new_mail_from = "new@example.com"

    def fake_render(base_config, payload):  # type: ignore[override]
        refreshed = dict(base_config)
        refreshed["mail_from"] = new_mail_from
        refreshed["header"] = client_main._ensure_message_id_header(
            "From: New Sender <new@example.com>\nSubject: Test\n",
            auto_enabled=True,
            pattern_value=None,
            mail_from=new_mail_from,
            helo=base_config.get("helo"),
        )
        refreshed["message_id_auto"] = True
        refreshed["message_id_pattern"] = "<NEW-ID@example.com>"
        return refreshed, set()

    monkeypatch.setattr(client, "_render_all_headers_unique_config", fake_render)

    verify_calls = []

    def fake_verify_delivery(*, message_id=None, **kwargs):
        verify_calls.append(message_id)
        if len(verify_calls) == 1:
            return {
                "status": "failure",
                "reason": "latency exceeded",
                "latency": None,
                "received_at": None,
                "sent_display": "sent",
                "received_display": "-",
            }
        return {
            "status": "success",
            "latency": 0.5,
            "received_at": datetime.now(timezone.utc).isoformat(),
            "reason": None,
            "sent_display": "sent",
            "received_display": "received",
        }

    monkeypatch.setattr(client_main, "verify_delivery", fake_verify_delivery)

    def fake_run_sent_probe_mail(*, smtp_context, **kwargs):  # type: ignore[override]
        header_text = smtp_context.get("header") or ""
        message_id = client_main._extract_message_id_from_text(header_text) or "FALLBACK-ID"
        return client_main.SentProbeResult(
            success=True,
            sent_at=datetime.now(timezone.utc),
            status_line="250 2.0.0 OK",
            detail_line=None,
            message_id=message_id,
            mail_from=smtp_context.get("mail_from"),
            header_from=f"Tester <{smtp_context.get('mail_from')}>",
            rcpt_to="imap-user@naver.com",
        )

    monkeypatch.setattr(client, "_run_sent_probe_mail", fake_run_sent_probe_mail)

    probe = _probe_result("PROBE-ID")
    future = client._submit_imap_check(
        domain="naver",
        job_id="job-4",
        send_type="sent-threshold",
        mail_from="old@example.com",
        header_from="Old Sender <old@example.com>",
        sent_at=datetime.now(timezone.utc),
        has_anchor=False,
        delay_before_check=0,
        allowed_delay=15,
        context_reason="threshold reached (1)",
        force=False,
        sent_window_count=1,
        sent_threshold=1,
        smtp_context=smtp_context,
        probe_result=probe,
        precheck_purge=None,
        message_id_snapshot=None,
        recheck_attempts=2,
    )
    report = future.result()
    assert report["status"] == "success"
    assert report["reroll_applied"] is True
    assert report["reroll_success_count"] == 1
    assert report["mail_from"] == new_mail_from
    assert report.get("smtp_context", {}).get("mail_from") == new_mail_from
    assert isinstance(report.get("config_snapshot"), dict)
    assert verify_calls[0] == "PROBE-ID"
    assert len(verify_calls) == 2
    assert verify_calls[-1] != "PROBE-ID"


def test_submit_imap_check_reroll_keeps_template_and_refreshes_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _build_client()
    settings = client._imap_settings_for_domain("naver")
    settings.update(
        {
            "reroll_on_retry": True,
            "enabled": True,
        }
    )
    client._substitution_lock_active["naver"] = False
    client._substitution_lock_modes["naver"] = "auto"

    helo_value = "helo.example.com"
    initial_mail_from = "initial@example.com"
    initial_header_base = (
        "From: Initial Sender <initial@example.com>\n"
        "Subject: 재시도 Value-0\n"
    )
    initial_header = client_main._ensure_message_id_header(
        initial_header_base,
        auto_enabled=True,
        pattern_value=None,
        mail_from=initial_mail_from,
        helo=helo_value,
    )

    config_snapshot = {
        "smtp_host": "smtp.example.com",
        "smtp_port": 25,
        "helo": helo_value,
        "mail_from": initial_mail_from,
        "header": initial_header,
        "message_id_auto": True,
        "message_id_pattern": None,
    }
    substitution_template = {
        "smtp_host": "smtp.example.com",
        "smtp_port": 25,
        "helo": helo_value,
        "mail_from": "token@example.com",
        "header": (
            "From: Token Sender <token@example.com>\n"
            "Subject: 재시도 ${TOKEN}\n"
        ),
        "message_id_auto": True,
        "message_id_pattern": None,
    }
    substitution_payload = {
        "template": copy.deepcopy(substitution_template),
        "rules": [],
    }
    smtp_context = {
        "smtp_host": "smtp.example.com",
        "smtp_port": 25,
        "helo": helo_value,
        "mail_from": initial_mail_from,
        "header": initial_header,
        "config_snapshot": copy.deepcopy(config_snapshot),
        "substitution": copy.deepcopy(substitution_payload),
    }

    render_calls = []
    generated_headers = []
    generated_mail_from = []

    def fake_render(base_config, payload):  # type: ignore[override]
        render_calls.append(copy.deepcopy(payload))
        assert "${TOKEN}" in payload["template"]["header"]
        refreshed = dict(base_config)
        attempt_index = len(render_calls)
        new_mail_from = f"reroll{attempt_index}@example.com"
        header_with_value = payload["template"]["header"].replace("${TOKEN}", f"Value-{attempt_index}")
        refreshed["mail_from"] = new_mail_from
        refreshed["header"] = client_main._ensure_message_id_header(
            header_with_value,
            auto_enabled=True,
            pattern_value=None,
            mail_from=new_mail_from,
            helo=refreshed.get("helo"),
        )
        refreshed["message_id_auto"] = True
        refreshed["message_id_pattern"] = None
        generated_headers.append(refreshed["header"])
        generated_mail_from.append(new_mail_from)
        return refreshed, set()

    monkeypatch.setattr(client, "_render_all_headers_unique_config", fake_render)

    verify_calls = []

    def fake_verify_delivery(*, message_id=None, **kwargs):
        verify_calls.append(message_id)
        if len(verify_calls) == 1:
            return {
                "status": "failure",
                "reason": "latency exceeded",
                "latency": None,
                "received_at": None,
                "sent_display": "sent",
                "received_display": "-",
            }
        return {
            "status": "success",
            "latency": 0.3,
            "received_at": datetime.now(timezone.utc).isoformat(),
            "reason": None,
            "sent_display": "sent",
            "received_display": "received",
        }

    monkeypatch.setattr(client_main, "verify_delivery", fake_verify_delivery)

    def fake_run_sent_probe_mail(*, smtp_context, **kwargs):  # type: ignore[override]
        header_text = smtp_context.get("header") or ""
        message_id = client_main._extract_message_id_from_text(header_text) or "FALLBACK-ID"
        return client_main.SentProbeResult(
            success=True,
            sent_at=datetime.now(timezone.utc),
            status_line="250 2.0.0 OK",
            detail_line=None,
            message_id=message_id,
            mail_from=smtp_context.get("mail_from"),
            header_from=f"Tester <{smtp_context.get('mail_from')}>",
            rcpt_to="imap-user@naver.com",
        )

    monkeypatch.setattr(client, "_run_sent_probe_mail", fake_run_sent_probe_mail)

    probe = _probe_result("PROBE-ID")
    future = client._submit_imap_check(
        domain="naver",
        job_id="job-template",
        send_type="sent-threshold",
        mail_from=initial_mail_from,
        header_from="Initial Sender <initial@example.com>",
        sent_at=datetime.now(timezone.utc),
        has_anchor=False,
        delay_before_check=0,
        allowed_delay=15,
        context_reason="threshold reached (1)",
        force=False,
        sent_window_count=1,
        sent_threshold=1,
        smtp_context=smtp_context,
        probe_result=probe,
        precheck_purge=None,
        message_id_snapshot=None,
        recheck_attempts=2,
    )
    report = future.result()

    assert report["status"] == "success"
    assert report["reroll_applied"] is True
    assert len(render_calls) == 1
    assert "${TOKEN}" in render_calls[0]["template"]["header"]
    assert report["mail_from"] == generated_mail_from[-1]
    assert verify_calls[0] == "PROBE-ID"
    assert verify_calls[-1] == report["message_id"]
    assert verify_calls[-1] != "PROBE-ID"
    substitution_report = report.get("smtp_context", {}).get("substitution")
    assert substitution_report is not None
    assert "${TOKEN}" in substitution_report["template"]["header"]
    assert substitution_report.get("last_snapshot")
    last_snapshot_header = substitution_report["last_snapshot"]["header"]
    assert "Value-1" in last_snapshot_header
    assert client_main._extract_message_id_from_text(last_snapshot_header) == report["message_id"]
    config_snapshot_header = report.get("smtp_context", {}).get("config_snapshot", {}).get("header")
    assert config_snapshot_header and "Value-1" in config_snapshot_header
    assert report["reroll_success_count"] == 1


def test_submit_imap_check_reroll_reencodes_rule_values(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _build_client()
    settings = client._imap_settings_for_domain("naver")
    settings.update(
        {
            "reroll_on_retry": True,
            "enabled": True,
            "recheck_attempts": 3,
        }
    )
    client._substitution_lock_active["naver"] = False
    client._substitution_lock_modes["naver"] = "auto"

    class IncrementingRandom(random.Random):
        counter = 0

        def __init__(self):  # type: ignore[override]
            super().__init__(IncrementingRandom.counter)
            IncrementingRandom.counter += 1

    monkeypatch.setattr(client_main.random, "SystemRandom", IncrementingRandom)

    helo_value = "helo.example.com"
    initial_mail_from = "initial@example.com"
    initial_header = client_main._ensure_message_id_header(
        "From: Initial Sender <initial@example.com>\nSubject: 재시도 템플릿\n",
        auto_enabled=True,
        pattern_value=None,
        mail_from=initial_mail_from,
        helo=helo_value,
    )

    config_snapshot = {
        "smtp_host": "smtp.example.com",
        "smtp_port": 25,
        "helo": helo_value,
        "mail_from": initial_mail_from,
        "header": initial_header,
        "message_id_auto": True,
        "message_id_pattern": None,
    }
    substitution_template = {
        "smtp_host": "smtp.example.com",
        "smtp_port": 25,
        "helo": helo_value,
        "mail_from": initial_mail_from,
        "header": (
            "From: Token Sender <initial@example.com>\n"
            "Subject: ${TITLE}\n"
        ),
        "message_id_auto": True,
        "message_id_pattern": None,
    }
    substitution_rules = [
        {
            "key": "TITLE",
            "source": "제목-${랜덤:영소:4}",
            "encoding": "mime_b64_utf8",
            "value": "=?UTF-8?B?7IS47JqUIOydtA==?=",
            "mode": "static",
            "values": [],
            "description": "",
        }
    ]
    substitution_payload = {
        "template": copy.deepcopy(substitution_template),
        "rules": copy.deepcopy(substitution_rules),
    }
    smtp_context = {
        "smtp_host": "smtp.example.com",
        "smtp_port": 25,
        "helo": helo_value,
        "mail_from": initial_mail_from,
        "header": initial_header,
        "config_snapshot": copy.deepcopy(config_snapshot),
        "substitution": copy.deepcopy(substitution_payload),
    }

    captured_values: list[str] = []

    def fake_render(base_config, payload):  # type: ignore[override]
        rules = payload.get("rules") or []
        if rules:
            captured_values.append(str(rules[0].get("value") or ""))
        refreshed = dict(base_config)
        header_subject = payload["template"]["header"].replace(
            "${TITLE}",
            rules[0]["value"] if rules else "fallback",
        )
        refreshed["header"] = client_main._ensure_message_id_header(
            header_subject,
            auto_enabled=True,
            pattern_value=None,
            mail_from=refreshed.get("mail_from"),
            helo=refreshed.get("helo"),
        )
        return refreshed, set()

    monkeypatch.setattr(client, "_render_all_headers_unique_config", fake_render)

    verify_calls = []

    def fake_verify_delivery(*, message_id=None, **kwargs):
        verify_calls.append(message_id)
        if len(verify_calls) < 3:
            return {
                "status": "failure",
                "reason": "latency exceeded",
                "latency": None,
                "received_at": None,
                "sent_display": "sent",
                "received_display": "-",
            }
        return {
            "status": "success",
            "latency": 0.5,
            "received_at": datetime.now(timezone.utc).isoformat(),
            "reason": None,
            "sent_display": "sent",
            "received_display": "received",
        }

    monkeypatch.setattr(client_main, "verify_delivery", fake_verify_delivery)

    def fake_run_sent_probe_mail(*, smtp_context, **kwargs):  # type: ignore[override]
        header_text = smtp_context.get("header") or ""
        message_id = client_main._extract_message_id_from_text(header_text) or "FALLBACK-ID"
        return client_main.SentProbeResult(
            success=True,
            sent_at=datetime.now(timezone.utc),
            status_line="250 2.0.0 OK",
            detail_line=None,
            message_id=message_id,
            mail_from=smtp_context.get("mail_from"),
            header_from=f"Tester <{smtp_context.get('mail_from')}>",
            rcpt_to="imap-user@naver.com",
        )

    monkeypatch.setattr(client, "_run_sent_probe_mail", fake_run_sent_probe_mail)

    probe = _probe_result("PROBE-ID")
    future = client._submit_imap_check(
        domain="naver",
        job_id="job-reencode",
        send_type="sent-threshold",
        mail_from=initial_mail_from,
        header_from="Initial Sender <initial@example.com>",
        sent_at=datetime.now(timezone.utc),
        has_anchor=False,
        delay_before_check=0,
        allowed_delay=15,
        context_reason="threshold reached (1)",
        force=False,
        sent_window_count=1,
        sent_threshold=1,
        smtp_context=smtp_context,
        probe_result=probe,
        precheck_purge=None,
        message_id_snapshot=None,
        recheck_attempts=3,
    )

    report = future.result()
    assert report["status"] == "success"
    assert report["reroll_applied"] is True
    assert len(captured_values) == 2, "2차 재시도까지 치환 값이 재계산되어야 합니다."
    first_value, second_value = captured_values
    assert first_value.startswith("=?UTF-8?B?")
    assert second_value.startswith("=?UTF-8?B?")
    assert first_value != second_value, "각 재시도마다 인코딩된 값이 달라져야 합니다."
    assert report["reroll_success_count"] == 1


def test_reroll_success_counter_accumulates(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _build_client()
    settings = client._imap_settings_for_domain("naver")
    settings.update(
        {
            "reroll_on_retry": True,
            "enabled": True,
            "recheck_attempts": 3,
        }
    )
    client._substitution_lock_active["naver"] = False
    client._substitution_lock_modes["naver"] = "auto"

    helo_value = "helo.example.com"
    initial_mail_from = "initial@example.com"
    base_header = client_main._ensure_message_id_header(
        "From: Initial Sender <initial@example.com>\nSubject: Reroll Counter\n",
        auto_enabled=True,
        pattern_value=None,
        mail_from=initial_mail_from,
        helo=helo_value,
    )
    config_snapshot = {
        "smtp_host": "smtp.example.com",
        "smtp_port": 25,
        "helo": helo_value,
        "mail_from": initial_mail_from,
        "header": base_header,
        "message_id_auto": True,
        "message_id_pattern": None,
    }
    substitution_payload = {
        "template": copy.deepcopy(config_snapshot),
        "rules": [],
    }
    smtp_context = {
        "smtp_host": "smtp.example.com",
        "smtp_port": 25,
        "helo": helo_value,
        "mail_from": initial_mail_from,
        "header": base_header,
        "config_snapshot": copy.deepcopy(config_snapshot),
        "substitution": copy.deepcopy(substitution_payload),
    }

    rerender_attempts = []

    def fake_render(base_config, payload):  # type: ignore[override]
        rerender_attempts.append(1)
        refreshed = dict(base_config)
        attempt_index = len(rerender_attempts)
        refreshed["mail_from"] = f"reroll{attempt_index}@example.com"
        refreshed["header"] = client_main._ensure_message_id_header(
            f"From: Reroll {attempt_index} <reroll{attempt_index}@example.com>\nSubject: Reroll Counter\n",
            auto_enabled=True,
            pattern_value=None,
            mail_from=refreshed["mail_from"],
            helo=helo_value,
        )
        refreshed["message_id_auto"] = True
        refreshed["message_id_pattern"] = None
        return refreshed, set()

    monkeypatch.setattr(client, "_render_all_headers_unique_config", fake_render)

    probe = _probe_result("PROBE-ID")
    monkeypatch.setattr(client, "_run_sent_probe_mail", lambda **kwargs: probe)

    verify_results = deque(
        [
            {
                "status": "failure",
                "reason": "latency exceeded",
                "latency": None,
                "received_at": None,
                "sent_display": "sent",
                "received_display": "-",
            },
            {
                "status": "success",
                "latency": 0.7,
                "received_at": datetime.now(timezone.utc).isoformat(),
                "reason": None,
                "sent_display": "sent",
                "received_display": "received",
            },
            {
                "status": "failure",
                "reason": "latency exceeded",
                "latency": None,
                "received_at": None,
                "sent_display": "sent",
                "received_display": "-",
            },
            {
                "status": "success",
                "latency": 0.6,
                "received_at": datetime.now(timezone.utc).isoformat(),
                "reason": None,
                "sent_display": "sent",
                "received_display": "received",
            },
        ]
    )

    def fake_verify_delivery(*, message_id=None, **kwargs):
        assert verify_results, "verify_delivery 호출 초과"
        return verify_results.popleft()

    monkeypatch.setattr(client_main, "verify_delivery", fake_verify_delivery)

    def run_check():
        future = client._submit_imap_check(
            domain="naver",
            job_id="job-counter",
            send_type="sent-threshold",
            mail_from=initial_mail_from,
            header_from="Initial Sender <initial@example.com>",
            sent_at=datetime.now(timezone.utc),
            has_anchor=False,
            delay_before_check=0,
            allowed_delay=15,
            context_reason="threshold reached (1)",
            force=False,
            sent_window_count=1,
            sent_threshold=1,
            smtp_context=copy.deepcopy(smtp_context),
            probe_result=probe,
            precheck_purge=None,
            message_id_snapshot=None,
            recheck_attempts=3,
        )
        return future.result()

    first_report = run_check()
    assert first_report["reroll_applied"] is True
    assert first_report["reroll_success_count"] == 1
    assert client._get_reroll_success_count("naver") == 1

    second_report = run_check()
    assert second_report["reroll_applied"] is True
    assert second_report["reroll_success_count"] == 2
    assert client._get_reroll_success_count("naver") == 2
    assert not verify_results


def test_submit_imap_check_skips_reroll_when_locked(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _build_client()
    settings = client._imap_settings_for_domain("naver")
    settings.update(
        {
            "reroll_on_retry": True,
            "enabled": True,
        }
    )
    client._substitution_lock_active["naver"] = True
    client._substitution_lock_modes["naver"] = "lock"

    base_header = (
        "From: Old Sender <old@example.com>\n"
        "Subject: Test\n"
        "Message-ID: <OLD-ID@example.com>\n"
    )
    config_snapshot = {
        "smtp_host": "smtp.example.com",
        "smtp_port": 25,
        "helo": "helo.example.com",
        "mail_from": "old@example.com",
        "header": base_header,
        "message_id_auto": True,
        "message_id_pattern": "<OLD-ID@example.com>",
    }
    substitution_payload = {
        "template": copy.deepcopy(config_snapshot),
        "rules": [],
    }
    smtp_context = {
        "smtp_host": "smtp.example.com",
        "smtp_port": 25,
        "helo": "helo.example.com",
        "mail_from": "old@example.com",
        "header": base_header,
        "config_snapshot": copy.deepcopy(config_snapshot),
        "substitution": copy.deepcopy(substitution_payload),
    }

    def fake_render(base_config, payload):  # type: ignore[override]
        refreshed = dict(base_config)
        refreshed["header"] = base_header
        refreshed["mail_from"] = "locked@example.com"
        return refreshed, set()

    monkeypatch.setattr(client, "_render_all_headers_unique_config", fake_render)

    verify_calls = []

    def fake_verify_delivery(*, message_id=None, **kwargs):
        verify_calls.append(message_id)
        if len(verify_calls) == 1:
            return {
                "status": "failure",
                "reason": "latency exceeded",
                "latency": None,
                "received_at": None,
                "sent_display": "sent",
                "received_display": "-",
            }
        return {
            "status": "success",
            "latency": 0.4,
            "received_at": datetime.now(timezone.utc).isoformat(),
            "reason": None,
            "sent_display": "sent",
            "received_display": "received",
        }

    monkeypatch.setattr(client_main, "verify_delivery", fake_verify_delivery)

    def fake_run_sent_probe_mail(*, smtp_context, **kwargs):  # type: ignore[override]
        header_text = smtp_context.get("header") or ""
        message_id = client_main._extract_message_id_from_text(header_text) or "FALLBACK-ID"
        return client_main.SentProbeResult(
            success=True,
            sent_at=datetime.now(timezone.utc),
            status_line="250 2.0.0 OK",
            detail_line=None,
            message_id=message_id,
            mail_from=smtp_context.get("mail_from"),
            header_from=f"Tester <{smtp_context.get('mail_from')}>",
            rcpt_to="imap-user@naver.com",
        )

    monkeypatch.setattr(client, "_run_sent_probe_mail", fake_run_sent_probe_mail)

    probe = _probe_result("PROBE-ID")
    future = client._submit_imap_check(
        domain="naver",
        job_id="job-5",
        send_type="sent-threshold",
        mail_from="old@example.com",
        header_from="Old Sender <old@example.com>",
        sent_at=datetime.now(timezone.utc),
        has_anchor=False,
        delay_before_check=0,
        allowed_delay=15,
        context_reason="threshold reached (1)",
        force=False,
        sent_window_count=1,
        sent_threshold=1,
        smtp_context=smtp_context,
        probe_result=probe,
        precheck_purge=None,
        message_id_snapshot=None,
        recheck_attempts=2,
    )
    report = future.result()
    assert report["status"] == "success"
    assert report["reroll_applied"] is False
    assert report["reroll_success_count"] == 0
    assert "smtp_context" not in report or report["smtp_context"]["mail_from"] == "old@example.com"
    assert len(verify_calls) == 2
    assert verify_calls[0] == verify_calls[1] == "PROBE-ID"
