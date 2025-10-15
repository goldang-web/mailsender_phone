import importlib.util
import sys
import types
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CLIENT_DIR = PROJECT_ROOT / "client"

if str(CLIENT_DIR) not in sys.path:
    sys.path.insert(0, str(CLIENT_DIR))

spec = importlib.util.spec_from_file_location("client_main", CLIENT_DIR / "main.py")
client_main = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(client_main)


def _make_client() -> "client_main.MailClient":
    config = {
        "server_url": "http://localhost:8000",
        "device_name": "TestDevice",
        "device_id": "device-test",
        "interval": 1,
        "timeout": 1,
        "local_versions": {},
        "domain_cycles": {},
        "stop_schedule": {},
        "imap_settings": {},
        "telnet_debug_mode": False,
    }
    original_save_config = client_main.save_config
    client_main.save_config = lambda cfg: None
    try:
        client = client_main.MailClient(config)
    finally:
        client_main.save_config = original_save_config
    client._run_priority_jobs = types.MethodType(lambda self: None, client)
    client.apply_configs = types.MethodType(lambda self, _: None, client)
    client._queue_jobs = types.MethodType(lambda self, __: None, client)
    return client


class JobReportingTests(unittest.TestCase):
    def test_send_job_report_retries_on_failure(self) -> None:
        client = _make_client()
        call_count = {"value": 0}
        reports_history = []

        def failing_heartbeat(self, domain_states, job_reports):  # type: ignore[override]
            call_count["value"] += 1
            reports_history.append([report.status for report in job_reports])
            raise RuntimeError("temporary failure")

        client.heartbeat = types.MethodType(failing_heartbeat, client)

        client.send_job_report(client_main.JobResult(job_id="job-1", status="success"))

        self.assertEqual(call_count["value"], 1)
        self.assertEqual(len(client._pending_job_reports), 1)

        def recovering_heartbeat(self, domain_states, job_reports):  # type: ignore[override]
            call_count["value"] += 1
            reports_history.append([report.status for report in job_reports])
            return {}

        client.heartbeat = types.MethodType(recovering_heartbeat, client)

        client.send_job_report(client_main.JobResult(job_id="job-2", status="running"))

        self.assertEqual(call_count["value"], 2)
        self.assertFalse(client._pending_job_reports)
        self.assertTrue(any("success" in batch for batch in reports_history))


if __name__ == "__main__":
    unittest.main()
