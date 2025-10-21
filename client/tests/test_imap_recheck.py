import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parents[1]))

from main import (  # type: ignore  # noqa: E402
    IMAP_DEFAULT_RECHECK_ATTEMPTS,
    IMAP_RECHECK_ATTEMPTS_MAX,
    IMAP_RECHECK_ATTEMPTS_MIN,
    sanitize_imap_recheck_attempts,
)


@pytest.mark.parametrize(
    "value,expected",
    [
        (None, IMAP_DEFAULT_RECHECK_ATTEMPTS),
        ("", IMAP_DEFAULT_RECHECK_ATTEMPTS),
        (IMAP_RECHECK_ATTEMPTS_MIN, IMAP_RECHECK_ATTEMPTS_MIN),
        (IMAP_RECHECK_ATTEMPTS_MAX, IMAP_RECHECK_ATTEMPTS_MAX),
        (0, IMAP_RECHECK_ATTEMPTS_MIN),
        (999, IMAP_RECHECK_ATTEMPTS_MAX),
    ],
)
def test_sanitize_imap_recheck_attempts(value, expected):
    assert sanitize_imap_recheck_attempts(value) == expected
