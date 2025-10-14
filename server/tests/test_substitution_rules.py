import random
import re
import string
import sys
import unittest
from collections import deque
from pathlib import Path

TEST_DIR = Path(__file__).resolve().parent
SERVER_DIR = TEST_DIR.parent
if str(SERVER_DIR) not in sys.path:
    sys.path.insert(0, str(SERVER_DIR))

from server.encoding_utils import encode_substitution_value
from server.main import (
    build_substitution_context,
    canonicalize_substitution_rules,
    apply_substitutions_to_config,
    substitute_tokens,
    SubstitutionRule,
    SubstitutionPreviewItem,
    SubstitutionPreviewRequest,
    preview_substitution_endpoint,
)


class SequencedRandom(random.Random):
    def __init__(self, picks):
        super().__init__()
        self._picks = deque(picks)

    def randrange(self, stop, *args, **kwargs):  # type: ignore[override]
        if args or kwargs:
            return super().randrange(stop, *args, **kwargs)
        if not self._picks:
            return 0
        value = self._picks.popleft()
        return value % max(1, stop)


class SubstitutionRuleTests(unittest.TestCase):
    def test_canonicalize_static_and_list(self) -> None:
        raw_rules = [
            {
                "key": "도메인",
                "source": "example.com",
                "encoding": "none",
            },
            {
                "key": "발신닉",
                "mode": "list",
                "values": [" Alpha ", "Beta", "Alpha"],
                "description": "  샘플 목록  ",
            },
        ]
        sanitized = canonicalize_substitution_rules(raw_rules, strict=True)
        self.assertEqual(len(sanitized), 2)

        static_rule = sanitized[0]
        list_rule = sanitized[1]
        self.assertEqual(static_rule["key"], "도메인")
        self.assertEqual(static_rule["mode"], "static")
        self.assertEqual(static_rule["value"], "example.com")
        self.assertEqual(static_rule["description"], "")

        self.assertEqual(list_rule["key"], "발신닉")
        self.assertEqual(list_rule["mode"], "list")
        self.assertEqual(list_rule["values"], ["Alpha", "Beta"])
        self.assertEqual(list_rule["description"], "샘플 목록")

    def test_canonicalize_static_resolves_list_tokens_before_encoding(self) -> None:
        rng = random.Random(123)
        sanitized = canonicalize_substitution_rules(
            [
                {"key": "닉네임", "mode": "list", "values": ["테스트값"]},
                {
                    "key": "문구",
                    "source": "안녕 ${목록:닉네임}",
                    "encoding": "html_hex_min",
                },
            ],
            strict=True,
            random_generator=rng,
        )
        self.assertEqual(len(sanitized), 2)
        static_rule = next(rule for rule in sanitized if rule["key"] == "문구")
        expected = encode_substitution_value("안녕 테스트값", "html_hex_min")
        self.assertEqual(static_rule["value"], expected)
        self.assertNotIn("${", static_rule["value"])

    def test_substitute_tokens_static_random_list(self) -> None:
        sanitized = canonicalize_substitution_rules(
            [
                {"key": "도메인", "source": "sample.io", "encoding": "none"},
                {"key": "문구", "source": "Hello ${이름}", "encoding": "none"},
                {"key": "이름", "source": "World", "encoding": "none"},
                {
                    "key": "닉네임",
                    "mode": "list",
                    "values": ["Alpha", "Beta"],
                },
            ],
            strict=True,
        )
        context = build_substitution_context(sanitized)
        rng = random.Random(123)
        template = "A=${문구}, B=${랜덤:영소:5}, C=${목록:닉네임}"

        result, missing = substitute_tokens(
            template,
            sanitized,
            random_generator=rng,
            context=context,
        )
        self.assertFalse(missing)
        self.assertIn("A=Hello World", result)

        # 재현 가능한 랜덤 결과 검증
        confirm_rng = random.Random(123)
        expected_length = confirm_rng.randint(5, 5)
        expected_random = "".join(confirm_rng.choice(string.ascii_lowercase) for _ in range(expected_length))
        expected_list = confirm_rng.choice(["Alpha", "Beta"])

        random_match = re.search(r"B=([a-z]+)", result)
        self.assertIsNotNone(random_match)
        self.assertEqual(random_match.group(1), expected_random)
        self.assertIn(f"C={expected_list}", result)

    def test_substitute_tokens_invalid_random(self) -> None:
        sanitized = canonicalize_substitution_rules(
            [{"key": "도메인", "source": "example.com", "encoding": "none"}],
            strict=True,
        )
        context = build_substitution_context(sanitized)
        result, missing = substitute_tokens(
            "값=${랜덤:없는:5}",
            sanitized,
            context=context,
            random_generator=random.Random(0),
        )
        self.assertEqual(result, "값=")
        self.assertIn("랜덤:없는:5", missing)

    def test_substitute_tokens_missing_list(self) -> None:
        sanitized = canonicalize_substitution_rules([], strict=True)
        context = build_substitution_context(sanitized)
        result, missing = substitute_tokens(
            "닉=${목록:존재하지않음}",
            sanitized,
            context=context,
            random_generator=random.Random(0),
        )
        self.assertEqual(result, "닉=")
        self.assertIn("목록:존재하지않음", missing)

    def test_apply_substitutions_with_list(self) -> None:
        sanitized = canonicalize_substitution_rules(
            [
                {"key": "닉네임", "mode": "list", "values": ["Alpha", "Beta"]},
            ],
            strict=True,
        )
        context = build_substitution_context(sanitized)
        config = {"header": "닉=${목록:닉네임}"}
        rng = random.Random(7)
        missing = apply_substitutions_to_config(config, sanitized, context=context, random_generator=rng)
        self.assertFalse(missing)
        self.assertIn(config["header"], {"닉=Alpha", "닉=Beta"})

    def test_list_pattern_uses_random_sequence(self) -> None:
        sanitized = canonicalize_substitution_rules(
            [
                {"key": "닉", "mode": "list", "values": ["Alpha", "Beta", "Gamma"]},
            ],
            strict=True,
        )
        context = build_substitution_context(sanitized)
        rng = SequencedRandom((0, 1, 2, 1, 0))
        outputs = []
        for _ in range(5):
            value, missing = substitute_tokens("${목록:닉}", sanitized, context=context, random_generator=rng)
            self.assertFalse(missing)
            outputs.append(value)
        self.assertEqual(outputs, ["Alpha", "Beta", "Gamma", "Beta", "Alpha"])

    def test_preview_endpoint_resolves_lists_before_encoding(self) -> None:
        request = SubstitutionPreviewRequest(
            items=[
                SubstitutionPreviewItem(
                    key="문구",
                    source="${목록:닉}",
                    encoding="html_hex_min",
                )
            ],
            rules=[
                SubstitutionRule(key="닉", mode="list", values=["테스트"]),
                SubstitutionRule(key="문구", source="${목록:닉}", encoding="html_hex_min"),
            ],
        )
        response = preview_substitution_endpoint(request)
        self.assertEqual(len(response.results), 1)
        expected = encode_substitution_value("테스트", "html_hex_min")
        self.assertEqual(response.results[0], expected)


if __name__ == "__main__":
    unittest.main()
