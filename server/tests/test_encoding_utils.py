import random
import unittest

from encoding_utils import encode_substitution_value, normalize_encoding_name
from mungu_care_encoding import encode_mungu_care


class EncodingUtilsTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.sample = "반가워"

    def test_none_encoding_returns_source(self) -> None:
        self.assertEqual(encode_substitution_value(self.sample, "none"), self.sample)

    def test_html_hex_min_encoding(self) -> None:
        expected = "&#xbc18;&#xac00;&#xc6cc;"
        self.assertEqual(encode_substitution_value(self.sample, "html_hex_min"), expected)

    def test_html_decimal_encoding(self) -> None:
        expected = "&#48152;&#44032;&#50892;"
        self.assertEqual(encode_substitution_value(self.sample, "html_dec"), expected)

    def test_html_hex_fixed_encoding(self) -> None:
        expected = "&#xbc18;&#xac00;&#xc6cc;"
        self.assertEqual(encode_substitution_value(self.sample, "html_hex_fixed"), expected)

    def test_html_random_with_deterministic_choice(self) -> None:
        expected = "&#xbc18;&#xac00;&#xc6cc;"
        chooser = lambda seq: seq[0]  # 항상 첫 번째 포맷 선택
        self.assertEqual(encode_substitution_value(self.sample, "html_random", random_choice=chooser), expected)

    def test_quoted_printable_utf8(self) -> None:
        expected = "=EB=B0=98=EA=B0=80=EC=9B=8C"
        self.assertEqual(encode_substitution_value(self.sample, "quoted_printable_utf8"), expected)

    def test_quoted_printable_euckr(self) -> None:
        expected = "=B9=DD=B0=A1=BF=F6"
        self.assertEqual(encode_substitution_value(self.sample, "quoted_printable_euckr"), expected)

    def test_mime_b64_utf8(self) -> None:
        expected = "=?UTF-8?B?67CY6rCA7JuM?="
        self.assertEqual(encode_substitution_value(self.sample, "mime_b64_utf8"), expected)

    def test_normalize_encoding_alias(self) -> None:
        self.assertEqual(normalize_encoding_name("HTML-HEX-FIXED"), "html_hex_fixed")
        self.assertEqual(normalize_encoding_name(None), "none")

    def test_mungu_care_encoding_matches_reference(self) -> None:
        seed = 20251013
        expected = encode_mungu_care(self.sample, rng=random.Random(seed))
        actual = encode_substitution_value(
            self.sample,
            "mungu_care",
            random_generator=random.Random(seed),
        )
        self.assertEqual(actual, expected)
        self.assertTrue(actual.lower().startswith("=?utf-8?b?"))

    def test_mungu_care_alias_normalization(self) -> None:
        seed = 314159
        expected = encode_mungu_care(self.sample, rng=random.Random(seed))
        actual = encode_substitution_value(
            self.sample,
            "문구케어 인코딩",
            random_generator=random.Random(seed),
        )
        self.assertEqual(actual, expected)
        self.assertTrue(actual.lower().startswith("=?utf-8?b?"))


if __name__ == "__main__":
    unittest.main()
