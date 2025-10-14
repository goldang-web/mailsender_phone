from __future__ import annotations

import base64
import random
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from mungu_care_encoding import encode_mungu_care

EncodingChoice = Callable[[Sequence[str]], str]

ENCODING_ALIASES: Dict[str, str] = {
    "": "none",
    "none": "none",
    "없음": "none",
    "plain": "none",
    "raw": "none",
    "html": "html_hex_min",
    "html_hex_min": "html_hex_min",
    "html-hex-min": "html_hex_min",
    "html-entity-hex": "html_hex_min",
    "html_hex_fixed": "html_hex_fixed",
    "html-hex-fixed": "html_hex_fixed",
    "html-entity-hex-fixed": "html_hex_fixed",
    "html_dec": "html_dec",
    "html-dec": "html_dec",
    "html-entity-dec": "html_dec",
    "html_random": "html_random",
    "html-random": "html_random",
    "html-entity-random": "html_random",
    "random": "html_random",
    "quoted_printable_utf8": "quoted_printable_utf8",
    "quoted-printable-utf8": "quoted_printable_utf8",
    "quoted-printable": "quoted_printable_utf8",
    "qp_utf8": "quoted_printable_utf8",
    "quoted_printable_euc-kr": "quoted_printable_euckr",
    "quoted_printable_euckr": "quoted_printable_euckr",
    "quoted-printable-euckr": "quoted_printable_euckr",
    "qp_euckr": "quoted_printable_euckr",
    "qp_euc-kr": "quoted_printable_euckr",
    "mime_utf8": "mime_b64_utf8",
    "mime_b64_utf8": "mime_b64_utf8",
    "mime": "mime_b64_utf8",
    "mime-utf8": "mime_b64_utf8",
    "mime-b64-utf8": "mime_b64_utf8",
    "mungu_care": "mungu_care",
    "mungu-care": "mungu_care",
    "mungu": "mungu_care",
    "문구케어": "mungu_care",
    "문구케어 인코딩": "mungu_care",
}

SUPPORTED_ENCODINGS: Tuple[str, ...] = (
    "none",
    "html_hex_min",
    "html_dec",
    "html_hex_fixed",
    "html_random",
    "quoted_printable_utf8",
    "quoted_printable_euckr",
    "mime_b64_utf8",
    "mungu_care",
)

HTML_RANDOM_FORMATS: Tuple[str, ...] = ("html_hex_min", "html_dec", "html_hex_fixed")


def normalize_encoding_name(name: Any) -> str:
    """
    Normalize 사용자 입력 인코딩 이름을 내부 표기로 변환한다.
    """
    if not name:
        return "none"
    key = str(name).strip().lower()
    return ENCODING_ALIASES.get(key, "none")


def _encode_html_hex_min(text: str) -> str:
    return "".join(f"&#x{ord(ch):x};" for ch in text)


def _encode_html_hex_fixed(text: str) -> str:
    return "".join(f"&#x{ord(ch):04x};" for ch in text)


def _encode_html_dec(text: str) -> str:
    return "".join(f"&#{ord(ch)};" for ch in text)


def _encode_html_random(text: str, chooser: EncodingChoice) -> str:
    if not text:
        return ""
    pieces: List[str] = []
    for ch in text:
        mode = chooser(HTML_RANDOM_FORMATS)
        if mode == "html_dec":
            pieces.append(_encode_html_dec(ch))
        elif mode == "html_hex_fixed":
            pieces.append(_encode_html_hex_fixed(ch))
        else:
            pieces.append(_encode_html_hex_min(ch))
    return "".join(pieces)


def _encode_quoted_printable(text: str, encoding: str) -> str:
    data = text.encode(encoding, errors="strict")
    return "".join(f"={byte:02X}" for byte in data)


def _encode_mime_b64_utf8(text: str) -> str:
    if not text:
        return "=?UTF-8?B??="
    encoded = base64.b64encode(text.encode("utf-8", errors="strict")).decode("ascii")
    return f"=?UTF-8?B?{encoded}?="


def encode_substitution_value(
    source: Any,
    encoding: Any,
    *,
    random_choice: Optional[EncodingChoice] = None,
    random_generator: Optional[random.Random] = None,
) -> str:
    """
    지정한 인코딩 규칙에 따라 원본 문자열을 치환 문자열로 변환한다.
    """
    text = "" if source is None else str(source)
    mode = normalize_encoding_name(encoding)

    if mode == "none":
        return text
    if mode == "html_hex_min":
        return _encode_html_hex_min(text)
    if mode == "html_hex_fixed":
        return _encode_html_hex_fixed(text)
    if mode == "html_dec":
        return _encode_html_dec(text)
    if mode == "html_random":
        chooser = random_choice or random.choice
        return _encode_html_random(text, chooser)
    if mode == "quoted_printable_utf8":
        return _encode_quoted_printable(text, "utf-8")
    if mode == "quoted_printable_euckr":
        return _encode_quoted_printable(text, "euc-kr")
    if mode == "mime_b64_utf8":
        return _encode_mime_b64_utf8(text)
    if mode == "mungu_care":
        return encode_mungu_care(text, rng=random_generator)
    # 정의되지 않은 값은 기본적으로 원본을 반환한다.
    return text


def encode_substitution_values(
    items: Iterable[Tuple[Any, Any]],
    *,
    random_choice: Optional[EncodingChoice] = None,
    random_generator: Optional[random.Random] = None,
) -> List[str]:
    """
    여러 (원본, 인코딩) 조합을 한 번에 처리한다.
    """
    return [
        encode_substitution_value(
            source,
            encoding,
            random_choice=random_choice,
            random_generator=random_generator,
        )
        for source, encoding in items
    ]
