from __future__ import annotations

import base64
import random
import unicodedata
from typing import Optional


def _get_random_generator(rng: Optional[random.Random] = None):
    if rng is None:
        return random.random
    return rng.random


def random_decompose(text: str, probability: float = 0.7, *, rng: Optional[random.Random] = None) -> str:
    """
    원본 문구케어 도구와 동일한 방식으로 한글 음절을 확률적으로 분해한다.
    """
    generator = _get_random_generator(rng)
    pieces: list[str] = []
    for ch in text:
        if "가" <= ch <= "힣" and generator() < probability:
            pieces.append(unicodedata.normalize("NFD", ch))
        else:
            pieces.append(ch)
    return "".join(pieces)


def jamo_to_compatibility(text: str) -> str:
    """
    분해된 자모(U+1100 ~ U+11FF)를 호환 자모(U+3130 ~ U+318F)로 변환한다.
    """
    result: list[str] = []
    for ch in text:
        code = ord(ch)
        if 0x1100 <= code <= 0x11FF:
            try:
                name = unicodedata.name(ch)
            except ValueError:
                result.append(ch)
                continue

            replacement_key = None
            for token in ("CHOSEONG", "JUNGSEONG", "JONGSEONG"):
                if token in name:
                    replacement_key = token
                    break

            if replacement_key is None:
                result.append(ch)
                continue

            lookup_name = name.replace(replacement_key, "LETTER")
            try:
                result.append(unicodedata.lookup(lookup_name))
            except KeyError:
                result.append(ch)
        else:
            result.append(ch)
    return "".join(result)


def text_to_mime_encoded(text: str) -> str:
    """
    원본 문구케어 도구와 동일한 방식으로 MIME Encoded-Word를 생성한다.
    """
    encoded_bytes = text.encode("utf-8", errors="strict")
    base64_encoded = base64.b64encode(encoded_bytes).decode("ascii")
    return f"=?utf-8?b?{base64_encoded}?="


def encode_mungu_care(text: str, *, rng: Optional[random.Random] = None, probability: float = 0.7) -> str:
    """
    문구케어 인코딩을 적용한 최종 문자열을 반환한다.
    """
    decomposed = random_decompose(text, probability=probability, rng=rng)
    return text_to_mime_encoded(decomposed)


__all__ = [
    "random_decompose",
    "jamo_to_compatibility",
    "text_to_mime_encoded",
    "encode_mungu_care",
]
