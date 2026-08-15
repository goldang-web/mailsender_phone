# -*- coding: utf-8 -*-
import random
import re
import string
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from .encoding_utils import encode_substitution_value, normalize_encoding_name

SUBSTITUTION_PATTERN = re.compile(r"\$\{([^{}]+)\}")
FIELD_TOKEN_PATTERN = re.compile(r"^필드:([A-Za-z0-9_]+)$")
RANDOM_TOKEN_PATTERN = re.compile(r"^랜덤:([^:]+):(\d+(?:-\d+)?)$")
LIST_TOKEN_PATTERN = re.compile(r"^목록:(.+)$")
RANDOM_TOKEN_MAX_LENGTH = 128
RANDOM_TOKEN_CHARSETS: Dict[str, str] = {
    "영소": string.ascii_lowercase,
    "영대": string.ascii_uppercase,
    "숫자": string.digits,
    "영문": string.ascii_letters,
    "영소숫자": string.ascii_lowercase + string.digits,
    "영대숫자": string.ascii_uppercase + string.digits,
    "영숫자": string.ascii_letters + string.digits,
}
SUBSTITUTION_TARGET_FIELDS = ("helo", "mail_from", "header", "rcpt_to", "anchor_email", "message_id_pattern")


def _sanitize_hostname_component(value: Optional[Any]) -> str:
    raw = str(value or "").strip().lower()
    if not raw:
        return ""
    normalized = re.sub(r"[^a-z0-9.-]+", "-", raw)
    normalized = re.sub(r"-{2,}", "-", normalized)
    normalized = re.sub(r"\.{2,}", ".", normalized)
    normalized = normalized.strip(".-")
    if not normalized:
        return ""
    labels = [label.strip("-") for label in normalized.split(".") if label.strip("-")]
    return ".".join(labels)


def _extract_mail_domain(mail_from: Optional[str]) -> Optional[str]:
    if not mail_from or "@" not in mail_from:
        return None
    _, domain_part = mail_from.rsplit("@", 1)
    candidate = domain_part.strip().strip(">")
    candidate = candidate.strip().strip(".")
    if not candidate:
        return None
    return candidate.lower()


def _resolve_reserved_token(name: str, field_ctx: Optional[Dict[str, Any]]) -> Optional[str]:
    context = field_ctx or {}
    token_name = (name or "").strip().upper()
    if token_name == "MAIL_DOMAIN":
        domain = _extract_mail_domain(context.get("mail_from"))
        sanitized = _sanitize_hostname_component(domain)
        return sanitized or "mailsender"
    if token_name == "HELO":
        sanitized = _sanitize_hostname_component(context.get("helo"))
        if sanitized:
            return sanitized
        fallback = _sanitize_hostname_component(_extract_mail_domain(context.get("mail_from")))
        return fallback or "mailsender"
    if token_name == "HELO_SUFFIX":
        sanitized = _sanitize_hostname_component(context.get("helo"))
        return f".{sanitized}" if sanitized else ""
    return None


def log_substitution_error(message: str) -> None:
    print(f"[SUBSTITUTION] {message}")


def normalize_substitution_mode(value: Any) -> str:
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"list", "목록"}:
            return "list"
    return "static"


def _extract_list_values(data: Dict[str, Any]) -> List[str]:
    candidates: List[Any] = []
    raw_values = data.get("values")
    if isinstance(raw_values, (list, tuple, set)):
        candidates.extend(raw_values)
    elif isinstance(raw_values, str):
        candidates.extend(raw_values.splitlines())
    raw_items = data.get("items")
    if isinstance(raw_items, str):
        candidates.extend(raw_items.splitlines())
    raw_source = data.get("source")
    if isinstance(raw_source, str):
        candidates.extend(raw_source.splitlines())
    raw_value_field = data.get("value")
    if isinstance(raw_value_field, str):
        candidates.extend(raw_value_field.splitlines())

    normalized: List[str] = []
    seen: Set[str] = set()
    for item in candidates:
        if item is None:
            continue
        text = str(item).strip()
        if not text or text in seen:
            continue
        normalized.append(text)
        seen.add(text)
    return normalized


def canonicalize_substitution_rules(
    raw: Any,
    *,
    random_generator: Optional[random.Random] = None,
) -> List[Dict[str, Any]]:
    if not isinstance(raw, (list, tuple)):
        return []

    rng = random_generator or random.SystemRandom()
    seen_keys: Set[str] = set()
    ordered_entries: List[Tuple[str, Dict[str, Any]]] = []
    list_entries: List[Dict[str, Any]] = []
    static_entries: Dict[str, Dict[str, Any]] = {}

    for item in raw:
        if not isinstance(item, dict):
            continue
        key = str(item.get("key") or "").strip()
        if not key:
            continue
        key_token = key.lower()
        if key_token in seen_keys:
            continue
        mode = normalize_substitution_mode(item.get("mode"))
        description = str(item.get("description") or "").strip()
        if mode == "list":
            values = _extract_list_values(item)
            if not values:
                continue
            entry = {
                "key": key,
                "mode": "list",
                "values": values,
                "description": description,
                "source": "",
                "encoding": "none",
                "value": "",
            }
            ordered_entries.append(("list", entry))
            list_entries.append(entry)
            seen_keys.add(key_token)
            continue

        source = str(item.get("source") or "")
        raw_value = item.get("value")
        if not source and raw_value is not None:
            source = str(raw_value)
        if not source:
            continue
        entry = {
            "key": key,
            "source": source,
            "encoding": normalize_encoding_name(item.get("encoding")),
            "value": "",
            "mode": "static",
            "values": [],
            "description": description,
        }
        ordered_entries.append(("static", entry))
        static_entries[key] = entry
        seen_keys.add(key_token)

    list_map: Dict[str, Tuple[str, ...]] = {
        entry["key"]: tuple(entry["values"])
        for entry in list_entries
    }
    static_results: Dict[str, str] = {}
    pending = {key: entry for key, entry in static_entries.items()}
    max_iterations = max(1, len(pending) * 2)

    for _ in range(max_iterations):
        if not pending:
            break
        progressed = False
        removal_queue: List[str] = []
        for key, entry in list(pending.items()):
            context = {
                "static": static_results,
                "lists": list_map,
            }
            substituted, missing = substitute_tokens(
                entry.get("source", ""),
                [],
                random_generator=rng,
                context=context,
            )
            blocking_tokens = []
            unresolved_dependency = False
            for token in missing:
                stripped = token.strip()
                if stripped in pending:
                    unresolved_dependency = True
                    continue
                if stripped in static_results:
                    continue
                blocking_tokens.append(stripped)
            if blocking_tokens or unresolved_dependency:
                continue
            encoded_value = encode_substitution_value(
                substituted,
                entry.get("encoding"),
                random_choice=rng.choice if hasattr(rng, "choice") else None,
                random_generator=rng,
            )
            if not encoded_value:
                continue
            entry["value"] = encoded_value
            static_results[key] = encoded_value
            removal_queue.append(key)
            progressed = True
        for key in removal_queue:
            pending.pop(key, None)
        if not progressed:
            break

    unresolved_keys = set(pending.keys())
    sanitized: List[Dict[str, Any]] = []
    for kind, entry in ordered_entries:
        if kind == "list":
            sanitized.append(entry)
        elif entry.get("key") not in unresolved_keys and entry.get("key") in static_results:
            sanitized.append(entry)
    return sanitized


def build_substitution_context(rules: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    static_map: Dict[str, str] = {}
    list_map: Dict[str, Tuple[str, ...]] = {}
    for rule in rules or []:
        if not isinstance(rule, dict):
            continue
        key = str(rule.get("key") or "").strip()
        if not key:
            continue
        mode = normalize_substitution_mode(rule.get("mode"))
        if mode == "list":
            values_field = rule.get("values")
            values: List[str] = []
            if isinstance(values_field, (list, tuple)):
                seen: Set[str] = set()
                for item in values_field:
                    if item is None:
                        continue
                    text = str(item).strip()
                    if not text or text in seen:
                        continue
                    values.append(text)
                    seen.add(text)
            if values:
                list_map[key] = tuple(values)
            continue
        value_field = rule.get("value")
        if isinstance(value_field, str) and value_field:
            static_map[key] = value_field
    return {"static": static_map, "lists": list_map}


def _parse_random_length(spec: str) -> Tuple[int, int]:
    cleaned = (spec or "").strip()
    if not cleaned:
        raise ValueError("길이 정보가 비어 있습니다.")
    if "-" in cleaned:
        start_str, end_str = cleaned.split("-", 1)
    else:
        start_str = cleaned
        end_str = cleaned
    try:
        min_len = int(start_str)
        max_len = int(end_str)
    except (TypeError, ValueError) as exc:
        raise ValueError("길이는 정수로 입력해야 합니다.") from exc
    if min_len <= 0 or max_len <= 0:
        raise ValueError("길이는 1 이상이어야 합니다.")
    if min_len > max_len:
        raise ValueError("최소 길이가 최대 길이보다 큽니다.")
    if max_len > RANDOM_TOKEN_MAX_LENGTH:
        raise ValueError(f"최대 길이는 {RANDOM_TOKEN_MAX_LENGTH} 이하로 입력하세요.")
    return min_len, max_len


def _resolve_random_token(kind: str, length_spec: str, rng: random.Random) -> str:
    normalized_kind = (kind or "").strip()
    charset = RANDOM_TOKEN_CHARSETS.get(normalized_kind)
    if charset is None:
        charset = RANDOM_TOKEN_CHARSETS.get(normalized_kind.lower())
    if charset is None:
        raise ValueError(f"지원하지 않는 랜덤 조합입니다: {normalized_kind or '?'}")
    min_len, max_len = _parse_random_length(length_spec)
    length = rng.randint(min_len, max_len)
    if length <= 0:
        return ""
    return "".join(rng.choice(charset) for _ in range(length))


def _choose_list_value(values: Sequence[str], rng: random.Random) -> str:
    if not values:
        raise ValueError("목록 패턴 값이 비어 있습니다.")
    if hasattr(rng, "randrange"):
        index = rng.randrange(len(values))
    else:
        random_func = getattr(rng, "random", None)
        index = int(float(random_func()) * len(values)) if callable(random_func) else 0
    if index >= len(values):
        index = len(values) - 1
    if index < 0:
        index = 0
    return values[index]


def substitute_tokens(
    value: Any,
    rules: List[Dict[str, Any]],
    *,
    random_generator: Optional[random.Random] = None,
    context: Optional[Dict[str, Dict[str, Any]]] = None,
) -> Tuple[Any, Set[str]]:
    if not isinstance(value, str) or not value or "${" not in value:
        return value, set()
    ctx = context or build_substitution_context(rules or [])
    static_map = ctx.get("static") or {}
    list_map = ctx.get("lists") or {}
    field_ctx: Dict[str, Any] = {}
    raw_field_ctx = ctx.get("fields") if isinstance(ctx, dict) else None
    if isinstance(raw_field_ctx, dict):
        field_ctx = raw_field_ctx
    rng = random_generator or random.SystemRandom()
    result = value
    missing: Set[str] = set()
    max_iterations = max(1, len(static_map) + len(list_map) + 5)

    for _ in range(max_iterations):
        changed = False

        def replace(match: re.Match) -> str:
            nonlocal changed
            token_raw = match.group(1)
            if token_raw in static_map:
                changed = True
                return static_map[token_raw]
            stripped = token_raw.strip()
            random_match = RANDOM_TOKEN_PATTERN.match(stripped)
            if random_match:
                kind_raw = random_match.group(1)
                length_spec = random_match.group(2)
                try:
                    replacement = _resolve_random_token(kind_raw, length_spec, rng)
                except ValueError as exc:
                    log_substitution_error(f"랜덤 패턴 처리 실패({stripped}): {exc}")
                    missing.add(stripped)
                    return ""
                changed = True
                return replacement
            list_match = LIST_TOKEN_PATTERN.match(stripped)
            if list_match:
                list_name = list_match.group(1).strip()
                if not list_name:
                    log_substitution_error("목록 패턴 이름이 비어 있습니다.")
                    missing.add(stripped)
                    return ""
                values = list_map.get(list_name)
                if values:
                    try:
                        replacement = _choose_list_value(values, rng)
                    except ValueError:
                        log_substitution_error(f"'{list_name}' 목록이 정의되지 않았습니다.")
                        missing.add(f"목록:{list_name}")
                        return ""
                    changed = True
                    return replacement
                missing.add(f"목록:{list_name}")
                log_substitution_error(f"'{list_name}' 목록이 비어 있거나 정의되지 않았습니다.")
                return ""
            reserved_value = _resolve_reserved_token(stripped, field_ctx)
            if reserved_value is not None:
                changed = True
                return reserved_value
            field_match = FIELD_TOKEN_PATTERN.match(stripped)
            if field_match:
                key_name = field_match.group(1)
                replacement = str(field_ctx.get(key_name, ""))
                changed = True
                return replacement
            if token_raw in static_map:
                changed = True
                return static_map[token_raw]
            return match.group(0)

        new_result = SUBSTITUTION_PATTERN.sub(replace, result)
        if new_result == result:
            break
        result = new_result

    leftovers = SUBSTITUTION_PATTERN.findall(result)
    static_keys = set(static_map.keys())
    for token in leftovers:
        if token in static_keys:
            missing.add(token)
            continue
        stripped = token.strip()
        if RANDOM_TOKEN_PATTERN.match(stripped) or LIST_TOKEN_PATTERN.match(stripped):
            missing.add(stripped)
            continue
        missing.add(token)
    return result, missing


def apply_substitutions_to_config(
    config: Dict[str, Any],
    rules: List[Dict[str, Any]],
    *,
    context: Optional[Dict[str, Dict[str, Any]]] = None,
    random_generator: Optional[random.Random] = None,
) -> Set[str]:
    if not config or not rules:
        return set()
    ctx_base = context or build_substitution_context(rules)
    ctx = dict(ctx_base)
    rng = random_generator or random.SystemRandom()
    missing: Set[str] = set()
    for field in SUBSTITUTION_TARGET_FIELDS:
        raw_value = config.get(field)
        ctx["fields"] = config
        substituted, unresolved = substitute_tokens(
            raw_value,
            rules,
            random_generator=rng,
            context=ctx,
        )
        if isinstance(substituted, str):
            config[field] = substituted
        missing.update(unresolved)
    return missing


def resolve_config(
    template: Dict[str, Any],
    rules: List[Dict[str, Any]],
    *,
    random_generator: Optional[random.Random] = None,
) -> Tuple[Dict[str, Any], Set[str]]:
    working = dict(template or {})
    rng = random_generator or random.SystemRandom()
    context = build_substitution_context(rules or [])
    missing = apply_substitutions_to_config(
        working,
        rules or [],
        context=context,
        random_generator=rng,
    )
    return working, missing
