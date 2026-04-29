"""Stateless utilities for AMO API clients."""

RETRYABLE_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}
MAX_CUSTOM_FIELD_VALUE_LEN = 256


def compute_retry_delay(attempt: int, retry_after_header: str | None = None) -> float:
    if retry_after_header:
        try:
            return max(1.0, float(retry_after_header))
        except ValueError:
            pass
    return min(30.0, float(2 ** (attempt - 1)))


def trim_text(value: str, max_len: int = 700) -> str:
    if len(value) <= max_len:
        return value
    return f"{value[:max_len]}...<truncated>"


def sanitize_custom_field_value(value, max_len: int = MAX_CUSTOM_FIELD_VALUE_LEN) -> str:
    s = str(value)
    if len(s) <= max_len:
        return s
    return s[:max_len]
