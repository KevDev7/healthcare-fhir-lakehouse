from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class PrivacyPattern:
    name: str
    regex: str
    description: str


PRIVACY_PATTERNS = (
    PrivacyPattern(
        name="email",
        regex=r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b",
        description="Email-address-like value.",
    ),
    PrivacyPattern(
        name="phone",
        regex=r"\b(?:\+?1[-.\s]?)?(?:\(?\d{3}\)?[-.\s])\d{3}[-.\s]\d{4}\b",
        description="US phone-number-like value.",
    ),
    PrivacyPattern(
        name="ssn",
        regex=r"\b\d{3}-\d{2}-\d{4}\b",
        description="US SSN-like value.",
    ),
    PrivacyPattern(
        name="ipv4",
        regex=(
            r"\b(?:(?:25[0-5]|2[0-4]\d|1?\d?\d)\.){3}"
            r"(?:25[0-5]|2[0-4]\d|1?\d?\d)\b"
        ),
        description="IPv4-address-like value.",
    ),
    PrivacyPattern(
        name="url",
        regex=r"\bhttps?://[^\s]+",
        description="URL-like value.",
    ),
)

PATTERN_SCAN_COLUMNS = {
    "patient": ("source_patient_identifier", "synthetic_patient_name"),
    "encounter": ("admit_source", "discharge_disposition"),
    "observation": ("display", "value"),
    "condition": ("display",),
    "medication": ("medication_display", "medication_text"),
    "medication_request": ("medication_display",),
    "medication_administration": ("medication_display",),
    "medication_dispense": ("medication_display", "medication_text"),
    "medication_statement": ("medication_display", "medication_text"),
    "procedure": ("procedure_display", "body_site_display"),
}


def find_privacy_pattern_matches(value: object) -> dict[str, list[str]]:
    if value is None:
        return {}

    text = str(value)
    matches: dict[str, list[str]] = {}
    for pattern in PRIVACY_PATTERNS:
        pattern_matches = sorted(set(re.findall(pattern.regex, text)))
        if pattern_matches:
            matches[pattern.name] = pattern_matches
    return matches


__all__ = [
    "PATTERN_SCAN_COLUMNS",
    "PRIVACY_PATTERNS",
    "PrivacyPattern",
    "find_privacy_pattern_matches",
]
