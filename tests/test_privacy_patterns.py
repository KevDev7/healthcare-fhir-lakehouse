from healthcare_fhir_lakehouse.privacy.patterns import find_privacy_pattern_matches


def test_find_privacy_pattern_matches_detects_expected_patterns() -> None:
    matches = find_privacy_pattern_matches(
        "email a@example.com phone 555-123-4567 ssn 123-45-6789 "
        "ip 192.168.1.1 url https://example.org/path"
    )

    assert matches["email"] == ["a@example.com"]
    assert matches["phone"] == ["555-123-4567"]
    assert matches["ssn"] == ["123-45-6789"]
    assert matches["ipv4"] == ["192.168.1.1"]
    assert matches["url"] == ["https://example.org/path"]


def test_find_privacy_pattern_matches_avoids_plain_source_subject_ids() -> None:
    matches = find_privacy_pattern_matches("Patient_10007795")

    assert matches == {}
