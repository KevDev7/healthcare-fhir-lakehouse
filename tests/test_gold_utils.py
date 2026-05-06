from healthcare_fhir_lakehouse.gold.utils import (
    duckdb_escape_identifier,
    stable_key,
    timestamp_from_iso_text_sql,
)


def test_stable_key_is_namespaced_and_repeatable() -> None:
    assert stable_key("patient", "abc") == stable_key("patient", "abc")
    assert stable_key("patient", "abc") != stable_key("encounter", "abc")
    assert stable_key("patient", None) is None


def test_timestamp_from_iso_text_sql_escapes_identifier() -> None:
    assert duckdb_escape_identifier('bad"name') == '"bad""name"'
    assert timestamp_from_iso_text_sql("effective_datetime") == (
        'try_cast(substr("effective_datetime", 1, 19) as timestamp)'
    )
