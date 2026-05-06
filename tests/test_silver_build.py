import pytest

from healthcare_fhir_lakehouse.silver.build import SILVER_BUILDERS, build_silver_table


def test_silver_builders_include_core_tables() -> None:
    assert set(SILVER_BUILDERS) == {
        "patient",
        "encounter",
        "observation",
        "condition",
        "medication",
        "medication_ingredient",
        "medication_request",
        "medication_administration",
        "medication_dispense",
        "medication_statement",
        "procedure",
    }


def test_build_silver_table_rejects_unknown_table() -> None:
    with pytest.raises(ValueError, match="Unsupported Silver table"):
        build_silver_table(None, "unknown")  # type: ignore[arg-type]
