import pytest

from healthcare_fhir_lakehouse.common.table_registry import SILVER_TABLE_NAMES
from healthcare_fhir_lakehouse.silver.build import SILVER_BUILDERS, build_silver_table


def test_silver_builders_match_table_registry() -> None:
    assert tuple(SILVER_BUILDERS) == SILVER_TABLE_NAMES


def test_build_silver_table_rejects_unknown_table() -> None:
    with pytest.raises(ValueError, match="Unsupported Silver table"):
        build_silver_table(None, "unknown")  # type: ignore[arg-type]
