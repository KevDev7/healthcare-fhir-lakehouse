from __future__ import annotations

import re
from pathlib import Path

from healthcare_fhir_lakehouse.common.table_registry import (
    GOLD_TABLES,
    SILVER_TABLE_NAMES,
    SILVER_TABLES,
)
from healthcare_fhir_lakehouse.silver.relationships import (
    MISSING_REFERENCE_SPECS,
    ORPHAN_REFERENCE_SPECS,
    ROW_COUNT_SPECS,
)

ROOT = Path(__file__).resolve().parents[1]
CLOUD_PIPELINE = ROOT / "src" / "healthcare_fhir_lakehouse_spark" / "cloud_pipeline.py"


def cloud_source() -> str:
    return CLOUD_PIPELINE.read_text(encoding="utf-8")


def function_body(source: str, function_name: str) -> str:
    match = re.search(
        rf"^def {function_name}\(.*?\n(?=^def |\Z)",
        source,
        flags=re.DOTALL | re.MULTILINE,
    )
    assert match is not None, f"missing cloud function {function_name}"
    return match.group(0)


def test_cloud_silver_build_order_matches_local_registry() -> None:
    body = function_body(cloud_source(), "build_silver")
    actual_calls = re.findall(r"build_([a-z_]+)\(spark, config\)", body)
    expected_calls = [
        table_name
        for table_name in SILVER_TABLE_NAMES
        if table_name != "medication_ingredient"
    ]
    assert actual_calls == expected_calls


def test_cloud_relationship_audit_covers_local_metric_specs() -> None:
    source = cloud_source()
    metric_names = (
        *(spec.metric_name for spec in ROW_COUNT_SPECS),
        *(spec.metric_name for spec in MISSING_REFERENCE_SPECS),
        *(spec.metric_name for spec in ORPHAN_REFERENCE_SPECS),
    )
    for metric_name in metric_names:
        assert f"as {metric_name}" in source


def test_cloud_data_quality_checks_cover_local_tables() -> None:
    source = cloud_source()
    for spec in SILVER_TABLES:
        assert f"silver_{spec.name}_rows" in source
    for spec in GOLD_TABLES:
        assert f"gold_{spec.name}_rows" in source


def test_cloud_data_quality_orphan_check_covers_relationship_specs() -> None:
    body = function_body(cloud_source(), "build_data_quality")
    for spec in ORPHAN_REFERENCE_SPECS:
        assert spec.metric_name in body
