from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from healthcare_fhir_lakehouse.common.config import (
    DatasetSettings,
    PathSettings,
    ProjectConfig,
)
from healthcare_fhir_lakehouse.privacy.audit import (
    build_privacy_audit,
    render_privacy_report,
    scan_column_for_patterns,
)


def write_silver_table(tmp_path: Path, table_name: str, rows: list[dict]) -> None:
    output_dir = tmp_path / "output" / "silver" / table_name
    output_dir.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(rows), output_dir / "part-00001.parquet")


def write_core_silver_fixture(
    tmp_path: Path,
    observation_value: str = "Normal",
) -> None:
    lineage = {
        "source_file": "source.ndjson.gz",
        "resource_family": "Mimic",
        "profile_url": "http://example.org/profile",
        "source_dataset_name": "dataset",
        "source_dataset_version": "1",
        "bronze_ingested_at": "now",
        "bronze_resource_id": "bronze-1",
    }
    write_silver_table(
        tmp_path,
        "patient",
        [
            {
                "patient_id": "patient-1",
                "source_patient_identifier": "10007795",
                "synthetic_patient_name": "Patient_10007795",
                "birth_date": "2083-04-10",
                "gender": "female",
                **lineage,
            }
        ],
    )
    write_silver_table(
        tmp_path,
        "encounter",
        [
            {
                "encounter_id": "encounter-1",
                "patient_id": "patient-1",
                "start_datetime": "2180-05-06T22:23:00-04:00",
                "end_datetime": "2180-05-07T17:15:00-04:00",
                "admit_source": "EMERGENCY ROOM",
                "discharge_disposition": "HOME",
                **lineage,
            }
        ],
    )
    write_silver_table(
        tmp_path,
        "observation",
        [
            {
                "observation_id": "observation-1",
                "patient_id": "patient-1",
                "encounter_id": "encounter-1",
                "effective_datetime": "2180-05-06T22:23:00-04:00",
                "issued_datetime": "2180-05-06T22:24:00-04:00",
                "display": "Heart Rate",
                "value": observation_value,
                **lineage,
            }
        ],
    )
    write_silver_table(
        tmp_path,
        "condition",
        [
            {
                "condition_id": "condition-1",
                "patient_id": "patient-1",
                "encounter_id": "encounter-1",
                "display": "Hypertension",
                **lineage,
            }
        ],
    )


def make_config(tmp_path: Path) -> ProjectConfig:
    return ProjectConfig(
        repo_root=tmp_path,
        paths=PathSettings(output_dir="output"),
        dataset=DatasetSettings(name="test-dataset", version="1"),
    )


def test_build_privacy_audit_reports_expected_silver_sensitive_columns(
    tmp_path: Path,
) -> None:
    write_core_silver_fixture(tmp_path)

    audit = build_privacy_audit(make_config(tmp_path))

    assert audit.passed is True
    assert any(
        finding.table_name == "patient"
        and finding.column_name == "source_patient_identifier"
        and finding.present
        for finding in audit.column_findings
    )
    assert audit.pattern_findings == []


def test_scan_column_for_patterns_returns_needs_review_findings(
    tmp_path: Path,
) -> None:
    write_core_silver_fixture(tmp_path, observation_value="call 555-123-4567")

    findings = scan_column_for_patterns(make_config(tmp_path), "observation", "value")

    assert len(findings) == 1
    assert findings[0].pattern_name == "phone"
    assert findings[0].match_count == 1


def test_render_privacy_report_includes_safe_harbor_boundary(tmp_path: Path) -> None:
    write_core_silver_fixture(tmp_path)
    audit = build_privacy_audit(make_config(tmp_path))

    report = render_privacy_report(audit)

    assert "# Privacy Audit" in report
    assert "not a legal HIPAA compliance certification" in report
    assert "source_patient_identifier" in report
