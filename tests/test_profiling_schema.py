import gzip
import json
from pathlib import Path

from healthcare_fhir_lakehouse.common.config import ProjectConfig
from healthcare_fhir_lakehouse.ingest.profiling import (
    SCHEMA_PROFILE_FILENAME,
    build_schema_profile,
    get_path_value,
    write_schema_profile,
)


def write_gzipped_ndjson(path: Path, rows: list[dict[str, object]]) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as file:
        for row in rows:
            file.write(f"{json.dumps(row)}\n")


def test_get_path_value_reads_nested_dict_values() -> None:
    resource = {
        "meta": {"profile": ["profile-url"]},
        "subject": {"reference": "Patient/123"},
    }

    assert get_path_value(resource, "meta.profile") == ["profile-url"]
    assert get_path_value(resource, "subject.reference") == "Patient/123"
    assert get_path_value(resource, "encounter.reference") is None


def test_build_schema_profile_samples_keys_and_field_coverage(tmp_path: Path) -> None:
    source_dir = tmp_path / "fhir"
    source_dir.mkdir()
    write_gzipped_ndjson(
        source_dir / "A.ndjson.gz",
        [
            {
                "id": "a-1",
                "resourceType": "Observation",
                "meta": {"profile": ["observation-profile"]},
                "subject": {"reference": "Patient/1"},
                "encounter": {"reference": "Encounter/1"},
                "effectiveDateTime": "2180-01-01T00:00:00-04:00",
            },
            {"id": "a-2", "resourceType": "Observation"},
        ],
    )
    config = ProjectConfig(
        repo_root=tmp_path,
        paths={
            "source_dataset_dir": "dataset",
            "source_fhir_dir": "fhir",
            "output_dir": "output",
        },
    )

    profile = build_schema_profile(config, sample_limit=10)

    assert profile.total_files == 1
    assert profile.sample_limit_per_file == 10
    assert profile.files[0].sampled_rows == 2
    assert profile.files[0].top_level_keys == [
        "effectiveDateTime",
        "encounter",
        "id",
        "meta",
        "resourceType",
        "subject",
    ]
    assert profile.files[0].field_coverage["id"] == 2
    assert profile.files[0].field_coverage["resourceType"] == 2
    assert profile.files[0].field_coverage["meta.profile"] == 1
    assert profile.files[0].field_coverage["subject.reference"] == 1
    assert profile.files[0].field_coverage["encounter.reference"] == 1
    assert profile.files[0].field_coverage["effectiveDateTime"] == 1


def test_write_schema_profile_creates_json_artifact(tmp_path: Path) -> None:
    source_dir = tmp_path / "fhir"
    source_dir.mkdir()
    config = ProjectConfig(
        repo_root=tmp_path,
        paths={
            "source_dataset_dir": "dataset",
            "source_fhir_dir": "fhir",
            "output_dir": "output",
        },
    )
    profile = build_schema_profile(config)

    output_path = write_schema_profile(profile, config.output_dir)

    assert output_path == tmp_path / "output" / "profiling" / SCHEMA_PROFILE_FILENAME
    assert json.loads(output_path.read_text(encoding="utf-8"))["total_files"] == 0
