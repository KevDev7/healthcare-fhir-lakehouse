import gzip
import json
from pathlib import Path

from healthcare_fhir_lakehouse.common.config import ProjectConfig
from healthcare_fhir_lakehouse.ingest.profiling import (
    RESOURCE_INVENTORY_FILENAME,
    build_resource_inventory,
    write_resource_inventory,
)


def write_gzipped_ndjson(path: Path, rows: list[dict[str, object]]) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as file:
        for row in rows:
            file.write(f"{json.dumps(row)}\n")


def test_build_resource_inventory_counts_files_and_rows(tmp_path: Path) -> None:
    source_dir = tmp_path / "fhir"
    source_dir.mkdir()
    write_gzipped_ndjson(
        source_dir / "A.ndjson.gz",
        [
            {
                "id": "a-1",
                "resourceType": "Patient",
                "meta": {"profile": ["patient-profile"]},
            },
            {"id": "a-2", "resourceType": "Patient"},
        ],
    )
    write_gzipped_ndjson(
        source_dir / "B.ndjson.gz",
        [{"id": "b-1", "resourceType": "Encounter"}],
    )
    config = ProjectConfig(
        repo_root=tmp_path,
        paths={
            "source_dataset_dir": "dataset",
            "source_fhir_dir": "fhir",
            "output_dir": "output",
        },
    )

    profile = build_resource_inventory(config)

    assert profile.total_files == 2
    assert profile.total_resources == 3
    assert profile.files[0].source_file == "A.ndjson.gz"
    assert profile.files[0].resource_type == "Patient"
    assert profile.files[0].profile_url == "patient-profile"
    assert profile.files[0].row_count == 2
    assert profile.files[1].resource_type == "Encounter"
    assert profile.files[1].row_count == 1


def test_write_resource_inventory_creates_json_artifact(tmp_path: Path) -> None:
    (tmp_path / "fhir").mkdir()
    config = ProjectConfig(repo_root=tmp_path)
    profile = build_resource_inventory(
        ProjectConfig(
            repo_root=tmp_path,
            paths={
                "source_dataset_dir": "dataset",
                "source_fhir_dir": "fhir",
                "output_dir": "output",
            },
        )
    )

    output_path = write_resource_inventory(profile, config.output_dir)

    assert output_path == (
        tmp_path / "output" / "profiling" / RESOURCE_INVENTORY_FILENAME
    )
    assert json.loads(output_path.read_text(encoding="utf-8"))["total_files"] == 0
