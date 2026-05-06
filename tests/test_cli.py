import gzip
import json
from pathlib import Path

from typer.testing import CliRunner

from healthcare_fhir_lakehouse.cli import app

runner = CliRunner()


def write_gzipped_ndjson(path: Path, rows: list[dict[str, object]]) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as file:
        for row in rows:
            file.write(f"{json.dumps(row)}\n")


def write_test_config(tmp_path: Path) -> Path:
    source_dir = tmp_path / "fhir"
    output_dir = tmp_path / "output"
    source_dir.mkdir()
    output_dir.mkdir()
    write_gzipped_ndjson(
        source_dir / "MimicPatient.ndjson.gz",
        [
            {
                "id": "patient-1",
                "resourceType": "Patient",
                "meta": {"profile": ["patient-profile"]},
            }
        ],
    )
    config_path = tmp_path / "local.toml"
    config_path.write_text(
        f"""
[paths]
source_dataset_dir = "{tmp_path}"
source_fhir_dir = "{source_dir}"
output_dir = "{output_dir}"

[dataset]
name = "test-dataset"
version = "0.0.1"
""".strip(),
        encoding="utf-8",
    )
    return config_path


def test_version_command() -> None:
    result = runner.invoke(app, ["version"])

    assert result.exit_code == 0
    assert result.stdout.strip()


def test_config_command_prints_resolved_paths() -> None:
    result = runner.invoke(app, ["config"])

    assert result.exit_code == 0
    assert "dataset.name=mimic-iv-clinical-database-demo-on-fhir" in result.stdout
    assert "source_fhir_dir=" in result.stdout
    assert "output_dir=" in result.stdout


def test_doctor_command_passes_for_current_scaffold(tmp_path: Path) -> None:
    config_path = write_test_config(tmp_path)

    result = runner.invoke(app, ["doctor", "--config", str(config_path)])

    assert result.exit_code == 0
    assert "Project check passed." in result.stdout


def test_profile_inventory_command_writes_artifact(tmp_path: Path) -> None:
    config_path = write_test_config(tmp_path)

    result = runner.invoke(app, ["profile", "inventory", "--config", str(config_path)])

    assert result.exit_code == 0
    assert "Wrote resource inventory:" in result.stdout


def test_profile_schema_command_writes_artifact(tmp_path: Path) -> None:
    config_path = write_test_config(tmp_path)

    result = runner.invoke(
        app,
        ["profile", "schema", "--config", str(config_path), "--sample-limit", "1"],
    )

    assert result.exit_code == 0
    assert "Wrote schema profile:" in result.stdout


def test_profile_report_command_writes_artifact(tmp_path: Path) -> None:
    config_path = write_test_config(tmp_path)

    result = runner.invoke(
        app,
        ["profile", "report", "--config", str(config_path), "--sample-limit", "1"],
    )

    assert result.exit_code == 0
    assert "Wrote source data profile:" in result.stdout


def test_bronze_ingest_and_validate_commands(tmp_path: Path) -> None:
    config_path = write_test_config(tmp_path)

    ingest_result = runner.invoke(
        app,
        ["bronze", "ingest", "--config", str(config_path), "--batch-size", "1"],
    )
    validate_result = runner.invoke(
        app,
        ["bronze", "validate", "--config", str(config_path)],
    )

    assert ingest_result.exit_code == 0
    assert "Wrote Bronze rows: 1" in ingest_result.stdout
    assert validate_result.exit_code == 0
    assert "Bronze validation passed." in validate_result.stdout


def test_silver_build_rejects_unknown_table() -> None:
    result = runner.invoke(app, ["silver", "build", "unknown"])

    assert result.exit_code != 0
    assert "Unsupported table" in result.output


def test_gold_build_rejects_unknown_table() -> None:
    result = runner.invoke(app, ["gold", "build", "unknown"])

    assert result.exit_code != 0
    assert "Unsupported table" in result.output


def test_relationship_commands_fail_cleanly_without_silver_tables(
    tmp_path: Path,
) -> None:
    config_path = write_test_config(tmp_path)

    result = runner.invoke(
        app,
        ["relationships", "audit", "--config", str(config_path)],
    )

    assert result.exit_code != 0


def test_privacy_commands_fail_cleanly_without_silver_tables(tmp_path: Path) -> None:
    config_path = write_test_config(tmp_path)

    result = runner.invoke(
        app,
        ["privacy", "audit", "--config", str(config_path)],
    )

    assert result.exit_code != 0


def test_quality_commands_fail_cleanly_without_outputs(tmp_path: Path) -> None:
    config_path = write_test_config(tmp_path)

    result = runner.invoke(
        app,
        ["quality", "check", "--config", str(config_path)],
    )

    assert result.exit_code != 0
    assert "Data quality status: failed" in result.output
