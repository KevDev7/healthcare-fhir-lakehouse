from pathlib import Path

from healthcare_fhir_lakehouse.common.config import load_config


def test_default_config_resolves_project_paths() -> None:
    config = load_config()

    assert config.repo_root.name == "healthcare-fhir-lakehouse"
    assert config.source_dataset_dir == (
        config.repo_root / "mimic-iv-clinical-database-demo-on-fhir-2.1.0"
    )
    assert config.source_fhir_dir == (
        config.repo_root / "mimic-iv-clinical-database-demo-on-fhir-2.1.0" / "fhir"
    )
    assert config.output_dir == config.repo_root / "output"


def test_config_file_overrides_defaults(tmp_path: Path) -> None:
    config_path = tmp_path / "local.toml"
    config_path.write_text(
        """
[paths]
source_dataset_dir = "custom-dataset"
source_fhir_dir = "custom-dataset/fhir"
output_dir = "custom-output"

[dataset]
name = "custom"
version = "0.0.1"
""".strip(),
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.dataset.name == "custom"
    assert config.dataset.version == "0.0.1"
    assert config.source_dataset_dir == config.repo_root / "custom-dataset"
    assert config.source_fhir_dir == config.repo_root / "custom-dataset" / "fhir"
    assert config.output_dir == config.repo_root / "custom-output"
