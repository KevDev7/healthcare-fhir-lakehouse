from importlib.metadata import version
from pathlib import Path

from healthcare_fhir_lakehouse.common.config import load_config


def test_project_foundation_is_wired(tmp_path: Path) -> None:
    source_dir = tmp_path / "fhir"
    output_dir = tmp_path / "output"
    source_dir.mkdir()
    output_dir.mkdir()
    config_path = tmp_path / "local.toml"
    config_path.write_text(
        f"""
[paths]
source_dataset_dir = "{tmp_path}"
source_fhir_dir = "{source_dir}"
output_dir = "{output_dir}"
""".strip(),
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert version("healthcare-fhir-lakehouse") == "0.1.0"
    assert config.source_fhir_dir.is_dir()
    assert config.output_dir.is_dir()
