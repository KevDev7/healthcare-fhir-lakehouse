from importlib.metadata import version

from healthcare_fhir_lakehouse.common.config import load_config


def test_project_foundation_is_wired() -> None:
    config = load_config()

    assert version("healthcare-fhir-lakehouse") == "0.1.0"
    assert config.source_fhir_dir.is_dir()
    assert config.output_dir.is_dir()
