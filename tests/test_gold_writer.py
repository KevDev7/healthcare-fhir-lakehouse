from healthcare_fhir_lakehouse.common.config import PathSettings, ProjectConfig
from healthcare_fhir_lakehouse.gold.writer import gold_parquet_glob, write_gold_query


def test_write_gold_query_writes_repeatable_parquet_output(tmp_path) -> None:
    config = ProjectConfig(
        repo_root=tmp_path,
        paths=PathSettings(output_dir="output"),
    )

    result = write_gold_query(config, "demo", "select 1 as value")

    assert result.total_rows == 1
    assert result.parquet_file.exists()
    assert gold_parquet_glob(config, "demo").endswith("output/gold/demo/*.parquet")
