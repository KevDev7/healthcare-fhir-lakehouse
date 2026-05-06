from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from healthcare_fhir_lakehouse.common.config import PathSettings, ProjectConfig
from healthcare_fhir_lakehouse.gold.build import build_all_gold_tables
from healthcare_fhir_lakehouse.gold.condition_summary import build_condition_summary
from healthcare_fhir_lakehouse.gold.encounter_summary import build_encounter_summary
from healthcare_fhir_lakehouse.gold.observation_daily import (
    build_labs_daily,
    build_vitals_daily,
)
from healthcare_fhir_lakehouse.gold.validation import validate_gold_tables
from healthcare_fhir_lakehouse.gold.writer import gold_parquet_glob


def write_parquet(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


def write_silver_gold_fixture(tmp_path: Path) -> ProjectConfig:
    output = tmp_path / "output" / "silver"
    write_parquet(
        output / "encounter" / "part-00001.parquet",
        [
            {
                "encounter_id": "enc-1",
                "patient_id": "pat-1",
                "status": "finished",
                "class_code": "EMER",
                "class_display": "emergency",
                "start_datetime": "2180-05-06T22:00:00-04:00",
                "end_datetime": "2180-05-07T00:00:00-04:00",
                "discharge_disposition": "HOME",
            }
        ],
    )
    write_parquet(
        output / "observation" / "part-00001.parquet",
        [
            {
                "observation_id": "obs-1",
                "patient_id": "pat-1",
                "encounter_id": "enc-1",
                "category_code": "Routine Vital Signs",
                "display": "Heart Rate",
                "value": "80",
                "unit": "beats/min",
                "effective_datetime": "2180-05-06T23:00:00-04:00",
            },
            {
                "observation_id": "obs-2",
                "patient_id": "pat-1",
                "encounter_id": "enc-1",
                "category_code": "laboratory",
                "display": "Hemoglobin",
                "value": "12.5",
                "unit": "g/dL",
                "effective_datetime": "2180-05-07T01:00:00-04:00",
            },
        ],
    )
    write_parquet(
        output / "condition" / "part-00001.parquet",
        [
            {
                "condition_id": "cond-1",
                "patient_id": "pat-1",
                "encounter_id": "enc-1",
                "code": "I10",
                "display": "Hypertension",
            }
        ],
    )
    return ProjectConfig(repo_root=tmp_path, paths=PathSettings(output_dir="output"))


def test_build_encounter_and_condition_gold_tables(tmp_path: Path) -> None:
    config = write_silver_gold_fixture(tmp_path)

    encounter_result = build_encounter_summary(config)
    condition_result = build_condition_summary(config)

    assert encounter_result.total_rows == 1
    assert condition_result.total_rows == 1
    encounter_row = duckdb.sql(
        "select * from read_parquet(?)",
        params=[gold_parquet_glob(config, "encounter_summary")],
    ).fetchone()
    assert encounter_row is not None


def test_build_observation_daily_gold_tables(tmp_path: Path) -> None:
    config = write_silver_gold_fixture(tmp_path)

    vitals_result = build_vitals_daily(config)
    labs_result = build_labs_daily(config)

    assert vitals_result.total_rows == 1
    assert labs_result.total_rows == 1


def test_validate_gold_tables_accepts_publishable_column_surface(
    tmp_path: Path,
) -> None:
    config = write_silver_gold_fixture(tmp_path)

    build_all_gold_tables(config)
    results = validate_gold_tables(config)

    assert {result.table_name for result in results} == {
        "encounter_summary",
        "condition_summary",
        "vitals_daily",
        "labs_daily",
    }
