from __future__ import annotations

from healthcare_fhir_lakehouse.common.config import ProjectConfig
from healthcare_fhir_lakehouse.gold.writer import (
    GoldWriteResult,
    write_registered_gold_query,
)
from healthcare_fhir_lakehouse.silver.writer import silver_parquet_glob

VITALS_DAILY_TABLE = "vitals_daily"
LABS_DAILY_TABLE = "labs_daily"

VITAL_DISPLAYS = (
    "Heart Rate",
    "Respiratory Rate",
    "O2 saturation pulseoxymetry",
    "Temperature Fahrenheit",
    "Non Invasive Blood Pressure systolic",
    "Non Invasive Blood Pressure diastolic",
    "Non Invasive Blood Pressure mean",
)


def quoted_values(values: tuple[str, ...]) -> str:
    return ", ".join("'" + value.replace("'", "''") + "'" for value in values)


def build_vitals_daily(config: ProjectConfig) -> GoldWriteResult:
    where_clause = f"display in ({quoted_values(VITAL_DISPLAYS)})"
    return build_observation_daily(config, VITALS_DAILY_TABLE, where_clause)


def build_labs_daily(config: ProjectConfig) -> GoldWriteResult:
    return build_observation_daily(
        config,
        LABS_DAILY_TABLE,
        "category_code in ('laboratory', 'Labs')",
    )


def build_observation_daily(
    config: ProjectConfig,
    table_name: str,
    where_clause: str,
) -> GoldWriteResult:
    observation_glob = silver_parquet_glob(config, "observation")
    encounter_glob = silver_parquet_glob(config, "encounter")

    sql = f"""
    with observation_base as (
      select
        patient_id,
        encounter_id,
        display,
        unit,
        try_cast(value as double) as numeric_value,
        try_cast(substr(effective_datetime, 1, 19) as timestamp) as event_ts
      from read_parquet(?)
      where {where_clause}
    ),
    numeric_observations as (
      select *
      from observation_base
      where numeric_value is not null
        and event_ts is not null
        and patient_id is not null
    ),
    enriched as (
      select
        o.*,
        try_cast(substr(e.start_datetime, 1, 19) as timestamp) as encounter_start_ts,
        min(o.event_ts) over (partition by o.patient_id) as patient_first_event_ts
      from numeric_observations o
      left join read_parquet(?) e on o.encounter_id = e.encounter_id
    )
    select
      md5('patient:' || patient_id) as patient_key,
      case
        when encounter_id is not null then md5('encounter:' || encounter_id)
        else null
      end as encounter_key,
      case
        when encounter_start_ts is not null
        then date_diff('day', encounter_start_ts, event_ts)
        else date_diff('day', patient_first_event_ts, event_ts)
      end as event_day_index,
      display as measurement_name,
      unit,
      count(*) as measurement_count,
      min(numeric_value) as min_value,
      avg(numeric_value) as avg_value,
      max(numeric_value) as max_value
    from enriched
    group by
      patient_key,
      encounter_key,
      event_day_index,
      measurement_name,
      unit
    order by patient_key, encounter_key, event_day_index, measurement_name
    """
    return write_registered_gold_query(
        config,
        table_name,
        sql,
        [observation_glob, encounter_glob],
    )


__all__ = [
    "LABS_DAILY_TABLE",
    "VITALS_DAILY_TABLE",
    "VITAL_DISPLAYS",
    "build_labs_daily",
    "build_observation_daily",
    "build_vitals_daily",
    "quoted_values",
]
