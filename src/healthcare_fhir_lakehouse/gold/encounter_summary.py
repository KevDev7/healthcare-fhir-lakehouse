from __future__ import annotations

from healthcare_fhir_lakehouse.common.config import ProjectConfig
from healthcare_fhir_lakehouse.gold.writer import (
    GoldWriteResult,
    write_registered_gold_query,
)
from healthcare_fhir_lakehouse.silver.writer import silver_parquet_glob

TABLE_NAME = "encounter_summary"


def build_encounter_summary(config: ProjectConfig) -> GoldWriteResult:
    encounter_glob = silver_parquet_glob(config, "encounter")
    observation_glob = silver_parquet_glob(config, "observation")
    condition_glob = silver_parquet_glob(config, "condition")
    medication_request_glob = silver_parquet_glob(config, "medication_request")
    medication_administration_glob = silver_parquet_glob(
        config,
        "medication_administration",
    )
    medication_dispense_glob = silver_parquet_glob(config, "medication_dispense")
    medication_statement_glob = silver_parquet_glob(config, "medication_statement")
    procedure_glob = silver_parquet_glob(config, "procedure")

    sql = """
    with encounter_base as (
      select
        encounter_id,
        patient_id,
        status,
        class_code,
        class_display,
        start_datetime,
        end_datetime,
        discharge_disposition,
        try_cast(substr(start_datetime, 1, 19) as timestamp) as start_ts,
        try_cast(substr(end_datetime, 1, 19) as timestamp) as end_ts
      from read_parquet(?)
    ),
    observation_counts as (
      select encounter_id, count(*) as observation_count
      from read_parquet(?)
      where encounter_id is not null
      group by encounter_id
    ),
    condition_counts as (
      select
        encounter_id,
        count(*) as condition_count,
        count(distinct coalesce(code, '') || '|' || coalesce(display, ''))
          as distinct_condition_count
      from read_parquet(?)
      where encounter_id is not null
      group by encounter_id
    ),
    medication_request_counts as (
      select encounter_id, count(*) as medication_request_count
      from read_parquet(?)
      where encounter_id is not null
      group by encounter_id
    ),
    medication_administration_counts as (
      select encounter_id, count(*) as medication_administration_count
      from read_parquet(?)
      where encounter_id is not null
      group by encounter_id
    ),
    medication_dispense_counts as (
      select encounter_id, count(*) as medication_dispense_count
      from read_parquet(?)
      where encounter_id is not null
      group by encounter_id
    ),
    medication_statement_counts as (
      select encounter_id, count(*) as medication_statement_count
      from read_parquet(?)
      where encounter_id is not null
      group by encounter_id
    ),
    medication_distinct_counts as (
      select
        encounter_id,
        count(distinct coalesce(medication_code, '') || '|'
          || coalesce(medication_display, '')) as distinct_medication_count
      from (
        select encounter_id, medication_code, medication_display
        from read_parquet(?)
        union all
        select encounter_id, medication_code, medication_display
        from read_parquet(?)
        union all
        select encounter_id, medication_code, medication_display
        from read_parquet(?)
        union all
        select encounter_id, medication_code, medication_display
        from read_parquet(?)
      )
      where encounter_id is not null
      group by encounter_id
    ),
    procedure_counts as (
      select
        encounter_id,
        count(*) as procedure_count,
        count(distinct coalesce(procedure_code, '') || '|'
          || coalesce(procedure_display, '')) as distinct_procedure_count
      from read_parquet(?)
      where encounter_id is not null
      group by encounter_id
    )
    select
      md5('encounter:' || e.encounter_id) as encounter_key,
      md5('patient:' || e.patient_id) as patient_key,
      e.status as encounter_status,
      e.class_code as encounter_class,
      e.class_display as encounter_class_display,
      cast(date_part('year', e.start_ts) as integer) as encounter_start_year,
      cast(date_part('month', e.start_ts) as integer) as encounter_start_month,
      case
        when e.start_ts is not null and e.end_ts is not null
        then date_diff('hour', e.start_ts, e.end_ts)
        else null
      end as length_of_stay_hours,
      coalesce(o.observation_count, 0) as observation_count,
      coalesce(c.condition_count, 0) as condition_count,
      coalesce(c.distinct_condition_count, 0) as distinct_condition_count,
      coalesce(mr.medication_request_count, 0) as medication_request_count,
      coalesce(ma.medication_administration_count, 0)
        as medication_administration_count,
      coalesce(md.medication_dispense_count, 0) as medication_dispense_count,
      coalesce(ms.medication_statement_count, 0) as medication_statement_count,
      coalesce(pc.procedure_count, 0) as procedure_count,
      coalesce(mc.distinct_medication_count, 0) as distinct_medication_count,
      coalesce(pc.distinct_procedure_count, 0) as distinct_procedure_count,
      e.discharge_disposition
    from encounter_base e
    left join observation_counts o on e.encounter_id = o.encounter_id
    left join condition_counts c on e.encounter_id = c.encounter_id
    left join medication_request_counts mr on e.encounter_id = mr.encounter_id
    left join medication_administration_counts ma on e.encounter_id = ma.encounter_id
    left join medication_dispense_counts md on e.encounter_id = md.encounter_id
    left join medication_statement_counts ms on e.encounter_id = ms.encounter_id
    left join medication_distinct_counts mc on e.encounter_id = mc.encounter_id
    left join procedure_counts pc on e.encounter_id = pc.encounter_id
    order by encounter_start_year, encounter_start_month, encounter_key
    """
    return write_registered_gold_query(
        config,
        TABLE_NAME,
        sql,
        [
            encounter_glob,
            observation_glob,
            condition_glob,
            medication_request_glob,
            medication_administration_glob,
            medication_dispense_glob,
            medication_statement_glob,
            medication_request_glob,
            medication_administration_glob,
            medication_dispense_glob,
            medication_statement_glob,
            procedure_glob,
        ],
    )


__all__ = ["TABLE_NAME", "build_encounter_summary"]
