from __future__ import annotations

from healthcare_fhir_lakehouse.common.config import ProjectConfig
from healthcare_fhir_lakehouse.gold.writer import GoldWriteResult, write_gold_query
from healthcare_fhir_lakehouse.silver.writer import silver_output_dir

TABLE_NAME = "medication_activity"


def build_medication_activity(config: ProjectConfig) -> GoldWriteResult:
    request_glob = str(silver_output_dir(config, "medication_request") / "*.parquet")
    administration_glob = str(
        silver_output_dir(config, "medication_administration") / "*.parquet"
    )
    dispense_glob = str(silver_output_dir(config, "medication_dispense") / "*.parquet")
    statement_glob = str(
        silver_output_dir(config, "medication_statement") / "*.parquet"
    )
    encounter_glob = str(silver_output_dir(config, "encounter") / "*.parquet")

    sql = """
    with medication_events as (
      select
        patient_id,
        encounter_id,
        medication_code,
        medication_display,
        'request' as activity_type,
        'order' as source_system
      from read_parquet(?)
      union all
      select
        patient_id,
        encounter_id,
        medication_code,
        medication_display,
        'administration' as activity_type,
        source_system
      from read_parquet(?)
      union all
      select
        patient_id,
        encounter_id,
        medication_code,
        medication_display,
        'dispense' as activity_type,
        source_system
      from read_parquet(?)
      union all
      select
        patient_id,
        encounter_id,
        medication_code,
        medication_display,
        'statement' as activity_type,
        source_system
      from read_parquet(?)
    )
    select
      coalesce(m.medication_code, 'unknown') as medication_code,
      coalesce(m.medication_display, m.medication_code, 'unknown')
        as medication_display,
      m.activity_type,
      m.source_system,
      e.class_code as encounter_class,
      e.class_display as encounter_class_display,
      count(distinct m.patient_id) as patient_count,
      count(distinct m.encounter_id) as encounter_count,
      count(*) as event_count,
      count(*) filter (where m.encounter_id is not null)
        as with_encounter_context_count,
      count(*) filter (where m.encounter_id is null)
        as without_encounter_context_count
    from medication_events m
    left join read_parquet(?) e on m.encounter_id = e.encounter_id
    group by
      coalesce(m.medication_code, 'unknown'),
      coalesce(m.medication_display, m.medication_code, 'unknown'),
      m.activity_type,
      m.source_system,
      e.class_code,
      e.class_display
    order by event_count desc, medication_display, activity_type
    """
    return write_gold_query(
        config,
        TABLE_NAME,
        sql,
        [
            request_glob,
            administration_glob,
            dispense_glob,
            statement_glob,
            encounter_glob,
        ],
    )


__all__ = ["TABLE_NAME", "build_medication_activity"]
