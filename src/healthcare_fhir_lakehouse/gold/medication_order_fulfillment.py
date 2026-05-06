from __future__ import annotations

from healthcare_fhir_lakehouse.common.config import ProjectConfig
from healthcare_fhir_lakehouse.gold.writer import GoldWriteResult, write_gold_query
from healthcare_fhir_lakehouse.silver.writer import silver_output_dir

TABLE_NAME = "medication_order_fulfillment"


def build_medication_order_fulfillment(config: ProjectConfig) -> GoldWriteResult:
    request_glob = str(silver_output_dir(config, "medication_request") / "*.parquet")
    administration_glob = str(
        silver_output_dir(config, "medication_administration") / "*.parquet"
    )
    dispense_glob = str(silver_output_dir(config, "medication_dispense") / "*.parquet")

    sql = """
    with administration_counts as (
      select
        medication_request_id,
        count(*) as administration_count,
        min(effective_start_datetime) as first_administration_datetime
      from read_parquet(?)
      where medication_request_id is not null
      group by medication_request_id
    ),
    dispense_counts as (
      select
        medication_request_id,
        count(*) as dispense_count,
        min(when_handed_over_datetime) as first_dispense_datetime
      from read_parquet(?)
      where medication_request_id is not null
      group by medication_request_id
    )
    select
      md5('medication_request:' || r.medication_request_id)
        as medication_request_key,
      md5('patient:' || r.patient_id) as patient_key,
      case
        when r.encounter_id is not null then md5('encounter:' || r.encounter_id)
        else null
      end as encounter_key,
      r.medication_code,
      r.medication_display,
      r.status as request_status,
      r.intent as request_intent,
      cast(date_part(
        'year',
        try_cast(substr(r.authored_datetime, 1, 19) as timestamp)
      ) as integer) as authored_year,
      coalesce(a.administration_count, 0) as administration_count,
      coalesce(d.dispense_count, 0) as dispense_count,
      a.first_administration_datetime,
      d.first_dispense_datetime,
      coalesce(a.administration_count, 0) > 0 as has_administration,
      coalesce(d.dispense_count, 0) > 0 as has_dispense,
      case
        when coalesce(a.administration_count, 0) > 0
          and coalesce(d.dispense_count, 0) > 0
          then 'administered_and_dispensed'
        when coalesce(a.administration_count, 0) > 0 then 'administered'
        when coalesce(d.dispense_count, 0) > 0 then 'dispensed'
        else 'no_linked_event'
      end as fulfillment_path
    from read_parquet(?) r
    left join administration_counts a
      on r.medication_request_id = a.medication_request_id
    left join dispense_counts d
      on r.medication_request_id = d.medication_request_id
    order by authored_year, medication_request_key
    """
    return write_gold_query(
        config,
        TABLE_NAME,
        sql,
        [administration_glob, dispense_glob, request_glob],
    )


__all__ = ["TABLE_NAME", "build_medication_order_fulfillment"]
