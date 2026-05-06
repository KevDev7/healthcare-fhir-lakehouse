from __future__ import annotations

import argparse
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DATASET_NAME = "mimic-iv-clinical-database-demo-on-fhir"
DATASET_VERSION = "2.1.0"


@dataclass(frozen=True)
class CloudConfig:
    catalog: str
    raw_schema: str
    bronze_schema: str
    silver_schema: str
    gold_schema: str
    audit_schema: str
    raw_volume: str
    raw_subdir: str

    @property
    def raw_volume_path(self) -> str:
        return (
            f"/Volumes/{self.catalog}/{self.raw_schema}/"
            f"{self.raw_volume}/{self.raw_subdir}"
        )

    @property
    def bronze_table(self) -> str:
        return table_name(self.catalog, self.bronze_schema, "fhir_resources")


def parse_args() -> CloudConfig:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--raw-schema", required=True)
    parser.add_argument("--bronze-schema", required=True)
    parser.add_argument("--silver-schema", required=True)
    parser.add_argument("--gold-schema", required=True)
    parser.add_argument("--audit-schema", required=True)
    parser.add_argument("--raw-volume", required=True)
    parser.add_argument("--raw-subdir", required=True)
    args = parser.parse_args()
    return CloudConfig(
        catalog=args.catalog,
        raw_schema=args.raw_schema,
        bronze_schema=args.bronze_schema,
        silver_schema=args.silver_schema,
        gold_schema=args.gold_schema,
        audit_schema=args.audit_schema,
        raw_volume=args.raw_volume,
        raw_subdir=args.raw_subdir,
    )


def table_name(catalog: str, schema: str, table: str) -> str:
    return f"`{catalog}`.`{schema}`.`{table}`"


def json(path: str):
    return F.get_json_object(F.col("raw_json"), path)


def reference_id(path: str):
    extracted = F.regexp_extract(json(path), r"([^/]+)$", 1)
    return F.when(extracted == "", None).otherwise(extracted)


def source_file_col() -> F.Column:
    return F.regexp_extract(F.col("_metadata.file_path"), r"([^/]+)$", 1)


def lineage_columns() -> list[F.Column]:
    return [
        F.col("source_file"),
        F.col("resource_family"),
        F.col("profile_url"),
        F.col("source_dataset_name"),
        F.col("source_dataset_version"),
        F.col("bronze_ingested_at"),
        F.col("bronze_resource_id"),
    ]


def write_delta(df: DataFrame, full_table_name: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table_name)
    )


def build_bronze(spark: SparkSession, config: CloudConfig) -> None:
    raw_path = f"{config.raw_volume_path}/*.ndjson.gz"
    bronze = (
        spark.read.text(raw_path)
        .withColumn("resource_type", F.get_json_object("value", "$.resourceType"))
        .withColumn("resource_id", F.get_json_object("value", "$.id"))
        .withColumn("source_file", source_file_col())
        .withColumn(
            "resource_family",
            F.regexp_replace(F.col("source_file"), r"\.ndjson\.gz$", ""),
        )
        .withColumn("profile_url", F.get_json_object("value", "$.meta.profile[0]"))
        .withColumn("source_dataset_name", F.lit(DATASET_NAME))
        .withColumn("source_dataset_version", F.lit(DATASET_VERSION))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumnRenamed("value", "raw_json")
        .select(
            "resource_type",
            "resource_id",
            "source_file",
            "resource_family",
            "profile_url",
            "source_dataset_name",
            "source_dataset_version",
            "ingested_at",
            "raw_json",
        )
    )
    write_delta(bronze, config.bronze_table)


def bronze_resources(spark: SparkSession, config: CloudConfig, resource_type: str):
    return (
        spark.table(config.bronze_table)
        .where(F.col("resource_type") == resource_type)
        .withColumnRenamed("resource_id", "bronze_resource_id")
        .withColumnRenamed("ingested_at", "bronze_ingested_at")
    )


def build_patient(spark: SparkSession, config: CloudConfig) -> None:
    df = bronze_resources(spark, config, "Patient")
    source_identifier = json("$.identifier[0].value")
    patient = df.select(
        F.col("bronze_resource_id").alias("patient_id"),
        source_identifier.alias("source_patient_identifier"),
        F.coalesce(
            json("$.name[0].text"),
            F.concat(F.lit("Patient_"), source_identifier),
        ).alias("synthetic_patient_name"),
        json("$.gender").alias("gender"),
        json("$.birthDate").alias("birth_date"),
        json("$.deceasedDateTime").alias("deceased_datetime"),
        F.lit(None).cast("string").alias("race"),
        F.lit(None).cast("string").alias("ethnicity"),
        F.lit(None).cast("string").alias("birth_sex"),
        json("$.maritalStatus.coding[0].code").alias("marital_status_code"),
        *lineage_columns(),
    )
    write_delta(patient, table_name(config.catalog, config.silver_schema, "patient"))


def build_encounter(spark: SparkSession, config: CloudConfig) -> None:
    df = bronze_resources(spark, config, "Encounter")
    encounter = df.select(
        F.col("bronze_resource_id").alias("encounter_id"),
        reference_id("$.subject.reference").alias("patient_id"),
        json("$.status").alias("status"),
        json("$.class.code").alias("class_code"),
        json("$.class.display").alias("class_display"),
        json("$.period.start").alias("start_datetime"),
        json("$.period.end").alias("end_datetime"),
        json("$.serviceType.coding[0].code").alias("service_type_code"),
        json("$.hospitalization.admitSource.text").alias("admit_source"),
        json("$.hospitalization.dischargeDisposition.text")
        .alias("discharge_disposition"),
        json("$.hospitalization.dischargeDisposition.coding[0].display")
        .alias("discharge_disposition_display"),
        *lineage_columns(),
    )
    write_delta(
        encounter,
        table_name(config.catalog, config.silver_schema, "encounter"),
    )


def build_observation(spark: SparkSession, config: CloudConfig) -> None:
    df = bronze_resources(spark, config, "Observation")
    value_quantity = json("$.valueQuantity.value")
    value_string = json("$.valueString")
    observation = df.select(
        F.col("bronze_resource_id").alias("observation_id"),
        reference_id("$.subject.reference").alias("patient_id"),
        reference_id("$.encounter.reference").alias("encounter_id"),
        json("$.status").alias("status"),
        json("$.effectiveDateTime").alias("effective_datetime"),
        json("$.issued").alias("issued_datetime"),
        json("$.category[0].coding[0].code").alias("category_code"),
        json("$.category[0].coding[0].system").alias("category_system"),
        json("$.category[0].coding[0].display").alias("category_display"),
        json("$.code.coding[0].code").alias("code"),
        json("$.code.coding[0].system").alias("code_system"),
        json("$.code.coding[0].display").alias("display"),
        F.when(value_quantity.isNotNull(), "quantity")
        .when(value_string.isNotNull(), "string")
        .otherwise(None)
        .alias("value_type"),
        F.coalesce(value_quantity, value_string).alias("value"),
        json("$.valueQuantity.unit").alias("unit"),
        reference_id("$.specimen.reference").alias("specimen_id"),
        *lineage_columns(),
    )
    write_delta(
        observation,
        table_name(config.catalog, config.silver_schema, "observation"),
    )


def build_condition(spark: SparkSession, config: CloudConfig) -> None:
    df = bronze_resources(spark, config, "Condition")
    condition = df.select(
        F.col("bronze_resource_id").alias("condition_id"),
        reference_id("$.subject.reference").alias("patient_id"),
        reference_id("$.encounter.reference").alias("encounter_id"),
        json("$.category[0].coding[0].code").alias("category_code"),
        json("$.category[0].coding[0].system").alias("category_system"),
        json("$.category[0].coding[0].display").alias("category_display"),
        json("$.code.coding[0].code").alias("code"),
        json("$.code.coding[0].system").alias("code_system"),
        json("$.code.coding[0].display").alias("display"),
        *lineage_columns(),
    )
    write_delta(
        condition,
        table_name(config.catalog, config.silver_schema, "condition"),
    )


def build_silver(spark: SparkSession, config: CloudConfig) -> None:
    build_patient(spark, config)
    build_encounter(spark, config)
    build_observation(spark, config)
    build_condition(spark, config)


def build_relationship_audit(spark: SparkSession, config: CloudConfig) -> None:
    patient = table_name(config.catalog, config.silver_schema, "patient")
    encounter = table_name(config.catalog, config.silver_schema, "encounter")
    observation = table_name(config.catalog, config.silver_schema, "observation")
    condition = table_name(config.catalog, config.silver_schema, "condition")
    audit = spark.sql(
        f"""
        select
          current_timestamp() as generated_at,
          (select count(*) from {patient}) as patient_rows,
          (select count(*) from {encounter}) as encounter_rows,
          (select count(*) from {observation}) as observation_rows,
          (select count(*) from {condition}) as condition_rows,
          (
            select count(*) from {observation}
            where patient_id is null
          ) as observation_missing_patient_id,
          (
            select count(*) from {observation}
            where encounter_id is null
          ) as observation_missing_encounter_id,
          (
            select count(*) from {condition}
            where patient_id is null
          ) as condition_missing_patient_id,
          (
            select count(*) from {condition}
            where encounter_id is null
          ) as condition_missing_encounter_id,
          (
            select count(*)
            from {observation} o
            left join {patient} p on o.patient_id = p.patient_id
            where o.patient_id is not null and p.patient_id is null
          ) as observation_orphan_patient_id,
          (
            select count(*)
            from {observation} o
            left join {encounter} e on o.encounter_id = e.encounter_id
            where o.encounter_id is not null and e.encounter_id is null
          ) as observation_orphan_encounter_id,
          (
            select count(*)
            from {condition} c
            left join {patient} p on c.patient_id = p.patient_id
            where c.patient_id is not null and p.patient_id is null
          ) as condition_orphan_patient_id,
          (
            select count(*)
            from {condition} c
            left join {encounter} e on c.encounter_id = e.encounter_id
            where c.encounter_id is not null and e.encounter_id is null
          ) as condition_orphan_encounter_id
        """
    )
    write_delta(
        audit,
        table_name(config.catalog, config.audit_schema, "relationship_audit"),
    )


def build_privacy_audit(spark: SparkSession, config: CloudConfig) -> None:
    rows = [
        ("patient", "source_patient_identifier", "direct_identifier"),
        ("patient", "synthetic_patient_name", "direct_identifier"),
        ("patient", "birth_date", "date_precision"),
        ("encounter", "start_datetime", "date_precision"),
        ("encounter", "end_datetime", "date_precision"),
        ("observation", "effective_datetime", "date_precision"),
        ("observation", "issued_datetime", "date_precision"),
        ("observation", "value", "clinical_free_text"),
    ]
    audit = spark.createDataFrame(rows, ["table_name", "column_name", "classification"])
    audit = audit.withColumn("generated_at", F.current_timestamp())
    write_delta(audit, table_name(config.catalog, config.audit_schema, "privacy_audit"))


def build_gold(spark: SparkSession, config: CloudConfig) -> None:
    def silver(name: str) -> str:
        return table_name(config.catalog, config.silver_schema, name)

    def gold(name: str) -> str:
        return table_name(config.catalog, config.gold_schema, name)

    write_delta(
        spark.sql(
            f"""
            with observation_counts as (
              select encounter_id, count(*) as observation_count
              from {silver("observation")}
              where encounter_id is not null
              group by encounter_id
            ),
            condition_counts as (
              select
                encounter_id,
                count(*) as condition_count,
                count(distinct concat(coalesce(code, ''), '|', coalesce(display, '')))
                  as distinct_condition_count
              from {silver("condition")}
              where encounter_id is not null
              group by encounter_id
            )
            select
              md5(concat('encounter:', e.encounter_id)) as encounter_key,
              md5(concat('patient:', e.patient_id)) as patient_key,
              e.status as encounter_status,
              e.class_code as encounter_class,
              e.class_display as encounter_class_display,
              year(to_timestamp(substr(e.start_datetime, 1, 19)))
                as encounter_start_year,
              month(to_timestamp(substr(e.start_datetime, 1, 19)))
                as encounter_start_month,
              timestampdiff(
                HOUR,
                to_timestamp(substr(e.start_datetime, 1, 19)),
                to_timestamp(substr(e.end_datetime, 1, 19))
              ) as length_of_stay_hours,
              coalesce(o.observation_count, 0) as observation_count,
              coalesce(c.condition_count, 0) as condition_count,
              coalesce(c.distinct_condition_count, 0) as distinct_condition_count,
              e.discharge_disposition
            from {silver("encounter")} e
            left join observation_counts o on e.encounter_id = o.encounter_id
            left join condition_counts c on e.encounter_id = c.encounter_id
            """
        ),
        gold("encounter_summary"),
    )

    write_delta(
        spark.sql(
            f"""
            select
              c.code as condition_code,
              c.display as condition_display,
              e.class_code as encounter_class,
              e.class_display as encounter_class_display,
              count(distinct c.patient_id) as patient_count,
              count(distinct c.encounter_id) as encounter_count,
              count(*) as condition_row_count
            from {silver("condition")} c
            left join {silver("encounter")} e on c.encounter_id = e.encounter_id
            group by c.code, c.display, e.class_code, e.class_display
            """
        ),
        gold("condition_summary"),
    )

    build_observation_daily(
        spark,
        silver("observation"),
        silver("encounter"),
        gold("vitals_daily"),
        """
        display in (
          'Heart Rate',
          'Respiratory Rate',
          'O2 saturation pulseoxymetry',
          'Temperature Fahrenheit',
          'Non Invasive Blood Pressure systolic',
          'Non Invasive Blood Pressure diastolic',
          'Non Invasive Blood Pressure mean'
        )
        """,
    )
    build_observation_daily(
        spark,
        silver("observation"),
        silver("encounter"),
        gold("labs_daily"),
        "category_code in ('laboratory', 'Labs')",
    )


def build_observation_daily(
    spark: SparkSession,
    observation_table: str,
    encounter_table: str,
    output_table: str,
    where_clause: str,
) -> None:
    daily = spark.sql(
        f"""
        with numeric_observations as (
          select
            patient_id,
            encounter_id,
            display,
            unit,
            try_cast(value as double) as numeric_value,
            to_timestamp(substr(effective_datetime, 1, 19)) as event_ts
          from {observation_table}
          where {where_clause}
        ),
        filtered as (
          select *
          from numeric_observations
          where numeric_value is not null
            and event_ts is not null
            and patient_id is not null
        ),
        enriched as (
          select
            o.*,
            to_timestamp(substr(e.start_datetime, 1, 19)) as encounter_start_ts,
            min(o.event_ts) over (partition by o.patient_id) as patient_first_event_ts
          from filtered o
          left join {encounter_table} e on o.encounter_id = e.encounter_id
        )
        select
          md5(concat('patient:', patient_id)) as patient_key,
          case
            when encounter_id is not null then md5(concat('encounter:', encounter_id))
            else null
          end as encounter_key,
          case
            when encounter_start_ts is not null
            then datediff(event_ts, encounter_start_ts)
            else datediff(event_ts, patient_first_event_ts)
          end as event_day_index,
          display as measurement_name,
          unit,
          count(*) as measurement_count,
          min(numeric_value) as min_value,
          avg(numeric_value) as avg_value,
          max(numeric_value) as max_value
        from enriched
        group by patient_key, encounter_key, event_day_index, measurement_name, unit
        """
    )
    write_delta(daily, output_table)


def build_data_quality(spark: SparkSession, config: CloudConfig) -> None:
    bronze = table_name(config.catalog, config.bronze_schema, "fhir_resources")

    def silver(name: str) -> str:
        return table_name(config.catalog, config.silver_schema, name)

    def gold(name: str) -> str:
        return table_name(config.catalog, config.gold_schema, name)

    relationship = table_name(
        config.catalog,
        config.audit_schema,
        "relationship_audit",
    )

    checks = spark.sql(
        f"""
        select 'bronze_rows' as check_name, 'bronze' as layer,
          case when count(*) = 928935 then 'pass' else 'fail' end as status,
          cast(count(*) as string) as observed,
          '928935' as expected
        from {bronze}
        union all
        select 'silver_patient_rows', 'silver',
          case when count(*) = 100 then 'pass' else 'fail' end,
          cast(count(*) as string), '100'
        from {silver("patient")}
        union all
        select 'silver_encounter_rows', 'silver',
          case when count(*) = 637 then 'pass' else 'fail' end,
          cast(count(*) as string), '637'
        from {silver("encounter")}
        union all
        select 'silver_observation_rows', 'silver',
          case when count(*) = 813540 then 'pass' else 'fail' end,
          cast(count(*) as string), '813540'
        from {silver("observation")}
        union all
        select 'silver_condition_rows', 'silver',
          case when count(*) = 5051 then 'pass' else 'fail' end,
          cast(count(*) as string), '5051'
        from {silver("condition")}
        union all
        select 'relationship_orphans', 'relationships',
          case
            when observation_orphan_patient_id = 0
              and observation_orphan_encounter_id = 0
              and condition_orphan_patient_id = 0
              and condition_orphan_encounter_id = 0
            then 'pass' else 'fail'
          end,
          cast(
            observation_orphan_patient_id
            + observation_orphan_encounter_id
            + condition_orphan_patient_id
            + condition_orphan_encounter_id
            as string
          ),
          '0'
        from {relationship}
        union all
        select 'gold_encounter_summary_rows', 'gold',
          case when count(*) > 0 then 'pass' else 'fail' end,
          cast(count(*) as string), '> 0'
        from {gold("encounter_summary")}
        union all
        select 'gold_condition_summary_rows', 'gold',
          case when count(*) > 0 then 'pass' else 'fail' end,
          cast(count(*) as string), '> 0'
        from {gold("condition_summary")}
        union all
        select 'gold_vitals_daily_rows', 'gold',
          case when count(*) > 0 then 'pass' else 'fail' end,
          cast(count(*) as string), '> 0'
        from {gold("vitals_daily")}
        union all
        select 'gold_labs_daily_rows', 'gold',
          case when count(*) > 0 then 'pass' else 'fail' end,
          cast(count(*) as string), '> 0'
        from {gold("labs_daily")}
        """
    )
    checks = checks.withColumn("generated_at", F.current_timestamp())
    write_delta(
        checks,
        table_name(config.catalog, config.audit_schema, "data_quality_report"),
    )

    failures = checks.where(F.col("status") == "fail").count()
    if failures:
        raise ValueError(f"Cloud data quality failed with {failures} failed checks.")


def main() -> None:
    config = parse_args()
    spark = SparkSession.builder.getOrCreate()

    build_bronze(spark, config)
    build_silver(spark, config)
    build_relationship_audit(spark, config)
    build_privacy_audit(spark, config)
    build_gold(spark, config)
    build_data_quality(spark, config)


if __name__ == "__main__":
    main()
