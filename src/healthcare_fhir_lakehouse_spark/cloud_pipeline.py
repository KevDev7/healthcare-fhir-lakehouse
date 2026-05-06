from __future__ import annotations

import argparse
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

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


def reference_id_from_column(column: F.Column) -> F.Column:
    extracted = F.regexp_extract(column, r"([^/]+)$", 1)
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


def medication_source_system(resource_family: F.Column, ed_family: str) -> F.Column:
    return F.when(resource_family == ed_family, "ed").otherwise("inpatient")


def medication_ingredient_schema() -> T.ArrayType:
    quantity = T.StructType(
        [
            T.StructField("value", T.DoubleType()),
            T.StructField("unit", T.StringType()),
            T.StructField("code", T.StringType()),
        ]
    )
    coding = T.StructType(
        [
            T.StructField("code", T.StringType()),
            T.StructField("system", T.StringType()),
            T.StructField("display", T.StringType()),
        ]
    )
    codeable = T.StructType(
        [
            T.StructField("coding", T.ArrayType(coding)),
            T.StructField("text", T.StringType()),
        ]
    )
    reference = T.StructType([T.StructField("reference", T.StringType())])
    strength = T.StructType(
        [
            T.StructField("numerator", quantity),
            T.StructField("denominator", quantity),
        ]
    )
    ingredient = T.StructType(
        [
            T.StructField("itemReference", reference),
            T.StructField("itemCodeableConcept", codeable),
            T.StructField("strength", strength),
        ]
    )
    return T.ArrayType(ingredient)


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


def build_medication(spark: SparkSession, config: CloudConfig) -> None:
    df = bronze_resources(spark, config, "Medication")
    medication = df.select(
        F.col("bronze_resource_id").alias("medication_id"),
        json("$.code.coding[0].code").alias("medication_code"),
        json("$.code.coding[0].system").alias("medication_code_system"),
        F.coalesce(
            json("$.code.coding[0].display"),
            json("$.code.text"),
            json("$.identifier[2].value"),
        ).alias("medication_display"),
        json("$.code.text").alias("medication_text"),
        json("$.form.coding[0].code").alias("form_code"),
        json("$.form.coding[0].display").alias("form_display"),
        (
            (F.col("resource_family") == "MimicMedicationMix")
            | (F.get_json_object("raw_json", "$.ingredient[0]").isNotNull())
        ).alias("is_mix"),
        F.coalesce(
            F.size(
                F.from_json(
                    json("$.identifier"),
                    T.ArrayType(T.MapType(T.StringType(), T.StringType())),
                )
            ),
            F.lit(0),
        ).alias("identifier_count"),
        F.coalesce(
            F.size(F.from_json(json("$.ingredient"), medication_ingredient_schema())),
            F.lit(0),
        ).alias("ingredient_count"),
        *lineage_columns(),
    )
    write_delta(
        medication,
        table_name(config.catalog, config.silver_schema, "medication"),
    )

    ingredients = (
        df.withColumn(
            "ingredients",
            F.from_json(json("$.ingredient"), medication_ingredient_schema()),
        )
        .select(
            F.col("bronze_resource_id").alias("medication_id"),
            F.posexplode("ingredients").alias("ingredient_index", "ingredient"),
            *lineage_columns(),
        )
        .select(
            "medication_id",
            "ingredient_index",
            reference_id_from_column(
                F.col("ingredient.itemReference.reference")
            ).alias("ingredient_medication_id"),
            F.col("ingredient.itemCodeableConcept.coding")[0]["code"].alias(
                "ingredient_code"
            ),
            F.col("ingredient.itemCodeableConcept.coding")[0]["system"].alias(
                "ingredient_code_system"
            ),
            F.coalesce(
                F.col("ingredient.itemCodeableConcept.coding")[0]["display"],
                F.col("ingredient.itemCodeableConcept.text"),
            ).alias("ingredient_display"),
            F.col("ingredient.strength.numerator.value").cast("string").alias(
                "strength_numerator_value"
            ),
            F.coalesce(
                F.col("ingredient.strength.numerator.unit"),
                F.col("ingredient.strength.numerator.code"),
            ).alias("strength_numerator_unit"),
            F.col("ingredient.strength.denominator.value").cast("string").alias(
                "strength_denominator_value"
            ),
            F.coalesce(
                F.col("ingredient.strength.denominator.unit"),
                F.col("ingredient.strength.denominator.code"),
            ).alias("strength_denominator_unit"),
            *lineage_columns(),
        )
    )
    write_delta(
        ingredients,
        table_name(config.catalog, config.silver_schema, "medication_ingredient"),
    )


def build_medication_request(spark: SparkSession, config: CloudConfig) -> None:
    df = bronze_resources(spark, config, "MedicationRequest")
    medication = spark.table(
        table_name(config.catalog, config.silver_schema, "medication")
    )
    request = (
        df.select(
            F.col("bronze_resource_id").alias("medication_request_id"),
            reference_id("$.subject.reference").alias("patient_id"),
            reference_id("$.encounter.reference").alias("encounter_id"),
            json("$.status").alias("status"),
            json("$.intent").alias("intent"),
            json("$.authoredOn").alias("authored_datetime"),
            reference_id("$.medicationReference.reference").alias("medication_id"),
            json("$.medicationCodeableConcept.coding[0].code").alias("inline_code"),
            json("$.medicationCodeableConcept.coding[0].system").alias("inline_system"),
            F.coalesce(
                json("$.medicationCodeableConcept.coding[0].display"),
                json("$.medicationCodeableConcept.text"),
            ).alias("inline_display"),
            json("$.dosageInstruction[0].route.coding[0].code").alias("route_code"),
            json("$.dosageInstruction[0].route.coding[0].display").alias(
                "route_display"
            ),
            F.coalesce(
                json("$.dosageInstruction[0].doseAndRate[0].doseQuantity.value"),
                json("$.dosageInstruction[0].dose.value"),
            ).alias("dose_value"),
            F.coalesce(
                json("$.dosageInstruction[0].doseAndRate[0].doseQuantity.unit"),
                json("$.dosageInstruction[0].doseAndRate[0].doseQuantity.code"),
                json("$.dosageInstruction[0].dose.unit"),
                json("$.dosageInstruction[0].dose.code"),
            ).alias("dose_unit"),
            F.coalesce(
                json("$.dosageInstruction[0].timing.repeat.frequency"),
                json("$.dosageInstruction[0].timing.code.coding[0].code"),
            ).alias("frequency"),
            json("$.dosageInstruction[0].timing.repeat.period").alias("period"),
            json("$.dosageInstruction[0].timing.repeat.periodUnit").alias(
                "period_unit"
            ),
            json("$.dispenseRequest.validityPeriod.start").alias(
                "validity_start_datetime"
            ),
            json("$.dispenseRequest.validityPeriod.end").alias(
                "validity_end_datetime"
            ),
            F.when(json("$.dosageInstruction[0]").isNotNull(), 1)
            .otherwise(0)
            .alias("dosage_instruction_count"),
            *lineage_columns(),
        )
        .join(
            medication.select(
                "medication_id",
                "medication_code",
                "medication_code_system",
                "medication_display",
            ),
            "medication_id",
            "left",
        )
        .select(
            "medication_request_id",
            "patient_id",
            "encounter_id",
            "status",
            "intent",
            "authored_datetime",
            "medication_id",
            F.coalesce("inline_code", "medication_code").alias("medication_code"),
            F.coalesce("inline_system", "medication_code_system").alias(
                "medication_code_system"
            ),
            F.coalesce("inline_display", "medication_display").alias(
                "medication_display"
            ),
            F.when(F.col("medication_id").isNotNull(), "reference")
            .otherwise("inline_code")
            .alias("medication_source_type"),
            "route_code",
            "route_display",
            "dose_value",
            "dose_unit",
            "frequency",
            "period",
            "period_unit",
            "validity_start_datetime",
            "validity_end_datetime",
            "dosage_instruction_count",
            *lineage_columns(),
        )
    )
    write_delta(
        request,
        table_name(config.catalog, config.silver_schema, "medication_request"),
    )


def build_medication_administration(spark: SparkSession, config: CloudConfig) -> None:
    df = bronze_resources(spark, config, "MedicationAdministration")
    administration = df.select(
        F.col("bronze_resource_id").alias("medication_administration_id"),
        reference_id("$.subject.reference").alias("patient_id"),
        reference_id("$.context.reference").alias("encounter_id"),
        json("$.status").alias("status"),
        json("$.category.coding[0].code").alias("category_code"),
        json("$.category.coding[0].display").alias("category_display"),
        F.coalesce(json("$.effectiveDateTime"), json("$.effectivePeriod.start")).alias(
            "effective_start_datetime"
        ),
        json("$.effectivePeriod.end").alias("effective_end_datetime"),
        json("$.medicationCodeableConcept.coding[0].code").alias("medication_code"),
        json("$.medicationCodeableConcept.coding[0].system").alias(
            "medication_code_system"
        ),
        F.coalesce(
            json("$.medicationCodeableConcept.coding[0].display"),
            json("$.medicationCodeableConcept.text"),
        ).alias("medication_display"),
        reference_id("$.request.reference").alias("medication_request_id"),
        json("$.dosage.dose.value").alias("dose_value"),
        F.coalesce(json("$.dosage.dose.unit"), json("$.dosage.dose.code")).alias(
            "dose_unit"
        ),
        json("$.dosage.method.coding[0].code").alias("method_code"),
        json("$.dosage.method.coding[0].display").alias("method_display"),
        F.when(F.col("resource_family") == "MimicMedicationAdministrationICU", "icu")
        .otherwise("hospital")
        .alias("source_system"),
        reference_id("$.request.reference").isNotNull().alias("has_request_reference"),
        reference_id("$.context.reference").isNotNull().alias("has_encounter_context"),
        *lineage_columns(),
    )
    write_delta(
        administration,
        table_name(config.catalog, config.silver_schema, "medication_administration"),
    )


def build_medication_dispense(spark: SparkSession, config: CloudConfig) -> None:
    df = bronze_resources(spark, config, "MedicationDispense")
    dispense = df.select(
        F.col("bronze_resource_id").alias("medication_dispense_id"),
        reference_id("$.subject.reference").alias("patient_id"),
        reference_id("$.context.reference").alias("encounter_id"),
        json("$.status").alias("status"),
        json("$.whenHandedOver").alias("when_handed_over_datetime"),
        json("$.medicationCodeableConcept.coding[0].code").alias("medication_code"),
        json("$.medicationCodeableConcept.coding[0].system").alias(
            "medication_code_system"
        ),
        F.coalesce(
            json("$.medicationCodeableConcept.coding[0].display"),
            json("$.medicationCodeableConcept.text"),
        ).alias("medication_display"),
        json("$.medicationCodeableConcept.text").alias("medication_text"),
        reference_id("$.authorizingPrescription[0].reference").alias(
            "medication_request_id"
        ),
        F.when(json("$.authorizingPrescription[0]").isNotNull(), 1)
        .otherwise(0)
        .alias("authorizing_prescription_count"),
        json("$.dosageInstruction[0].route.coding[0].code").alias("route_code"),
        json("$.dosageInstruction[0].route.coding[0].display").alias("route_display"),
        F.coalesce(
            json("$.dosageInstruction[0].timing.repeat.frequency"),
            json("$.dosageInstruction[0].timing.code.coding[0].code"),
        ).alias("frequency"),
        json("$.dosageInstruction[0].timing.repeat.period").alias("period"),
        json("$.dosageInstruction[0].timing.repeat.periodUnit").alias("period_unit"),
        medication_source_system(
            F.col("resource_family"), "MimicMedicationDispenseED"
        ).alias("source_system"),
        reference_id("$.authorizingPrescription[0].reference")
        .isNotNull()
        .alias("has_request_reference"),
        *lineage_columns(),
    )
    write_delta(
        dispense,
        table_name(config.catalog, config.silver_schema, "medication_dispense"),
    )


def build_medication_statement(spark: SparkSession, config: CloudConfig) -> None:
    df = bronze_resources(spark, config, "MedicationStatement")
    statement = df.select(
        F.col("bronze_resource_id").alias("medication_statement_id"),
        reference_id("$.subject.reference").alias("patient_id"),
        reference_id("$.context.reference").alias("encounter_id"),
        json("$.status").alias("status"),
        json("$.dateAsserted").alias("date_asserted_datetime"),
        json("$.medicationCodeableConcept.coding[0].code").alias("medication_code"),
        json("$.medicationCodeableConcept.coding[0].system").alias(
            "medication_code_system"
        ),
        F.coalesce(
            json("$.medicationCodeableConcept.coding[0].display"),
            json("$.medicationCodeableConcept.text"),
        ).alias("medication_display"),
        json("$.medicationCodeableConcept.text").alias("medication_text"),
        F.lit("ed").alias("source_system"),
        *lineage_columns(),
    )
    write_delta(
        statement,
        table_name(config.catalog, config.silver_schema, "medication_statement"),
    )


def build_procedure(spark: SparkSession, config: CloudConfig) -> None:
    df = bronze_resources(spark, config, "Procedure")
    procedure = df.select(
        F.col("bronze_resource_id").alias("procedure_id"),
        reference_id("$.subject.reference").alias("patient_id"),
        reference_id("$.encounter.reference").alias("encounter_id"),
        json("$.status").alias("status"),
        F.coalesce(json("$.performedDateTime"), json("$.performedPeriod.start")).alias(
            "performed_start_datetime"
        ),
        json("$.performedPeriod.end").alias("performed_end_datetime"),
        json("$.category.coding[0].code").alias("category_code"),
        json("$.category.coding[0].display").alias("category_display"),
        json("$.code.coding[0].code").alias("procedure_code"),
        json("$.code.coding[0].system").alias("procedure_code_system"),
        json("$.code.coding[0].display").alias("procedure_display"),
        json("$.bodySite[0].coding[0].code").alias("body_site_code"),
        json("$.bodySite[0].coding[0].display").alias("body_site_display"),
        F.when(F.col("resource_family") == "MimicProcedureED", "ed")
        .when(F.col("resource_family") == "MimicProcedureICU", "icu")
        .otherwise("hospital")
        .alias("source_system"),
        *lineage_columns(),
    )
    write_delta(
        procedure,
        table_name(config.catalog, config.silver_schema, "procedure"),
    )


def build_silver(spark: SparkSession, config: CloudConfig) -> None:
    build_patient(spark, config)
    build_encounter(spark, config)
    build_observation(spark, config)
    build_condition(spark, config)
    build_medication(spark, config)
    build_medication_request(spark, config)
    build_medication_administration(spark, config)
    build_medication_dispense(spark, config)
    build_medication_statement(spark, config)
    build_procedure(spark, config)


def build_relationship_audit(spark: SparkSession, config: CloudConfig) -> None:
    patient = table_name(config.catalog, config.silver_schema, "patient")
    encounter = table_name(config.catalog, config.silver_schema, "encounter")
    observation = table_name(config.catalog, config.silver_schema, "observation")
    condition = table_name(config.catalog, config.silver_schema, "condition")
    medication = table_name(config.catalog, config.silver_schema, "medication")
    ingredient = table_name(
        config.catalog,
        config.silver_schema,
        "medication_ingredient",
    )
    request = table_name(config.catalog, config.silver_schema, "medication_request")
    administration = table_name(
        config.catalog,
        config.silver_schema,
        "medication_administration",
    )
    dispense = table_name(config.catalog, config.silver_schema, "medication_dispense")
    statement = table_name(config.catalog, config.silver_schema, "medication_statement")
    procedure = table_name(config.catalog, config.silver_schema, "procedure")
    audit = spark.sql(
        f"""
        select
          current_timestamp() as generated_at,
          (select count(*) from {patient}) as patient_rows,
          (select count(*) from {encounter}) as encounter_rows,
          (select count(*) from {observation}) as observation_rows,
          (select count(*) from {condition}) as condition_rows,
          (select count(*) from {medication}) as medication_rows,
          (select count(*) from {ingredient}) as medication_ingredient_rows,
          (select count(*) from {request}) as medication_request_rows,
          (select count(*) from {administration})
            as medication_administration_rows,
          (select count(*) from {dispense}) as medication_dispense_rows,
          (select count(*) from {statement}) as medication_statement_rows,
          (select count(*) from {procedure}) as procedure_rows,
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
            select count(*) from {request} where patient_id is null
          ) as medication_request_missing_patient_id,
          (
            select count(*) from {request} where encounter_id is null
          ) as medication_request_missing_encounter_id,
          (
            select count(*) from {request}
            where medication_id is null and medication_code is null
          ) as medication_request_missing_medication_concept,
          (
            select count(*) from {administration} where patient_id is null
          ) as medication_administration_missing_patient_id,
          (
            select count(*) from {administration} where encounter_id is null
          ) as medication_administration_missing_encounter_id,
          (
            select count(*) from {administration}
            where medication_request_id is null
          ) as medication_administration_missing_request_id,
          (
            select count(*) from {dispense} where patient_id is null
          ) as medication_dispense_missing_patient_id,
          (
            select count(*) from {dispense} where encounter_id is null
          ) as medication_dispense_missing_encounter_id,
          (
            select count(*) from {dispense} where medication_request_id is null
          ) as medication_dispense_missing_request_id,
          (
            select count(*) from {statement} where patient_id is null
          ) as medication_statement_missing_patient_id,
          (
            select count(*) from {statement} where encounter_id is null
          ) as medication_statement_missing_encounter_id,
          (
            select count(*) from {procedure} where patient_id is null
          ) as procedure_missing_patient_id,
          (
            select count(*) from {procedure} where encounter_id is null
          ) as procedure_missing_encounter_id,
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
          ,
          (
            select count(*)
            from {ingredient} i
            left join {medication} m on i.medication_id = m.medication_id
            where i.medication_id is not null and m.medication_id is null
          ) as medication_ingredient_orphan_medication_id,
          (
            select count(*)
            from {ingredient} i
            left join {medication} m
              on i.ingredient_medication_id = m.medication_id
            where i.ingredient_medication_id is not null
              and m.medication_id is null
          ) as medication_ingredient_orphan_ingredient_medication_id,
          (
            select count(*)
            from {request} r
            left join {patient} p on r.patient_id = p.patient_id
            where r.patient_id is not null and p.patient_id is null
          ) as medication_request_orphan_patient_id,
          (
            select count(*)
            from {request} r
            left join {encounter} e on r.encounter_id = e.encounter_id
            where r.encounter_id is not null and e.encounter_id is null
          ) as medication_request_orphan_encounter_id,
          (
            select count(*)
            from {request} r
            left join {medication} m on r.medication_id = m.medication_id
            where r.medication_id is not null and m.medication_id is null
          ) as medication_request_orphan_medication_id,
          (
            select count(*)
            from {administration} a
            left join {patient} p on a.patient_id = p.patient_id
            where a.patient_id is not null and p.patient_id is null
          ) as medication_administration_orphan_patient_id,
          (
            select count(*)
            from {administration} a
            left join {encounter} e on a.encounter_id = e.encounter_id
            where a.encounter_id is not null and e.encounter_id is null
          ) as medication_administration_orphan_encounter_id,
          (
            select count(*)
            from {administration} a
            left join {request} r
              on a.medication_request_id = r.medication_request_id
            where a.medication_request_id is not null
              and r.medication_request_id is null
          ) as medication_administration_orphan_request_id,
          (
            select count(*)
            from {dispense} d
            left join {patient} p on d.patient_id = p.patient_id
            where d.patient_id is not null and p.patient_id is null
          ) as medication_dispense_orphan_patient_id,
          (
            select count(*)
            from {dispense} d
            left join {encounter} e on d.encounter_id = e.encounter_id
            where d.encounter_id is not null and e.encounter_id is null
          ) as medication_dispense_orphan_encounter_id,
          (
            select count(*)
            from {dispense} d
            left join {request} r
              on d.medication_request_id = r.medication_request_id
            where d.medication_request_id is not null
              and r.medication_request_id is null
          ) as medication_dispense_orphan_request_id,
          (
            select count(*)
            from {statement} s
            left join {patient} p on s.patient_id = p.patient_id
            where s.patient_id is not null and p.patient_id is null
          ) as medication_statement_orphan_patient_id,
          (
            select count(*)
            from {statement} s
            left join {encounter} e on s.encounter_id = e.encounter_id
            where s.encounter_id is not null and e.encounter_id is null
          ) as medication_statement_orphan_encounter_id,
          (
            select count(*)
            from {procedure} pr
            left join {patient} p on pr.patient_id = p.patient_id
            where pr.patient_id is not null and p.patient_id is null
          ) as procedure_orphan_patient_id,
          (
            select count(*)
            from {procedure} pr
            left join {encounter} e on pr.encounter_id = e.encounter_id
            where pr.encounter_id is not null and e.encounter_id is null
          ) as procedure_orphan_encounter_id
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
        ("medication", "medication_display", "clinical_attribute"),
        ("medication_request", "authored_datetime", "date_precision"),
        ("medication_request", "medication_display", "clinical_attribute"),
        (
            "medication_administration",
            "effective_start_datetime",
            "date_precision",
        ),
        (
            "medication_administration",
            "medication_display",
            "clinical_attribute",
        ),
        ("medication_dispense", "when_handed_over_datetime", "date_precision"),
        ("medication_dispense", "medication_display", "clinical_attribute"),
        ("medication_statement", "date_asserted_datetime", "date_precision"),
        ("medication_statement", "medication_display", "clinical_attribute"),
        ("procedure", "performed_start_datetime", "date_precision"),
        ("procedure", "procedure_display", "clinical_attribute"),
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
            ),
            medication_request_counts as (
              select encounter_id, count(*) as medication_request_count
              from {silver("medication_request")}
              where encounter_id is not null
              group by encounter_id
            ),
            medication_administration_counts as (
              select encounter_id, count(*) as medication_administration_count
              from {silver("medication_administration")}
              where encounter_id is not null
              group by encounter_id
            ),
            medication_dispense_counts as (
              select encounter_id, count(*) as medication_dispense_count
              from {silver("medication_dispense")}
              where encounter_id is not null
              group by encounter_id
            ),
            medication_statement_counts as (
              select encounter_id, count(*) as medication_statement_count
              from {silver("medication_statement")}
              where encounter_id is not null
              group by encounter_id
            ),
            medication_distinct_counts as (
              select
                encounter_id,
                count(distinct concat(
                  coalesce(medication_code, ''),
                  '|',
                  coalesce(medication_display, '')
                )) as distinct_medication_count
              from (
                select encounter_id, medication_code, medication_display
                from {silver("medication_request")}
                union all
                select encounter_id, medication_code, medication_display
                from {silver("medication_administration")}
                union all
                select encounter_id, medication_code, medication_display
                from {silver("medication_dispense")}
                union all
                select encounter_id, medication_code, medication_display
                from {silver("medication_statement")}
              )
              where encounter_id is not null
              group by encounter_id
            ),
            procedure_counts as (
              select
                encounter_id,
                count(*) as procedure_count,
                count(distinct concat(
                  coalesce(procedure_code, ''),
                  '|',
                  coalesce(procedure_display, '')
                )) as distinct_procedure_count
              from {silver("procedure")}
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
              coalesce(mr.medication_request_count, 0)
                as medication_request_count,
              coalesce(ma.medication_administration_count, 0)
                as medication_administration_count,
              coalesce(md.medication_dispense_count, 0)
                as medication_dispense_count,
              coalesce(ms.medication_statement_count, 0)
                as medication_statement_count,
              coalesce(pc.procedure_count, 0) as procedure_count,
              coalesce(mc.distinct_medication_count, 0)
                as distinct_medication_count,
              coalesce(pc.distinct_procedure_count, 0)
                as distinct_procedure_count,
              e.discharge_disposition
            from {silver("encounter")} e
            left join observation_counts o on e.encounter_id = o.encounter_id
            left join condition_counts c on e.encounter_id = c.encounter_id
            left join medication_request_counts mr
              on e.encounter_id = mr.encounter_id
            left join medication_administration_counts ma
              on e.encounter_id = ma.encounter_id
            left join medication_dispense_counts md
              on e.encounter_id = md.encounter_id
            left join medication_statement_counts ms
              on e.encounter_id = ms.encounter_id
            left join medication_distinct_counts mc
              on e.encounter_id = mc.encounter_id
            left join procedure_counts pc on e.encounter_id = pc.encounter_id
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

    write_delta(
        spark.sql(
            f"""
            with medication_events as (
              select patient_id, encounter_id, medication_code,
                medication_display, 'request' as activity_type,
                'order' as source_system
              from {silver("medication_request")}
              union all
              select patient_id, encounter_id, medication_code,
                medication_display, 'administration', source_system
              from {silver("medication_administration")}
              union all
              select patient_id, encounter_id, medication_code,
                medication_display, 'dispense', source_system
              from {silver("medication_dispense")}
              union all
              select patient_id, encounter_id, medication_code,
                medication_display, 'statement', source_system
              from {silver("medication_statement")}
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
              count_if(m.encounter_id is not null) as with_encounter_context_count,
              count_if(m.encounter_id is null) as without_encounter_context_count
            from medication_events m
            left join {silver("encounter")} e on m.encounter_id = e.encounter_id
            group by
              coalesce(m.medication_code, 'unknown'),
              coalesce(m.medication_display, m.medication_code, 'unknown'),
              m.activity_type,
              m.source_system,
              e.class_code,
              e.class_display
            """
        ),
        gold("medication_activity"),
    )

    write_delta(
        spark.sql(
            f"""
            with administration_counts as (
              select medication_request_id,
                count(*) as administration_count,
                min(effective_start_datetime) as first_administration_datetime
              from {silver("medication_administration")}
              where medication_request_id is not null
              group by medication_request_id
            ),
            dispense_counts as (
              select medication_request_id,
                count(*) as dispense_count,
                min(when_handed_over_datetime) as first_dispense_datetime
              from {silver("medication_dispense")}
              where medication_request_id is not null
              group by medication_request_id
            )
            select
              md5(concat('medication_request:', r.medication_request_id))
                as medication_request_key,
              md5(concat('patient:', r.patient_id)) as patient_key,
              case
                when r.encounter_id is not null
                then md5(concat('encounter:', r.encounter_id))
                else null
              end as encounter_key,
              r.medication_code,
              r.medication_display,
              r.status as request_status,
              r.intent as request_intent,
              year(to_timestamp(substr(r.authored_datetime, 1, 19)))
                as authored_year,
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
            from {silver("medication_request")} r
            left join administration_counts a
              on r.medication_request_id = a.medication_request_id
            left join dispense_counts d
              on r.medication_request_id = d.medication_request_id
            """
        ),
        gold("medication_order_fulfillment"),
    )

    write_delta(
        spark.sql(
            f"""
            select
              p.procedure_code,
              p.procedure_display,
              p.source_system,
              e.class_code as encounter_class,
              e.class_display as encounter_class_display,
              count(distinct p.patient_id) as patient_count,
              count(distinct p.encounter_id) as encounter_count,
              count(*) as procedure_count,
              count_if(p.body_site_code is not null) as with_body_site_count
            from {silver("procedure")} p
            left join {silver("encounter")} e on p.encounter_id = e.encounter_id
            group by
              p.procedure_code,
              p.procedure_display,
              p.source_system,
              e.class_code,
              e.class_display
            """
        ),
        gold("procedure_summary"),
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
        select 'silver_medication_rows', 'silver',
          case when count(*) = 1794 then 'pass' else 'fail' end,
          cast(count(*) as string), '1794'
        from {silver("medication")}
        union all
        select 'silver_medication_request_rows', 'silver',
          case when count(*) = 17552 then 'pass' else 'fail' end,
          cast(count(*) as string), '17552'
        from {silver("medication_request")}
        union all
        select 'silver_medication_administration_rows', 'silver',
          case when count(*) = 56535 then 'pass' else 'fail' end,
          cast(count(*) as string), '56535'
        from {silver("medication_administration")}
        union all
        select 'silver_medication_dispense_rows', 'silver',
          case when count(*) = 15375 then 'pass' else 'fail' end,
          cast(count(*) as string), '15375'
        from {silver("medication_dispense")}
        union all
        select 'silver_medication_statement_rows', 'silver',
          case when count(*) = 2411 then 'pass' else 'fail' end,
          cast(count(*) as string), '2411'
        from {silver("medication_statement")}
        union all
        select 'silver_procedure_rows', 'silver',
          case when count(*) = 3450 then 'pass' else 'fail' end,
          cast(count(*) as string), '3450'
        from {silver("procedure")}
        union all
        select 'relationship_orphans', 'relationships',
          case
            when observation_orphan_patient_id = 0
              and observation_orphan_encounter_id = 0
              and condition_orphan_patient_id = 0
              and condition_orphan_encounter_id = 0
              and medication_ingredient_orphan_medication_id = 0
              and medication_ingredient_orphan_ingredient_medication_id = 0
              and medication_request_orphan_patient_id = 0
              and medication_request_orphan_encounter_id = 0
              and medication_request_orphan_medication_id = 0
              and medication_administration_orphan_patient_id = 0
              and medication_administration_orphan_encounter_id = 0
              and medication_administration_orphan_request_id = 0
              and medication_dispense_orphan_patient_id = 0
              and medication_dispense_orphan_encounter_id = 0
              and medication_dispense_orphan_request_id = 0
              and medication_statement_orphan_patient_id = 0
              and medication_statement_orphan_encounter_id = 0
              and procedure_orphan_patient_id = 0
              and procedure_orphan_encounter_id = 0
            then 'pass' else 'fail'
          end,
          cast(
            observation_orphan_patient_id
            + observation_orphan_encounter_id
            + condition_orphan_patient_id
            + condition_orphan_encounter_id
            + medication_ingredient_orphan_medication_id
            + medication_ingredient_orphan_ingredient_medication_id
            + medication_request_orphan_patient_id
            + medication_request_orphan_encounter_id
            + medication_request_orphan_medication_id
            + medication_administration_orphan_patient_id
            + medication_administration_orphan_encounter_id
            + medication_administration_orphan_request_id
            + medication_dispense_orphan_patient_id
            + medication_dispense_orphan_encounter_id
            + medication_dispense_orphan_request_id
            + medication_statement_orphan_patient_id
            + medication_statement_orphan_encounter_id
            + procedure_orphan_patient_id
            + procedure_orphan_encounter_id
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
        union all
        select 'gold_medication_activity_rows', 'gold',
          case when count(*) > 0 then 'pass' else 'fail' end,
          cast(count(*) as string), '> 0'
        from {gold("medication_activity")}
        union all
        select 'gold_medication_order_fulfillment_rows', 'gold',
          case when count(*) > 0 then 'pass' else 'fail' end,
          cast(count(*) as string), '> 0'
        from {gold("medication_order_fulfillment")}
        union all
        select 'gold_procedure_summary_rows', 'gold',
          case when count(*) > 0 then 'pass' else 'fail' end,
          cast(count(*) as string), '> 0'
        from {gold("procedure_summary")}
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
