.PHONY: install test lint doctor profile profile-inventory profile-schema profile-report bronze bronze-ingest bronze-validate silver silver-build silver-validate relationships relationships-audit relationships-report privacy privacy-audit privacy-report gold gold-build gold-validate quality quality-check quality-report pipeline cloud-validate clean-output

install:
	uv sync

test:
	uv run pytest

lint:
	uv run ruff check .

doctor:
	uv run healthcare-fhir-lakehouse doctor

profile: profile-report

profile-inventory:
	uv run healthcare-fhir-lakehouse profile inventory

profile-schema:
	uv run healthcare-fhir-lakehouse profile schema

profile-report:
	uv run healthcare-fhir-lakehouse profile report

bronze: bronze-ingest bronze-validate

bronze-ingest:
	uv run healthcare-fhir-lakehouse bronze ingest

bronze-validate:
	uv run healthcare-fhir-lakehouse bronze validate

silver: silver-build silver-validate

silver-build:
	uv run healthcare-fhir-lakehouse silver build all

silver-validate:
	uv run healthcare-fhir-lakehouse silver validate

relationships: relationships-report

relationships-audit:
	uv run healthcare-fhir-lakehouse relationships audit

relationships-report:
	uv run healthcare-fhir-lakehouse relationships report

privacy: privacy-report

privacy-audit:
	uv run healthcare-fhir-lakehouse privacy audit

privacy-report:
	uv run healthcare-fhir-lakehouse privacy report

gold: gold-build gold-validate

gold-build:
	uv run healthcare-fhir-lakehouse gold build all

gold-validate:
	uv run healthcare-fhir-lakehouse gold validate

quality: quality-report

quality-check:
	uv run healthcare-fhir-lakehouse quality check

quality-report:
	uv run healthcare-fhir-lakehouse quality report

pipeline:
	uv run healthcare-fhir-lakehouse pipeline run

cloud-validate:
	databricks bundle validate

clean-output:
	find output -mindepth 1 ! -name .gitkeep -exec rm -rf {} +
