SHELL := /bin/bash

.PHONY: help env-check gcp-auth-check setup-infra destroy-infra run-bruin run-dbt run-dashboard run-spark run-kafka test lint fmt

help:
	@echo "Available targets:"
	@echo "  env-check       Validate required environment variables"
	@echo "  gcp-auth-check  Validate Application Default Credentials"
	@echo "  setup-infra     Initialize and apply Terraform"
	@echo "  destroy-infra   Destroy Terraform-managed infrastructure"
	@echo "  run-bruin       Run Bruin ingestion + staging assets"
	@echo "  run-dbt         Run dbt models and tests"
	@echo "  run-dashboard   Start Streamlit dashboard"
	@echo "  run-spark       Execute Spark join job in container"
	@echo "  run-kafka       Start local Kafka stack"
	@echo "  test            Run lightweight project checks"
	@echo "  lint            Validate Terraform formatting and syntax"
	@echo "  fmt             Format Terraform files"


env-check:
	@test -n "$$GCP_PROJECT_ID" || (echo "Missing GCP_PROJECT_ID" && exit 1)
	@test -n "$$GCP_REGION" || (echo "Missing GCP_REGION" && exit 1)

gcp-auth-check:
	@command -v gcloud >/dev/null 2>&1 || (echo "gcloud CLI not found on PATH" && exit 1)
	@gcloud auth application-default print-access-token >/dev/null 2>&1 || (echo "Missing Application Default Credentials. Run: gcloud auth application-default login" && exit 1)

setup-infra:
	$(MAKE) env-check
	$(MAKE) gcp-auth-check
	cd terraform && terraform init && terraform apply -auto-approve -var "gcp_project_id=$$GCP_PROJECT_ID" -var "gcp_region=$$GCP_REGION" -var "existing_pipeline_sa_email=$${EXISTING_PIPELINE_SA_EMAIL:-}"

destroy-infra:
	$(MAKE) env-check
	$(MAKE) gcp-auth-check
	cd terraform && terraform destroy -auto-approve -var "gcp_project_id=$$GCP_PROJECT_ID" -var "gcp_region=$$GCP_REGION" -var "existing_pipeline_sa_email=$${EXISTING_PIPELINE_SA_EMAIL:-}"

run-bruin:
	bruin run bruin_pipeline/

run-dbt:
	cd dbt_concert && dbt deps && dbt build --target prod

run-dashboard:
	streamlit run dashboard/streamlit_app.py

run-spark:
	@test -n "$$DATA_LAKE_BUCKET" || (echo "Missing DATA_LAKE_BUCKET" && exit 1)
	docker compose run --rm spark /opt/bitnami/spark/bin/spark-submit /opt/spark_jobs/join_raw_sources.py --bucket $$DATA_LAKE_BUCKET

run-kafka:
	docker compose up -d kafka kafka-ui

test:
	cd dbt_concert && dbt test --target prod

lint:
	cd terraform && terraform fmt -check && terraform validate

fmt:
	cd terraform && terraform fmt
