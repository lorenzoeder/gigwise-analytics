SHELL := /bin/bash
MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
ROOT_DIR := $(dir $(MAKEFILE_PATH))
DBT_BUILD_FLAGS ?= --full-refresh --fail-fast

.PHONY: help env-check gcp-auth-check bruin-check setup-infra destroy-infra run-dlt-snapshots run-dlt-snapshots-dry run-bruin run-bruin-dry run-dbt run-dbt-debug run-dashboard run-dashboard-detached run-dashboard-logs stop-dashboard run-spark run-kafka test lint fmt

help:
	@echo "Available targets:"
	@echo "  env-check       Validate required environment variables"
	@echo "  gcp-auth-check  Validate Application Default Credentials"
	@echo "  bruin-check     Verify Bruin CLI is installed"
	@echo "  setup-infra     Initialize and apply Terraform"
	@echo "  destroy-infra   Destroy Terraform-managed infrastructure"
	@echo "  run-dlt-snapshots      Load Ticketmaster/Setlist.fm/MusicBrainz/Spotify via dlt"
	@echo "  run-dlt-snapshots-dry  Fetch all ingestion sources (no load)"
	@echo "  run-bruin       Run Bruin orchestration assets (includes dlt ingestion task)"
	@echo "  run-bruin-dry   Validate Bruin pipeline only (no ingestion)"
	@echo "  run-dbt-debug   Validate dbt profile/connection"
	@echo "  run-dbt         Run dbt models and tests"
	@echo "  run-dashboard   Start Streamlit dashboard"
	@echo "  run-dashboard-detached Start Streamlit in background (logs to logs/dashboard.log)"
	@echo "  run-dashboard-logs     Tail Streamlit dashboard logs"
	@echo "  stop-dashboard         Stop background Streamlit process"
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

bruin-check:
	@command -v bruin >/dev/null 2>&1 || (echo "Bruin CLI not found on PATH. Install with: curl -fsSL https://raw.githubusercontent.com/bruin-data/bruin/main/install.sh | bash" && exit 1)

setup-infra:
	$(MAKE) -f $(MAKEFILE_PATH) env-check
	$(MAKE) -f $(MAKEFILE_PATH) gcp-auth-check
	cd $(ROOT_DIR)terraform && terraform init && terraform apply -auto-approve -var "gcp_project_id=$$GCP_PROJECT_ID" -var "gcp_region=$$GCP_REGION" -var "existing_pipeline_sa_email=$${EXISTING_PIPELINE_SA_EMAIL:-}"

destroy-infra:
	$(MAKE) -f $(MAKEFILE_PATH) env-check
	$(MAKE) -f $(MAKEFILE_PATH) gcp-auth-check
	cd $(ROOT_DIR)terraform && terraform destroy -auto-approve -var "gcp_project_id=$$GCP_PROJECT_ID" -var "gcp_region=$$GCP_REGION" -var "existing_pipeline_sa_email=$${EXISTING_PIPELINE_SA_EMAIL:-}"

run-dlt-snapshots:
	cd $(ROOT_DIR) && set -a && source .env && set +a && PYTHONUNBUFFERED=1 uv run python $(ROOT_DIR)dlt_pipeline/ingest_pipeline.py

run-dlt-snapshots-dry:
	cd $(ROOT_DIR) && set -a && source .env && set +a && PYTHONUNBUFFERED=1 uv run python $(ROOT_DIR)dlt_pipeline/ingest_pipeline.py --dry-run

run-bruin:
	$(MAKE) -f $(MAKEFILE_PATH) bruin-check
	cd $(ROOT_DIR) && set -a && source .env && set +a && cd bruin_pipeline && bruin run . --env dev

run-bruin-dry:
	$(MAKE) -f $(MAKEFILE_PATH) bruin-check
	cd $(ROOT_DIR) && set -a && source .env && set +a && cd bruin_pipeline && bruin validate . --env dev --fast

run-dbt-debug:
	cd $(ROOT_DIR) && set -a && source .env && set +a && cd dbt_concert && uv run dbt debug --target prod

run-dbt:
	$(MAKE) -f $(MAKEFILE_PATH) run-dbt-debug
	cd $(ROOT_DIR) && set -a && source .env && set +a && cd dbt_concert && uv run dbt deps && uv run dbt build --target prod $(DBT_BUILD_FLAGS)

run-dashboard:
	cd $(ROOT_DIR) && set -a && source .env && set +a && STREAMLIT_BROWSER_GATHER_USAGE_STATS=false uv run streamlit run dashboard/streamlit_app.py

run-dashboard-detached:
	cd $(ROOT_DIR) && mkdir -p logs && set -a && source .env && set +a && \
	STREAMLIT_BROWSER_GATHER_USAGE_STATS=false nohup uv run streamlit run dashboard/streamlit_app.py > logs/dashboard.log 2>&1 & echo $$! > logs/dashboard.pid && \
	echo "Started Streamlit in background with PID $$(cat logs/dashboard.pid)" && \
	echo "Logs: $(ROOT_DIR)logs/dashboard.log"

run-dashboard-logs:
	cd $(ROOT_DIR) && test -f logs/dashboard.log || (echo "No dashboard log found. Start with make run-dashboard-detached" && exit 1)
	cd $(ROOT_DIR) && tail -f logs/dashboard.log

stop-dashboard:
	cd $(ROOT_DIR) && test -f logs/dashboard.pid || (echo "No dashboard PID file found." && exit 1)
	cd $(ROOT_DIR) && kill $$(cat logs/dashboard.pid) && rm -f logs/dashboard.pid && echo "Stopped Streamlit dashboard"

run-spark:
	@test -n "$$DATA_LAKE_BUCKET" || (echo "Missing DATA_LAKE_BUCKET" && exit 1)
	docker compose run --rm spark /opt/spark/bin/spark-submit /opt/spark_jobs/join_raw_sources.py --bucket $$DATA_LAKE_BUCKET

run-kafka:
	docker compose up -d kafka kafka-ui

test:
	cd $(ROOT_DIR) && set -a && source .env && set +a && cd dbt_concert && uv run dbt test --target prod

lint:
	cd terraform && terraform fmt -check && terraform validate

fmt:
	cd terraform && terraform fmt
