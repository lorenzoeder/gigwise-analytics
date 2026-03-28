SHELL := /bin/bash
MAKEFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
ROOT_DIR := $(dir $(MAKEFILE_PATH))
DBT_BUILD_FLAGS ?= --fail-fast

.PHONY: help env-check gcp-auth-check bruin-check setup-infra destroy-infra wipe-ingestion wipe-all run-dlt run-dlt-dry run-bruin run-bruin-dry run-dbt run-dbt-debug run-dashboard run-dashboard-logs stop-dashboard run-spark run-streaming stop-streaming test lint fmt

help:
	@echo "Available targets:"
	@echo "  env-check       Validate required environment variables"
	@echo "  gcp-auth-check  Validate Application Default Credentials"
	@echo "  bruin-check     Verify Bruin CLI is installed"
	@echo "  setup-infra     Initialize and apply Terraform"
	@echo "  destroy-infra   Destroy Terraform-managed infrastructure"
	@echo "  wipe-ingestion  Truncate raw ingestion tables + drop staging view (requires CONFIRM_WIPE=1)"
	@echo "  wipe-all        Wipe ALL data: raw, dlt state, staging, dbt models, streaming, Spark (requires CONFIRM_WIPE=1)"
	@echo "  run-dlt         Load Ticketmaster/Setlist.fm/MusicBrainz via dlt"
	@echo "  run-dlt-dry     Fetch all ingestion sources (no load)"
	@echo "  run-bruin       Full pipeline: dlt ingestion + SQL staging + quality + dbt build"
	@echo "  run-bruin-dry   Validate Bruin pipeline only (no execution)"
	@echo "  run-dbt-debug   Validate dbt profile/connection"
	@echo "  run-dbt         Run dbt models and tests (incremental by default; full refresh: make run-dbt DBT_BUILD_FLAGS='--full-refresh --fail-fast')"
	@echo "  run-dashboard   Start Streamlit dashboard (background, logs to logs/dashboard.log)"
	@echo "  run-dashboard-logs     Tail Streamlit dashboard logs"
	@echo "  stop-dashboard         Stop background Streamlit process"
	@echo "  run-spark       Submit PySpark artist similarity job to Dataproc Serverless"
	@echo "  run-streaming   Start Kafka broker + producer + consumer (streaming add-on)"
	@echo "  stop-streaming  Stop all streaming processes"
	@echo "  test            Run lightweight project checks"
	@echo "  lint            Validate Terraform formatting and syntax"
	@echo "  fmt             Format Terraform files"


env-check:
	@cd $(ROOT_DIR) && \
	set -a; [ -f .env ] && source .env; set +a; \
	test -n "$$GCP_PROJECT_ID" || (echo "Missing GCP_PROJECT_ID" && exit 1); \
	test -n "$$GCP_REGION" || (echo "Missing GCP_REGION" && exit 1)

gcp-auth-check:
	@command -v gcloud >/dev/null 2>&1 || (echo "gcloud CLI not found on PATH" && exit 1)
	@gcloud auth application-default print-access-token >/dev/null 2>&1 || (echo "Missing Application Default Credentials. Run: gcloud auth application-default login" && exit 1)

bruin-check:
	@command -v bruin >/dev/null 2>&1 || (echo "Bruin CLI not found on PATH. Install with: curl -fsSL https://raw.githubusercontent.com/bruin-data/bruin/main/install.sh | bash" && exit 1)

setup-infra:
	$(MAKE) -f $(MAKEFILE_PATH) env-check
	$(MAKE) -f $(MAKEFILE_PATH) gcp-auth-check
	cd $(ROOT_DIR) && set -a; [ -f .env ] && source .env; set +a; \
	cd $(ROOT_DIR)terraform && terraform init && terraform apply -auto-approve -var "gcp_project_id=$$GCP_PROJECT_ID" -var "gcp_region=$$GCP_REGION" -var "existing_pipeline_sa_email=$${EXISTING_PIPELINE_SA_EMAIL:-}"

destroy-infra:
	$(MAKE) -f $(MAKEFILE_PATH) env-check
	$(MAKE) -f $(MAKEFILE_PATH) gcp-auth-check
	cd $(ROOT_DIR) && set -a; [ -f .env ] && source .env; set +a; \
	cd $(ROOT_DIR)terraform && terraform destroy -auto-approve -var "gcp_project_id=$$GCP_PROJECT_ID" -var "gcp_region=$$GCP_REGION" -var "existing_pipeline_sa_email=$${EXISTING_PIPELINE_SA_EMAIL:-}"

wipe-ingestion:
	@test "$$CONFIRM_WIPE" = "1" || (echo "Refusing to wipe data. Re-run with: make wipe-ingestion CONFIRM_WIPE=1" && exit 1)
	cd $(ROOT_DIR) && set -a && source .env && set +a && \
	bq query --nouse_legacy_sql "TRUNCATE TABLE \`$$GCP_PROJECT_ID.$$BQ_DATASET_RAW.ticketmaster_events\`" && \
	bq query --nouse_legacy_sql "TRUNCATE TABLE \`$$GCP_PROJECT_ID.$$BQ_DATASET_RAW.setlistfm_setlists\`" && \
	bq query --nouse_legacy_sql "TRUNCATE TABLE \`$$GCP_PROJECT_ID.$$BQ_DATASET_RAW.musicbrainz_artists\`" && \
	bq query --nouse_legacy_sql "TRUNCATE TABLE \`$$GCP_PROJECT_ID.$$BQ_DATASET_STREAMING.live_event_updates\`" && \
	bq query --nouse_legacy_sql "TRUNCATE TABLE \`$$GCP_PROJECT_ID.$$BQ_DATASET_ANALYTICS.spark_artist_similarity\`" && \
	bq query --nouse_legacy_sql "DROP VIEW IF EXISTS \`$$GCP_PROJECT_ID.$$BQ_DATASET_STAGING.stg_concerts_union\`"
	@echo "Ingestion data wiped: raw source tables truncated, materialized analytics tables truncated, staging view dropped."

wipe-all:
	@test "$$CONFIRM_WIPE" = "1" || (echo "Refusing to wipe data. Re-run with: make wipe-all CONFIRM_WIPE=1" && exit 1)
	cd $(ROOT_DIR) && set -a && source .env && set +a && \
	echo "--- Truncating raw tables ---" && \
	bq query --nouse_legacy_sql "TRUNCATE TABLE \`$$GCP_PROJECT_ID.$$BQ_DATASET_RAW.ticketmaster_events\`" && \
	bq query --nouse_legacy_sql "TRUNCATE TABLE \`$$GCP_PROJECT_ID.$$BQ_DATASET_RAW.setlistfm_setlists\`" && \
	bq query --nouse_legacy_sql "TRUNCATE TABLE \`$$GCP_PROJECT_ID.$$BQ_DATASET_RAW.musicbrainz_artists\`" && \
	echo "--- Clearing dlt state ---" && \
	(bq query --nouse_legacy_sql "DELETE FROM \`$$GCP_PROJECT_ID.$$BQ_DATASET_RAW._dlt_loads\` WHERE true" 2>/dev/null || true) && \
	(bq query --nouse_legacy_sql "DELETE FROM \`$$GCP_PROJECT_ID.$$BQ_DATASET_RAW._dlt_pipeline_state\` WHERE true" 2>/dev/null || true) && \
	(bq query --nouse_legacy_sql "DELETE FROM \`$$GCP_PROJECT_ID.$$BQ_DATASET_RAW._dlt_version\` WHERE true" 2>/dev/null || true) && \
	echo "--- Dropping Bruin staging view ---" && \
	bq query --nouse_legacy_sql "DROP VIEW IF EXISTS \`$$GCP_PROJECT_ID.$$BQ_DATASET_STAGING.stg_concerts_union\`" && \
	echo "--- Dropping dbt models ---" && \
	bq rm -f -t "$$GCP_PROJECT_ID:$$BQ_DATASET_ANALYTICS.stg_ticketmaster__events" && \
	bq rm -f -t "$$GCP_PROJECT_ID:$$BQ_DATASET_ANALYTICS.stg_setlistfm__setlists" && \
	bq rm -f -t "$$GCP_PROJECT_ID:$$BQ_DATASET_ANALYTICS.int_concerts_unified" && \
	bq rm -f -t "$$GCP_PROJECT_ID:$$BQ_DATASET_ANALYTICS.dim_artist" && \
	bq rm -f -t "$$GCP_PROJECT_ID:$$BQ_DATASET_ANALYTICS.fact_concert" && \
	bq rm -f -t "$$GCP_PROJECT_ID:$$BQ_DATASET_ANALYTICS.mart_artist_setlist_freshness" && \
	bq rm -f -t "$$GCP_PROJECT_ID:$$BQ_DATASET_ANALYTICS.mart_artist_touring_intensity" && \
	bq rm -f -t "$$GCP_PROJECT_ID:$$BQ_DATASET_ANALYTICS.mart_artist_yearly_repertoire" && \
	echo "--- Truncating streaming table ---" && \
	(bq query --nouse_legacy_sql "TRUNCATE TABLE \`$$GCP_PROJECT_ID.$$BQ_DATASET_STREAMING.live_event_updates\`" 2>/dev/null || true) && \
	echo "--- Truncating Spark output ---" && \
	(bq query --nouse_legacy_sql "TRUNCATE TABLE \`$$GCP_PROJECT_ID.$$BQ_DATASET_ANALYTICS.spark_artist_similarity\`" 2>/dev/null || true) && \
	echo "--- Deleting MusicBrainz cache from GCS ---" && \
	(gsutil rm "gs://$$DATA_LAKE_BUCKET/cache/mb_artist_cache.json" 2>/dev/null || true)
	@echo ""
	@echo "All data wiped. Rebuild with: make run-bruin && make run-spark"

run-dlt:
	cd $(ROOT_DIR) && set -a && source .env && set +a && PYTHONUNBUFFERED=1 uv run python $(ROOT_DIR)dlt/ingest_pipeline.py

run-dlt-dry:
	cd $(ROOT_DIR) && set -a && source .env && set +a && PYTHONUNBUFFERED=1 uv run python $(ROOT_DIR)dlt/ingest_pipeline.py --dry-run

run-bruin:
	$(MAKE) -f $(MAKEFILE_PATH) bruin-check
	cd $(ROOT_DIR) && set -a && source .env && set +a && cd bruin && bruin run . --env dev

run-bruin-dry:
	$(MAKE) -f $(MAKEFILE_PATH) bruin-check
	cd $(ROOT_DIR) && set -a && source .env && set +a && cd bruin && bruin validate . --env dev --fast

run-dbt-debug:
	cd $(ROOT_DIR) && set -a && source .env && set +a && cd dbt && uv run dbt debug --target prod

run-dbt:
	$(MAKE) -f $(MAKEFILE_PATH) run-dbt-debug
	cd $(ROOT_DIR) && set -a && source .env && set +a && cd dbt && uv run dbt deps && uv run dbt build --target prod $(DBT_BUILD_FLAGS)

run-dashboard:
	cd $(ROOT_DIR) && mkdir -p logs && set -a && source .env && set +a && \
	STREAMLIT_BROWSER_GATHER_USAGE_STATS=false nohup uv run streamlit run streamlit/streamlit_app.py > logs/dashboard.log 2>&1 & \
	DASH_PID=$$!; echo "$$DASH_PID" > logs/dashboard.pid; \
	echo "Started Streamlit in background (PID $$DASH_PID)"; \
	echo "Logs: $(ROOT_DIR)logs/dashboard.log"

run-dashboard-logs:
	cd $(ROOT_DIR) && test -f logs/dashboard.log || (echo "No dashboard log found. Start with make run-dashboard" && exit 1)
	cd $(ROOT_DIR) && tail -f logs/dashboard.log

stop-dashboard:
	@cd $(ROOT_DIR) && \
	if pgrep -f "[s]treamlit run streamlit/streamlit_app.py" > /dev/null 2>&1; then \
		pkill -f "[s]treamlit run streamlit/streamlit_app.py" 2>/dev/null; \
		rm -f logs/dashboard.pid; \
		echo "Stopped Streamlit dashboard"; \
	else \
		echo "No running Streamlit dashboard found"; \
	fi

run-spark:
	@cd $(ROOT_DIR) && set -a && source .env && set +a && \
	uv run python spark_jobs/run_standalone.py

run-streaming:
	@cd $(ROOT_DIR) && mkdir -p logs && set -a && source .env && set +a && \
	if [ -f logs/producer.pid ] && kill -0 $$(cat logs/producer.pid) 2>/dev/null; then \
		echo "Streaming is already running. Run 'make stop-streaming' first."; exit 1; \
	fi && \
	docker compose up -d kafka kafka-ui && \
	echo "Waiting for Kafka broker to be ready..." && \
	for i in $$(seq 1 30); do \
		if docker compose exec -T kafka rpk cluster health --api-urls kafka:9644 2>/dev/null | grep -q 'Healthy'; then break; fi; \
		if [ $$i -eq 30 ]; then echo "Kafka broker failed to start"; exit 1; fi; \
		sleep 2; \
	done && \
	echo "Kafka broker is ready" && \
	nohup uv run python kafka/consumer.py >> logs/consumer.log 2>&1 & \
	sleep 3 && \
	nohup uv run python kafka/producer.py >> logs/producer.log 2>&1 & \
	sleep 2 && \
	echo "Streaming started in background" && \
	echo "  Producer log: logs/producer.log" && \
	echo "  Consumer log: logs/consumer.log" && \
	echo "  Kafka UI:     http://localhost:8082" && \
	echo "  Stop with:    make stop-streaming"

stop-streaming:
	@cd $(ROOT_DIR) && \
	stopped=0; \
	for proc in producer consumer; do \
		if [ -f logs/$${proc}.pid ]; then \
			pid=$$(cat logs/$${proc}.pid); \
			if kill -0 $$pid 2>/dev/null; then \
				kill $$pid 2>/dev/null; \
				for i in $$(seq 1 10); do \
					kill -0 $$pid 2>/dev/null || break; \
					if [ $$i -eq 10 ]; then kill -9 $$pid 2>/dev/null; fi; \
					sleep 0.5; \
				done; \
				echo "Stopped $${proc} (PID $$pid)"; \
				stopped=$$((stopped + 1)); \
			fi; \
			rm -f logs/$${proc}.pid; \
		fi; \
	done; \
	docker compose stop kafka kafka-ui 2>/dev/null; \
	if [ $$stopped -eq 0 ]; then echo "No streaming processes were running"; fi; \
	echo "Streaming stack stopped"

test:
	cd $(ROOT_DIR) && set -a && source .env && set +a && cd dbt && uv run dbt test --target prod

lint:
	cd terraform && terraform fmt -check && terraform validate

fmt:
	cd terraform && terraform fmt
