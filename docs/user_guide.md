# GigWise Analytics — User Guide

Step-by-step instructions to set up and run this project from scratch.

---

## Prerequisites

You need the following tools installed before starting:

| Tool | Purpose |
|---|---|
| Docker + Compose | Runs Kestra (scheduler), Kafka/Redpanda (streaming) |
| Terraform CLI | Provisions cloud infrastructure (GCS, BigQuery) |
| Google Cloud CLI (`gcloud`) | Authenticates with GCP |
| uv | Fast Python package manager |
| Bruin CLI | Orchestrates the ingestion → transform pipeline |

It is technically possible to run this project locally (with DuckDB for example), however this setup is cloud-native within the Google Cloud Platform.

Verify each is available:

```bash
docker --version && docker compose version
terraform version
gcloud --version
uv --version
bruin --version
```

Install Bruin if missing:

```bash
curl -fsSL https://raw.githubusercontent.com/bruin-data/bruin/main/install.sh | bash
source ~/.bashrc
```

---

## 1. Clone and Install Dependencies

```bash
git clone https://github.com/lorenzoeder/gigwise-analytics.git
cd gigwise-analytics
uv sync
```

`uv sync` reads `pyproject.toml` and installs all Python packages (dlt, dbt, Streamlit, PySpark, kafka-python, etc.) into a managed virtual environment.

---

## 2. Authenticate with GCP

```bash
gcloud auth login
gcloud config set project <YOUR_GCP_PROJECT_ID>
gcloud auth application-default login
gcloud auth application-default set-quota-project <YOUR_GCP_PROJECT_ID>
```

Verify:

```bash
gcloud auth application-default print-access-token | head -c 20 && echo
```

Enable required APIs:

```bash
gcloud services enable bigquery.googleapis.com storage.googleapis.com iam.googleapis.com --project <YOUR_GCP_PROJECT_ID>
```

---

## 3. Get API Keys (all are free, with generous constraints)

| API | Where to sign up | What it provides |
|---|---|---|
| Ticketmaster Discovery API | https://developer.ticketmaster.com/ | Upcoming concert events |
| Setlist.fm API | https://api.setlist.fm/ | Historical setlists with songs played |
| MusicBrainz API | https://musicbrainz.org/doc/MusicBrainz_API | Artist metadata and genre tags (set a `User-Agent` string) |

---

## 4. Configure Environment Variables

```bash
cp .env.example .env
```

Open `.env` and fill in every value. The critical fields are:

```bash
GCP_PROJECT_ID=your-project-id
GCP_REGION=europe-west2
DATA_LAKE_BUCKET=your-bucket-name
TICKETMASTER_API_KEY=your_key
SETLISTFM_API_KEY=your_key
PIPELINE_MODE=prototype          # toggle with 'production' later
TRACKED_ARTISTS=Metallica,BTS,Ariana Grande    # pick 3 widely touring artists to ease prototype
```

See `.env.example` for the full list of settings.

> **Security:** Never commit `.env` or key files. They are gitignored by default. Verify with `git check-ignore -v .env`.

---

## 5. Provision Cloud Infrastructure

```bash
make setup-infra
```

This runs `terraform init && terraform apply` and creates:
- GCS bucket (data lake, used for MusicBrainz artist cache)
- BigQuery datasets: `raw`, `staging`, `analytics`, `streaming`
- Pipeline service account (optional)

If the service account already exists:

```bash
export EXISTING_PIPELINE_SA_EMAIL="your-sa@your-project.iam.gserviceaccount.com"
make setup-infra
```

Verify:

```bash
bq ls --project_id=$GCP_PROJECT_ID
```

---

## 6. Start Docker Services

```bash
docker compose up -d
docker compose ps
```

This starts:

| Service | Port | Purpose |
|---|---|---|
| Kestra | 8081 | Pipeline scheduler with web UI |
| Kafka (Redpanda) | 9092 | Streaming message broker |
| Kafka UI | 8082 | Web interface for Kafka topics |

---

## 7. Run the Pipeline

### Prototype mode (recommended first run)

```bash
make run-bruin
```

This executes the full Bruin DAG in sequence:
1. **dlt ingestion** — fetches data from Ticketmaster, Setlist.fm, and MusicBrainz APIs → loads into BigQuery `raw` dataset
2. **SQL staging** — builds a staging view joining raw tables
3. **Quality checks** — validates event date ranges
4. **dbt build** — transforms raw → staging → intermediate → core → marts in BigQuery `analytics`

Prototype mode takes ~5 minutes and uses a small subset (3 tracked artists, limited pages).

### Production mode

Set `PIPELINE_MODE=production` in your `.env`, then:

```bash
make run-bruin
```

Production mode takes up to ~1 hour and ingests all Ticketmaster artists across 5 countries (US, CA, GB, DE, IT), with full Setlist.fm history.

### Run individual steps

```bash
make run-dlt          # Just ingestion (APIs → BigQuery raw tables)
make run-dlt-dry      # Fetch + transform only, no load (good for testing)
make run-dbt          # Just dbt build (raw → analytics models)
```

---

## 8. Run the Dashboard

```bash
make run-dashboard
```

Open the URL shown in the terminal (default: `http://localhost:8501`).

The dashboard has two main tiles:

1. **Touring Intensity** — which artists have the most upcoming concerts, by country and genre
2. **Setlist Evolution** — how each artist's song repertoire changes year over year, with a freshness index

Optional sections appear when enabled:
- **Similar Artists** — Jaccard similarity by shared venues (requires Spark, see below)
- **Live Event Stream** — real-time Ticketmaster event updates (requires streaming, see below)

```bash
make run-dashboard-logs   # Tail the dashboard log
make stop-dashboard       # Stop the background dashboard process
```

---

## 9. Optional: Spark Batch Processing

PySpark computes artist similarity based on shared concert venues and writes the results back to BigQuery.

```bash
make run-spark
```

This submits a PySpark job to Google Cloud Dataproc Serverless. The result appears as the "Similar Artists" panel in the dashboard.

---

## 10. Optional: Kafka Streaming

A Kafka producer polls the Ticketmaster API every 5 minutes for live event updates, and a consumer writes them to BigQuery in near-real-time.

```bash
make run-streaming        # Start producer + consumer (Kafka broker auto-starts)
make stop-streaming       # Stop everything
```

When streaming is running, the dashboard shows a "Live Event Stream" section with event counts, status breakdown, and recent updates.

---

## 11. Schedule with Kestra

Open http://localhost:8081, import the flow from `kestra/flows/concert_pipeline_daily.yml`, and enable it. Kestra runs `make run-bruin` daily at 06:00 UTC.

You can also trigger runs manually from the Kestra UI.

---

## 12. Export Dashboard for Streamlit Cloud

To deploy the dashboard without live BigQuery access:

```bash
make export-dashboard-data
```

This exports all dashboard queries to Parquet files in `streamlit/data/`. When deployed to Streamlit Cloud with `DASHBOARD_MODE=cloud`, the dashboard reads from these snapshots instead of BigQuery.

---

## 13. Cleanup and Cost Control

### Stop local services

```bash
docker compose down -v         # Stops Kestra, Kafka containers, all volumes
make stop-streaming            # Stops producer + consumer processes
make stop-dashboard            # Stops Streamlit
```

### Wipe data (reversible — pipeline can rebuild it)

```bash
make wipe-ingestion CONFIRM_WIPE=1   # Truncate raw tables + staging view
make wipe-all CONFIRM_WIPE=1         # Wipe everything: raw, dlt state, dbt models, streaming, Spark output, MB cache
```

After wiping, rebuild with:

```bash
make run-bruin && make run-spark
```

### Destroy cloud infrastructure

```bash
make destroy-infra
```

This runs `terraform destroy` and removes the GCS bucket, BigQuery datasets, and service account.

---

## Quick Reference: Make Targets

| Target | What it does |
|---|---|
| `make help` | List all available targets |
| `make env-check` | Validate required environment variables |
| `make setup-infra` | Provision GCS + BigQuery with Terraform |
| `make destroy-infra` | Tear down cloud resources |
| `make run-bruin` | Full pipeline: ingestion → staging → quality → dbt |
| `make run-dlt` | Run dlt ingestion only |
| `make run-dbt` | Run dbt build only |
| `make run-spark` | Submit PySpark similarity job |
| `make run-streaming` | Start Kafka producer + consumer |
| `make stop-streaming` | Stop streaming processes |
| `make run-dashboard` | Start Streamlit dashboard |
| `make stop-dashboard` | Stop Streamlit |
| `make export-dashboard-data` | Export dashboard data to Parquet snapshots |
| `make wipe-ingestion CONFIRM_WIPE=1` | Truncate raw tables |
| `make wipe-all CONFIRM_WIPE=1` | Wipe all data |
| `make test` | Run dbt tests |
| `make lint` | Validate Terraform formatting |
