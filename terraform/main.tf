locals {
  lake_bucket_name = "gigwise-analytics-${var.environment}-${var.gcp_project_id}"
}

resource "google_storage_bucket" "data_lake" {
  name                        = local.lake_bucket_name
  location                    = var.gcp_region
  force_destroy               = var.bucket_force_destroy
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }
}

resource "google_bigquery_dataset" "datasets" {
  for_each   = toset(var.datasets)
  dataset_id = each.key
  location   = var.gcp_region
}

resource "google_service_account" "pipeline" {
  count        = var.create_pipeline_sa ? 1 : 0
  account_id   = "gigwise-pipeline-${var.environment}"
  display_name = "Gigwise pipeline service account (${var.environment})"
}
