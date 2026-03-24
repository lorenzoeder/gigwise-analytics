locals {
  lake_bucket_name       = "gigwise-analytics-${var.environment}-${var.gcp_project_id}"
  pipeline_service_email = var.existing_pipeline_sa_email != "" ? var.existing_pipeline_sa_email : (var.create_pipeline_sa ? google_service_account.pipeline[0].email : null)
}

resource "google_project_service" "dataproc" {
  service            = "dataproc.googleapis.com"
  disable_on_destroy = false
}

resource "google_storage_bucket" "data_lake" {
  name                        = local.lake_bucket_name
  location                    = var.gcp_region
  force_destroy               = var.bucket_force_destroy
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age                = 7
      with_state         = "ARCHIVED"
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age            = 1
      matches_prefix = ["spark-bigquery-"]
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_bigquery_dataset" "datasets" {
  for_each   = toset(var.datasets)
  dataset_id = each.key
  location   = var.gcp_region
}

resource "google_service_account" "pipeline" {
  count        = var.create_pipeline_sa && var.existing_pipeline_sa_email == "" ? 1 : 0
  account_id   = "gigwise-pipeline-${var.environment}"
  display_name = "Gigwise pipeline service account (${var.environment})"
}
