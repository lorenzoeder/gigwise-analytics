output "data_lake_bucket" {
  value       = google_storage_bucket.data_lake.name
  description = "GCS data lake bucket name"
}

output "bigquery_datasets" {
  value       = [for ds in google_bigquery_dataset.datasets : ds.dataset_id]
  description = "BigQuery datasets"
}

output "pipeline_service_account_email" {
  value       = var.create_pipeline_sa ? google_service_account.pipeline[0].email : null
  description = "Service account email for pipeline execution"
}
