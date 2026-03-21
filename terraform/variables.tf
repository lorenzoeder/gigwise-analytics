variable "gcp_project_id" {
  description = "GCP project ID"
  type        = string
}

variable "gcp_region" {
  description = "GCP region for regional resources"
  type        = string
  default     = "europe-west2"
}

variable "environment" {
  description = "Deployment environment label"
  type        = string
  default     = "dev"
}

variable "bucket_force_destroy" {
  description = "Allow bucket deletion even when objects exist"
  type        = bool
  default     = false
}

variable "datasets" {
  description = "BigQuery datasets to create"
  type        = list(string)
  default     = ["raw", "staging", "analytics", "streaming"]
}

variable "create_pipeline_sa" {
  description = "Whether to create a dedicated pipeline service account"
  type        = bool
  default     = true
}
