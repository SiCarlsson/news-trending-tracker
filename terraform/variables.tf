variable "BIGQUERY_PROJECT_ID" {
  description = "Google Cloud project ID"
  type        = string
}

variable "BIGQUERY_DATASET_ID" {
  description = "BigQuery dataset ID"
  type        = string
}

variable "BIGQUERY_STAGING_DATASET_ID" {
  description = "BigQuery staging dataset ID"
  type        = string
}

variable "BIGQUERY_DATASET_LOCATION" {
  description = "BigQuery dataset location"
  type        = string
  default     = "EU"
}
