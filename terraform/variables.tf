variable "BIGQUERY_PROJECT_ID" {
  description = "Google Cloud project ID"
  type        = string
  default     = "news-trending-tracker"
}

variable "BIGQUERY_DATASET_ID" {
  description = "BigQuery dataset ID"
  type        = string
  default     = "raw"
}

variable "BIGQUERY_STAGING_DATASET_ID" {
  description = "BigQuery staging dataset ID"
  type        = string
  default     = "staging"
}

variable "BIGQUERY_METRICS_DATASET_ID" {
  description = "BigQuery metrics dataset ID"
  type        = string
  default     = "metrics"
}

variable "BIGQUERY_DATASET_LOCATION" {
  description = "BigQuery dataset location"
  type        = string
  default     = "europe-west1"
}
