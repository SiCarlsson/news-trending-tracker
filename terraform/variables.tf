variable "bigquery_project_id" {
  description = "Google Cloud project ID"
  type        = string
}

variable "bigquery_dataset_id" {
  description = "BigQuery dataset ID"
  type        = string
}

variable "bigquery_staging_dataset_id" {
  description = "BigQuery staging dataset ID"
  type        = string
}

variable "bigquery_dataset_location" {
  description = "BigQuery dataset location"
  type        = string
}
