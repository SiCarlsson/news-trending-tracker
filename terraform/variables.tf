variable "project_id" {
  description = "Google Cloud project ID"
  type        = string
}

variable "dataset_id" {
  description = "BigQuery dataset ID"
  type        = string
}

variable "staging_dataset_id" {
  description = "BigQuery staging dataset ID"
  type        = string
}

variable "dataset_location" {
  description = "BigQuery dataset location"
  type        = string
}
