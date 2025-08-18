output "raw_dataset_id" {
  description = "BigQuery raw dataset ID"
  value       = google_bigquery_dataset.raw_dataset.dataset_id
}

output "staging_dataset_id" {
  description = "BigQuery staging dataset ID"
  value       = google_bigquery_dataset.staging_dataset.dataset_id
}

output "metrics_dataset_id" {
  description = "BigQuery metrics dataset ID"
  value       = google_bigquery_dataset.metrics_dataset.dataset_id
}

output "raw_tables_created" {
  description = "List of raw data tables created"
  value       = keys(local.raw_tables_schema)
}

output "metrics_tables_created" {
  description = "List of metrics tables created"
  value       = keys(local.activity_metrics_schema)
}
