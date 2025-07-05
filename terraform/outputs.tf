output "dataset_id" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.custom_dataset.dataset_id
}

output "tables_created" {
  description = "List of tables created"
  value       = keys(local.tables)
}
