output "raw_dataset_id" {
  description = "BigQuery raw dataset ID"
  value       = google_bigquery_dataset.raw_dataset.dataset_id
}

output "staging_dataset_id" {
  description = "BigQuery staging dataset ID"
  value       = google_bigquery_dataset.staging_dataset.dataset_id
}

output "aggregation_dataset_id" {
  description = "BigQuery aggregation dataset ID"
  value       = google_bigquery_dataset.aggregation_dataset.dataset_id
}

output "raw_tables_created" {
  description = "List of raw data tables created"
  value       = keys(local.raw_tables_schema)
}

output "aggregation_tables_created" {
  description = "List of aggregation tables created"
  value       = [google_bigquery_table.word_trends_10min.table_id]
}