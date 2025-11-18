# This file creates the BigQuery infrastructure using Terraform

resource "google_bigquery_dataset" "raw_dataset" {
  dataset_id = var.BIGQUERY_DATASET_ID
  location   = var.BIGQUERY_DATASET_LOCATION
}

resource "google_bigquery_dataset" "staging_dataset" {
  dataset_id = var.BIGQUERY_STAGING_DATASET_ID
  location   = var.BIGQUERY_DATASET_LOCATION
}

resource "google_bigquery_dataset" "aggregation_dataset" {
  dataset_id = var.BIGQUERY_AGGREGATION_DATASET_ID
  location   = var.BIGQUERY_DATASET_LOCATION
}

locals {
  raw_tables_schema = {
    websites = [
      { name = "website_id", type = "STRING", mode = "REQUIRED" },
      { name = "website_name", type = "STRING", mode = "REQUIRED" },
      { name = "website_url", type = "STRING", mode = "REQUIRED" }
    ]
    articles = [
      { name = "article_id", type = "STRING", mode = "REQUIRED" },
      { name = "website_id", type = "STRING", mode = "REQUIRED" },
      { name = "article_title", type = "STRING", mode = "REQUIRED" },
      { name = "article_url", type = "STRING", mode = "REQUIRED" }
    ]
    words = [
      { name = "word_id", type = "STRING", mode = "REQUIRED" },
      { name = "word_text", type = "STRING", mode = "REQUIRED" }
    ]
    occurrences = [
      { name = "occurrence_id", type = "STRING", mode = "REQUIRED" },
      { name = "word_id", type = "STRING", mode = "REQUIRED" },
      { name = "website_id", type = "STRING", mode = "REQUIRED" },
      { name = "article_id", type = "STRING", mode = "REQUIRED" },
      { name = "timestamp", type = "TIMESTAMP", mode = "REQUIRED" }
    ]
  }
}

resource "google_bigquery_table" "tables" {
  for_each            = local.raw_tables_schema
  dataset_id          = google_bigquery_dataset.raw_dataset.dataset_id
  table_id            = each.key
  schema              = jsonencode(each.value)
  deletion_protection = false
}

resource "google_bigquery_table" "staging_tables" {
  for_each            = local.raw_tables_schema
  dataset_id          = google_bigquery_dataset.staging_dataset.dataset_id
  table_id            = "staging_${each.key}"
  schema              = jsonencode(each.value)
  deletion_protection = false
}

resource "google_bigquery_table" "word_trends_10min" {
  dataset_id          = google_bigquery_dataset.aggregation_dataset.dataset_id
  table_id            = "word_trends_10min"
  deletion_protection = false

  schema = jsonencode([
    { name = "window_start", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "window_end", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "word_id", type = "STRING", mode = "REQUIRED" },
    { name = "total_occurrences", type = "INTEGER", mode = "REQUIRED" }
  ])
}