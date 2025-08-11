# This file creates the BigQuery infrastructure using Terraform

resource "google_bigquery_dataset" "custom_dataset" {
  dataset_id = var.BIGQUERY_DATASET_ID
  location   = var.BIGQUERY_DATASET_LOCATION
}

resource "google_bigquery_dataset" "staging_dataset" {
  dataset_id = var.BIGQUERY_STAGING_DATASET_ID
  location   = var.BIGQUERY_DATASET_LOCATION
}

locals {
  tables = {
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
  for_each   = local.tables
  dataset_id = google_bigquery_dataset.custom_dataset.dataset_id
  table_id   = each.key
  schema     = jsonencode(each.value)
}

resource "google_bigquery_table" "staging_table" {
  for_each   = local.tables
  dataset_id = google_bigquery_dataset.staging_dataset.dataset_id
  table_id   = "staging_${each.key}"
  schema     = jsonencode(each.value)
}
