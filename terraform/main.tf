# This file creates the BigQuery infrastructure using Terraform

resource "google_bigquery_dataset" "raw_dataset" {
  dataset_id = var.BIGQUERY_DATASET_ID
  location   = var.BIGQUERY_DATASET_LOCATION
}

resource "google_bigquery_dataset" "staging_dataset" {
  dataset_id = var.BIGQUERY_STAGING_DATASET_ID
  location   = var.BIGQUERY_DATASET_LOCATION
}

resource "google_bigquery_dataset" "metrics_dataset" {
  dataset_id = var.BIGQUERY_METRICS_DATASET_ID
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

  activity_metrics_schema = {
    activity_metrics = [
      { name = "window_start", type = "TIMESTAMP", mode = "REQUIRED" },
      { name = "window_end", type = "TIMESTAMP", mode = "REQUIRED" },
      { name = "website_id", type = "STRING", mode = "NULLABLE" },
      { name = "articles_delta", type = "INTEGER", mode = "NULLABLE" },
      { name = "unique_words_delta", type = "INTEGER", mode = "NULLABLE" },
      { name = "total_word_occurrences_delta", type = "INTEGER", mode = "NULLABLE" },
      { name = "articles_cumulative", type = "INTEGER", mode = "NULLABLE" },
      { name = "unique_words_cumulative", type = "INTEGER", mode = "NULLABLE" },
      { name = "total_word_occurrences_cumulative", type = "INTEGER", mode = "NULLABLE" },
      { name = "active_websites_cumulative", type = "INTEGER", mode = "NULLABLE" },
      { name = "avg_words_per_article", type = "FLOAT", mode = "NULLABLE" },
      { name = "processing_timestamp", type = "TIMESTAMP", mode = "NULLABLE" }
    ]
  }
}

resource "google_bigquery_table" "tables" {
  for_each   = local.raw_tables_schema
  dataset_id = google_bigquery_dataset.raw_dataset.dataset_id
  table_id   = each.key
  schema     = jsonencode(each.value)
}

resource "google_bigquery_table" "staging_tables" {
  for_each   = local.raw_tables_schema
  dataset_id = google_bigquery_dataset.staging_dataset.dataset_id
  table_id   = "staging_${each.key}"
  schema     = jsonencode(each.value)
}

# Create activity_metrics table in metrics dataset
resource "google_bigquery_table" "activity_metrics" {
  for_each = local.activity_metrics_schema
  dataset_id = google_bigquery_dataset.metrics_dataset.dataset_id
  table_id   = each.key
  schema = jsonencode(each.value)

  time_partitioning {
    type  = "DAY"
    field = "window_start"
  }

  clustering = ["website_id", "window_start"]
}
