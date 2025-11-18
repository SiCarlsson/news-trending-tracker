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
      { name = "website_id", type = "STRING", mode = "REQUIRED", description = "Primary key. Unique identifier for each news website." },
      { name = "website_name", type = "STRING", mode = "REQUIRED", description = "The display name of the news website (e.g., 'SVT', 'Expressen', 'Aftonbladet')." },
      { name = "website_url", type = "STRING", mode = "REQUIRED", description = "The base URL of the news website (e.g., 'https://www.svt.se')." }
    ]
    articles = [
      { name = "article_id", type = "STRING", mode = "REQUIRED", description = "Unique identifier for each article." },
      { name = "website_id", type = "STRING", mode = "REQUIRED", description = "Foreign key referencing websites.website_id." },
      { name = "article_title", type = "STRING", mode = "REQUIRED", description = "The title of the article." },
      { name = "article_url", type = "STRING", mode = "REQUIRED", description = "The URL of the article. (e.g., '/nyheter/ekonomi/sparare-ratar-usa-fonder-valjer-svenskt')" }
    ]
    words = [
      { name = "word_id", type = "STRING", mode = "REQUIRED", description = "Primary key. Unique identifier for each word." },
      { name = "word_text", type = "STRING", mode = "REQUIRED", description = "The actual word." }
    ]
    occurrences = [
      { name = "occurrence_id", type = "STRING", mode = "REQUIRED", description = "Primary key. Unique identifier for each word occurrence." },
      { name = "word_id", type = "STRING", mode = "REQUIRED", description = "Foreign key referencing words.word_id. Identifies which word appeared." },
      { name = "website_id", type = "STRING", mode = "REQUIRED", description = "Foreign key referencing websites.website_id. Identifies which website the word appeared on." },
      { name = "article_id", type = "STRING", mode = "REQUIRED", description = "Foreign key referencing articles.article_id. Links to the specific article where the word appeared." },
      { name = "timestamp", type = "TIMESTAMP", mode = "REQUIRED", description = "The exact date and time when the word was scraped/recorded." }
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
    { name = "window_start", type = "TIMESTAMP", mode = "REQUIRED", description = "Start timestamp of the 10-minute aggregation window." },
    { name = "window_end", type = "TIMESTAMP", mode = "REQUIRED", description = "End timestamp of the 10-minute aggregation window." },
    { name = "word_id", type = "STRING", mode = "REQUIRED", description = "Foreign key referencing words.word_id." },
    { name = "total_occurrences", type = "INTEGER", mode = "REQUIRED", description = "Total count of word occurrences within the time window." }
  ])
}