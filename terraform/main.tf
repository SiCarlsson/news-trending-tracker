terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  credentials = file("../credentials/backend-bigquery-service-account.json")
  project     = var.project_id
}

resource "google_bigquery_dataset" "custom_dataset" {
  dataset_id = var.dataset_id
  location   = var.dataset_location
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
