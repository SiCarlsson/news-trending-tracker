# This file contains the Terraform provider configuration

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  credentials = file(var.GOOGLE_CREDENTIALS_FILE)
  project     = var.BIGQUERY_PROJECT_ID
}

variable "GOOGLE_CREDENTIALS_FILE" {
  type        = string
  description = "Path to the Google Cloud service account credentials file"
  default     = "../credentials/backend-bigquery-service-account.json"
}
