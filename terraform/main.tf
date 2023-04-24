terraform {
  required_version = ">= 1.0"
  backend "local" {}
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
  }
}

# Provider
provider "google" {
  project = var.project
  region = var.region
  credentials = file(var.credentials_file)
}

# Bucket
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket
resource "google_storage_bucket" "latam-cooliving_datalake-bucket" {
  name                        = "${local.data_lake_bucket}_${var.project}"
  location                    = var.region
  storage_class               = var.storage_class
  uniform_bucket_level_access = true
  force_destroy               = true

  versioning {
    enabled     = true
  }
  
}

# Dataset
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
resource "google_bigquery_dataset" "raw-dataset" {
  dataset_id                 = var.raw_latam-cooliving_dataset
  project                    = var.project
  location                   = var.region
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "core-dbt-dataset" {
  dataset_id                 = var.core_latam-cooliving_dataset
  project                    = var.project
  location                   = var.region
  delete_contents_on_destroy = true
}