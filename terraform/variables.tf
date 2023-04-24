### Editable variables
variable "project" {
  description = "Your GCP Project ID"
  default = "latam-cooliving13987"  # The name of your project
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "us-east1" # Set your prefered region
  type = string
}

variable "credentials_file" {
  description = "The path to GCP credentials file"
  default = "~/latam-cooliving/credentials/gcp_service_account.json" # Set the path to the GCP credential file. Not neccesary if you use some linux OS.
}

### DO NOT edit the following variables
# Global
locals {
  auth = "Ein"
}

# Bucket
locals {
  data_lake_bucket = "latam_cooliving"
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

# Dataset
variable "raw_latam-cooliving_dataset" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "raw_latam_cooliving"
}

variable "core_latam-cooliving_dataset" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "dbt_latam_cooliving"
}
