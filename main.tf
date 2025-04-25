terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.31.0"
    }
  }
}

resource "google_storage_bucket" "bucket" {
  name          = "scenic-precinct-457404-h2-bucket"
  location      = "US"
  force_destroy = true


  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

provider "google" {
  project = "scenic-precinct-457404-h2"
  region  = "us-central1"
}