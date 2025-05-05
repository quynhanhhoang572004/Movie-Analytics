terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.31.0"
    }
  }
}

resource "google_storage_bucket" "bucket" {
  name          = var.bucket_name
  location      = var.location
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
  project = var.project_id
  region  = var.region
}

resource "google_dataproc_cluster" "pyspark_cluster" {
  name   = "pyspark-cluster"
  region = "us-central1"

  cluster_config {
    master_config {
      num_instances = 1
      machine_type  = "n1-standard-2"
       disk_config {
    boot_disk_size_gb = 100
  }
    }

    worker_config {
      num_instances = 2
      machine_type  = "n1-standard-2"

       disk_config {
    boot_disk_size_gb = 100
  }
    }

    software_config {
      image_version = "2.0-debian10" 
      optional_components = [ "JUPYTER"]
      
    }
  }
}