variable "project_id" {
  description = "The ID of the GCP project."
  type        = string
}

variable "bucket_name" {
  description = "The name of the GCS bucket."
  type        = string
}

variable "region" {
  description = "The region to deploy resources."
  type        = string
  default     = "us-central1"
}

variable "location" {
  description = "Location for bucket."
  type        = string
  default     = "US"
}
