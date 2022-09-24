locals {
  data_lake_bucket = "<Name of data like in Google Cloud Storage to save the files>"
}

variable "project" {
    description = "National Highway Traffic Safety Administration (NHTSA) Fatality Analysis"
    default = "<Name of the Google Cloud Provider Project>"
}

variable "region" {
  default = "<Region for Google Cloud Provider Services. Should be chosen based on your location>"
  type = string
}

variable "storage_class" {
  description = "Data lake storage class"
  default = "STANDARD"
}

variable "BQ_DATASET" {
  type = string
  default = "<Name of BigQuery Dataset that will be used>"
}
