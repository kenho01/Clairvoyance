output "bank_bucket" {
  description = "GCS bucket for bank eStatements"
  value       = google_storage_bucket.bank.name
}

output "investments_bucket" {
  description = "GCS bucket for investment data"
  value       = google_storage_bucket.investments.name
}

output "pipeline_service_account" {
  description = "Pipeline service account email"
  value       = google_service_account.pipeline_sa.email
}

output "artifact_registry_repo" {
  description = "Artifact Registry repo URI — use as the docker push prefix"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/clairvoyance"
}

output "pipeline_image_uri" {
  description = "Full image URI — set this as pipeline_image in terraform.tfvars after docker push"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/clairvoyance/pipeline:latest"
}
