variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "asia-southeast1"
}

variable "gcs_bucket_bank" {
  description = "GCS bucket name for raw bank eStatements and categorised transactions"
  type        = string
}

variable "gcs_bucket_investments" {
  description = "GCS bucket name for raw investment data (IBKR, Tiger Brokers, Gemini)"
  type        = string
}

variable "gcs_bucket_cpf" {
  description = "GCS bucket name for CPF PDF uploads (Eventarc trigger source)"
  type        = string
}

variable "pipeline_image" {
  description = "Container image URI (Artifact Registry) for all pipelines — set after first docker push"
  type        = string
  default     = ""
}

# ── Secrets (values set once via gcloud, referenced here for Secret Manager resources) ──
variable "anthropic_api_key" {
  description = "Anthropic API key for Claude Haiku categoriser"
  type        = string
  sensitive   = true
  default     = ""
}

variable "gemini_api_key" {
  description = "Gemini exchange API key"
  type        = string
  sensitive   = true
  default     = ""
}

variable "gemini_api_secret" {
  description = "Gemini exchange API secret"
  type        = string
  sensitive   = true
  default     = ""
}

variable "tiger_id" {
  description = "Tiger Brokers tiger_id"
  type        = string
  sensitive   = true
  default     = ""
}

variable "tiger_account" {
  description = "Tiger Brokers account number"
  type        = string
  sensitive   = true
  default     = ""
}

variable "tiger_private_key" {
  description = "Tiger Brokers RSA private key (PEM string)"
  type        = string
  sensitive   = true
  default     = ""
}

variable "ibkr_flex_token" {
  description = "IBKR Flex Web Service token"
  type        = string
  sensitive   = true
  default     = ""
}

variable "ibkr_flex_query_id" {
  description = "IBKR Flex Query ID"
  type        = string
  sensitive   = true
  default     = ""
}
