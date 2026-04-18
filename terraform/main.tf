terraform {
  required_version = ">= 1.5.7"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ── Enable APIs ────────────────────────────────────────────────────────────────
resource "google_project_service" "apis" {
  for_each = toset([
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "run.googleapis.com",
    "cloudscheduler.googleapis.com",
    "eventarc.googleapis.com",
    "cloudbuild.googleapis.com",
    "artifactregistry.googleapis.com",
    "secretmanager.googleapis.com",
  ])
  service            = each.key
  disable_on_destroy = false
}

# ── Artifact Registry ──────────────────────────────────────────────────────────
resource "google_artifact_registry_repository" "pipelines" {
  repository_id = "clairvoyance"
  location      = var.region
  format        = "DOCKER"
  description   = "Clairvoyance pipeline images"
  depends_on    = [google_project_service.apis]
}

# ── GCS Buckets ────────────────────────────────────────────────────────────────
resource "google_storage_bucket" "bank" {
  name                        = var.gcs_bucket_bank
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = false

  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
}

resource "google_storage_bucket" "cpf" {
  name                        = var.gcs_bucket_cpf
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = false
}

resource "google_storage_bucket" "investments" {
  name                        = var.gcs_bucket_investments
  location                    = var.region
  uniform_bucket_level_access = true
  force_destroy               = false

  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
}

# ── BigQuery Datasets ──────────────────────────────────────────────────────────
resource "google_bigquery_dataset" "ods" {
  dataset_id  = "ods"
  description = "ODS — raw ingested data"
  location    = var.region
}

resource "google_bigquery_dataset" "dwd" {
  dataset_id  = "dwd"
  description = "DWD — cleansed and typed tables (dbt)"
  location    = var.region
}

resource "google_bigquery_dataset" "dws" {
  dataset_id  = "dws"
  description = "DWS — aggregated tables (dbt)"
  location    = var.region
}

resource "google_bigquery_dataset" "ads" {
  dataset_id  = "ads"
  description = "ADS — presentation layer for Looker Studio (dbt)"
  location    = var.region
}

# ── Service Account ────────────────────────────────────────────────────────────
resource "google_service_account" "pipeline_sa" {
  account_id   = "clairvoyance-pipeline"
  display_name = "Clairvoyance Pipeline Service Account"
}

resource "google_project_iam_member" "sa_roles" {
  for_each = toset([
    "roles/storage.objectAdmin",
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/run.invoker",
    "roles/secretmanager.secretAccessor",
  ])
  project = var.project_id
  role    = each.key
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# ── Secret Manager ─────────────────────────────────────────────────────────────
locals {
  secret_ids = toset([
    "anthropic_api_key",
    "gemini_api_key",
    "gemini_api_secret",
    "tiger_id",
    "tiger_account",
    "tiger_private_key",
    "ibkr_flex_token",
    "ibkr_flex_query_id",
  ])

  secret_values = {
    anthropic_api_key  = var.anthropic_api_key
    gemini_api_key     = var.gemini_api_key
    gemini_api_secret  = var.gemini_api_secret
    tiger_id           = var.tiger_id
    tiger_account      = var.tiger_account
    tiger_private_key  = var.tiger_private_key
    ibkr_flex_token    = var.ibkr_flex_token
    ibkr_flex_query_id = var.ibkr_flex_query_id
  }

  image = var.pipeline_image

  common_plain_env = [
    { name = "GCP_PROJECT_ID",         value = var.project_id },
    { name = "GCS_BUCKET_BANK",        value = var.gcs_bucket_bank },
    { name = "GCS_BUCKET_INVESTMENTS", value = var.gcs_bucket_investments },
    { name = "GCS_BUCKET_CPF",         value = var.gcs_bucket_cpf },
  ]

  all_secret_env = [
    { name = "ANTHROPIC_API_KEY",  secret = "anthropic_api_key" },
    { name = "GEMINI_API_KEY",     secret = "gemini_api_key" },
    { name = "GEMINI_API_SECRET",  secret = "gemini_api_secret" },
    { name = "TIGER_ID",           secret = "tiger_id" },
    { name = "TIGER_ACCOUNT",      secret = "tiger_account" },
    { name = "TIGER_PRIVATE_KEY",  secret = "tiger_private_key" },
    { name = "IBKR_FLEX_TOKEN",    secret = "ibkr_flex_token" },
    { name = "IBKR_FLEX_QUERY_ID", secret = "ibkr_flex_query_id" },
  ]
}

resource "google_secret_manager_secret" "secrets" {
  for_each  = local.secret_ids
  secret_id = each.key

  replication {
    auto {}
  }

  depends_on = [google_project_service.apis]
}

resource "google_secret_manager_secret_version" "secret_values" {
  for_each    = local.secret_ids
  secret      = google_secret_manager_secret.secrets[each.key].id
  secret_data = local.secret_values[each.key]
}

# ── Cloud Run Jobs ─────────────────────────────────────────────────────────────
# All jobs use the same image; PIPELINE env var selects the entrypoint.
# Created only once pipeline_image is set in terraform.tfvars.

resource "google_cloud_run_v2_job" "bank" {
  count    = local.image != "" ? 1 : 0
  name     = "clairvoyance-bank"
  location = var.region

  template {
    template {
      service_account = google_service_account.pipeline_sa.email
      max_retries     = 1

      containers {
        image = local.image

        env {
          name  = "PIPELINE"
          value = "bank"
        }

        dynamic "env" {
          for_each = local.common_plain_env
          content {
            name  = env.value.name
            value = env.value.value
          }
        }

        env {
          name = "ANTHROPIC_API_KEY"
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.secrets["anthropic_api_key"].secret_id
              version = "latest"
            }
          }
        }

        resources {
          limits = {
            cpu    = "1"
            memory = "512Mi"
          }
        }
      }
    }
  }

  depends_on = [google_project_service.apis]
}

resource "google_cloud_run_v2_job" "investment" {
  count    = local.image != "" ? 1 : 0
  name     = "clairvoyance-investment"
  location = var.region

  template {
    template {
      service_account = google_service_account.pipeline_sa.email
      max_retries     = 1

      containers {
        image = local.image

        env {
          name  = "PIPELINE"
          value = "investment"
        }

        dynamic "env" {
          for_each = local.common_plain_env
          content {
            name  = env.value.name
            value = env.value.value
          }
        }

        dynamic "env" {
          for_each = local.all_secret_env
          content {
            name = env.value.name
            value_source {
              secret_key_ref {
                secret  = google_secret_manager_secret.secrets[env.value.secret].secret_id
                version = "latest"
              }
            }
          }
        }

        resources {
          limits = {
            cpu    = "1"
            memory = "512Mi"
          }
        }
      }
    }
  }

  depends_on = [google_project_service.apis]
}

resource "google_cloud_run_v2_job" "ssb" {
  count    = local.image != "" ? 1 : 0
  name     = "clairvoyance-ssb"
  location = var.region

  template {
    template {
      service_account = google_service_account.pipeline_sa.email
      max_retries     = 1

      containers {
        image = local.image

        env {
          name  = "PIPELINE"
          value = "ssb"
        }

        dynamic "env" {
          for_each = local.common_plain_env
          content {
            name  = env.value.name
            value = env.value.value
          }
        }

        resources {
          limits = {
            cpu    = "1"
            memory = "512Mi"
          }
        }
      }
    }
  }

  depends_on = [google_project_service.apis]
}

resource "google_cloud_run_v2_job" "dbt" {
  count    = local.image != "" ? 1 : 0
  name     = "clairvoyance-dbt"
  location = var.region

  template {
    template {
      service_account = google_service_account.pipeline_sa.email
      max_retries     = 1

      containers {
        image = local.image

        env {
          name  = "PIPELINE"
          value = "dbt"
        }

        dynamic "env" {
          for_each = local.common_plain_env
          content {
            name  = env.value.name
            value = env.value.value
          }
        }

        resources {
          limits = {
            cpu    = "1"
            memory = "512Mi"
          }
        }
      }
    }
  }

  depends_on = [google_project_service.apis]
}

# ── Cloud Scheduler ────────────────────────────────────────────────────────────

# Investment pipeline: daily at 6am SGT (22:00 UTC previous day) — after all markets close
resource "google_cloud_scheduler_job" "investment_daily" {
  count     = local.image != "" ? 1 : 0
  name      = "clairvoyance-investment-daily"
  region    = var.region
  schedule  = "0 22 * * *"
  time_zone = "UTC"

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/clairvoyance-investment:run"

    oauth_token {
      service_account_email = google_service_account.pipeline_sa.email
    }
  }

  depends_on = [google_cloud_run_v2_job.investment]
}

# dbt: daily at 6:30am SGT (22:30 UTC previous day) — 30 min after investment pipeline
resource "google_cloud_scheduler_job" "dbt_daily" {
  count     = local.image != "" ? 1 : 0
  name      = "clairvoyance-dbt-daily"
  region    = var.region
  schedule  = "30 22 * * *"
  time_zone = "UTC"

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/clairvoyance-dbt:run"

    oauth_token {
      service_account_email = google_service_account.pipeline_sa.email
    }
  }

  depends_on = [google_cloud_run_v2_job.dbt]
}

# SSB: 1st of every month at 9am SGT (01:00 UTC)
resource "google_cloud_scheduler_job" "ssb_monthly" {
  count     = local.image != "" ? 1 : 0
  name      = "clairvoyance-ssb-monthly"
  region    = var.region
  schedule  = "0 1 1 * *"
  time_zone = "UTC"

  http_target {
    http_method = "POST"
    uri         = "https://${var.region}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${var.project_id}/jobs/clairvoyance-ssb:run"

    oauth_token {
      service_account_email = google_service_account.pipeline_sa.email
    }
  }

  depends_on = [google_cloud_run_v2_job.ssb]
}

# ── Cloud Run Service — bank pipeline (Eventarc target) ───────────────────────
resource "google_cloud_run_v2_service" "bank_service" {
  count    = local.image != "" ? 1 : 0
  name     = "clairvoyance-bank-service"
  location = var.region

  template {
    service_account = google_service_account.pipeline_sa.email

    containers {
      image = local.image

      env {
        name  = "PIPELINE"
        value = "bank-service"
      }

      dynamic "env" {
        for_each = local.common_plain_env
        content {
          name  = env.value.name
          value = env.value.value
        }
      }

      env {
        name = "ANTHROPIC_API_KEY"
        value_source {
          secret_key_ref {
            secret  = google_secret_manager_secret.secrets["anthropic_api_key"].secret_id
            version = "latest"
          }
        }
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
      }
    }

    scaling {
      min_instance_count = 0
      max_instance_count = 1
    }
  }

  depends_on = [google_project_service.apis]
}

# Allow Eventarc to invoke the bank service
resource "google_cloud_run_v2_service_iam_member" "bank_service_invoker" {
  count    = local.image != "" ? 1 : 0
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.bank_service[0].name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# ── Cloud Run Service — CPF pipeline (Eventarc target) ────────────────────────
resource "google_cloud_run_v2_service" "cpf_service" {
  count    = local.image != "" ? 1 : 0
  name     = "clairvoyance-cpf-service"
  location = var.region

  template {
    service_account = google_service_account.pipeline_sa.email

    containers {
      image = local.image

      env {
        name  = "PIPELINE"
        value = "cpf-service"
      }

      dynamic "env" {
        for_each = local.common_plain_env
        content {
          name  = env.value.name
          value = env.value.value
        }
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
      }
    }

    scaling {
      min_instance_count = 0
      max_instance_count = 1
    }
  }

  depends_on = [google_project_service.apis]
}

# Allow Eventarc to invoke the CPF service
resource "google_cloud_run_v2_service_iam_member" "cpf_service_invoker" {
  count    = local.image != "" ? 1 : 0
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_service.cpf_service[0].name
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# ── Eventarc — CPF PDF trigger → Cloud Run Service ────────────────────────────
resource "google_eventarc_trigger" "cpf_pdf_upload" {
  count           = local.image != "" ? 1 : 0
  name            = "clairvoyance-cpf-pdf-upload"
  location        = var.region
  service_account = google_service_account.pipeline_sa.email

  matching_criteria {
    attribute = "type"
    value     = "google.cloud.storage.object.v1.finalized"
  }

  matching_criteria {
    attribute = "bucket"
    value     = var.gcs_bucket_cpf
  }

  destination {
    cloud_run_service {
      service = google_cloud_run_v2_service.cpf_service[0].name
      region  = var.region
    }
  }

  depends_on = [google_cloud_run_v2_service.cpf_service]
}

# ── Eventarc — bank PDF trigger → Cloud Run Service ───────────────────────────
resource "google_eventarc_trigger" "bank_pdf_upload" {
  count           = local.image != "" ? 1 : 0
  name            = "clairvoyance-bank-pdf-upload"
  location        = var.region
  service_account = google_service_account.pipeline_sa.email

  matching_criteria {
    attribute = "type"
    value     = "google.cloud.storage.object.v1.finalized"
  }

  matching_criteria {
    attribute = "bucket"
    value     = var.gcs_bucket_bank
  }

  destination {
    cloud_run_service {
      service = google_cloud_run_v2_service.bank_service[0].name
      region  = var.region
    }
  }

  depends_on = [google_cloud_run_v2_service.bank_service]
}
