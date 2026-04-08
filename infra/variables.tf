# ── Identity ──────────────────────────────────────────────────────────────────

variable "environment" {
  description = "Deployment environment: dev, staging, or prod"
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging, or prod"
  }
}

variable "name_prefix" {
  description = "Short prefix used in all resource names (e.g. edp)"
  type        = string
  default     = "edp"
}

variable "region" {
  description = "AWS region"
  type        = string
  default     = "eu-central-1"
}

variable "profile" {
  description = "AWS CLI profile"
  type        = string
  default     = "dev-admin"
}

# ── Networking (from terraform-platform-infra-live outputs) ───────────────────

variable "vpc_id" {
  description = "VPC ID — output from terraform-platform-infra-live networking module"
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnet IDs for ECS task placement"
  type        = list(string)
}

# ── Data Lake (from terraform-platform-infra-live outputs) ────────────────────

variable "bronze_bucket_name" {
  description = "Bronze S3 bucket name — agent reads metadata/dbt/* and writes metadata/agent-audit/*"
  type        = string
}

variable "gold_bucket_name" {
  description = "Gold S3 bucket name — agent reads Gold Parquet files for Athena queries"
  type        = string
}

variable "athena_results_bucket" {
  description = "Athena query results bucket — agent reads and writes query output"
  type        = string
}

variable "kms_key_arn" {
  description = "Platform KMS key ARN — needed to decrypt S3 objects and SSM parameters"
  type        = string
}

# ── Glue Catalog ──────────────────────────────────────────────────────────────

variable "glue_gold_database" {
  description = "Glue Catalog database name for the Gold layer (e.g. edp_dev_gold)"
  type        = string
}

# ── SSM ───────────────────────────────────────────────────────────────────────

variable "ssm_api_key_param" {
  description = "SSM parameter path for the Anthropic API key (e.g. /edp/dev/anthropic_api_key)"
  type        = string
}

# ── ECS task sizing ───────────────────────────────────────────────────────────

variable "task_cpu" {
  description = "ECS task CPU units (256 = 0.25 vCPU)"
  type        = number
  default     = 512
}

variable "task_memory" {
  description = "ECS task memory in MB"
  type        = number
  default     = 1024
}
