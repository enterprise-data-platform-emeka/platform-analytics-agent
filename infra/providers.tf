provider "aws" {
  region  = var.region
  profile = var.profile

  default_tags {
    tags = {
      Environment = var.environment
      ManagedBy   = "Terraform"
      Project     = "EnterpriseDataPlatform"
      Component   = "AnalyticsAgent"
    }
  }
}
