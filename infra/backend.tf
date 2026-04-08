terraform {
  backend "s3" {
    bucket         = "enterprise-data-platform-tfstate-dev"
    key            = "dev/analytics-agent/terraform.tfstate"
    region         = "eu-central-1"
    dynamodb_table = "enterprise-data-platform-tf-lock-dev"
    profile        = "dev-admin"
    encrypt        = true
  }
}
