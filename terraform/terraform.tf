variable "use_profile" { default = true }
variable "aws_region" { default = "ca-central-1" }

locals {
  aws_profile = var.use_profile ? local.aws_profiles[local.environment] : ""
  trader_env_tag = {
    sandbox = "sandbox"
    dev     = "dev"
    stage   = "stg"
    qa      = "qa"
    prod    = "prd"
  }
  aws_profiles = {
    sandbox = "mi-sandbox"
    dev     = "mi-dev"
    stage   = "mi-stage"
    qa      = "mi-qa"
    prod    = "tier3_cluster"
  }
  aws_account_ids = {
    sandbox = "698588692291"
    dev     = "418278274242"
    stage   = "756814365861"
    qa      = "350718436548"
    prod    = "150454740545" # tier3_cluster
  }
}

data "aws_caller_identity" "current" {}

terraform {
  backend "s3" {
    bucket         = "tr-mi-infra-tf-state"
    key            = "tier3_canada-reporting-etl"
    region         = "ca-central-1"
    dynamodb_table = "tr-mi-infra-tf-state"
    encrypt        = true
    profile        = "mi-shared"
  }
  required_providers {
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.2"
    }
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.50"
    }
    cloudflare = {
      source  = "cloudflare/cloudflare"
      version = "~> 3.32"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.4"
    }
    template = {
      source  = "hashicorp/template"
      version = "~> 2.2"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.2"
    }
    local = {
      source  = "hashicorp/local"
      version = "~> 2.3"
    }
  }
  required_version = "~> 1.4"
}

# NOTE: When applying locally set AWS_PROFILE environment variable before
# applying:
#   AWS_PROFILE=mi-stage tfa -var-file=tfvars/t3usa-stage.tfvars
provider "aws" {
  region  = var.aws_region
  profile = local.aws_profile
  assume_role {
    role_arn = "arn:aws:iam::${local.aws_account_ids[local.environment]}:role/mi-${local.environment}-deployer"
  }
  default_tags {
    tags = {
      "tr:aws:app-tier-type" = "t1"
      "tr:aws:application"   = "motocommerce-canada"
      "tr:aws:business-unit" = "autosync"
      "tr:aws:company"       = "motoinsight"
      "tr:aws:environment"   = local.trader_env_tag[local.environment]
      "tr:aws:product"       = "tier3"
      "tr:aws:repo-branch"   = "unhaggle/t3can-cluster"
    }
  }
}