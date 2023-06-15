
locals {
  environment = lookup(var.environments, terraform.workspace, "sandbox")
  name        = "${var.project_name}-${local.environment}"

  common_tags = {
    Name        = local.name
    Component   = var.component_name
    Environment = local.environment
  }

  workflow_config = merge([
         for each_workflow in var.workflow :
         {
             for trigger in each_workflow["triggers"]:
             "${each_workflow["workflow_name"]}-${trigger["trigger_name"]}" =>
              {
                  workflow_name = each_workflow["workflow_name"]
                  trigger_name = trigger["trigger_name"]
                  type = trigger["type"]
                  schedule_expression = trigger["schedule_expression"]
                  actions = trigger["actions"]
                  predicate = trigger["predicate"]
              }
         }
      ]...)

  workflow_list = [for workflow in var.workflow : workflow["workflow_name"]]
}

variable "environments" {
  type = map(string)
  default = {
    sandbox = "sandbox"
    stage   = "stage"
    qa      = "qa"
    prod    = "prod"
    dev     = "dev"
  }
}

# ---------------------------------------------------------------------------------
# Cloudwatch log group for AWS Glue jobs
# ---------------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "log_group" {
  name              = "/aws-glue/jobs/${local.name}-log"
  retention_in_days = var.log_group_retention_in_days
  tags  = local.common_tags
}

# ---------------------------------------------------------------------------------
# Security Configuration for AWS Glue jobs
# ---------------------------------------------------------------------------------


resource "aws_glue_security_configuration" "sec_cfg" {
  count = var.create_security_configuration ? 1 : 0
  name  = "${local.name}-security-config"

  encryption_configuration {
    dynamic "cloudwatch_encryption" {
      for_each = [var.security_configuration_cloudwatch_encryption]

      content {
        cloudwatch_encryption_mode = cloudwatch_encryption.value.cloudwatch_encryption_mode
        kms_key_arn                = cloudwatch_encryption.value.kms_key_arn
      }
    }

    dynamic "job_bookmarks_encryption" {
      for_each = [var.security_configuration_job_bookmarks_encryption]

      content {
        job_bookmarks_encryption_mode = job_bookmarks_encryption.value.job_bookmarks_encryption_mode
        kms_key_arn                   = job_bookmarks_encryption.value.kms_key_arn
      }
    }

    dynamic "s3_encryption" {
      for_each = [var.security_configuration_s3_encryption]

      content {
        s3_encryption_mode = s3_encryption.value.s3_encryption_mode
        kms_key_arn        = s3_encryption.value.kms_key_arn
      }
    }
  }
}


# ---------------------------------------------------------------------------------
# Creating AWS Glue jobs in Loop
# ---------------------------------------------------------------------------------


module "glue_job" {
  source = "./tfmodule-digital_retailer_report/glue-job"

  for_each   = {
    for index, job in var.glue_job_config:
    job.name => job
  }
  name                             = each.value.name
  description                      = each.value.description
  connections                      = length(each.value.connections) > 0 ? each.value.connections : ["${local.name}-glue-db"]
  script_location                  = "s3://${local.name}-glue/scripts/${each.value.script_location}"
  command_name                     = each.value.command_name
  python_version                   = each.value.python_version
  glue_version                     = each.value.glue_version
  max_concurrent_runs              = each.value.max_concurrent_runs
  max_retries                      = each.value.max_retries       
  role_arn                         = length(each.value.iam_role_name) > 0 ? "arn:aws:iam::${local.aws_account_ids[local.environment]}:role/${each.value.iam_role_name}": "arn:aws:iam::${local.aws_account_ids[local.environment]}:role/${local.name}-glue"
  execution_class                  = length(each.value.execution_class) > 0 ? each.value.execution_class : null
  timeout                          = tonumber(each.value.timeout) > 0 ? each.value.timeout : null
  number_of_workers                = tonumber(each.value.workers) > 0 ? each.value.workers : null
  worker_type                      = length(each.value.worker_type) > 0 ? each.value.worker_type : "G.1X"
  job_language                     = length(each.value.job_language) > 0 ? each.value.job_language : "python"
  extra_py_files                   = length(each.value.extra_py_files) > 0 ? each.value.extra_py_files : []
  additional_python_modules        = each.value.additional_python_modules
  job_bookmark_option              = length(each.value.job_bookmark_option) > 0 ? each.value.job_bookmark_option : null
  enable_glue_datacatalog          = each.value.enable_glue_datacatalog ? each.value.enable_glue_datacatalog : null
  enable_metrics                   = each.value.enable_metrics ? each.value.enable_metrics : null
  enable_continuous_log_filter     = each.value.enable_continuous_log_filter ? each.value.enable_continuous_log_filter : null
  enable_spark_ui                  = each.value.enable_spark_ui ? each.value.enable_spark_ui : null
  enable_job_insights              = each.value.enable_job_insights ? each.value.enable_job_insights : null
  enable_continuous_cloudwatch_log = each.value.enable_continuous_cloudwatch_log ? each.value.enable_continuous_cloudwatch_log : null
  enable_auto_scaling              = each.value.enable_auto_scaling ? each.value.enable_auto_scaling : null
  spark_event_logs_path            = length(each.value.spark_event_logs_path) > 0 ? "s3://${local.name}-glue/${each.value.spark_event_logs_path}/" : null
  temp_dir                         = length(each.value.temp_dir) > 0 ? "s3://${local.name}-glue/${each.value.temp_dir}/" : null
  user_jars_first                  = each.value.user_jars_first ? each.value.user_jars_first : null
  use_postgres_driver              = each.value.use_postgres_driver ? each.value.use_postgres_driver : null
  extra_files                      = each.value.extra_files
  enable_s3_parquet_optimized      = each.value.enable_s3_parquet_optimized_committer ? each.value.enable_s3_parquet_optimized_committer : null
  enable_rename_algorithm_v2       = each.value.enable_rename_algorithm_v2 ? each.value.enable_rename_algorithm_v2 : null
  log_group_name                   = aws_cloudwatch_log_group.log_group.name
  continuous_log_stream_prefix     = length(each.value.continuous_log_stream_prefix) > 0 ? each.value.continuous_log_stream_prefix : null
  notify_delay_after               = tonumber(each.value.notify_delay_after) > 0 ? each.value.notify_delay_after : null
  security_configuration           = var.create_security_configuration ? join("", aws_glue_security_configuration.sec_cfg.*.id) : each.value.security_configuration
  additional_job_parameters        = each.value.additional_job_parameters
  tags                             = local.common_tags

}


# ---------------------------------------------------------------------------------
# Creating AWS Glue workflow
# ---------------------------------------------------------------------------------


resource "aws_glue_workflow" "glue_workflow" {
    for_each = toset(local.workflow_list)
    name = each.key
} 


# ---------------------------------------------------------------------------------
# Creating AWS Glue triggers
# ---------------------------------------------------------------------------------


module "glue_trigger" {
  source = "./tfmodule-digital_retailer_report/glue-trigger"
  name = local.name
  environment = local.environment
  for_each =   local.workflow_config
  enable_glue_trigger = true
  glue_trigger_type = each.value.type
  glue_trigger_workflow_name = each.value.workflow_name
  glue_trigger_name   = each.value.trigger_name
  glue_trigger_actions = each.value.actions
  glue_trigger_schedule = each.value.schedule_expression
  glue_trigger_predicate = each.value.predicate

  tags = local.common_tags
  

  depends_on = [
    aws_glue_workflow.glue_workflow,
    module.glue_job
  ]
}
