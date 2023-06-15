variable "project_name" { default = "tier3" }
variable "display_name" { default = "tier3_canada-cluster" }
variable "component_name" { default = "motocommerce-canada" }

variable "log_group_retention_in_days" {
  type        = number
  default     = 7
  description = "(Optional) The default number of days log events retained in the glue job log group."
}

variable "create_security_configuration" {
  type        = bool
  default     = false
  description = "(Optional) Create AWS Glue Security Configuration associated with the job."
}

variable "security_configuration_cloudwatch_encryption" {
  type        = object({
    cloudwatch_encryption_mode = string
    kms_key_arn                = string
  })
  default     = {
    cloudwatch_encryption_mode = "DISABLED"
    kms_key_arn                = null
  }
  description = "(Optional) A cloudwatch_encryption block which contains encryption configuration for CloudWatch."
}

variable "security_configuration_job_bookmarks_encryption" {
  type        = object({
    job_bookmarks_encryption_mode = string
    kms_key_arn                   = string
  })
  default     = {
    job_bookmarks_encryption_mode = "DISABLED"
    kms_key_arn                   = null
  }
  description = "(Optional) A job_bookmarks_encryption block which contains encryption configuration for job bookmarks."
}

variable "security_configuration_s3_encryption" {
  type        = object({
    s3_encryption_mode = string
    kms_key_arn        = string
  })
  default     = {
    s3_encryption_mode = "DISABLED"
    kms_key_arn        = null
  }
  description = "(Optional) A s3_encryption block which contains encryption configuration for S3 data."
}

variable "glue_job_config" {
  type = set(object({
      name = string
      description= string
      script_location = string
      glue_version = string
      connections = list(string)
      python_version = number
      max_concurrent_runs = number
      max_retries = number
      security_configuration = string
      execution_class = string
      worker_type = string
      job_language = string
      extra_py_files = list(string)
      additional_python_modules = list(string)
      enable_spark_ui = bool
      enable_job_insights = bool
      enable_continuous_cloudwatch_log = bool
      enable_auto_scaling = bool
      spark_event_logs_path = string
      temp_dir= string
      user_jars_first = bool
      use_postgres_driver = bool
      extra_files = list(string)
      continuous_log_stream_prefix = string
      enable_s3_parquet_optimized_committer = bool
      enable_rename_algorithm_v2 = bool
      notify_delay_after = number
      timeout = number
      workers = number
      job_bookmark_option = string
      enable_glue_datacatalog = bool
      enable_metrics = bool
      enable_continuous_log_filter = bool
      command_name = string
      iam_role_name = string
      additional_job_parameters = map(string)
  }))
}

variable "workflow" {
     type = list(object({
      workflow_name = string
      triggers= any
  }))
}
