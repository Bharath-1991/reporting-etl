# variable "project_name" { default = "tier3" }
# variable "display_name" { default = "tier3_canada-cluster" }
# variable "component_name" { default = "motocommerce-canada" }
# variable "gh_project_name" { default = "tier3" }

variable "name" {
  type        = string
  description = "(Required) Name that will be used for identify resources."
}

variable "tags" {
  type        = map(string)
  default     = {}
  description = "(Optional) Key-value map of resource tags."
}


variable "script_location" {
  type        = string
  description = "(Required) Specifies the S3 path to a script that executes a job."
}

variable "python_version" {
  type        = number
  default     = 3
  description = "(Optional) The Python version being used to execute a Python shell job."

  validation {
    condition     = contains([2, 3], var.python_version)
    error_message = "Allowed values are 2 or 3."
  }
}

variable "connections" {
  type        = list(string)
  default     = []
  description = "(Optional) The list of connections used for this job."
}

variable "description" {
  type        = string
  default     = ""
  description = "(Optional) Description of the job."
}

variable "max_concurrent_runs" {
  type        = number
  default     = 1
  description = "(Optional) The maximum number of concurrent runs allowed for a job."
}

variable "glue_version" {
  type        = string
  default     = "3.0"
  description = "(Optional) The version of glue to use."
}

variable "max_retries" {
  type        = number
  default     = 0
  description = "(Optional) The maximum number of times to retry this job if it fails."
}

variable "notify_delay_after" {
  type        = number
  default     = null
  description = "(Optional) After a job run starts, the number of minutes to wait before sending a job run delay notification."
}

variable "role_arn" {
  type        = string
  default     = ""
  description = "(Optional) The ARN of the IAM role associated with this job."
}

variable "timeout" {
  type        = number
  default     = 2880
  description = "(Optional) The job timeout in minutes."
}

variable "worker_type" {
  type        = string
  default     = "G.1X"
  description = "(Optional) The type of predefined worker that is allocated when a job runs."

  validation {
    condition     = contains(["Standard", "G.1X", "G.2X", "G.4X", "G.8X"], var.worker_type)
    error_message = "Accepts a value of Standard, G.1X, or G.2X."
  }
}

variable "number_of_workers" {
  type        = number
  default     = 2
  description = "(Optional) The number of workers of a defined workerType that are allocated when a job runs."
}


variable "security_configuration" {
  type        = string
  default     = ""
  description = "(Optional) The name of the Security Configuration to be associated with the job."
}


variable "job_language" {
  type        = string
  default     = "python"
  description = "(Optional) The script programming language."

  validation {
    condition     = contains(["scala", "python"], var.job_language)
    error_message = "Accepts a value of 'scala' or 'python'."
  }
}

variable "class" {
  type        = string
  default     = null
  description = "(Optional) The Scala class that serves as the entry point for your Scala script."
}

variable "extra_py_files" {
  type        = list(string)
  default     = []
  description = "(Optional) The Amazon S3 paths to additional Python modules that AWS Glue adds to the Python path before executing your script."
}

variable "extra_jars" {
  type        = list(string)
  default     = []
  description = "(Optional) The Amazon S3 paths to additional Java .jar files that AWS Glue adds to the Java classpath before executing your script."
}

variable "user_jars_first" {
  type        = bool
  default     = null
  description = "(Optional) Prioritizes the customer's extra JAR files in the classpath."
}

variable "use_postgres_driver" {
  type        = bool
  default     = null
  description = "(Optional) Prioritizes the Postgres JDBC driver in the class path to avoid a conflict with the Amazon Redshift JDBC driver."
}

variable "extra_files" {
  type        = list(string)
  default     = []
  description = "(Optional) The Amazon S3 paths to additional files, such as configuration files that AWS Glue copies to the working directory of your script before executing it."
}

variable "job_bookmark_option" {
  type        = string
  default     = "job-bookmark-disable"
  description = "(Optional) Controls the behavior of a job bookmark."

  validation {
    condition     = contains(["job-bookmark-enable", "job-bookmark-disable", "job-bookmark-pause"], var.job_bookmark_option)
    error_message = "Accepts a value of 'job-bookmark-enable', 'job-bookmark-disable' or 'job-bookmark-pause'."
  }
}

variable "temp_dir" {
  type        = string
  default     = null
  description = "(Optional) Specifies an Amazon S3 path to a bucket that can be used as a temporary directory for the job."
}

variable "enable_s3_parquet_optimized" {
  type        = bool
  default     = true
  description = "(Optional) Enables the EMRFS S3-optimized committer for writing Parquet data into Amazon S3."
}

variable "enable_rename_algorithm_v2" {
  type        = bool
  default     = true
  description = "(Optional) Sets the EMRFS rename algorithm version to version 2."
}

variable "enable_glue_datacatalog" {
  type        = bool
  default     = true
  description = "(Optional) Enables you to use the AWS Glue Data Catalog as an Apache Spark Hive metastore."
}

variable "enable_metrics" {
  type        = bool
  default     = false
  description = "(Optional) Enables the collection of metrics for job profiling for job run."
}

variable "enable_continuous_cloudwatch_log" {
  type        = bool
  default     = false
  description = "(Optional) Enables real-time continuous logging for AWS Glue jobs."
}

variable "enable_continuous_log_filter" {
  type        = bool
  default     = true
  description = "(Optional) Specifies a standard filter or no filter when you create or edit a job enabled for continuous logging."
}

variable "continuous_log_stream_prefix" {
  type        = string
  default     = null
  description = "(Optional) Specifies a custom CloudWatch log stream prefix for a job enabled for continuous logging."
}

variable "continuous_log_conversion_pattern" {
  type        = string
  default     = null
  description = "(Optional) Specifies a custom conversion log pattern for a job enabled for continuous logging."
}

variable "enable_spark_ui" {
  type        = bool
  default     = false
  description = "(Optional) Enable Spark UI to monitor and debug AWS Glue ETL jobs."
}

variable "enable_job_insights" {
  type        = bool
  default     = false
  description = "(Optional) Enables additional error analysis monitoring with AWS Glue job run insights"
}

variable "enable_auto_scaling" {
  type        = bool
  default     = false
  description = "(Optional) Turns on auto scaling and per-worker billing when you set the value to true"
}

variable "spark_event_logs_path" {
  type        = string
  default     = null
  description = "(Optional) Specifies an Amazon S3 path. When using the Spark UI monitoring feature."
}

variable "additional_python_modules" {
  type        = list(string)
  default     = []
  description = "(Optional) List of Python modules to add a new module or change the version of an existing module."
}

variable "log_group_name" {
  type        = string
}

variable "execution_class" {
  type        = string
  default     = "STANDARD"
}

variable "command_name" {
  type        = string
  default     = "glueetl"
}

variable "additional_job_parameters" {
  type        = map(string)
  default     = {}
}
