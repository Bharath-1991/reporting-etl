# variable "project_name" { default = "tier3" }
# variable "display_name" { default = "tier3_canada-cluster" }
# variable "component_name" { default = "motocommerce-canada" }
# variable "gh_project_name" { default = "tier3" }

variable "name" {
  type        = string
  description = "(Required) Name that will be used for identify resources."
}

variable "environment" {
  type        = string
  description = "(Required) Environment name that will be used for identify resources."
}

variable "tags" {
  type        = map(string)
  default     = {}
  description = "(Optional) Key-value map of resource tags."
}



#---------------------------------------------------
# AWS Glue trigger
#---------------------------------------------------
variable "enable_glue_trigger" {
  description = "Enable glue trigger usage"
  default     = false
}

variable "glue_trigger_name" {
  description = "The name of the trigger."
  default     = ""
}

variable "glue_trigger_type" {
  description = "(Required) The type of trigger. Valid values are CONDITIONAL, ON_DEMAND, and SCHEDULED."
  default     = "ON_DEMAND"
}

variable "glue_trigger_description" {
  description = "(Optional) A description of the new trigger."
  default     = null
}

variable "glue_trigger_enabled" {
  description = "(Optional) Start the trigger. Defaults to true. Not valid to disable for ON_DEMAND type."
  default     = null
}

variable "glue_trigger_schedule" {
  description = "(Optional) A cron expression used to specify the schedule. Time-Based Schedules for Jobs and Crawlers"
  default     = null
}

variable "glue_trigger_workflow_name" {
  description = "(Optional) A workflow to which the trigger should be associated to. Every workflow graph (DAG) needs a starting trigger (ON_DEMAND or SCHEDULED type) and can contain multiple additional CONDITIONAL triggers."
  default     = null
}

variable "glue_trigger_actions" {
  description = "(Required) List of actions initiated by this trigger when it fires. "
  default     = []
}

variable "glue_trigger_timeouts" {
  description = "Set timeouts for glue trigger"
  default     = {}
}

variable "glue_trigger_predicate" {
  description = "(Optional) A predicate to specify when the new trigger should fire. Required when trigger type is CONDITIONAL"
  default     = {}
}