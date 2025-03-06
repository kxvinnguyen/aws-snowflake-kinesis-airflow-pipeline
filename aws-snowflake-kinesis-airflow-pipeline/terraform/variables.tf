variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Name of the project"
  type        = string
  default     = "snowflake-kinesis-pipeline"
}

variable "environment" {
  description = "Deployment environment (dev, test, prod)"
  type        = string
  default     = "dev"
}

variable "kinesis_shard_count" {
  description = "Number of shards for Kinesis Data Stream"
  type        = number
  default     = 1
}

variable "snowflake_username" {
  description = "Snowflake username"
  type        = string
  sensitive   = true
}

variable "snowflake_password" {
  description = "Snowflake password"
  type        = string
  sensitive   = true
}

variable "snowflake_account" {
  description = "Snowflake account identifier"
  type        = string
}

variable "snowflake_database" {
  description = "Snowflake database name"
  type        = string
  default     = "KINESIS_DATA"
}

variable "snowflake_warehouse" {
  description = "Snowflake warehouse name"
  type        = string
  default     = "KINESIS_WH"
}

variable "snowflake_role" {
  description = "Snowflake role name"
  type        = string
  default     = "ACCOUNTADMIN"
}

variable "snowflake_schema" {
  description = "Snowflake schema name"
  type        = string
  default     = "PUBLIC"
}