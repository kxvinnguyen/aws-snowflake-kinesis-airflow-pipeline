# AWS Snowflake Data Pipeline Terraform Configuration

provider "aws" {
  region = var.aws_region
}

# Random ID for unique resource naming
resource "random_id" "unique_suffix" {
  byte_length = 4
}

locals {
  resource_prefix = "${var.project_name}-${random_id.unique_suffix.hex}"
}

# S3 Bucket for data storage
resource "aws_s3_bucket" "data_bucket" {
  bucket = "${local.resource_prefix}-data"
  force_destroy = true

  tags = {
    Name        = "${local.resource_prefix}-data"
    Environment = var.environment
    Project     = var.project_name
  }
}

# S3 Bucket for Airflow logs
resource "aws_s3_bucket" "airflow_logs" {
  bucket = "${local.resource_prefix}-airflow-logs"
  force_destroy = true

  tags = {
    Name        = "${local.resource_prefix}-airflow-logs"
    Environment = var.environment
    Project     = var.project_name
  }
}

# S3 Bucket public access block
resource "aws_s3_bucket_public_access_block" "data_bucket_access" {
  bucket = aws_s3_bucket.data_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# S3 Bucket public access block for Airflow logs
resource "aws_s3_bucket_public_access_block" "airflow_logs_access" {
  bucket = aws_s3_bucket.airflow_logs.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# IAM Role for Kinesis Firehose
resource "aws_iam_role" "firehose_role" {
  name = "${local.resource_prefix}-firehose-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "firehose.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

# IAM Policy for Kinesis Firehose to access S3
resource "aws_iam_policy" "firehose_s3_access" {
  name        = "${local.resource_prefix}-firehose-s3-access"
  description = "IAM policy for Kinesis Firehose to access S3 bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:AbortMultipartUpload",
          "s3:GetBucketLocation",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:ListBucketMultipartUploads",
          "s3:PutObject"
        ]
        Effect = "Allow"
        Resource = [
          aws_s3_bucket.data_bucket.arn,
          "${aws_s3_bucket.data_bucket.arn}/*"
        ]
      }
    ]
  })
}

# Attach policy to role
resource "aws_iam_role_policy_attachment" "firehose_s3_attachment" {
  role       = aws_iam_role.firehose_role.name
  policy_arn = aws_iam_policy.firehose_s3_access.arn
}

# Kinesis Data Stream
resource "aws_kinesis_stream" "data_stream" {
  name             = "${local.resource_prefix}-stream"
  shard_count      = var.kinesis_shard_count
  retention_period = 24

  shard_level_metrics = [
    "IncomingBytes",
    "OutgoingBytes",
    "IncomingRecords",
    "OutgoingRecords"
  ]

  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

# IAM Role for accessing Kinesis Data Stream
resource "aws_iam_role" "kinesis_access_role" {
  name = "${local.resource_prefix}-kinesis-access-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

# IAM Policy for Kinesis access
resource "aws_iam_policy" "kinesis_access_policy" {
  name        = "${local.resource_prefix}-kinesis-access-policy"
  description = "IAM policy for accessing Kinesis Data Stream"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "kinesis:DescribeStream",
          "kinesis:GetRecords",
          "kinesis:GetShardIterator",
          "kinesis:ListShards",
          "kinesis:PutRecord",
          "kinesis:PutRecords"
        ]
        Effect = "Allow"
        Resource = [
          aws_kinesis_stream.data_stream.arn
        ]
      }
    ]
  })
}

# Attach policy to role
resource "aws_iam_role_policy_attachment" "kinesis_access_attachment" {
  role       = aws_iam_role.kinesis_access_role.name
  policy_arn = aws_iam_policy.kinesis_access_policy.arn
}

# Kinesis Firehose Delivery Stream
resource "aws_kinesis_firehose_delivery_stream" "s3_stream" {
  name        = "${local.resource_prefix}-delivery-stream"
  destination = "s3"

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.data_stream.arn
    role_arn           = aws_iam_role.firehose_role.arn
  }

  s3_configuration {
    role_arn   = aws_iam_role.firehose_role.arn
    bucket_arn = aws_s3_bucket.data_bucket.arn
    prefix     = "raw/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/"
    buffer_size        = 5
    buffer_interval    = 300
    compression_format = "GZIP"
  }

  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

# Optional: Create Secrets Manager secrets for Snowflake credentials
resource "aws_secretsmanager_secret" "snowflake_credentials" {
  name        = "${local.resource_prefix}-snowflake-credentials"
  description = "Snowflake credentials for data pipeline"

  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

resource "aws_secretsmanager_secret_version" "snowflake_credentials_version" {
  secret_id     = aws_secretsmanager_secret.snowflake_credentials.id
  secret_string = jsonencode({
    username     = var.snowflake_username
    password     = var.snowflake_password
    account      = var.snowflake_account
    database     = var.snowflake_database
    warehouse    = var.snowflake_warehouse
    role         = var.snowflake_role
    schema       = var.snowflake_schema
  })
}

# Outputs
output "kinesis_stream_name" {
  value = aws_kinesis_stream.data_stream.name
}

output "firehose_delivery_stream_name" {
  value = aws_kinesis_firehose_delivery_stream.s3_stream.name
}

output "s3_bucket_name" {
  value = aws_s3_bucket.data_bucket.bucket
}

output "airflow_logs_bucket_name" {
  value = aws_s3_bucket.airflow_logs.bucket
}

output "secrets_manager_snowflake_secret_name" {
  value = aws_secretsmanager_secret.snowflake_credentials.name
}
