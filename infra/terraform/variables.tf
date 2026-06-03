variable "aws_region" {
  description = "AWS region used for Kinesis"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Tag for identifying project resources"
  type        = string
  default     = "dijkfood-demo"
}

variable "kinesis_stream_name" {
  description = "Kinesis stream name for order lifecycle events"
  type        = string
  default     = "dijkfood-order-events"
}

variable "kinesis_shard_count" {
  description = "Number of shards for Kinesis stream"
  type        = number
  default     = 1
}

variable "kinesis_retention_hours" {
  description = "Retention period for Kinesis records"
  type        = number
  default     = 24
}
