output "kinesis_stream_name" {
  description = "Created Kinesis stream name"
  value       = aws_kinesis_stream.order_events.name
}

output "kinesis_stream_arn" {
  description = "Created Kinesis stream ARN"
  value       = aws_kinesis_stream.order_events.arn
}
