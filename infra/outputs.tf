output "ecr_repository_url" {
  description = "ECR repository URL — use this in CI to tag and push images"
  value       = aws_ecr_repository.agent.repository_url
}

output "ecs_cluster_name" {
  description = "ECS cluster name — use this to run one-off tasks from the CLI"
  value       = aws_ecs_cluster.agent.name
}

output "task_definition_arn" {
  description = "Latest registered task definition ARN"
  value       = aws_ecs_task_definition.agent.arn
}

output "task_role_arn" {
  description = "IAM task role ARN — the identity the agent process runs as"
  value       = aws_iam_role.task.arn
}

output "security_group_id" {
  description = "ECS task security group ID"
  value       = aws_security_group.agent.id
}

output "log_group_name" {
  description = "CloudWatch log group — query here for structured JSON agent logs"
  value       = aws_cloudwatch_log_group.agent.name
}
