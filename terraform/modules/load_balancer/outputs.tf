output "alb_arn" {
  description = "ALB ARN."
  value       = aws_lb.api.arn
}

output "alb_dns_name" {
  description = "ALB DNS name."
  value       = aws_lb.api.dns_name
}

output "alb_zone_id" {
  description = "ALB Route53 zone ID."
  value       = aws_lb.api.zone_id
}

output "api_target_group_arn" {
  description = "API target group ARN."
  value       = aws_lb_target_group.api.arn
}
