output "instance_ids" {
  value = { for role, inst in aws_instance.bench : role => inst.id }
}

output "public_ips" {
  value = { for role, inst in aws_instance.bench : role => inst.public_ip }
}

output "private_ips" {
  value = { for role, inst in aws_instance.bench : role => inst.private_ip }
}

output "security_group_id" {
  value = aws_security_group.bench.id
}
