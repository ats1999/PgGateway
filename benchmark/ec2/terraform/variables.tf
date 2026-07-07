variable "region" {
  type        = string
  description = "AWS region to launch instances in"
}

variable "instance_type" {
  type        = string
  description = "EC2 instance type for all 3 benchmark instances"
}

variable "run_id" {
  type        = string
  description = "Unique identifier for this benchmark run (used in resource names/tags)"
}
