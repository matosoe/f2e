locals {
  file_config_path        = "/f2e/${var.environment}/file-config"
  global_limits_parameter = "/f2e/${var.environment}/global-limits"
}

resource "aws_ssm_parameter" "global_limits" {
  name  = local.global_limits_parameter
  type  = "String"
  value = jsonencode(local.global_limits)
  tags  = local.tags
}

resource "aws_ssm_parameter" "file_configuration" {
  for_each = local.file_configurations

  name  = "${local.file_config_path}/${var.f2e_input_bucket}/${each.key}"
  type  = "String"
  value = jsonencode(each.value)
  tags  = local.tags
}
