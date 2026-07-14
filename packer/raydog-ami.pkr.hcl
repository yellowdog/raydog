# Packer template for building RayDog node AMIs.
#
# Bakes the YellowDog agent, uv, a Python venv containing Ray and RayDog,
# and Grafana Alloy (disabled) into an Ubuntu 26.04 LTS base image, so that
# boot-time userdata is reduced to per-node YellowDog agent configuration.
#
# Usage (via the Makefile at the repository root):
#   make ami [PYTHON_VERSION=…] [RAY_VERSION=…] [RAYDOG_VERSION=…]
#            [AWS_REGION=…] [SSH_PUBLIC_KEY="ssh-rsa …"]

packer {
  required_version = ">= 1.10.0"
  required_plugins {
    amazon = {
      source  = "github.com/hashicorp/amazon"
      version = ">= 1.3.0"
    }
  }
}

variable "python_version" {
  type        = string
  default     = "3.12.11"
  description = "Python version to install in the venv"
}

variable "ray_version" {
  type        = string
  default     = "2.56.0"
  description = "Ray version to install"
}

variable "raydog_version" {
  type        = string
  default     = ""
  description = "yellowdog-ray version to install; empty means latest from PyPI"
}

variable "aws_region" {
  type        = string
  default     = "eu-west-2"
  description = "AWS region in which to build and register the AMI"
}

variable "instance_type" {
  type        = string
  default     = "t3.large"
  description = "EC2 instance type for the temporary build instance"
}

variable "ssh_public_key" {
  type        = string
  default     = ""
  description = "Optional public key text to authorise for the yd-agent user"
}

variable "volume_size_gb" {
  type        = number
  default     = 32
  description = "Root EBS volume size for the AMI, in GB"
}

variable "extra_pip_packages" {
  type        = string
  default     = ""
  description = "Space-separated pip requirement specifiers to bake into the venv, e.g. \"numpy==2.3.1 pandas\""
}

variable "subnet_id" {
  type        = string
  default     = ""
  description = "Subnet for the build instance; required if the account/region has no default VPC. Must be a public subnet (routes to an internet gateway)."
}

locals {
  timestamp = regex_replace(timestamp(), "[- TZ:]", "")
  ami_name  = "raydog-ray${var.ray_version}-py${var.python_version}-${local.timestamp}"
}

source "amazon-ebs" "raydog" {
  region        = var.aws_region
  instance_type = var.instance_type
  ssh_username  = "ubuntu"
  ami_name      = local.ami_name

  # Empty subnet_id means "use the default VPC". In accounts without a
  # default VPC, a public subnet must be supplied (SUBNET_ID via make).
  subnet_id                   = var.subnet_id
  associate_public_ip_address = true

  source_ami_filter {
    filters = {
      # Codename is wildcarded so the filter is robust to Canonical's
      # release-name string for 26.04.
      name                = "ubuntu/images/hvm-ssd-gp3/ubuntu-*-26.04-amd64-server-*"
      root-device-type    = "ebs"
      virtualization-type = "hvm"
    }
    owners      = ["099720109477"] # Canonical
    most_recent = true
  }

  launch_block_device_mappings {
    device_name           = "/dev/sda1"
    volume_size           = var.volume_size_gb
    volume_type           = "gp3"
    delete_on_termination = true
  }

  tags = {
    Name          = local.ami_name
    Project       = "raydog"
    PythonVersion = var.python_version
    RayVersion    = var.ray_version
    RayDogVersion = var.raydog_version == "" ? "latest" : var.raydog_version
    SourceAMI     = "{{ .SourceAMI }}"
    SourceAMIName = "{{ .SourceAMIName }}"
    BuiltBy       = "packer"
    BuildDate     = timestamp()
  }
}

build {
  sources = ["source.amazon-ebs.raydog"]

  provisioner "shell" {
    script = "${path.root}/scripts/provision.sh"
    # Vars are passed as explicit arguments to `sudo env` rather than via
    # `sudo -E`: Ubuntu 26.04's sudo-rs does not support preserving the
    # caller's environment, so `-E` is ignored and the vars would be lost.
    execute_command = "chmod +x {{ .Path }}; sudo env {{ .Vars }} bash '{{ .Path }}'"
    environment_vars = [
      "PYTHON_VERSION=${var.python_version}",
      "RAY_VERSION=${var.ray_version}",
      "RAYDOG_VERSION=${var.raydog_version}",
      "SSH_PUBLIC_KEY=${var.ssh_public_key}",
      "EXTRA_PIP_PACKAGES=${var.extra_pip_packages}",
    ]
  }
}
