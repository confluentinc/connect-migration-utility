"""
Apache Connect Migration Utility
Copyright 2024-2025 The Apache Software Foundation

This product includes software developed at The Apache Software Foundation.
"""

import json
import logging
import re
from pathlib import Path
from typing import Dict, Any, List, Optional

from connector_comparator import ConnectorComparator


class TerraformGenerator:
    """Generate Terraform files for Confluent Cloud connectors."""

    def __init__(self, output_dir: Path, environment_id: Optional[str], kafka_cluster_id: Optional[str], logger: Optional[logging.Logger] = None):
        """
        Initialize Terraform generator.

        Args:
            output_dir: Output directory for terraform files
            environment_id: Confluent Cloud environment ID (optional, will use placeholder if not provided)
            kafka_cluster_id: Confluent Cloud LKC cluster ID (optional, will use placeholder if not provided)
            logger: Logger instance
        """
        self.output_dir = output_dir
        self.environment_id = environment_id or "TO_BE_FILLED"
        self.kafka_cluster_id = kafka_cluster_id or "TO_BE_FILLED"
        self.logger = logger or logging.getLogger(__name__)

    def _sanitize_resource_name(self, name: str) -> str:
        """Sanitize connector name for Terraform resource name."""
        # Replace invalid characters with underscores
        sanitized = re.sub(r'[^a-zA-Z0-9_-]', '_', name)
        # Ensure it starts with a letter or underscore
        if sanitized and not (sanitized[0].isalpha() or sanitized[0] == '_'):
            sanitized = '_' + sanitized
        return sanitized

    def _escape_hcl_string(self, value: str) -> str:
        """Escape special characters in HCL string values."""
        # Escape backslashes and quotes
        value = value.replace('\\', '\\\\')
        value = value.replace('"', '\\"')
        return value

    def _format_config_value(self, value: Any) -> str:
        """Format a configuration value for HCL."""
        if isinstance(value, bool):
            return str(value).lower()
        elif isinstance(value, (int, float)):
            return str(value)
        elif isinstance(value, str):
            # Check if it's a variable reference (e.g., ${var.name})
            if value.startswith('${') and value.endswith('}'):
                return value
            # Escape and quote string values
            escaped = self._escape_hcl_string(value)
            return f'"{escaped}"'
        elif isinstance(value, list):
            items = [self._format_config_value(item) for item in value]
            return f'[{", ".join(items)}]'
        else:
            # Fallback: convert to string and quote
            escaped = self._escape_hcl_string(str(value))
            return f'"{escaped}"'

    def _is_sensitive_field(self, key: str, value: Any) -> bool:
        """Determine if a config field is sensitive."""
        key_lower = key.lower()
        # Check for common sensitive field patterns
        sensitive_patterns = ['password', 'secret', 'key', 'token', 'credential', 'auth']
        if any(pattern in key_lower for pattern in sensitive_patterns):
            return True
        # Check for key vault references
        if isinstance(value, str) and ('${keyVault:' in value or '${azurekeyvault:' in value):
            return True
        return False

    def _generate_connector_resource(self, connector_name: str, config: Dict[str, Any], warnings: Optional[List[Dict[str, str]]] = None) -> str:
        """Generate Terraform resource block for a single connector (matching Go template format)."""
        resource_name = self._sanitize_resource_name(connector_name).lower()
        
        lines = []
        
        # Add warnings comment if present
        if warnings:
            lines.append('/*')
            lines.append(' * The following warnings were returned by the connector config translation endpoint:')
            for warning in warnings:
                field = warning.get('field', '')
                message = warning.get('message', '')
                lines.append(f' * - [{field}] {message}')
            lines.append(' */')
            lines.append('')
        
        lines.append(f'resource "confluent_connector" "{resource_name}" {{')
        lines.append('  environment {')
        lines.append(f'    id = "{self.environment_id}"')
        lines.append('  }')
        lines.append('  kafka_cluster {')
        lines.append(f'    id = "{self.kafka_cluster_id}"')
        lines.append('  }')
        lines.append('')
        lines.append('  config_sensitive = {')
        lines.append('    /*')
        lines.append('    ## Choose one of the following options:')
        lines.append('    ## https://registry.terraform.io/providers/confluentinc/confluent/latest/docs/resources/confluent_connector')
        lines.append('')
        lines.append('      "kafka.auth.mode"  = "KAFKA_API_KEY",')
        lines.append('      "kafka.api.key"    = "<cluster_api_key>",')
        lines.append('      "kafka.api.secret" = "<cluster_api_secret>",')
        lines.append('    ## ----------------------------------------------- ##')
        lines.append('      "kafka.auth.mode"          = "SERVICE_ACCOUNT",')
        lines.append('      "kafka.service.account.id" = "<service_account_id>"')
        lines.append('    */')
        lines.append('  }')
        lines.append('')
        lines.append('  config_nonsensitive = {')

        # Add all configs to config_nonsensitive
        for key, value in sorted(config.items()):
            formatted_value = self._format_config_value(value)
            lines.append(f'    "{self._escape_hcl_string(key)}" = {formatted_value}')
        
        lines.append('  }')
        lines.append('}')
        lines.append('')

        return '\n'.join(lines)

    def _generate_provider_block(self) -> str:
        """Generate Terraform provider configuration block."""
        return '''terraform {
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 1.0"
    }
  }
}

provider "confluent" {
  # Configure provider with environment variables or terraform.tfvars
  # cloud_api_key    = var.confluent_cloud_api_key
  # cloud_api_secret = var.confluent_cloud_api_secret
}

'''

    def _generate_variables_file(self) -> str:
        """Generate Terraform variables file."""
        return '''variable "confluent_cloud_api_key" {
  description = "Confluent Cloud API Key"
  type        = string
  sensitive   = true
}

variable "confluent_cloud_api_secret" {
  description = "Confluent Cloud API Secret"
  type        = string
  sensitive   = true
}

variable "environment_id" {
  description = "Confluent Cloud Environment ID"
  type        = string
  default     = ""
}

variable "kafka_cluster_id" {
  description = "Confluent Cloud Kafka Cluster ID"
  type        = string
  default     = ""
}

'''

    def _generate_outputs_file(self, connector_names: List[str]) -> str:
        """Generate Terraform outputs file."""
        lines = ['output "connector_ids" {', '  description = "Map of connector names to their IDs"', '  value = {']
        
        for connector_name in connector_names:
            resource_name = self._sanitize_resource_name(connector_name)
            lines.append(f'    "{self._escape_hcl_string(connector_name)}" = confluent_connector.{resource_name}.id')
        
        lines.append('  }')
        lines.append('}')
        lines.append('')
        
        return '\n'.join(lines)

    def generate_from_successful_configs(self, successful_configs_dir: Path) -> Path:
        """
        Generate Terraform files from successful connector configurations.
        Generates individual files per connector (matching Go implementation).

        Args:
            successful_configs_dir: Directory containing successful connector configs

        Returns:
            Path to the generated terraform directory
        """
        terraform_dir = self.output_dir / 'terraform'
        terraform_dir.mkdir(parents=True, exist_ok=True)

        self.logger.info(f"Generating Terraform files in {terraform_dir}")

        # Find all successful connector JSON files
        connector_files = list(successful_configs_dir.glob('*.json'))
        
        if not connector_files:
            self.logger.warning(f"No connector configuration files found in {successful_configs_dir}")
            return terraform_dir

        connector_names = []

        # Process each connector file and generate individual Terraform files
        for config_file in connector_files:
            try:
                with open(config_file, 'r') as f:
                    connector_data = json.load(f)

                # Extract connector name and config
                connector_name = connector_data.get('name')
                config = connector_data.get('config', {})
                warnings = connector_data.get('warnings', [])

                if not connector_name:
                    self.logger.warning(f"Skipping {config_file}: missing 'name' field")
                    continue

                if not config:
                    self.logger.warning(f"Skipping {config_file}: missing 'config' field")
                    continue

                # Generate individual Terraform file for this connector (matching Go format)
                filename = f"{connector_name}-connector.tf"
                filepath = terraform_dir / filename

                resource_block = self._generate_connector_resource(connector_name, config, warnings)
                
                with open(filepath, 'w') as f:
                    f.write(resource_block)
                
                connector_names.append(connector_name)
                self.logger.info(f"✅ Generated: {filename}")

            except json.JSONDecodeError as e:
                self.logger.error(f"Failed to parse JSON file {config_file}: {e}")
            except Exception as e:
                self.logger.error(f"Error processing {config_file}: {e}")

        self.logger.info(f"✅ Successfully generated connector files for {len(connector_names)} connectors in {terraform_dir}")
        return terraform_dir

    def generate_from_fm_configs_dict(self, fm_configs: Dict[str, Any]) -> Path:
        """
        Generate Terraform files from FM configs dictionary.
        Generates individual files per connector (matching Go implementation).

        Args:
            fm_configs: Dictionary of FM connector configurations

        Returns:
            Path to the generated terraform directory
        """
        terraform_dir = self.output_dir / 'terraform'
        terraform_dir.mkdir(parents=True, exist_ok=True)

        self.logger.info(f"Generating Terraform files in {terraform_dir}")

        connector_names = []

        # Process each connector and generate individual Terraform files
        for connector_name, fm_config in fm_configs.items():
            # Only process successful configs (no mapping errors)
            mapping_errors = fm_config.get('mapping_errors', [])
            if mapping_errors:
                self.logger.debug(f"Skipping {connector_name}: has mapping errors")
                continue

            config = fm_config.get('config', {})
            if not config:
                self.logger.warning(f"Skipping {connector_name}: missing 'config' field")
                continue

            warnings = fm_config.get('warnings', [])

            # Generate individual Terraform file for this connector (matching Go format)
            filename = f"{connector_name}-connector.tf"
            filepath = terraform_dir / filename

            resource_block = self._generate_connector_resource(connector_name, config, warnings)
            
            with open(filepath, 'w') as f:
                f.write(resource_block)
            
            connector_names.append(connector_name)
            self.logger.info(f"✅ Generated: {filename}")

        if not connector_names:
            self.logger.warning("No successful connector configurations found for Terraform generation")
            return terraform_dir

        self.logger.info(f"✅ Successfully generated connector files for {len(connector_names)} connectors in {terraform_dir}")
        return terraform_dir


