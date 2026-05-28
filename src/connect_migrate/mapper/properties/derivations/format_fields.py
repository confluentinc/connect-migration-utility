"""Derive FM input/output key/value format fields (AVRO/JSON_SR/PROTOBUF/STRING)."""

import re
from typing import Any, Dict, List, Optional

from connect_migrate.mapper.properties.derivations.base import DerivationGroup


class FormatFieldDeriver(DerivationGroup):
    DERIVATIONS: Dict[str, str] = {
        'input.key.format': '_derive_input_key_format',
        'input.data.format': '_derive_input_data_format',
        'output.key.format': '_derive_output_key_format',
        'output.data.format': '_derive_output_data_format',
        'output.data.key.format': '_derive_output_data_key_format',
        'output.data.value.format': '_derive_output_data_value_format',
    }

    def _derive_input_key_format(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: Optional[List[Dict[str, Any]]] = None, config_name: Optional[str] = None) -> Optional[str]:
        """Derive input.key.format from user configs using reverse format mapping"""
        # Reverse data format mapping from template (converter class -> format key)
        reverse_format_mapping = {
            "io.confluent.connect.avro.AvroConverter": "AVRO",
            "io.confluent.connect.json.JsonSchemaConverter": "JSON_SR",
            "io.confluent.connect.protobuf.ProtobufConverter": "PROTOBUF",
            "org.apache.kafka.connect.converters.ByteArrayConverter": "BYTES",
            "org.apache.kafka.connect.json.JsonConverter": "JSON",
            "org.apache.kafka.connect.storage.StringConverter": "STRING"
        }

        # Try direct converter mapping first (reverse map)
        if 'key.converter' in user_configs:
            converter_class = user_configs['key.converter']
            if converter_class in reverse_format_mapping:
                return reverse_format_mapping[converter_class]
            return converter_class  # Return as-is if not in mapping

        # Try to get format from user configs (direct format key)
        format_key = user_configs.get('key.format') or user_configs.get('input.key.format')
        if format_key:
            return format_key

        # Try to infer from schema registry configs
        if 'key.converter.schemas.enable' in user_configs:
            return 'JSON_SR'

        # Try to get default from template if available
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'input.key.format')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default

        # Default fallback
        return 'JSON'

    def _derive_input_data_format(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: Optional[List[Dict[str, Any]]] = None, config_name: Optional[str] = None) -> Optional[str]:

        """Derive input.data.format from user configs using reverse format mapping"""
        # Reverse data format mapping from template (converter class -> format key)
        reverse_format_mapping = {
            "io.confluent.connect.avro.AvroConverter": "AVRO",
            "io.confluent.connect.json.JsonSchemaConverter": "JSON_SR",
            "io.confluent.connect.protobuf.ProtobufConverter": "PROTOBUF",
            "org.apache.kafka.connect.converters.ByteArrayConverter": "BYTES",
            "org.apache.kafka.connect.json.JsonConverter": "JSON",
            "org.apache.kafka.connect.storage.StringConverter": "STRING"
        }

        # Try direct converter mapping first (reverse map)
        if 'value.converter' in user_configs:
            converter_class = user_configs['value.converter']
            if converter_class in reverse_format_mapping:
                return reverse_format_mapping[converter_class]
            return converter_class  # Return as-is if not in mapping

        # Try to get format from user configs (direct format key)
        format_key = user_configs.get('value.format') or user_configs.get('input.data.format')
        if format_key:
            return format_key

        # Try to infer from schema registry configs
        if 'value.converter.schemas.enable' in user_configs:
            return 'JSON_SR'

        # Try to get default from template if available
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'input.data.format')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default

        # Default fallback
        return 'JSON'

    def _derive_output_key_format(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: Optional[List[Dict[str, Any]]] = None, config_name: Optional[str] = None) -> Optional[str]:

        """Derive output.key.format from user configs using reverse format mapping"""
        # Reverse data format mapping from template (converter class -> format key)
        reverse_format_mapping = {
            "io.confluent.connect.avro.AvroConverter": "AVRO",
            "io.confluent.connect.json.JsonSchemaConverter": "JSON_SR",
            "io.confluent.connect.protobuf.ProtobufConverter": "PROTOBUF",
            "org.apache.kafka.connect.converters.ByteArrayConverter": "BYTES",
            "org.apache.kafka.connect.json.JsonConverter": "JSON",
            "org.apache.kafka.connect.storage.StringConverter": "STRING"
        }

        # Try direct converter mapping first (reverse map)
        if 'key.converter' in user_configs:
            converter_class = user_configs['key.converter']
            if converter_class in reverse_format_mapping:
                return reverse_format_mapping[converter_class]
            return converter_class  # Return as-is if not in mapping

        # Try to get format from user configs (direct format key)
        format_key = user_configs.get('output.key.format') or user_configs.get('key.format')
        if format_key:
            return format_key

        # Try to get default from template if available
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'output.key.format')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default

        # Default fallback
        return 'JSON'

    def _derive_output_data_format(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: Optional[List[Dict[str, Any]]] = None, config_name: Optional[str] = None) -> Optional[str]:

        """Derive output.data.format from user configs using reverse format mapping"""
        # Reverse data format mapping from template (converter class -> format key)
        reverse_format_mapping = {
            "io.confluent.connect.avro.AvroConverter": "AVRO",
            "io.confluent.connect.json.JsonSchemaConverter": "JSON_SR",
            "io.confluent.connect.protobuf.ProtobufConverter": "PROTOBUF",
            "org.apache.kafka.connect.converters.ByteArrayConverter": "BYTES",
            "org.apache.kafka.connect.json.JsonConverter": "JSON",
            "org.apache.kafka.connect.storage.StringConverter": "STRING"
        }

        # Try direct converter mapping first (reverse map)
        if 'value.converter' in user_configs:
            converter_class = user_configs['value.converter']
            if converter_class in reverse_format_mapping:
                return reverse_format_mapping[converter_class]
            return converter_class  # Return as-is if not in mapping

        # Try to get format from user configs (direct format key)
        format_key = user_configs.get('output.data.format') or user_configs.get('value.format')
        if format_key:
          return format_key

        # Try to get default from template if available
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'output.data.format')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default

        # Default fallback
        return 'JSON'

    def _derive_output_data_key_format(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: Optional[List[Dict[str, Any]]] = None, config_name: Optional[str] = None) -> Optional[str]:

        """Derive output.data.key.format from user configs using reverse format mapping"""
        # Reverse data format mapping from template (converter class -> format key)
        reverse_format_mapping = {
            "io.confluent.connect.avro.AvroConverter": "AVRO",
            "io.confluent.connect.json.JsonSchemaConverter": "JSON_SR",
            "io.confluent.connect.protobuf.ProtobufConverter": "PROTOBUF",
            "org.apache.kafka.connect.converters.ByteArrayConverter": "BYTES",
            "org.apache.kafka.connect.json.JsonConverter": "JSON",
            "org.apache.kafka.connect.storage.StringConverter": "STRING"
        }

        # Try direct converter mapping first (reverse map)
        if 'key.converter' in user_configs:
            converter_class = user_configs['key.converter']
            if converter_class in reverse_format_mapping:
                return reverse_format_mapping[converter_class]
            return converter_class  # Return as-is if not in mapping

        # Try to get format from user configs (direct format key)
        format_key = user_configs.get('output.data.key.format') or user_configs.get('key.format')
        if format_key:
            return format_key

        # Try to infer from output key format if already derived
        if 'output.key.format' in fm_configs:
            return fm_configs['output.key.format']

        # Try to get default from template if available
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'output.data.key.format')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default

        # Default fallback
        return 'JSON'

    def _derive_output_data_value_format(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: Optional[List[Dict[str, Any]]] = None, config_name: Optional[str] = None) -> Optional[str]:

        """Derive output.data.value.format from user configs using reverse format mapping"""
        # Reverse data format mapping from template (converter class -> format key)
        reverse_format_mapping = {
            "io.confluent.connect.avro.AvroConverter": "AVRO",
            "io.confluent.connect.json.JsonSchemaConverter": "JSON_SR",
            "io.confluent.connect.protobuf.ProtobufConverter": "PROTOBUF",
            "org.apache.kafka.connect.converters.ByteArrayConverter": "BYTES",
            "org.apache.kafka.connect.json.JsonConverter": "JSON",
            "org.apache.kafka.connect.storage.StringConverter": "STRING"
        }

        # Try direct converter mapping first (reverse map)
        if 'value.converter' in user_configs:
            converter_class = user_configs['value.converter']
            if converter_class in reverse_format_mapping:
                return reverse_format_mapping[converter_class]
            return converter_class  # Return as-is if not in mapping

        # Try to get format from user configs (direct format key)
        format_key = user_configs.get('output.data.value.format') or user_configs.get('value.format')
        if format_key:
            return format_key

        # Try to infer from output data format if already derived
        if 'output.data.format' in fm_configs:
            return fm_configs['output.data.format']

        # Try to get default from template if available
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'output.data.value.format')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default

        # Default fallback
        return 'JSON'
