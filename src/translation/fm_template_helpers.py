"""Helpers for reading the FM template structure.

Extracts config defs, direct mappings, fixed values, recommended values, and
resolves ${placeholder} references inside template values. All methods read
from FM template dicts and produce derived views — no side effects on
connector state.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Set


class FmTemplateHelpersMixin:

    def _get_required_properties(self, fm_template: Dict[str, Any]) -> Dict[str, Any]:
        """Extract required properties from FM template"""
        required_props = {}

        if 'templates' in fm_template:
            for template in fm_template['templates']:
                if 'config_defs' in template:
                    for config_def in template['config_defs']:
                        # Skip internal properties as they are handled by the Cloud platform
                        # Check if required is explicitly set to "true" (string) or True (boolean)
                        is_required = config_def.get('required', False)
                        if isinstance(is_required, str):
                            is_required = is_required.lower() == 'true'
                        elif isinstance(is_required, bool):
                            is_required = is_required
                        else:
                            is_required = False

                        if is_required and not config_def.get('internal', False):
                            required_props[config_def['name']] = config_def

        return required_props

    def _is_source_connector(self, fm_template: Dict[str, Any]) -> bool:
        """Determine if a connector is a source or sink based on FM template connector_type"""
        if not fm_template:
            return True

        if fm_template.get('connector_type'):
            return fm_template['connector_type'] == 'SOURCE'

        if 'templates' in fm_template:
            for template in fm_template['templates']:
                if template.get('connector_type'):
                    return template['connector_type'] == 'SOURCE'

        connector_class = fm_template.get('connector.class', '')
        if not connector_class and 'templates' in fm_template and len(fm_template['templates']) > 0:
            connector_class = fm_template['templates'][0].get('connector.class', '')

        source_indicators = ['Source', 'CDC', 'XStream']
        sink_indicators = ['Sink']

        for indicator in source_indicators:
            if indicator in connector_class:
                return True

        for indicator in sink_indicators:
            if indicator in connector_class:
                return False

        return True

    def _create_direct_mappings_from_template(self, fm_template: Dict[str, Any]) -> Dict[str, str]:
        """Create direct property mappings from FM template connector_configs section"""
        mappings = {}

        if not fm_template or 'templates' not in fm_template:
            return mappings

        for template in fm_template['templates']:
            if 'connector_configs' in template:
                for config in template['connector_configs']:
                    if 'value' in config:
                        value = config['value']
                        sm_property_template = config['name']

                        if isinstance(value, str) and value.startswith('${') and value.endswith('}'):
                            fm_property_name = value[2:-1]
                            mappings[sm_property_template] = fm_property_name
                        else:
                            mappings[value] = sm_property_template
                    elif 'switch' in config:
                        sm_property_template = config['name']
                        switch_config = config['switch']

                        for switch_key, switch_values in switch_config.items():
                            if isinstance(switch_values, dict):
                                for condition, switch_value in switch_values.items():
                                    if isinstance(switch_value, str) and switch_value.startswith('${') and switch_value.endswith('}'):
                                        fm_property_name = switch_value[2:-1]
                                        mappings[sm_property_template] = fm_property_name
                                        break
                    else:
                        sm_property_template = config['name']
                        mappings[sm_property_template] = sm_property_template

        return mappings

    def _map_using_template_direct_mappings(self, config: Dict[str, Any], fm_template: Dict[str, Any]) -> Dict[str, Any]:
        """Map SM config to FM config using direct mappings from template"""
        mapped_config = {}
        mapping_errors = []
        direct_mappings = self._create_direct_mappings_from_template(fm_template)
        fixed_values = self._get_fixed_values_from_template(fm_template)
        recommended_values = self._get_recommended_values_from_template(fm_template)

        self.logger.info(f"Created {len(direct_mappings)} direct mappings from template")
        self.logger.info(f"Found {len(fixed_values)} fixed values from template")
        self.logger.info(f"Found {len(recommended_values)} properties with recommended values")

        for sm_property, fm_property in direct_mappings.items():
            if sm_property in config:
                if fm_property in fixed_values:
                    template_value = fixed_values[fm_property]
                    sm_value = config[sm_property]
                    if str(sm_value) != str(template_value):
                        mapped_config[fm_property] = template_value
                        error_msg = f"Property '{sm_property}' value '{sm_value}' overridden by template fixed value '{template_value}' for '{fm_property}'"
                        mapping_errors.append(error_msg)
                        self.logger.warning(f"Fixed value override: {sm_property}='{sm_value}' -> {fm_property}='{template_value}'")
                    else:
                        mapped_config[fm_property] = sm_value
                        self.logger.info(f"Direct template mapping (values match): {sm_property} -> {fm_property}")
                else:
                    sm_value = config[sm_property]
                    mapped_config[fm_property] = sm_value

                    if fm_property in recommended_values:
                        allowed_values = recommended_values[fm_property]
                        if str(sm_value) not in allowed_values:
                            error_msg = f"Property '{sm_property}' value '{sm_value}' is not in recommended values {allowed_values} for '{fm_property}'"
                            mapping_errors.append(error_msg)
                            self.logger.error(f"Value validation failed: {sm_property}='{sm_value}' not in {allowed_values}")
                            continue
                        else:
                            self.logger.info(f"Direct template mapping (validated): {sm_property} -> {fm_property}")

        for sm_property, value in config.items():
            if sm_property not in mapped_config and sm_property in direct_mappings.values():
                mapped_config[sm_property] = value
                self.logger.info(f"Same-name mapping: {sm_property}")

        return mapped_config, mapping_errors

    def _get_fixed_values_from_template(self, fm_template: Dict[str, Any]) -> Dict[str, str]:
        """Extract fixed values from FM template connector_configs section"""
        fixed_values = {}

        if not fm_template or 'templates' not in fm_template:
            return fixed_values

        for template in fm_template['templates']:
            if 'connector_configs' in template:
                for config in template['connector_configs']:
                    if 'value' in config:
                        value = config['value']
                        fm_property = config['name']

                        if not (isinstance(value, str) and value.startswith('${') and value.endswith('}')):
                            fixed_values[fm_property] = value

        return fixed_values

    def _get_recommended_values_from_template(self, fm_template: Dict[str, Any]) -> Dict[str, List[str]]:
        """Extract recommended values from FM template config_defs section"""
        recommended_values = {}

        if not fm_template or 'templates' not in fm_template:
            return recommended_values

        for template in fm_template['templates']:
            if 'config_defs' in template:
                for config_def in template['config_defs']:
                    if 'recommended_values' in config_def:
                        recommended_values[config_def['name']] = config_def['recommended_values']

        return recommended_values

    def _extract_connector_config_defs(self, fm_template: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract connector config definitions from FM template (following Java pattern)"""
        connector_config_defs = []

        if 'templates' in fm_template:
            if not isinstance(fm_template['templates'], (list, tuple)):
                self.logger.error(f"fm_template['templates'] is not a list, got {type(fm_template['templates'])}: {fm_template['templates']}")
                return connector_config_defs

            for i, template in enumerate(fm_template['templates']):
                self.logger.debug(f"Processing template {i}: {type(template)}")
                if not isinstance(template, dict):
                    self.logger.warning(f"Template {i} is not a dict: {type(template)}")
                    continue

                if 'connector_configs' in template:
                    self.logger.debug(f"Template {i} has connector_configs: {type(template['connector_configs'])}")
                    if isinstance(template['connector_configs'], (list, tuple)):
                        connector_config_defs.extend(template['connector_configs'])
                    else:
                        self.logger.warning(f"Expected connector_configs to be a list, got {type(template['connector_configs'])}: {template['connector_configs']}")
                        continue
        else:
            self.logger.warning("No 'templates' key found in fm_template")

        return connector_config_defs

    def _extract_template_config_defs(self, fm_template: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract template config definitions from FM template (following Java pattern)

        When multiple templates define the same config, uses the first definition encountered.
        """
        template_config_defs = []
        seen_config_names = set()

        if 'templates' in fm_template:
            if not isinstance(fm_template['templates'], (list, tuple)):
                self.logger.error(f"fm_template['templates'] is not a list, got {type(fm_template['templates'])}: {fm_template['templates']}")
                return template_config_defs

            for i, template in enumerate(fm_template['templates']):
                self.logger.debug(f"Processing template {i} for config_defs: {type(template)}")
                if not isinstance(template, dict):
                    self.logger.warning(f"Template {i} is not a dict: {type(template)}")
                    continue

                if 'config_defs' in template:
                    self.logger.debug(f"Template {i} has config_defs: {type(template['config_defs'])}")
                    if isinstance(template['config_defs'], (list, tuple)):
                        for config_def in template['config_defs']:
                            if not isinstance(config_def, dict) or 'name' not in config_def:
                                continue

                            config_name = config_def['name']

                            if config_name not in seen_config_names:
                                template_config_defs.append(config_def)
                                seen_config_names.add(config_name)
                                self.logger.debug(f"Added config '{config_name}' from template {i} (first definition)")
                            else:
                                self.logger.debug(f"Skipping duplicate config '{config_name}' from template {i} (using first definition)")
                    else:
                        self.logger.warning(f"Expected config_defs to be a list, got {type(template['config_defs'])}: {template['config_defs']}")
                        continue
        else:
            self.logger.warning("No 'templates' key found in fm_template")

        self.logger.debug(f"Extracted {len(template_config_defs)} unique config definitions (using first definition for duplicates)")
        return template_config_defs

    def _find_template_config_def_by_name(self, name: str, template_config_defs: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Find template config def by name (following Java pattern)"""
        for template_config_def in template_config_defs:
            if template_config_def.get('name') == name:
                return template_config_def
        return None

    def _find_referenced_keys(self, value: str, high_level_keys: Set[str]) -> Set[str]:
        """Find ${...} references inside a template value that match known high-level keys."""
        default_pattern = re.compile(r'\$\{([^}]+)\}')
        referenced_keys = set()

        for match in default_pattern.finditer(value):
            referenced_key = match.group(1)
            if referenced_key in high_level_keys:
                referenced_keys.add(referenced_key)

        return referenced_keys

    def _get_template_default_value(self, template_config_defs: List[Dict[str, Any]], config_name: str) -> Optional[str]:
        """Extract default value for a configuration from template definitions"""
        for template_config_def in template_config_defs:
            if template_config_def.get('name') == config_name:
                default_value = template_config_def.get('default_value')
                if default_value is not None:
                    return str(default_value)
        return None

    def _is_placeholder(self, value: str) -> bool:
        """Check if a value is a placeholder like ${xxxx}"""
        return value.startswith('${')

    def _extract_placeholder_name(self, placeholder: str) -> str:
        """Extract the placeholder name from ${xxxx} format"""
        if placeholder.startswith('${'):
            end_pos = placeholder.find('}', 2)
            if end_pos != -1:
                return placeholder[2:end_pos]
            else:
                return placeholder[2:]
        return placeholder

    def _resolve_template_default(self, template_default: str, fm_configs: Dict[str, str]) -> str:
        """Resolve template default value, handling placeholders like ${xxxx}"""
        if self._is_placeholder(template_default):
            placeholder_name = self._extract_placeholder_name(template_default)
            if placeholder_name in fm_configs:
                return fm_configs[placeholder_name]
            else:
                self.logger.warning(f"Placeholder '{placeholder_name}' not found in fm_configs")
                return None
        else:
            return template_default
