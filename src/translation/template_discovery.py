"""FM template lookup, JDBC auto-selection, CDC filtering, plugin resolution."""

from __future__ import annotations

import base64
import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import requests
from requests.auth import HTTPBasicAuth
from translation.matching.semantic_matcher import Property, SemanticMatcher


class TemplateDiscoveryMixin:

    def _get_plugin_name_for_connector(self, connector_class: str, config_dict: Dict[str, Any] = None) -> Optional[str]:
        """
        Get the FM plugin name (template_id) for a connector class.
        
        For JDBC connectors, uses the existing database type detection logic.
        For other connectors, extracts template_id from the FM template file.
        
        Args:
            connector_class: The connector.class value (e.g., 'io.debezium.connector.mysql.MySqlConnector')
            config_dict: Optional connector configuration (needed for JDBC connectors to detect database type)
            
        Returns:
            The FM plugin name (template_id) or None if not found
        """
        # Special handling for JDBC connectors - determine plugin based on database type
        if connector_class in ['io.confluent.connect.jdbc.JdbcSourceConnector', 'io.confluent.connect.jdbc.JdbcSinkConnector']:
            return self._get_jdbc_plugin_name(connector_class, config_dict)
        
        # For all other connectors, extract template_id from FM template
        return self._get_plugin_name_from_template(connector_class)
    

    def _get_jdbc_plugin_name(self, connector_class: str, config_dict: Dict[str, Any] = None) -> Optional[str]:
        """
        Determine the FM plugin name for JDBC connectors based on database type.
        
        Args:
            connector_class: The JDBC connector class
            config_dict: Connector configuration containing connection.url
            
        Returns:
            The FM plugin name (e.g., 'MySqlSource', 'PostgresSink') or None
        """
        if not config_dict:
            self.logger.warning("Cannot determine JDBC plugin name - no config provided")
            return None
        
        # Get database type from connection URL
        db_type = self._get_database_type(config_dict)
        if not db_type:
            self.logger.warning("Cannot determine JDBC plugin name - could not detect database type from connection URL")
            return None
        
        self.logger.info(f"Detected database type for JDBC connector: {db_type}")
        
        # Map database types to plugin names
        is_source = connector_class == 'io.confluent.connect.jdbc.JdbcSourceConnector'
        
        db_to_plugin_mapping = {
            'mysql': ('MySqlSource', 'MySqlSink'),
            'postgresql': ('PostgresSource', 'PostgresSink'),
            'oracle': ('OracleDatabaseSource', 'OracleDatabaseSink'),
            'sqlserver': ('MicrosoftSqlServerSource', 'MicrosoftSqlServerSink'),
            'snowflake': ('SnowflakeSource', 'SnowflakeSink'),
        }
        
        if db_type in db_to_plugin_mapping:
            source_plugin, sink_plugin = db_to_plugin_mapping[db_type]
            plugin_name = source_plugin if is_source else sink_plugin
            self.logger.info(f"Determined JDBC plugin name: {plugin_name}")
            return plugin_name
        
        self.logger.warning(f"No plugin mapping found for database type: {db_type}")
        return None
    

    def _get_plugin_name_from_template(self, connector_class: str) -> Optional[str]:
        """
        Extract the plugin name (template_id) from an FM template file.
        
        Args:
            connector_class: The connector.class to find
            
        Returns:
            The template_id (plugin name) or None if not found
        """
        if not self.fm_template_dir or not self.fm_template_dir.exists():
            self.logger.warning(f"FM template directory does not exist: {self.fm_template_dir}")
            return None
        
        # Handle Debezium v1 to v2 mapping if needed
        target_connector_class = connector_class
        if self.debezium_version == 'v2' and connector_class in self.debezium_v1_to_v2_mapping:
            target_connector_class = self.debezium_v1_to_v2_mapping[connector_class]
            self.logger.debug(f"Using v2 connector class for template lookup: {target_connector_class}")
        elif self.debezium_version == 'v1' and connector_class in self.debezium_v2_to_v1_mapping:
            target_connector_class = self.debezium_v2_to_v1_mapping[connector_class]
            self.logger.debug(f"Using v1 connector class for template lookup: {target_connector_class}")
        
        # Search through FM templates
        for template_file in self.fm_template_dir.glob('*.json'):
            try:
                with open(template_file, 'r') as f:
                    template_data = json.load(f)
                
                # Check if this template matches the connector class
                found_match = False
                
                # Check direct connector.class
                if template_data.get('connector.class') == target_connector_class:
                    found_match = True
                # Check nested templates structure
                elif 'templates' in template_data:
                    for template in template_data['templates']:
                        if template.get('connector.class') == target_connector_class:
                            found_match = True
                            break
                
                if found_match:
                    # Extract template_id
                    template_id = None
                    if template_data.get('template_id'):
                        template_id = template_data.get('template_id')
                    elif 'templates' in template_data and len(template_data['templates']) > 0:
                        template_id = template_data['templates'][0].get('template_id')
                    
                    if template_id:
                        self.logger.info(f"Found plugin name '{template_id}' for connector class '{connector_class}'")
                        return template_id
                    else:
                        self.logger.warning(f"Template found for {connector_class} but no template_id present")
                        
            except Exception as e:
                self.logger.debug(f"Error reading template {template_file}: {str(e)}")
                continue
        
        self.logger.warning(f"No FM template found for connector class: {connector_class}")
        return None


    def get_SM_template(self, connector_class: str, worker_url: str = None) -> Dict[str, Any]:
        """Get SM template for a connector class using the specified worker URL"""
        if not worker_url:
            self.logger.info(f"No worker URL provided - skipping SM template fetch for {connector_class}")
            return {}

        # Add http:// protocol if not present
        if not worker_url.startswith(('http://', 'https://')):
            worker_url = f"http://{worker_url}"
            self.logger.info(f"Added http:// protocol to worker URL: {worker_url}")

        try:
            url = f"{worker_url}/connector-plugins/{connector_class}/config/validate"
            data = {
                "connector.class": connector_class
            }
            headers = {
                "Content-Type": "application/json"
            }

            self.logger.info(f"Fetching SM template for {connector_class} from {url}")
            self.logger.info(f"Request body: {json.dumps(data, indent=2)}")
            response = requests.put(url, json=data, headers=headers, verify=not self.disable_ssl_verify, auth=self.worker_auth)
            response.raise_for_status()

            template_data = response.json()
            self.logger.info(f"Successfully fetched SM template for {connector_class} from {worker_url}")

            # Log detailed information about the SM template structure
            self.logger.info(f"=== SM Template Analysis for {connector_class} ===")
            self.logger.info(f"Template data type: {type(template_data)}")

            if isinstance(template_data, dict):
                self.logger.info(f"Template keys: {list(template_data.keys())}")

                # Log configs (newer format)
                if 'configs' in template_data:
                    configs = template_data['configs']
                    self.logger.info(f"Number of configs: {len(configs) if isinstance(configs, list) else 'Not a list'}")

                    if isinstance(configs, list) and configs:
                        self.logger.info(f"Sample configs (first 5):")
                        for i, config_def in enumerate(configs[:5]):
                            if isinstance(config_def, dict):
                                name = config_def.get('name', 'Unknown')
                                config_type = config_def.get('type', 'Unknown')
                                required = config_def.get('required', False)
                                default_value = config_def.get('default_value', 'None')
                                self.logger.info(f"  {i+1}. {name} (type: {config_type}, required: {required}, default: {default_value})")

                # Log groups (newer format)
                if 'groups' in template_data:
                    groups = template_data['groups']
                    self.logger.info(f"Number of groups: {len(groups) if isinstance(groups, list) else 'Not a list'}")

                    if isinstance(groups, list):
                        for i, group in enumerate(groups):
                            if isinstance(group, dict):
                                group_name = group.get('name', 'Unknown')
                                group_configs = group.get('configs', [])
                                self.logger.info(f"  Group {i+1}: {group_name} ({len(group_configs)} configs)")

                                # Log sample configs from each group
                                if group_configs:
                                    self.logger.info(f"    Sample configs in {group_name}:")
                                    for j, config_def in enumerate(group_configs[:3]):
                                        if isinstance(config_def, dict):
                                            name = config_def.get('name', 'Unknown')
                                            config_type = config_def.get('type', 'Unknown')
                                            self.logger.info(f"      {j+1}. {name} (type: {config_type})")

                # Log config definitions (older format)
                if 'config' in template_data:
                    config_defs = template_data['config']
                    self.logger.info(f"Number of config definitions: {len(config_defs) if isinstance(config_defs, list) else 'Not a list'}")

                    if isinstance(config_defs, list) and config_defs:
                        self.logger.info(f"Sample config definitions (first 5):")
                        for i, config_def in enumerate(config_defs[:5]):
                            if isinstance(config_def, dict):
                                name = config_def.get('name', 'Unknown')
                                config_type = config_def.get('type', 'Unknown')
                                required = config_def.get('required', False)
                                default_value = config_def.get('default_value', 'None')
                                self.logger.info(f"  {i+1}. {name} (type: {config_type}, required: {required}, default: {default_value})")

                # Log sections (older format)
                if 'sections' in template_data:
                    sections = template_data['sections']
                    self.logger.info(f"Number of sections: {len(sections) if isinstance(sections, list) else 'Not a list'}")

                    if isinstance(sections, list):
                        for i, section in enumerate(sections):
                            if isinstance(section, dict):
                                section_name = section.get('name', 'Unknown')
                                section_configs = section.get('config_defs', [])
                                self.logger.info(f"  Section {i+1}: {section_name} ({len(section_configs)} configs)")

                                # Log sample configs from each section
                                if section_configs:
                                    self.logger.info(f"    Sample configs in {section_name}:")
                                    for j, config_def in enumerate(section_configs[:3]):
                                        if isinstance(config_def, dict):
                                            name = config_def.get('name', 'Unknown')
                                            config_type = config_def.get('type', 'Unknown')
                                            self.logger.info(f"      {j+1}. {name} (type: {config_type})")

                # Log any other important fields
                for key in ['name', 'version', 'type']:
                    if key in template_data:
                        self.logger.info(f"{key.capitalize()}: {template_data[key]}")

            self.logger.info(f"=== End SM Template Analysis for {connector_class} ===")
            return template_data

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to fetch SM template for {connector_class} from {worker_url}: {str(e)}")
            return {}


    def _load_fm_transforms_fallback(self) -> Dict[str, List[str]]:
        """Load combined FM transforms from file as fallback"""
        fallback_file = Path("fm_transforms_list.json")
        if fallback_file.exists():
            try:
                with open(fallback_file, 'r') as f:
                    data = json.load(f)
                self.logger.info(f"Loaded FM transforms fallback with {len(data)} template IDs")
                return data
            except Exception as e:
                self.logger.warning(f"Failed to load FM transforms fallback: {str(e)}")
        else:
            self.logger.warning(f"FM transforms fallback file not found: {fallback_file}")
        return {}


    def _build_connector_class_mapping(self) -> Dict[str, Dict[str, List[str]]]:
        """Build mapping of connector.class to template paths"""
        mapping = {}

        # Map FM templates
        if self.fm_template_dir:
            for template_file in self.fm_template_dir.glob('*_resolved_templates.json'):
                try:
                    with open(template_file, 'r') as f:
                        template_data = json.load(f)
                        if 'connector.class' in template_data:
                            connector_class = template_data['connector.class']
                            if connector_class not in mapping:
                                mapping[connector_class] = {
                                    'fm_templates': []
                                }
                            mapping[connector_class]['fm_templates'].append(str(template_file))
                except Exception as e:
                    self.logger.error(f"Error reading template {template_file}: {str(e)}")

        return mapping


    def _find_fm_template_by_connector_class(self, connector_class: str, connector_name: str = None, config: Dict[str, Any] = None) -> Optional[str]:
        """Find FM template file that contains the specified connector.class"""
        if not self.fm_template_dir or not self.fm_template_dir.exists():
            self.logger.error(f"FM template directory does not exist: {self.fm_template_dir}")
            return None

        matching_templates = []
        template_info = []

        # For JDBC connectors, handle special cases (like Snowflake) in _auto_select_jdbc_template
        # For Debezium connectors, map v1 to v2 if debezium_version=v2
        target_connector_class = connector_class
        if self.debezium_version == 'v2' and connector_class in self.debezium_v1_to_v2_mapping:
            v2_connector_class = self.debezium_v1_to_v2_mapping[connector_class]
            self.logger.info(f"Migrating from v1 to v2: {connector_class} -> {v2_connector_class}")
            target_connector_class = v2_connector_class
        elif self.debezium_version == 'v1' and connector_class in self.debezium_v2_to_v1_mapping:
            v1_connector_class = self.debezium_v2_to_v1_mapping[connector_class]
            self.logger.info(f"Migrating from v2 to v1: {connector_class} -> {v1_connector_class}")
            target_connector_class = v1_connector_class
        # Search through all JSON files in the FM template directory
        for template_file in self.fm_template_dir.glob('*.json'):
            try:
                with open(template_file, 'r') as f:
                    template_data = json.load(f)

                # Check if this template has the matching connector.class
                # Handle both direct connector.class and nested templates structure
                found_connector_class = None

                # Check direct connector.class
                if template_data.get('connector.class') == target_connector_class:
                    found_connector_class = template_data.get('connector.class')
                # Check nested templates structure
                elif 'templates' in template_data:
                    for template in template_data['templates']:
                        if template.get('connector.class') == target_connector_class:
                            found_connector_class = template.get('connector.class')
                            break

                if found_connector_class:
                    template_path = str(template_file)

                    # Extract template_id from the correct location
                    template_id = 'Unknown'
                    if template_data.get('template_id'):
                        template_id = template_data.get('template_id')
                    elif 'templates' in template_data and len(template_data['templates']) > 0:
                        template_id = template_data['templates'][0].get('template_id', 'Unknown')

                    matching_templates.append(template_path)
                    template_info.append({
                        'path': template_path,
                        'template_id': template_id,
                        'filename': template_file.name
                    })
                    self.logger.debug(f"Found matching FM template: {template_file} (template_id: {template_id})")

            except Exception as e:
                self.logger.warning(f"Error reading template {template_file}: {str(e)}")
                continue

        if not matching_templates:
            # If we migrated to v2/v1 but didn't find a template, try the original connector class
            if target_connector_class != connector_class:
                self.logger.warning(f"No FM templates found for migrated connector.class: {target_connector_class}, trying original: {connector_class}")
                # Reset and try with original connector class
                matching_templates = []
                template_info = []
                target_connector_class = connector_class
                # Search again with original connector class
                for template_file in self.fm_template_dir.glob('*.json'):
                    try:
                        with open(template_file, 'r') as f:
                            template_data = json.load(f)
                        found_connector_class = None
                        if template_data.get('connector.class') == target_connector_class:
                            found_connector_class = template_data.get('connector.class')
                        elif 'templates' in template_data:
                            for template in template_data['templates']:
                                if template.get('connector.class') == target_connector_class:
                                    found_connector_class = template.get('connector.class')
                                    break
                        if found_connector_class:
                            template_path = str(template_file)
                            template_id = 'Unknown'
                            if template_data.get('template_id'):
                                template_id = template_data.get('template_id')
                            elif 'templates' in template_data and len(template_data['templates']) > 0:
                                template_id = template_data['templates'][0].get('template_id', 'Unknown')
                            matching_templates.append(template_path)
                            template_info.append({
                                'path': template_path,
                                'template_id': template_id,
                                'filename': template_file.name
                            })
                    except Exception as e:
                        self.logger.warning(f"Error reading template {template_file}: {str(e)}")
                        continue
            
            if not matching_templates:
                self.logger.warning(f"No FM templates found for connector.class: {connector_class}")
                return None

        if len(matching_templates) == 1:
            # Only one template found, use it
            self.logger.info(f"Using single FM template: {matching_templates[0]}")
            return matching_templates[0]
        else:
            # Multiple templates found - filter CDC templates by version first
            filtered_templates = self._filter_cdc_templates_by_version(connector_class, template_info)
            
            if len(filtered_templates) == 1:
                # Only one template after filtering, use it
                self.logger.info(f"Using filtered CDC template (debezium_version={self.debezium_version}): {filtered_templates[0]['path']}")
                return filtered_templates[0]['path']
            elif len(filtered_templates) < len(template_info):
                # Some templates were filtered out, continue with filtered list
                template_info = filtered_templates
                self.logger.info(f"Filtered to {len(filtered_templates)} CDC templates based on debezium_version={self.debezium_version}")
            
            # For JDBC connectors, auto-select based on connection URL
            if connector_class in ['io.confluent.connect.jdbc.JdbcSourceConnector', 'io.confluent.connect.jdbc.JdbcSinkConnector'] and config:
                return self._auto_select_jdbc_template(connector_class, template_info, config, connector_name)
            else:
                # For non-JDBC connectors, ask user to pick
                return self._get_user_template_selection(connector_class, template_info, connector_name)


    def _auto_select_jdbc_template(self, connector_class: str, template_info: List[Dict[str, str]], config: Dict[str, Any], connector_name: str = None) -> Optional[str]:
        """Automatically select JDBC template based on connection URL"""
        connector_display = f"{connector_name} ({connector_class})" if connector_name else connector_class

        # Get database type from connection URL
        db_type = self._get_database_type(config)
        self.logger.info(f"Detected database type: {db_type} for connector: {connector_display}")

        # Special handling for Snowflake database - search for Snowflake-specific templates
        if db_type == 'snowflake':
            self.logger.info(f"Detected Snowflake database for {connector_class}, looking for Snowflake-specific templates")
            # For Snowflake, look for Snowflake-specific templates instead of generic JDBC templates
            if connector_class == 'io.confluent.connect.jdbc.JdbcSourceConnector':
                target_connector_class = 'io.confluent.connect.snowflake.jdbc.SnowflakeSourceConnector'
            else:  # JdbcSinkConnector
                target_connector_class = 'io.confluent.connect.snowflake.jdbc.SnowflakeSinkConnector'
            self.logger.info(f"Looking for Snowflake template with connector class: {target_connector_class}")

            # Search for Snowflake-specific templates
            snowflake_template_info = []
            for template_file in self.fm_template_dir.glob('*.json'):
                try:
                    with open(template_file, 'r') as f:
                        template_data = json.load(f)

                    found_connector_class = None
                    if template_data.get('connector.class') == target_connector_class:
                        found_connector_class = template_data.get('connector.class')
                    elif 'templates' in template_data:
                        for template in template_data['templates']:
                            if template.get('connector.class') == target_connector_class:
                                found_connector_class = template.get('connector.class')
                                break

                    if found_connector_class:
                        template_path = str(template_file)
                        template_id = 'Unknown'
                        if template_data.get('template_id'):
                            template_id = template_data.get('template_id')
                        elif 'templates' in template_data and len(template_data['templates']) > 0:
                            template_id = template_data['templates'][0].get('template_id', 'Unknown')

                        snowflake_template_info.append({
                            'path': template_path,
                            'template_id': template_id,
                            'filename': template_file.name
                        })
                        self.logger.info(f"Found Snowflake template: {template_id} (File: {template_file.name})")

                except Exception as e:
                    self.logger.warning(f"Error reading template {template_file}: {str(e)}")
                    continue

            if snowflake_template_info:
                # Use the first Snowflake template found
                selected_template = snowflake_template_info[0]
                self.logger.info(f"Auto-selected Snowflake template for {connector_display}: {selected_template['template_id']} (File: {selected_template['filename']})")
                return selected_template['path']


        # Map database types to template names
        db_to_template_mapping = {
            'mysql': ['MySqlSource', 'MySqlSink'],
            'postgresql': ['PostgresSource', 'PostgresSink'],
            'oracle': ['OracleDatabaseSource', 'OracleDatabaseSink'],
            'sqlserver': ['MicrosoftSqlServerSource', 'MicrosoftSqlServerSink'],
            'snowflake': ['SnowflakeSource']
        }

        # Get expected template names for this database type
        expected_templates = db_to_template_mapping.get(db_type, [])

        self.logger.info(f"Expected templates for {db_type}: {expected_templates}")
        self.logger.info(f"Available templates: {[t['template_id'] for t in template_info]}")
        self.logger.info(f"Connector class: {connector_class}")
        # Find matching template
        for template in template_info:
            template_id = template['template_id']
            self.logger.info(f"Checking template: {template_id}")
            if template_id in expected_templates:
                self.logger.info(f"Template {template_id} is in expected templates")
                # For JDBC connectors, prefer the correct Source/Sink template
                if connector_class == 'io.confluent.connect.jdbc.JdbcSourceConnector' and 'Source' in template_id:
                    selected_path = template['path']
                    self.logger.info(f"Auto-selected JDBC Source template for {connector_display}: {template_id} (File: {template['filename']})")
                    return selected_path
                elif connector_class == 'io.confluent.connect.jdbc.JdbcSinkConnector' and 'Sink' in template_id:
                    selected_path = template['path']
                    self.logger.info(f"Auto-selected JDBC Sink template for {connector_display}: {template_id} (File: {template['filename']})")
                    return selected_path
                elif connector_class == 'io.confluent.connect.jdbc.JdbcSourceConnector' and 'Sink' not in template_id:
                    # Fallback for source connector if no specific Source template found
                    selected_path = template['path']
                    self.logger.info(f"Auto-selected JDBC template (fallback) for {connector_display}: {template_id} (File: {template['filename']})")
                    return selected_path
                elif connector_class == 'io.confluent.connect.jdbc.JdbcSinkConnector' and 'Source' not in template_id:
                    # Fallback for sink connector if no specific Sink template found
                    selected_path = template['path']
                    self.logger.info(f"Auto-selected JDBC template (fallback) for {connector_display}: {template_id} (File: {template['filename']})")
                    return selected_path
            else:
                self.logger.info(f"Template {template_id} is NOT in expected templates")

        # If no exact match found, try partial matching
        for template in template_info:
            template_id = template['template_id'].lower()
            if db_type in template_id:
                selected_path = template['path']
                self.logger.info(f"Auto-selected JDBC template (partial match) for {connector_display}: {template['template_id']} (File: {template['filename']})")
                return selected_path

        # If still no match, fall back to user selection
        self.logger.warning(f"Could not auto-select JDBC template for {connector_display} with database type {db_type}")
        return self._get_user_template_selection(connector_class, template_info, connector_name)


    def _filter_cdc_templates_by_version(self, connector_class: str, template_info: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """Filter CDC templates based on debezium_version parameter"""
        # Check if this is a Debezium CDC connector
        is_debezium_connector = (
            'debezium' in connector_class.lower() or
            connector_class.startswith('io.debezium.connector')
        )
        
        if not is_debezium_connector:
            # Not a Debezium connector, return all templates unchanged
            return template_info
        
        # Check if we have both v1 and v2 templates
        v2_templates = []
        v1_templates = []
        
        for template in template_info:
            template_id = template.get('template_id', '').lower()
            filename = template.get('filename', '').lower()
            
            # Check if this is a v2 template (has V2 in template_id or filename)
            is_v2 = 'v2' in template_id or 'v2' in filename
            
            # Also check the actual template file for connector.class
            try:
                with open(template['path'], 'r') as f:
                    template_data = json.load(f)
                    connector_class_in_template = None
                    if template_data.get('connector.class'):
                        connector_class_in_template = template_data.get('connector.class')
                    elif 'templates' in template_data and len(template_data['templates']) > 0:
                        connector_class_in_template = template_data['templates'][0].get('connector.class')
                    
                    if connector_class_in_template:
                        if '.v2.' in connector_class_in_template or 'ConnectorV2' in connector_class_in_template:
                            is_v2 = True
                        elif '.v2.' not in connector_class_in_template and 'ConnectorV2' not in connector_class_in_template:
                            # Explicitly v1 if it doesn't have v2 indicators
                            is_v2 = False
            except Exception as e:
                self.logger.debug(f"Could not read template file {template['path']}: {e}")
            
            if is_v2:
                v2_templates.append(template)
            else:
                v1_templates.append(template)
        
        # Filter based on debezium_version
        if self.debezium_version == 'v2':
            if v2_templates:
                self.logger.info(f"Filtering to {len(v2_templates)} v2 CDC templates (debezium_version=v2)")
                return v2_templates
            else:
                self.logger.info(f"No v2 templates found, using {len(v1_templates)} v1 templates")
                return v1_templates
        else:  # v1
            if v1_templates:
                self.logger.info(f"Filtering to {len(v1_templates)} v1 CDC templates (debezium_version=v1)")
                return v1_templates
            else:
                self.logger.info(f"No v1 templates found, using {len(v2_templates)} v2 templates")
                return v2_templates


    def _get_user_template_selection(self, connector_class: str, template_info: List[Dict[str, str]], connector_name: str = None) -> Optional[str]:
        """Ask user to select a template when multiple options are available"""
        connector_display = f"{connector_name} ({connector_class})" if connector_name else connector_class
        print(f"\nMultiple FM templates found for connector: {connector_display}")
        print("Available templates:")
        for i, info in enumerate(template_info, 1):
            template_id = info['template_id']
            filename = info['filename']
            print(f"{i}. Template ID: {template_id} (File: {filename})")

        while True:
            try:
                choice = int(input(f"\nPlease select an FM template for '{connector_display}' (1-{len(template_info)}): "))
                if 1 <= choice <= len(template_info):
                    selected_template = template_info[choice - 1]
                    selected_path = selected_template['path']
                    self.logger.info(f"User selected FM template for {connector_display}: {selected_path}")
                    return selected_path
                else:
                    print(f"Please enter a number between 1 and {len(template_info)}")
            except ValueError:
                print("Please enter a valid number")


    def _get_templates_for_connector(self, connector_class: str, connector_name: str = None, config: Dict[str, Any] = None) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Get SM and FM templates for a connector class"""
        # Get worker URL from connector config if available
        worker_url = None
        if config and 'worker' in config:
            worker_url = config['worker']
            self.logger.info(f"Using worker URL from connector config: {worker_url}")
        elif self.worker_urls and len(self.worker_urls) > 0:
            worker_url = self.worker_urls[0]
            self.logger.info(f"Using first worker URL from global list: {worker_url}")

        # Fetch SM template via HTTP (if worker URL available)
        if worker_url:
            self.logger.info(f"Fetching SM template for {connector_class} via HTTP from {worker_url}...")
            sm_template = self.get_SM_template(connector_class, worker_url)
        else:
            self.logger.info(f"No worker URL available - skipping SM template fetch for {connector_class}")
            sm_template = {}

        # Handle FM templates - find by connector.class
        fm_template_path = self._find_fm_template_by_connector_class(connector_class, connector_name, config)

        # Special mapping for SFTP connectors
        if not fm_template_path and connector_class == 'io.confluent.connect.sftp.SftpCsvSourceConnector':
            # Map to SftpSource template
            sftp_template_path = self.fm_template_dir / 'SftpSource_resolved_templates.json'
            if sftp_template_path.exists():
                fm_template_path = str(sftp_template_path)
                self.logger.info(f"Mapped SFTP connector to SftpSource template: {fm_template_path}")
            else:
                self.logger.warning(f"SftpSource template not found at expected path: {sftp_template_path}")

        if fm_template_path:
            try:
                with open(fm_template_path, 'r') as f:
                    self.fm_templates[fm_template_path] = json.load(f)
                self.logger.info(f"Loaded FM template by connector.class: {fm_template_path}")
                self.logger.info(f"Template file path: {fm_template_path}")
                # Log the template_id from the loaded template
                loaded_template = self.fm_templates[fm_template_path]
                if 'templates' in loaded_template and len(loaded_template['templates']) > 0:
                    template_id = loaded_template['templates'][0].get('template_id', 'NO_TEMPLATE_ID')
                    self.logger.info(f"Template ID from loaded template: {template_id}")
            except Exception as e:
                self.logger.error(f"Error loading FM template {fm_template_path}: {str(e)}")
                fm_template_path = None
        else:
            self.logger.error(f"No FM template found for connector.class: {connector_class}")
            fm_template_path = None

        # If FM template is missing, return empty templates
        if not fm_template_path:
            self.logger.error(f"Missing required FM template for {connector_class}")
            return sm_template, None  # Return None to indicate missing template

        # Log template selection
        self.logger.info(f"Selected templates for {connector_class}:")
        if worker_url:
            self.logger.info(f"  SM Template: Fetched via HTTP from {worker_url}")
        else:
            self.logger.info(f"  SM Template: Not available (no worker URL)")
        self.logger.info(f"  FM Template: {fm_template_path}")

        # Return templates
        return sm_template, self.fm_templates.get(fm_template_path, {})


    def _load_templates(self, template_dir: Path) -> Dict[str, Dict[str, Any]]:
        """Load all JSON template files from a directory"""
        templates = {}
        if template_dir.exists():
            for template_file in template_dir.glob('*.json'):
                try:
                    with open(template_file, 'r') as f:
                        templates[template_file.stem] = json.load(f)
                    self.logger.info(f"Loaded template: {template_file.name}")
                except Exception as e:
                    self.logger.error(f"Error loading template {template_file}: {str(e)}")
        return templates

