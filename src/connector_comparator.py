"""
Apache Connect Migration Utility
Copyright 2024-2025 The Apache Software Foundation

This product includes software developed at The Apache Software Foundation.
"""

import json
import logging
import os

from pathlib import Path
from typing import Dict, Any, List, Optional, Set
import re
from semantic_matcher import SemanticMatcher, Property
import base64
import requests


class ConnectorComparator:
    SUCCESSFUL_CONFIGS_DIR = "successful_configs"
    UNSUCCESSFUL_CONFIGS_DIR = "unsuccessful_configs_with_errors"

    def __init__(self, input_file: Path, output_dir: Path, worker_urls: List[str] = None,
                 env_id: str = None, lkc_id: str = None, bearer_token: str = None, disable_ssl_verify: bool = False):
        self.logger = logging.getLogger(__name__)
        self.input_file = input_file
        self.output_dir = output_dir
        # Use local model
        self.semantic_matcher = SemanticMatcher()



        # Worker URLs for fetching SM templates
        self.worker_urls = worker_urls or []

        # Confluent Cloud credentials for FM transforms (optional)
        self.env_id = env_id
        self.lkc_id = lkc_id
        self.bearer_token = bearer_token
        self.disable_ssl_verify = disable_ssl_verify

        # Log SSL verification status
        if self.disable_ssl_verify:
            self.logger.info("SSL certificate verification is DISABLED")
        else:
            self.logger.info("SSL certificate verification is ENABLED")

        # Log credential status (without exposing sensitive data)
        if env_id and lkc_id and bearer_token:
            self.logger.info(f"Confluent Cloud credentials provided: env_id={env_id}, lkc_id={lkc_id}, bearer_token=[HIDDEN]")
        elif env_id or lkc_id or bearer_token:
            self.logger.warning("Partial Confluent Cloud credentials provided - HTTP calls for FM transforms will be skipped")
        else:
            self.logger.info("No Confluent Cloud credentials provided - will use fallback FM transforms only")



        # Load template files - hardcoded FM template directory
        self.fm_template_dir = Path("templates/fm")
        self.fm_templates = self._load_templates(self.fm_template_dir) if self.fm_template_dir.exists() else {}

        # Build connector.class to template mapping
        self.connector_class_to_template = self._build_connector_class_mapping()

        # Load combined FM transforms as fallback
        self.fm_transforms_fallback = self._load_fm_transforms_fallback()

        # Database type mappings
        self.jdbc_database_types = {
            'mysql': {
                'url_patterns': ['mysql', 'mariadb'],
                'default_port': '3306',
            },
            'oracle': {
                'url_patterns': ['oracle', 'oracle:thin'],
                'default_port': '1521',
            },
            'sqlserver': {
                'url_patterns': ['sqlserver', 'mssql'],
                'default_port': '1433',
            },
            'postgresql': {
                'url_patterns': ['postgresql', 'postgres'],
                'default_port': '5432',
            },
            'snowflake': {
                'url_patterns': ['snowflake'],
                'default_port': '443',
            }
        }

        # Static property mappings to prevent incorrect semantic matching
        # These will be determined dynamically based on connector type (source vs sink)
        self.static_property_mappings_source = {
            'key.converter': 'output.key.format',
            'value.converter': 'output.data.format'
        }

        self.static_property_mappings_sink = {
            'key.converter': 'input.key.format',
            'value.converter': 'input.data.format'
        }

        # Converter class to format reverse mappings
        self.converter_to_format_mappings = {
            'io.confluent.connect.avro.AvroConverter': 'AVRO',
            'io.confluent.connect.json.JsonSchemaConverter': 'JSON_SR',
            'io.confluent.connect.protobuf.ProtobufConverter': 'PROTOBUF',
            'org.apache.kafka.connect.json.JsonConverter': 'JSON'
        }

    def encode_to_base64(self, input_string):
        # Convert the input string to bytes
        byte_data = input_string.encode('utf-8')
        # Encode the bytes to base64
        base64_bytes = base64.b64encode(byte_data)
        # Convert the base64 bytes back to string
        base64_string = base64_bytes.decode('utf-8')
        return base64_string

    def extract_transforms_config(config_dict):
        # Extract all keys starting with "transforms"
        return {k: v for k, v in config_dict.items() if isinstance(k, str) and k.startswith("transforms")}

    def extract_recommended_transform_types(self, response_json):
        configs = response_json.get("configs", [])
        for config in configs:
            value = config.get("value", {})
            if value.get("name") == "transforms.transform_0.type":
                return value.get("recommended_values", [])
        return []

    def extract_subject_name_strategy(self, config_key: str, config_value: str) -> Optional[str]:
        """
        Extract recommended subject name strategy value from config value.
        
        For configs ending with 'subject.name.strategy', checks if the config value
        contains any of the recommended values and returns the recommended value if found.
        
        Args:
            config_key: The configuration key
            config_value: The configuration value
            
        Returns:
            The recommended strategy value if found in the config value, None otherwise
        """
        if not config_key.endswith("subject.name.strategy"):
            return None
            
        # Common recommended values for subject name strategy
        recommended_strategies = [
            "TopicNameStrategy",
            "RecordNameStrategy", 
            "TopicRecordNameStrategy"
        ]
        
        # Check if any recommended strategy is contained in the config value
        for strategy in recommended_strategies:
            if strategy in config_value:
                return strategy
                
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
            response = requests.put(url, json=data, headers=headers, verify=not self.disable_ssl_verify)
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

    def get_FM_SMT(self, plugin_type) -> set[str]:
        # First try to get transforms via HTTP call if credentials are provided
        if self.env_id and self.lkc_id and self.bearer_token:
            try:
                url = (
                    f"https://confluent.cloud/api/internal/accounts/{self.env_id}/clusters/{self.lkc_id}/connector-plugins/{plugin_type}/config/validate"
                )
                params = {
                    "extra_fields": "configs/metadata,configs/internal"
                }
                data = {
                    "transforms": "transform_0",
                    "connector.class": plugin_type
                }
                headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Basic {self.encode_to_base64(self.bearer_token)}"
                }
                response = requests.put(url, params=params, json=data, headers=headers)
                response.raise_for_status()
                recommended_values = self.extract_recommended_transform_types(response.json())
                if recommended_values:
                    self.logger.info(f"Successfully fetched {len(recommended_values)} transforms for {plugin_type} via HTTP")
                    return recommended_values
            except Exception as e:
                self.logger.warning(f"Failed to fetch FM transforms for {plugin_type} via HTTP: {str(e)}")
        else:
            self.logger.info(f"Skipping HTTP call for {plugin_type} - no Confluent Cloud credentials provided")

        # Fallback to local file
        if plugin_type in self.fm_transforms_fallback:
            transforms = self.fm_transforms_fallback[plugin_type]
            self.logger.info(f"Using fallback transforms for {plugin_type}: {len(transforms)} transforms")
            return set(transforms)

        self.logger.warning(f"No transforms found for {plugin_type} in HTTP call or fallback file")
        return set()

    def get_transforms_config(self, config: Dict[str, Any], plugin_type: str) -> Dict[str, Dict[str, Any]]:

        fm_smt = self.get_FM_SMT(plugin_type)

        return self.classify_transform_configs_with_full_chain(config, fm_smt)

    def classify_transform_configs_with_full_chain(
        self,
        config: Dict[str, Any],
        allowed_transform_types: set
    ) -> Dict[str, Dict[str, Any]]:
        result: Dict[str, Dict[str, Any]] = {
            'allowed': {},
            'disallowed': {},
            'mapping_errors': []
        }

        transform_chain = config.get("transforms", "")
        aliases = [alias.strip() for alias in transform_chain.split(",") if alias.strip()]

        allowed_aliases = []
        disallowed_aliases = []

        # Track which predicates are associated with disallowed transforms
        disallowed_predicates = set()

        for alias in aliases:
            type_key = f"transforms.{alias}.type"
            transform_type = config.get(type_key)

            if not transform_type:
                disallowed_aliases.append(alias)
                error_msg = f"Transform '{alias}' has no type specified"
                result['mapping_errors'].append(error_msg)
                self.logger.warning(error_msg)
                for k, v in config.items():
                    if isinstance(k, str) and k.startswith(f"transforms.{alias}."):
                        result['disallowed'][k] = v
                        # Check if this transform references a predicate
                        if k == f"transforms.{alias}.predicate":
                            disallowed_predicates.add(v)
                continue

            if transform_type in allowed_transform_types:
                allowed_aliases.append(alias)
                for k, v in config.items():
                    if isinstance(k, str) and k.startswith(f"transforms.{alias}."):
                        result['allowed'][k] = v
            else:
                disallowed_aliases.append(alias)
                error_msg = f"Transform '{alias}' of type '{transform_type}' is not supported in Fully Managed Connector. Potentially Custom SMT can be used."
                result['mapping_errors'].append(error_msg)
                self.logger.warning(error_msg)
                for k, v in config.items():
                    if isinstance(k, str) and k.startswith(f"transforms.{alias}."):
                        result['disallowed'][k] = v
                        # Check if this transform references a predicate
                        if k == f"transforms.{alias}.predicate":
                            disallowed_predicates.add(v)
                            self.logger.info(f"Transform {alias} of type {transform_type} is not supported, so its predicate {v} will also be filtered out")

        # Handle predicates
        predicates_chain = config.get("predicates", "")
        predicate_aliases = [alias.strip() for alias in predicates_chain.split(",") if alias.strip()]

        allowed_predicate_aliases = []
        disallowed_predicate_aliases = []

        for predicate_alias in predicate_aliases:
            # Check if this predicate is associated with a disallowed transform
            if predicate_alias in disallowed_predicates:
                disallowed_predicate_aliases.append(predicate_alias)
                predicate_error_msg = f"Predicate '{predicate_alias}' is filtered out because it's associated with an unsupported transform."
                result['mapping_errors'].append(predicate_error_msg)
                for k, v in config.items():
                    if isinstance(k, str) and k.startswith(f"predicates.{predicate_alias}."):
                        result['disallowed'][k] = v
                self.logger.info(f"Predicate {predicate_alias} is associated with a disallowed transform, so it will be filtered out")
            else:
                # This predicate is not associated with any disallowed transform, so it's allowed
                allowed_predicate_aliases.append(predicate_alias)
                for k, v in config.items():
                    if isinstance(k, str) and k.startswith(f"predicates.{predicate_alias}."):
                        result['allowed'][k] = v

        if allowed_aliases:
            result['allowed']["transforms"] = ", ".join(allowed_aliases)
        if disallowed_aliases:
            result['disallowed']["transforms"] = ", ".join(disallowed_aliases)

        if allowed_predicate_aliases:
            result['allowed']["predicates"] = ", ".join(allowed_predicate_aliases)
        if disallowed_predicate_aliases:
            result['disallowed']["predicates"] = ", ".join(disallowed_predicate_aliases)

        return result

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
        target_connector_class = connector_class
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
            self.logger.warning(f"No FM templates found for connector.class: {connector_class}")
            return None

        if len(matching_templates) == 1:
            # Only one template found, use it
            self.logger.info(f"Using single FM template: {matching_templates[0]}")
            return matching_templates[0]
        else:
            # Multiple templates found - for JDBC connectors, auto-select based on connection URL
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

    def _get_templates_for_connector(self, connector_class: str, connector_name: str = None, config: Dict[str, Any] = None) -> tuple[Dict[str, Any], Dict[str, Any]]:
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

    def _get_database_type(self, config: Dict[str, Any]) -> str:
        """Determine database type from JDBC connector config"""
        # Check connection URL
        if 'connection.url' in config and isinstance(config['connection.url'], str):
            url = config['connection.url'].lower()
            self.logger.info(f"Analyzing JDBC URL for database type: {url}")
            
            # More precise pattern matching - look for jdbc:database_type:// pattern
            for db_type, info in self.jdbc_database_types.items():
                for pattern in info['url_patterns']:
                    # Look for the pattern in the JDBC protocol part specifically
                    if f'jdbc:{pattern}://' in url:
                        self.logger.info(f"Detected database type '{db_type}' using precise pattern 'jdbc:{pattern}://'")
                        return db_type
            
            # Fallback to the old method for backward compatibility
            for db_type, info in self.jdbc_database_types.items():
                if any(pattern in url for pattern in info['url_patterns']):
                    self.logger.info(f"Detected database type '{db_type}' using fallback pattern matching")
                    return db_type
            
            self.logger.warning(f"No database type detected for URL: {url}")
                    
        # Check specific database type config if available
        if 'database.type' in config:
            db_type = config['database.type'].lower()
            self.logger.info(f"Using database type from config: {db_type}")
            return db_type
            
        self.logger.warning("No database type detected, returning 'unknown'")
        return 'unknown'

    def _parse_jdbc_url(self, url: str) -> Dict[str, str]:
        """Parse JDBC URL to extract connection details from real JDBC URLs, including Oracle complex formats."""

        original_url = url
        url = url.lower()
        connection_info = {}

        self.logger.debug(f"Parsing JDBC URL: {original_url}")

        # Detect Oracle complex format by presence of '@(DESCRIPTION='
        # "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=<span>{connection.host})(PORT=</span>{connection.port}))(CONNECT_DATA=(<span>{db.connection.type}=</span>{db.name}))(SECURITY=(SSL_SERVER_CERT_DN="${ssl.server.cert.dn}")))"
        if '@(DESCRIPTION=' in original_url:
            self.logger.debug("Detected Oracle complex format with DESCRIPTION block (uppercase keywords only).")
            # Extract HOST
            host_match = re.search(r'\(HOST=([^\)]+)\)', original_url)
            if host_match:
                connection_info['host'] = host_match.group(1)
                self.logger.debug(f"[ORACLE] Extracted host: {connection_info['host']}")
            # Extract PORT
            port_match = re.search(r'\(PORT=([^\)]+)\)', original_url)
            if port_match:
                connection_info['port'] = port_match.group(1)
                self.logger.debug(f"[ORACLE] Extracted port: {connection_info['port']}")
            # Extract db.connection.type and db.name (fix group assignments)
            dbtype_dbname_match = re.search(r'\(CONNECT_DATA=\(([^=\)]+)=([^\)]+)\)\)', original_url)
            if dbtype_dbname_match:
                connection_info['db.connection.type'] = dbtype_dbname_match.group(1)
                connection_info['db.name'] = dbtype_dbname_match.group(2)
                self.logger.debug(f"[ORACLE] Extracted db.connection.type: {connection_info['db.connection.type']}")
                self.logger.debug(f"[ORACLE] Extracted db.name: {connection_info['db.name']}")
            # Extract ssl.server.cert.dn
            ssl_cert_dn_match = re.search(r'SSL_SERVER_CERT_DN=\\?"?([^\)\"]+)\\?"?\)', original_url)
            if ssl_cert_dn_match:
                connection_info['ssl.server.cert.dn'] = ssl_cert_dn_match.group(1)
                self.logger.debug(f"[ORACLE] Extracted ssl.server.cert.dn: {connection_info['ssl.server.cert.dn']}")
            self.logger.debug(f"[ORACLE] Final connection_info: {connection_info}")
            return connection_info
        else:
            self.logger.debug("Using standard JDBC URL parsing logic.")

        # Existing logic for standard format
        # Extract host - look for pattern like jdbc:postgresql://localhost:5432/dbname
        host_match = re.search(r'jdbc:[^:]+://([^:/]+)', url)
        if host_match:
            host = host_match.group(1)
            connection_info['host'] = host
            self.logger.debug(f"Extracted host: {connection_info['host']}")

        # Extract port - look for pattern like :5432/
        port_match = re.search(r'://[^:]+:(\d+)', url)
        if port_match:
            port = port_match.group(1)
            connection_info['port'] = port
            self.logger.debug(f"Extracted port: {connection_info['port']}")

        # Extract database name - look for pattern like /dbname? or /dbname
        db_match = re.search(r'://[^/]+/([^/?]+)', url)
        if db_match:
            db_name = db_match.group(1)
            connection_info['db.name'] = db_name
            self.logger.debug(f"Extracted db_name: {connection_info['db.name']}")

        # Extract user from query parameters
        user_match = re.search(r'[?&]user=([^&]+)', url)
        if user_match:
            user = user_match.group(1)
            connection_info['user'] = user
            self.logger.debug(f"Extracted user: {connection_info['user']}")

        # Extract password from query parameters
        password_match = re.search(r'[?&]password=([^&]+)', url)
        if password_match:
            password = password_match.group(1)
            connection_info['password'] = password
            self.logger.debug(f"Extracted password: {connection_info['password']}")

        self.logger.debug(f"Final connection_info: {connection_info}")
        return connection_info

    def _parse_mongodb_connection_string(self, url: str) -> Dict[str, str]:
        """Parse MongoDB connection string to extract connection details"""
        original_url = url
        url = url.lower()
        connection_info = {}

        self.logger.debug(f"Parsing MongoDB connection string: {original_url}")

        # Handle MongoDB Atlas connection strings (mongodb+srv://)
        # Format: mongodb+srv://username:password@cluster.mongodb.net/database?options
        if 'mongodb+srv://' in url:
            # Extract the part after mongodb+srv://
            srv_part = url.replace('mongodb+srv://', '')

            # Split by @ to separate credentials from host
            if '@' in srv_part:
                credentials_part, host_part = srv_part.split('@', 1)

                # Extract username and password from credentials
                if ':' in credentials_part:
                    user, password = credentials_part.split(':', 1)
                    connection_info['user'] = user
                    connection_info['password'] = password
                    self.logger.debug(f"Extracted user: {connection_info['user']}")
                    self.logger.debug(f"Extracted password: {connection_info['password']}")

                # Extract host from host part
                host = host_part.split('/')[0].split('?')[0]
                connection_info['host'] = host
                self.logger.debug(f"Extracted host: {connection_info['host']}")

                # Extract database if present
                if '/' in host_part:
                    db_part = host_part.split('/', 1)[1]
                    if '?' in db_part:
                        db_name = db_part.split('?')[0]
                    else:
                        db_name = db_part
                    connection_info['database'] = db_name
                    self.logger.debug(f"Extracted database: {connection_info['database']}")

        # Handle regular MongoDB connection strings (mongodb://)
        elif 'mongodb://' in url:
            # Extract the part after mongodb://
            mongo_part = url.replace('mongodb://', '')

            # Split by @ to separate credentials from host
            if '@' in mongo_part:
                credentials_part, host_part = mongo_part.split('@', 1)

                # Extract username and password from credentials
                if ':' in credentials_part:
                    user, password = credentials_part.split(':', 1)
                    connection_info['user'] = user
                    connection_info['password'] = password
                    self.logger.debug(f"Extracted user: {connection_info['user']}")
                    self.logger.debug(f"Extracted password: {connection_info['password']}")

                # Extract host from host part
                host = host_part.split('/')[0].split('?')[0]
                connection_info['host'] = host
                self.logger.debug(f"Extracted host: {connection_info['host']}")

                # Extract database if present
                if '/' in host_part:
                    db_part = host_part.split('/', 1)[1]
                    if '?' in db_part:
                        db_name = db_part.split('?')[0]
                    else:
                        db_name = db_part
                    connection_info['database'] = db_name
                    self.logger.debug(f"Extracted database: {connection_info['database']}")
            else:
                # No credentials, just host
                host = mongo_part.split('/')[0].split('?')[0]
                connection_info['host'] = host
                self.logger.debug(f"Extracted host: {connection_info['host']}")

                # Extract database if present
                if '/' in mongo_part:
                    db_part = mongo_part.split('/', 1)[1]
                    if '?' in db_part:
                        db_name = db_part.split('?')[0]
                    else:
                        db_name = db_part
                    connection_info['database'] = db_name
                    self.logger.debug(f"Extracted database: {connection_info['database']}")

        self.logger.debug(f"Final MongoDB connection_info: {connection_info}")
        return connection_info

    def _map_jdbc_properties(self, config: Dict[str, Any], db_type: str) -> Dict[str, Any]:
        """Map JDBC properties to database-specific properties"""
        self.logger.debug(f"Mapping JDBC properties for config: {config}")

        # Get database-specific property mappings
        db_info = self.jdbc_database_types.get(db_type, {})
        property_mappings = db_info.get('property_mappings', {})
        self.logger.debug(f"Property mappings for {db_type}: {property_mappings}")

        # Parse JDBC URL and map properties
        if 'connection.url' in config and isinstance(config['connection.url'], str) and config['connection.url'].startswith('jdbc:'):
            connection_info = self._parse_jdbc_url(config['connection.url'])

            # Map connection details to database-specific properties
            mapped_config = {}
            for fm_prop, jdbc_prop in property_mappings.items():
                if jdbc_prop in connection_info:
                    mapped_config[fm_prop] = connection_info[jdbc_prop]
                    self.logger.debug(f"Mapped {jdbc_prop} ({connection_info[jdbc_prop]}) to {fm_prop}")
                else:
                    self.logger.debug(f"JDBC property {jdbc_prop} not found in connection_info")

            self.logger.debug(f"Final JDBC mapped config: {mapped_config}")
            return mapped_config

        return {}

    def _get_required_properties(self, fm_template: Dict[str, Any]) -> Dict[str, Any]:
        """Extract required properties from FM template"""
        required_props = {}

        # Look for properties in config_defs within templates
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
            # Fallback to connector class name if no FM template
            return True

        # Check for connector_type in the main template
        if fm_template.get('connector_type'):
            return fm_template['connector_type'] == 'SOURCE'

        # Check for connector_type in templates array
        if 'templates' in fm_template:
            for template in fm_template['templates']:
                if template.get('connector_type'):
                    return template['connector_type'] == 'SOURCE'

        # Fallback to connector class name if no connector_type found
        connector_class = fm_template.get('connector.class', '')
        if not connector_class and 'templates' in fm_template and len(fm_template['templates']) > 0:
            connector_class = fm_template['templates'][0].get('connector.class', '')

        source_indicators = ['Source', 'CDC', 'XStream']
        sink_indicators = ['Sink']

        # Check for source indicators
        for indicator in source_indicators:
            if indicator in connector_class:
                return True

        # Check for sink indicators
        for indicator in sink_indicators:
            if indicator in connector_class:
                return False

        # Default to source if no clear indicator (this is a fallback)
        return True

    def _create_direct_mappings_from_template(self, fm_template: Dict[str, Any]) -> Dict[str, str]:
        """Create direct property mappings from FM template connector_configs section"""
        mappings = {}

        if not fm_template or 'templates' not in fm_template:
            return mappings

        # Look through all templates for connector_configs
        for template in fm_template['templates']:
            if 'connector_configs' in template:
                for config in template['connector_configs']:
                    # If the config has a 'value' field, it's a direct mapping
                    if 'value' in config:
                        value = config['value']
                        sm_property_template = config['name']

                        # Handle template variables like ${cleanup.policy}
                        if isinstance(value, str) and value.startswith('${') and value.endswith('}'):
                            # Extract the property name from the template variable
                            fm_property_name = value[2:-1]  # Remove ${ and }
                            # SM property (sm_property_template) maps to FM property (fm_property_name)
                            mappings[sm_property_template] = fm_property_name
                        else:
                            # Direct value mapping
                            mappings[value] = sm_property_template
                    # If the config has a 'switch' field, handle switch mappings
                    elif 'switch' in config:
                        sm_property_template = config['name']
                        switch_config = config['switch']

                        # Extract template variables from switch values
                        for switch_key, switch_values in switch_config.items():
                            if isinstance(switch_values, dict):
                                for condition, switch_value in switch_values.items():
                                    if isinstance(switch_value, str) and switch_value.startswith('${') and switch_value.endswith('}'):
                                        # Extract the property name from the template variable
                                        fm_property_name = switch_value[2:-1]  # Remove ${ and }
                                        # SM property (sm_property_template) maps to FM property (fm_property_name)
                                        mappings[sm_property_template] = fm_property_name
                                        break  # Use the first template variable found
                    # If no 'value' or 'switch' field, it's a direct name mapping (same name in SM and FM)
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

        # Apply direct mappings (SM property -> FM property)
        for sm_property, fm_property in direct_mappings.items():
            if sm_property in config:
                # Check if this FM property has a fixed value in template
                if fm_property in fixed_values:
                    template_value = fixed_values[fm_property]
                    sm_value = config[sm_property]
                    if str(sm_value) != str(template_value):
                        # Use template's fixed value and add error
                        mapped_config[fm_property] = template_value
                        error_msg = f"Property '{sm_property}' value '{sm_value}' overridden by template fixed value '{template_value}' for '{fm_property}'"
                        mapping_errors.append(error_msg)
                        self.logger.warning(f"Fixed value override: {sm_property}='{sm_value}' -> {fm_property}='{template_value}'")
                    else:
                        # Values match, use SM value
                        mapped_config[fm_property] = sm_value
                        self.logger.info(f"Direct template mapping (values match): {sm_property} -> {fm_property}")
                else:
                    # No fixed value, use SM value
                    sm_value = config[sm_property]
                    mapped_config[fm_property] = sm_value

                    # Validate against recommended values if available
                    if fm_property in recommended_values:
                        allowed_values = recommended_values[fm_property]
                        if str(sm_value) not in allowed_values:
                            error_msg = f"Property '{sm_property}' value '{sm_value}' is not in recommended values {allowed_values} for '{fm_property}'"
                            mapping_errors.append(error_msg)
                            self.logger.error(f"Value validation failed: {sm_property}='{sm_value}' not in {allowed_values}")
                            # Don't map the property if value is invalid
                            continue
                        else:
                            self.logger.info(f"Direct template mapping (validated): {sm_property} -> {fm_property}")

        # Also map properties that have the same name in both SM and FM
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

        # Look through all templates for connector_configs
        for template in fm_template['templates']:
            if 'connector_configs' in template:
                for config in template['connector_configs']:
                    # If the config has a 'value' field that's not a template variable, it's a fixed value
                    if 'value' in config:
                        value = config['value']
                        fm_property = config['name']

                        # Skip template variables like ${cleanup.policy}
                        if not (isinstance(value, str) and value.startswith('${') and value.endswith('}')):
                            fixed_values[fm_property] = value

        return fixed_values

    def _get_recommended_values_from_template(self, fm_template: Dict[str, Any]) -> Dict[str, List[str]]:
        """Extract recommended values from FM template config_defs section"""
        recommended_values = {}

        if not fm_template or 'templates' not in fm_template:
            return recommended_values

        # Look through all templates for config_defs
        for template in fm_template['templates']:
            if 'config_defs' in template:
                for config_def in template['config_defs']:
                    if 'recommended_values' in config_def:
                        recommended_values[config_def['name']] = config_def['recommended_values']

        return recommended_values

# not being used
    def _generate_fm_config(self, connector: Dict[str, Any]) -> Dict[str, Any]:
        """Generate FM configuration for a connector"""
        name = connector['name']
        config = connector['config']

        self.logger.info(f"Generating FM config for connector: {name}")

        # Get connector class
        connector_class = config.get('connector.class')
        if not connector_class:
            self.logger.error(f"No connector.class found in config for connector: {name}")
            return {
                'name': name,
                'config': {},
                'mapping_errors': ['No connector.class found in config'],
                'unmapped_configs': list(config.keys())
            }

        # Get templates based on connector class
        # Create a config dict that includes the worker URL for template fetching
        config_with_worker = config.copy()
        if 'worker' in connector:
            config_with_worker['worker'] = connector['worker']

        sm_template, fm_template = self._get_templates_for_connector(connector_class, name, config_with_worker)

        # Initialize mapped config with name
        mapped_config = {'name': name}

        # Always include connector.class as it's required
        if connector_class:
            mapped_config['connector.class'] = connector_class

        mapping_errors = []
        unmapped_configs = []

        # Map properties based on connector type
        jdbc_mapped = {}
        if 'connection.url' in config and isinstance(config['connection.url'], str) and config['connection.url'].startswith('jdbc:'):
            connector_type = self._get_database_type(config)
            jdbc_mapped = self._map_jdbc_properties(config, connector_type)
            mapped_config.update(jdbc_mapped)

        # Track all handled properties to avoid remapping
        handled_properties = set(mapped_config.keys())

        # Also exclude connection.url from semantic matching since it's handled by JDBC parsing
        if 'connection.url' in config:
            handled_properties.add('connection.url')

        # Apply template mappings if available
        if fm_template:  # Only require FM template, SM template is optional
            self.logger.debug(f"Applying template mappings for {connector_class}")

            # Get required properties from FM template
            required_props = self._get_required_properties(fm_template)

            # Extract template_id from the correct location
            plugin_type = "Unknown"
            if fm_template.get('template_id'):
                plugin_type = fm_template.get('template_id')
            elif 'templates' in fm_template and len(fm_template['templates']) > 0:
                plugin_type = fm_template['templates'][0].get('template_id', 'Unknown')

            self.logger.info(f"Using template_id for transforms: {plugin_type}")

            # Update connector.class to use template_id
            if plugin_type != "Unknown":
                mapped_config['connector.class'] = plugin_type
                self.logger.info(f"Updated connector.class to template_id: {plugin_type}")

            # Instead of required property error logic, just try to map required properties if present
            for prop_name, prop_info in required_props.items():
                # Skip connector.class and name as they're already handled
                if prop_name in ['connector.class', 'name']:
                    continue

                # Check if property is in input config
                if prop_name in config:
                    mapped_config[prop_name] = config[prop_name]
                    handled_properties.add(prop_name)
                    self.logger.info(f"Using input value for required property: {prop_name}")
                # Check if property was mapped from JDBC URL
                elif prop_name in jdbc_mapped:
                    mapped_config[prop_name] = jdbc_mapped[prop_name]
                    handled_properties.add(prop_name)
                    self.logger.info(f"Using JDBC mapped value for required property: {prop_name}")
                # Check if property has a default value
                elif prop_info.get('default_value') is not None:
                    mapped_config[prop_name] = prop_info['default_value']
                    handled_properties.add(prop_name)
                    self.logger.info(f"Using default value for required property: {prop_name}")
            transforms_data = self.get_transforms_config(config, plugin_type)
            mapped_config.update(transforms_data['allowed'])
            
            # Add mapping errors from transforms processing
            if 'mapping_errors' in transforms_data:
                mapping_errors.extend(transforms_data['mapping_errors'])
            # Step 1: Try direct mappings from template connector_configs first
            direct_mapped, direct_mapping_errors = self._map_using_template_direct_mappings(config, fm_template)
            for fm_prop_name, value in direct_mapped.items():
                if fm_prop_name not in handled_properties:
                    mapped_config[fm_prop_name] = value
                    handled_properties.add(fm_prop_name)
                    self.logger.info(f"Direct template mapping: {fm_prop_name}")

            # Add direct mapping errors to the main error list
            mapping_errors.extend(direct_mapping_errors)

            # Then map properties that exist in the input config
            for sm_prop_name, sm_prop_value in config.items():
                try:
                    # Skip connector.class and name as they're already handled
                    if sm_prop_name in ['connector.class', 'name']:
                        continue

                    # Skip if we already handled this property as a required property or via JDBC mapping
                    if sm_prop_name in handled_properties:
                        continue

                    # Skip transform properties as they are handled separately
                    if isinstance(sm_prop_name, str) and sm_prop_name.startswith('transforms'):
                        continue

                    self.logger.debug(f"\nProcessing property: {sm_prop_name}")
                    self.logger.debug(f"Property value: {sm_prop_value}")

                    # Step 2: Try exact name match first
                    property_found = False
                    if 'templates' in fm_template:
                        for template in fm_template['templates']:
                            # Check config_defs first
                            if 'config_defs' in template:
                                for config_def in template['config_defs']:
                                    if config_def['name'] == sm_prop_name:
                                        # Direct match found - only map if not already handled
                                        if sm_prop_name not in handled_properties:
                                            mapped_config[sm_prop_name] = sm_prop_value
                                            handled_properties.add(sm_prop_name)
                                            self.logger.info(f"Direct match found for property: {sm_prop_name}")
                                        else:
                                            self.logger.debug(f"Skipping direct match for {sm_prop_name} as it is already mapped")
                                        property_found = True
                                        break
                            if property_found:
                                break

                    if not property_found:
                        # Step 3: Check static mappings based on connector type
                        is_source = self._is_source_connector(fm_template)
                        static_mappings = self.static_property_mappings_source if is_source else self.static_property_mappings_sink

                        if sm_prop_name in static_mappings:
                            fm_prop_name = static_mappings[sm_prop_name]

                            # Only map if the target property hasn't been handled yet
                            if fm_prop_name not in handled_properties:
                                # Apply reverse value mapping for converter properties
                                if sm_prop_name in ['key.converter', 'value.converter'] and sm_prop_value in self.converter_to_format_mappings:
                                    mapped_value = self.converter_to_format_mappings[sm_prop_value]
                                    mapped_config[fm_prop_name] = mapped_value
                                    connector_type = "source" if is_source else "sink"
                                    self.logger.info(f"Static mapping with value conversion ({connector_type}): {sm_prop_name}='{sm_prop_value}' -> {fm_prop_name}='{mapped_value}'")
                                else:
                                    mapped_config[fm_prop_name] = sm_prop_value
                                    connector_type = "source" if is_source else "sink"
                                    self.logger.info(f"Static mapping found ({connector_type}): {sm_prop_name} -> {fm_prop_name}")

                                handled_properties.add(fm_prop_name)
                                property_found = True
                            else:
                                self.logger.debug(f"Skipping static mapping for {sm_prop_name} -> {fm_prop_name} as {fm_prop_name} is already mapped")
                                property_found = True

                    if not property_found:
                        # Step 4: Try semantic matching
                        sm_prop = {
                            'name': sm_prop_name,
                            'description': sm_template.get('documentation', ''),
                            'type': sm_template.get('type', 'STRING'),
                            'section': sm_template.get('group', 'General')
                        }

                        # Get FM properties for matching (as dictionary)
                        fm_properties_dict = {}
                        if 'templates' in fm_template:
                            for template in fm_template['templates']:
                                # Add config_defs properties
                                if 'config_defs' in template:
                                    for config_def in template['config_defs']:
                                        fm_properties_dict[config_def['name']] = config_def

                        # Find best match using semantic matching with threshold
                        result = self.semantic_matcher.find_best_match(sm_prop, fm_properties_dict, semantic_threshold=0.7)

                        if result and result.matched_fm_property:
                            # result.matched_fm_property is now a dictionary, so we need to get the property name
                            # The find_best_match method returns the property info, but we need the property name
                            # We'll need to find the property name by matching the property info
                            fm_prop_name = None
                            for prop_name, prop_info in fm_properties_dict.items():
                                if prop_info == result.matched_fm_property:
                                    fm_prop_name = prop_name
                                    break

                            if fm_prop_name and fm_prop_name not in handled_properties:
                                mapped_config[fm_prop_name] = sm_prop_value
                                handled_properties.add(fm_prop_name)
                                self.logger.info(f"Successfully mapped {sm_prop_name} to {fm_prop_name} using {result.match_type} matching")
                            elif fm_prop_name in handled_properties:
                                self.logger.debug(f"Skipping semantic mapping for {sm_prop_name} -> {fm_prop_name} as {fm_prop_name} is already mapped")
                            else:
                                error_msg = f"Could not determine property name for matched property: {sm_prop_name}"
                                mapping_errors.append(error_msg)
                                unmapped_configs.append(sm_prop_name)
                                self.logger.warning(f"Failed to map property '{sm_prop_name}' - could not determine property name")
                        else:
                            error_msg = f"Config '{sm_prop_name}' not exposed for fully managed connector"
                            mapping_errors.append(error_msg)
                            unmapped_configs.append(sm_prop_name)
                            self.logger.warning(f"Failed to map property '{sm_prop_name}'")
                except Exception as e:
                    error_msg = f"Error mapping {sm_prop_name}: {str(e)}"
                    mapping_errors.append(error_msg)
                    self.logger.error(f"Error mapping property '{sm_prop_name}': {e}")
            # After all mapping, check for missing required properties
            for prop_name, prop_info in required_props.items():
                if prop_name in ['connector.class', 'name']:
                    continue
                if prop_name not in mapped_config:
                    error_msg = f"Required property '{prop_name}' needs a value but none was provided in input config or default value"
                    mapping_errors.append(error_msg)
                    self.logger.error(error_msg)

        # Filter mapped config to only include properties defined in config_defs
        filtered_config = {}
        filtered_out_properties = []
        if fm_template and 'templates' in fm_template:
            # Collect all config_def names from all templates
            config_def_names = set()
            for template in fm_template['templates']:
                if 'config_defs' in template:
                    for config_def in template['config_defs']:
                        config_def_names.add(config_def['name'])

            # Only keep properties that are in config_defs, but exclude transform properties
            for prop_name, prop_value in mapped_config.items():
                # Transform properties are separate from config_defs and should always be included
                if prop_name.startswith('transforms') or prop_name == 'transforms':
                    filtered_config[prop_name] = prop_value
                    self.logger.debug(f"Including transform property '{prop_name}' (not subject to config_defs filtering)")
                elif prop_name in config_def_names:
                    filtered_config[prop_name] = prop_value
                else:
                    filtered_out_properties.append(prop_name)
                    error_msg = f"Config '{prop_name}' not exposed for fully managed connector"
                    mapping_errors.append(error_msg)
                    unmapped_configs.append(prop_name)
                    self.logger.warning(error_msg)

            self.logger.info(f"Filtered config from {len(mapped_config)} to {len(filtered_config)} properties (only config_defs)")
            if filtered_out_properties:
                self.logger.warning(f"Filtered out {len(filtered_out_properties)} properties not in config_defs: {', '.join(filtered_out_properties)}")
        else:
            # If no FM template, keep all mapped config
            filtered_config = mapped_config
            self.logger.warning("No FM template available - keeping all mapped properties")

        # Log summary of mapping results
        if unmapped_configs:
            self.logger.warning(f"Connector has {len(unmapped_configs)} unmapped configurations: {', '.join(unmapped_configs)}")
        else:
            self.logger.info("All configurations were successfully mapped")

        self.logger.info(f"Mapping completed with {len(filtered_config)} properties mapped and {len(mapping_errors)} errors")
        
        return {
            'name': name,
            'sm_config': config,  # Include original SM config
            'config': filtered_config,
            'mapping_errors': mapping_errors,
            'unmapped_configs': unmapped_configs
        }

    @staticmethod
    def parse_connector_file(file, all_connectors_dict, logger=None):
        if not os.path.exists(file):
            raise FileNotFoundError(f"File not found: {file}")
        if logger is None:
            logger = logging.getLogger("config_parser")

        if not (file.suffix == '.json' and file.is_file()):
            return
        try:
            # Output -> all_connectors_dict = { "connector_name": {"name":"", "config":""}, ... }
            with open(file, 'r') as f:
                data = json.load(f)
                if isinstance(data, dict) and 'connectors' in data:
                    # Structure: {"connectors": {"connector_name": {"name":"", "config":""}, ...}}
                    all_connectors_dict.update(data['connectors'])
                elif isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and 'name' in item and 'config' in item:
                            # Structure: [ {"connector_name_02": {"name":"", "config":""} }, ... ]
                            all_connectors_dict[item['name']] = item
                        elif isinstance(item, dict):
                            # list of configs [ { "name":..., "config":{...} }, { "name":..., "config":{...} }, ... ]
                            for value in item.values():
                                if isinstance(value, dict) and 'name' in value and 'config' in value:
                                    all_connectors_dict[value['name']] = value
                                else:
                                    logger.warning(f"Skipping non-connector dict item in list in {file}: {value}")
                elif isinstance(data, dict) and 'name' in data and 'config' in data:
                    # Structure: {"name": ..., "config": ...} (single connector config)
                    connector_name = data['name']
                    all_connectors_dict[connector_name] = data
                elif isinstance(data, dict):
                    # Structure: {"connector1": {...}, "connector2": {...}} (or just one)
                    for connector_name, connector_val in data.items():
                        if isinstance(connector_val, dict) and 'name' in connector_val and 'config' in connector_val:
                            # Structure: {"connector_name": {"name":"", "config":""}}
                            all_connectors_dict[connector_name] = connector_val
                        elif (
                                isinstance(connector_val, dict)
                                and 'Info' in connector_val
                                and isinstance(connector_val['Info'], dict)
                                and 'name' in connector_val['Info']
                                and 'config' in connector_val['Info']
                        ):
                            # Structure: {"connector_name": {"Info": {"name":"", "config":""}}}
                            all_connectors_dict[connector_name] = connector_val['Info']
                        else:
                            logger.warning(
                                f"Skipping connector '{connector_name}' in {file}: missing 'name' and 'config'")
                else:
                    logger.warning(f"Skipping unrecognized format in {file}")
        except Exception as e:
            logger.error(f"Failed to parse {file}: {e}")

    def process_connectors(self) -> Dict[str, Any]:
        """Process all connectors and generate FM configurations"""
        connectors_dict = {}
        ConnectorComparator.parse_connector_file(self.input_file, connectors_dict, self.logger)

        if not connectors_dict:
            self.logger.error("No connectors found after parsing the input file.")
            return
        connectors = list(connectors_dict.values())

        # Process each connector
        fm_configs = {}
        for i, connector in enumerate(connectors):
            try:
                # Handle case where connector might be a string or other type
                if not isinstance(connector, dict):
                    self.logger.error(f"Connector at index {i} is not a dictionary: {type(connector)}")
                    continue

                # Ensure connector has required fields
                if 'name' not in connector or 'config' not in connector:
                    self.logger.error(f"Connector at index {i} missing required fields 'name' or 'config'")
                    continue

                # Transform SM to FM using the new method
                result = self.transformSMToFm(connector['name'], connector['config'])

                # Create FM config object in the expected format
                fm_config = {
                    'name': connector['name'],
                    'sm_config': connector['config'],
                    'config': result['fm_configs'],
                    'mapping_errors': result['errors'],
                    'mapping_warnings': result['warnings'],
                }

                fm_configs[connector['name']] = fm_config

            except Exception as e:
                connector_name = connector.get('name', f'connector_{i}') if isinstance(connector, dict) else f'connector_{i}'
                self.logger.error(f"Error processing connector {connector_name}: {str(e)}")

        return fm_configs

    def transformSMToFm(self, connector_name:str, user_configs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform Self-Managed (SM) connector configurations to Fully Managed (FM) configurations.

        Args:
            user_configs: Dictionary of configuration key-value pairs where:
                - Key: Configuration property name (string)
                - Value: Configuration property value (will be converted to string)
                Example: {"connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector", "connection.url": "jdbc:mysql://localhost:3306/mydb"}

        Returns:
            Dictionary containing:
                - 'fm_configs': List of successfully transformed FM configurations
                - 'warnings': List of warning messages
                - 'errors': List of error messages
        """
        result = {
            'fm_configs': [],
            'warnings': [],
            'errors': []
        }

        # Validate input
        if not isinstance(user_configs, dict):
            result['errors'].append("Input must be a dictionary of configuration key-value pairs")
            return result

        if not user_configs:
            result['errors'].append("No configuration properties provided")
            return result

        # Convert all values to strings
        config_dict = {}
        for key, value in user_configs.items():
            config_dict[key] = str(value)

        # Check for required connector.class
        if 'connector.class' not in config_dict:
            result['errors'].append("Missing required 'connector.class' configuration")
            return result

        # Get template ID (connector class)
        template_id = config_dict.get('connector.class')

        sm_template, fm_template = self._get_templates_for_connector(template_id, connector_name, config_dict)


        if fm_template is None:
            result['errors'].append(f"No FM template found for connector class: {template_id}")
            # Continue without config processing - just return the basic structure
            result['fm_configs'] = {
                'connector.class': template_id,
                'name': connector_name,
            }
            return result

        # Extract template components (following Java TemplateEngine pattern)
        try:
            # Validate template structure
            if not isinstance(fm_template, dict):
                raise ValueError(f"Expected fm_template to be a dict, got {type(fm_template)}")
            
            if 'templates' not in fm_template:
                raise ValueError("fm_template missing 'templates' key")
            
            if not isinstance(fm_template['templates'], (list, tuple)):
                raise ValueError(f"Expected fm_template['templates'] to be a list, got {type(fm_template['templates'])}")
            
            # Log template structure for debugging
            self.logger.debug(f"Template structure: {list(fm_template.keys())}")
            self.logger.debug(f"Number of templates: {len(fm_template['templates'])}")
            
            connector_config_defs = self._extract_connector_config_defs(fm_template)
            template_config_defs = self._extract_template_config_defs(fm_template)

            self.logger.debug(f"Extracted {len(connector_config_defs)} connector config defs and {len(template_config_defs)} template config defs")

        except Exception as e:
            self.logger.error(f"Error extracting template components: {str(e)}")
            # Return basic structure with error
            result['errors'].append(f"Error extracting template components: {str(e)}")
            result['fm_configs'] = {
                'connector.class': template_id,
                'name': connector_name,
            }
            return result

        # Initialize FM configs and message lists (following Java pattern)
        fm_configs = {}
        transforms_configs = {}  # Separate dictionary for transforms and predicates
        warnings = []
        errors = []
        semantic_match_list = set()  # Track configs that need semantic matching

        # Step 1: Handle connector.class and name (following Java pattern)
        if 'connector.class' in config_dict:
            # Get template_id from the first template in the templates array
            template_id = None
            if 'templates' in fm_template and len(fm_template['templates']) > 0:
                template_id = fm_template['templates'][0].get('template_id')
                self.logger.info(f"Found template_id in FM template: {template_id}")
                self.logger.info(f"FM template structure: {list(fm_template.keys())}")
                if 'templates' in fm_template:
                    self.logger.info(f"Number of templates: {len(fm_template['templates'])}")
                    for i, template in enumerate(fm_template['templates']):
                        self.logger.info(f"Template {i}: {template.get('template_id', 'NO_TEMPLATE_ID')}")

            if template_id:
                fm_configs['connector.class'] = template_id
                self.logger.info(f"Set connector.class to template_id: {template_id}")
            else:
                # Fallback to the original connector.class if template_id is not found
                fm_configs['connector.class'] = config_dict['connector.class']
                self.logger.info(f"Set connector.class to original value: {config_dict['connector.class']}")
        else:
            errors.append(f"connector.class property is required.")

        # Always set the name from connector_name parameter
        fm_configs['name'] = connector_name
        self.logger.info(f"Set name in fm_configs from connector_name parameter: {connector_name}")

        if 'tasks.max' in config_dict:
            fm_configs['tasks.max'] = config_dict['tasks.max']
        else:
            fm_configs['tasks.max'] = "1"

        # Step 2: Process user configs (following Java pattern)
        # Validate template_config_defs is a list
        if not isinstance(template_config_defs, (list, tuple)):
            self.logger.error(f"template_config_defs is not a list, got {type(template_config_defs)}: {template_config_defs}")
            errors.append(f"Invalid template_config_defs type: {type(template_config_defs)}")
            return result

        # Validate connector_config_defs is a list
        if not isinstance(connector_config_defs, (list, tuple)):
            self.logger.error(f"connector_config_defs is not a list, got {type(connector_config_defs)}: {connector_config_defs}")
            errors.append(f"Invalid connector_config_defs type: {type(connector_config_defs)}")
            return result

        # Additional safety check - ensure template_config_defs is not empty
        if not template_config_defs:
            self.logger.error("template_config_defs is empty or None")
            errors.append("template_config_defs is empty or None")
            return result



        try:
            for user_config_key, user_config_value in config_dict.items():

                # Check if this is a transforms or predicates config
                if user_config_key.startswith('connector.class') or user_config_key.startswith('name'):
                    continue

                if user_config_key.startswith('transforms') or user_config_key.startswith('predicates'):
                    transforms_configs[user_config_key] = user_config_value
                    continue

              
                # Find if user config is present in Connector config def
                matching_connector_config_def = None
                if isinstance(connector_config_defs, (list, tuple)):
                    for connector_config_def in connector_config_defs:
                        if isinstance(connector_config_def, dict) and connector_config_def.get('name') == user_config_key:
                            matching_connector_config_def = connector_config_def
                            break
                        
                else:
                    self.logger.warning(f"Expected connector_config_defs to be a list, got {type(connector_config_defs)}")
                    continue

                if matching_connector_config_def is not None:
                    # User config present in Connector config def
                    self._process_user_config_in_connector_config_def(
                        matching_connector_config_def,
                        user_config_value,
                        template_config_defs,
                        fm_configs,
                        warnings,
                        errors,
                        config_dict,
                        semantic_match_list
                    )
                else:
                    # Check if this config is defined in template_config_defs
                    config_found_in_template = False

                    try:
                        for template_config_def in template_config_defs:
                            original_user_config_key = user_config_key
                            if (user_config_key.startswith('consumer.') or user_config_key.startswith('producer.')) and \
                            not user_config_key.startswith('consumer.override.') and not user_config_key.startswith('producer.override.'):
                                user_config_key = user_config_key.replace('consumer.', 'consumer.override.')
                                user_config_key = user_config_key.replace('producer.', 'producer.override.')
                            elif user_config_key.startswith('consumer.override.') or user_config_key.startswith('producer.override.'):
                                # Remove override. from config in template_config_def.get('name')
                                user_config_key = user_config_key.replace('consumer.override.', 'consumer.')
                                user_config_key = user_config_key.replace('producer.override.', 'producer.')

                            if isinstance(template_config_def, dict) and (template_config_def.get('name') == user_config_key or template_config_def.get('name') == original_user_config_key):
                                config_found_in_template = True
                                break
                    except Exception as e:
                        self.logger.error(f"Error iterating over template_config_defs for {user_config_key}: {str(e)}")
                        self.logger.error(f"template_config_defs type: {type(template_config_defs)}, content: {template_config_defs}")
                        config_found_in_template = False

                    if not config_found_in_template:
                        # User config not present in either Connector config def or template config defs - warn
                        warning_msg = f"Unused connector config '{user_config_key}'. Given value will be ignored. Default value will be used if any."
                        warnings.append(warning_msg)
                        self.logger.warning(warning_msg)

        except Exception as e:
            self.logger.error(f"Error processing user configs: {str(e)}")
            errors.append(f"Error processing user configs {user_config_key}: {str(e)}")
            # Continue with basic config

        # Step 3: Process template configs using config derivation methods
        self.logger.info(f"Processing {len(template_config_defs)} template config definitions")
        try:
            for template_config_def in template_config_defs:
                template_config_name = template_config_def.get("name")
                is_required = template_config_def.get("required", False)
                self.logger.debug(f"Processing template config: {template_config_name}, required: {is_required}")

                # Get the method to derive this config from user configs
                derivation_method = self._get_config_derivation_method(template_config_name, template_config_def)

                if derivation_method:
                    derived_value = derivation_method(config_dict, fm_configs, template_config_defs, template_config_name)
                    if derived_value is not None:
                        fm_configs[template_config_name] = derived_value
                        self.logger.debug(f"Derived value for {template_config_name}: {derived_value}")
                else:
                    self.logger.debug(f"No derivation method found for {template_config_name}")
        except Exception as e:
            self.logger.error(f"Error processing template configs: {str(e)}")
            errors.append(f"Error processing template configs: {str(e)}")

        # Step 4: Before semantic matching, check if user config keys directly match template config def names
        for user_config_key, user_config_value in config_dict.items():
            # Skip if already in fm_configs
            if user_config_key in fm_configs:
                if user_config_key in semantic_match_list:
                    semantic_match_list.remove(user_config_key)
                continue

            original_user_config_key = user_config_key
            if user_config_key.startswith('producer.override') or user_config_key.startswith('consumer.override'):
                user_config_key = user_config_key.replace('consumer.override.', 'consumer.')
                user_config_key = user_config_key.replace('producer.override.', 'producer.')
            elif user_config_key.startswith('producer.') or user_config_key.startswith('consumer.'):
                user_config_key = user_config_key.replace('producer.', 'producer.override.')
                user_config_key = user_config_key.replace('consumer.', 'consumer.override.')
                

            # Check if user config key matches any template config def name
            try:
                for template_config_def in template_config_defs:
                    if isinstance(template_config_def, dict):
                        template_config_name = template_config_def.get("name")
                    
                        if template_config_name == user_config_key:
                            # Direct match found - add to fm_configs
                            fm_configs[user_config_key] = user_config_value
                            self.logger.info(f"Direct match found: {user_config_key} = {user_config_value}")
                            if user_config_key in semantic_match_list or original_user_config_key in semantic_match_list:
                                semantic_match_list.remove(user_config_key)
                                semantic_match_list.remove(original_user_config_key)
                            break
                        if template_config_name == original_user_config_key:
                            # Direct match found - add to fm_configs
                            fm_configs[original_user_config_key] = user_config_value
                            self.logger.info(f"Direct match found: {original_user_config_key} = {user_config_value}")
                            if user_config_key in semantic_match_list or original_user_config_key in semantic_match_list:
                                semantic_match_list.remove(user_config_key)
                                semantic_match_list.remove(original_user_config_key)
                            break    
            except Exception as e:
                self.logger.error(f"Error checking template config match for {user_config_key}: {str(e)}")
                self.logger.error(f"template_config_defs type: {type(template_config_defs)}, content: {template_config_defs}")

        # Step 5: do semantic matching for the configs that are not present in the template
        self._do_semantic_matching(fm_configs, semantic_match_list, config_dict, template_config_defs, sm_template)

        # Check for required configs that are missing after semantic matching
        self._check_required_configs(fm_configs, template_config_defs, errors)

        # Process transforms configs
        # Extract template_id from the correct location
        plugin_type = "Unknown"
        if fm_template.get('template_id'):
            plugin_type = fm_template.get('template_id')
        elif 'templates' in fm_template and len(fm_template['templates']) > 0:
            plugin_type = fm_template['templates'][0].get('template_id', 'Unknown')

        transforms_data = self.get_transforms_config(config_dict, plugin_type)
        fm_configs.update(transforms_data['allowed'])

        # Add mapping errors from transforms processing
        if 'mapping_errors' in transforms_data:
            errors.extend(transforms_data['mapping_errors'])

        # Debug logging for final connector.class value
        if 'connector.class' in fm_configs:
            self.logger.info(f"Final connector.class value: {fm_configs['connector.class']}")
        else:
            self.logger.warning("connector.class not found in final fm_configs")

        # Return the result in the required format
        result = {
            "name": connector_name,
            "fm_configs": fm_configs,
            "warnings": warnings,
            "errors": errors
        }

        return result

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
                    # Ensure connector_configs is a list/iterable, not a boolean or other type
                    if isinstance(template['connector_configs'], (list, tuple)):
                        connector_config_defs.extend(template['connector_configs'])
                    else:
                        self.logger.warning(f"Expected connector_configs to be a list, got {type(template['connector_configs'])}: {template['connector_configs']}")
                        # Skip this template's connector_configs
                        continue
        else:
            self.logger.warning("No 'templates' key found in fm_template")

        return connector_config_defs

    def _extract_template_config_defs(self, fm_template: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract template config definitions from FM template (following Java pattern)"""
        template_config_defs = []
        
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
                    # Ensure config_defs is a list/iterable, not a boolean or other type
                    if isinstance(template['config_defs'], (list, tuple)):
                        template_config_defs.extend(template['config_defs'])
                    else:
                        self.logger.warning(f"Expected config_defs to be a list, got {type(template['config_defs'])}: {template['config_defs']}")
                        # Skip this template's config_defs
                        continue
        else:
            self.logger.warning("No 'templates' key found in fm_template")
            
        return template_config_defs

    def _get_config_derivation_method(self, template_config_name: str, template_config_def: Dict[str, Any]):
        """
        Get the method to derive a template config from user configs.
        This maps template config names to their derivation methods.
        """
        # Map of template config names to their derivation methods
        config_derivation_methods = {
            # JDBC-related configs
            'connection.url': self._derive_connection_url,
            'connection.host': self._derive_connection_host,
            'connection.port': self._derive_connection_port,
            'connection.user': self._derive_connection_user,
            'connection.password': self._derive_connection_password,
            'connection.database': self._derive_connection_database,
            'db.name': self._derive_db_name,
            'db.connection.type': self._derive_db_connection_type,
            'ssl.server.cert.dn': self._derive_ssl_server_cert_dn,


            # Data format configs
            'input.key.format': self._derive_input_key_format,
            'input.data.format': self._derive_input_data_format,
            'output.key.format': self._derive_output_key_format,
            'output.data.format': self._derive_output_data_format,
            'output.data.key.format': self._derive_output_data_key_format,
            'output.data.value.format': self._derive_output_data_value_format,


            # SSL configs
            'ssl.mode': self._derive_ssl_mode,

            # Redis configs
            'redis.hostname': self._derive_redis_hostname,
            'redis.portnumber': self._derive_redis_portnumber,
            'redis.ssl.mode': self._derive_redis_ssl_mode,

            # Service Bus configs
            'azure.servicebus.namespace': self._derive_servicebus_namespace,
            'azure.servicebus.sas.keyname': self._derive_azure_servicebus_sas_keyname,
            'azure.servicebus.sas.key': self._derive_azure_servicebus_sas_key,
            'azure.servicebus.entity.name': self._derive_azure_servicebus_entity_name,
            
            # Subject name strategy configs
            'key.converter.key.subject.name.strategy': self._derive_subject_name_strategy,
            'value.converter.value.subject.name.strategy': self._derive_subject_name_strategy,
            'key.subject.name.strategy': self._derive_subject_name_strategy,
            'subject.name.strategy': self._derive_subject_name_strategy,
            'value.subject.name.strategy': self._derive_subject_name_strategy,
            'value.converter.reference.subject.name.strategy': self._derive_reference_subject_name_strategy,
            'key.converter.reference.subject.name.strategy': self._derive_reference_subject_name_strategy,
            # Add more mappings as needed
        }

        return config_derivation_methods.get(template_config_name)

    def _process_user_config_in_connector_config_def(
        self,
        connector_config_def: Dict[str, Any],
        user_config_value: str,
        template_config_defs: List[Dict[str, Any]],
        fm_configs: Dict[str, str],
        warnings: List[str],
        errors: List[str],
        user_configs: Dict[str, str],
        semantic_match_list: set
    ):
        """Process a user config that is present in connector config def (following Java pattern)"""

        # Special logging for validate.non.null configuration
        config_name = connector_config_def.get('name')
        
        # Case 1: value is constant string
        if connector_config_def.get('value') is not None:
            self._process_value_case(connector_config_def, user_config_value, template_config_defs, fm_configs, warnings, user_configs, semantic_match_list)
            return

        # Case 2: Connector config value is switch case
        if connector_config_def.get('switch') is not None:
            self._process_switch_case(connector_config_def, user_configs, template_config_defs, fm_configs, warnings, errors, semantic_match_list)
            return

        # Case 3: Connector config def is a dynamic mapper
        if connector_config_def.get('dynamic.mapper') is not None:
            self._process_dynamic_mapper_case(connector_config_def, user_config_value, user_configs, template_config_defs, fm_configs, warnings, semantic_match_list)
            return

        # Case 4: Value is null
        if connector_config_def.get('value') is None:
            self._process_null_value_case(connector_config_def, user_config_value, template_config_defs, fm_configs, warnings)
            return

    def _process_value_case(
        self,
        connector_config_def: Dict[str, Any],
        user_config_value: str,
        template_config_defs: List[Dict[str, Any]],
        fm_configs: Dict[str, str],
        warnings: List[str],
        user_configs: Dict[str, str],
        semantic_match_list: set
    ):
        """Process value case (following Java pattern)"""
        value = connector_config_def.get('value')
        config_name = connector_config_def.get('name')

        # Handle non-string values (like validate.non.null: false, numbers, etc.)
        if not isinstance(value, str):

            # For non-string values, just set the config directly
            fm_configs[config_name] = str(value).lower() if isinstance(value, bool) else str(value)
            return

        # Check if value contains {{.logicalClusterId}} - this indicates internal config
        if value is not None and (
            'org.apache.kafka.common.security.plain.PlainLoginModule' in value or
            '/mnt/secrets/connect-sr' in value or
            ('{{.logicalClusterId}}' in value and not '/mnt/secrets/connect-external-secrets' in value)
        ):
            warnings.append(f"{connector_config_def.get('name')} is internal. User given value will be ignored.")
            return

        # Check if the value matches DEFAULT_PATTERN (contains ${CONFIG_KEY} references)
        import re
        default_pattern = re.compile(r'\$\{([^}]+)\}')
        matcher = default_pattern.search(value)

        if matcher:
            # It's not a true constant, it's a combination of multiple high level keys
            referenced_keys = self._find_referenced_keys(value, {td.get('name') for td in template_config_defs})

            # Process each referenced key
            for referenced_key in referenced_keys:
                config_name = connector_config_def.get('name')
                if fm_configs.get(config_name) is not None:
                    #we already have found value for this config, no need to reprocess.
                    return
                # Check if user config already has a value for this config - if yes, copy it
                if referenced_key in user_configs and user_configs[referenced_key].strip():
                    fm_configs[config_name] = user_config_value
                    return

                referenced_template_config_def = self._find_template_config_def_by_name(referenced_key, template_config_defs)

                if referenced_template_config_def is not None and not referenced_template_config_def.get('internal', False):
                    # Check if there's a derivation method defined for this referenced key
                    derivation_method = self._get_config_derivation_method(referenced_key, referenced_template_config_def)
                    if derivation_method:
                        # If there's a derivation method, return early as it will be handled later
                        return

                    # Check if connector config def value is of format ${<template def name>}
                    if value == f"${{{referenced_key}}}":
                        # Direct reference format, copy the value as is
                        fm_configs[referenced_key] = user_config_value
                elif referenced_template_config_def is not None and referenced_template_config_def.get('internal', False):
                    warnings.append(f"The transformed FM config is internal and will be inferred. User given value will be ignored.")
                else:
                    semantic_match_list.add(config_name)
                    self.logger.warning(f"'{config_name}' : Config transform not present in template for '{referenced_key}' which was referenced by connector config '{connector_config_def.get('name')}'. Will attempt a semantic match.")
            return
        else:
            config_name = connector_config_def.get('name')
            # It's a true constant value
            if value != user_config_value:
                # Case 1.1: not same as the value from user configs - Warn
                warnings.append(f"{config_name} : FM config has constant value '{value}' but user provided '{user_config_value}'. User given value will be ignored.")
            else:
                # Case 1.2: Same value given by user - Add it to fm key and value
                fm_configs[connector_config_def.get('name')] = user_config_value
            return

    def _process_switch_case(
        self,
        connector_config_def: Dict[str, Any],
        user_configs: Dict[str, str],
        template_config_defs: List[Dict[str, Any]],
        fm_configs: Dict[str, str],
        warnings: List[str],
        errors: List[str],
        semantic_match_list: set
    ):
        """Process switch case (following Java pattern)"""
        switch_cases = connector_config_def.get('switch', {})

        for template_config_key, switch_mapping in switch_cases.items():
            if fm_configs.get(template_config_key) is not None:
                # we already have found value for this config, no need to reprocess.
                return
            # Find corresponding template config def
            template_config_def = self._find_template_config_def_by_name(template_config_key, template_config_defs)

            if template_config_def is not None:
                if template_config_def.get('internal', False):
                    warnings.append(f"The transformed FM config is internal and will be inferred. User given value will be ignored.")
                else:
                    self._process_non_internal_switch_case(connector_config_def, template_config_def, switch_mapping, user_configs, fm_configs, warnings, semantic_match_list)
            else:
                self.logger.error(f"Switch case key '{template_config_key}' for config '{connector_config_def.get('name')}' is not part of template configs.")

    def _process_non_internal_switch_case(
        self,
        connector_config_def: Dict[str, Any],
        template_config_def: Dict[str, Any],
        switch_mapping: Dict[str, str],
        user_configs: Dict[str, str],
        fm_configs: Dict[str, str],
        warnings: List[str],
        semantic_match_list: set
    ):
        """Process non-internal switch case (following Java pattern)"""
        import re
        default_pattern = re.compile(r'\$\{([^}]+)\}')

        has_matchers = any(
            value is not None and default_pattern.search(value)
            for value in switch_mapping.values()
        )

        if has_matchers:
            # Check for derivation method for the template config
            template_config_name = template_config_def.get('name')
            derivation_method = self._get_config_derivation_method(template_config_name, template_config_def)
            if derivation_method:
                # If there's a derivation method, return early as it will be handled later
                return
            # If no derivation method, continue with normal switch processing
            # (This would be the complex matcher logic in the real implementation)
            config_name = connector_config_def.get('name')
            semantic_match_list.add(config_name)
            self.logger.error(f"'{config_name}' : Switch case has matchers but no derivation method for '{template_config_name}'. Complex matcher logic not implemented. Will attempt a semantic match.")
        else:
            user_value = user_configs.get(connector_config_def.get('name'))
            if user_value is not None:
                high_level_value = self._apply_reverse_switch(switch_mapping, user_value)
                if high_level_value is not None:
                    fm_configs[template_config_def.get('name')] = high_level_value
                else:
                    warnings.append(f"User value '{user_value}' for '{connector_config_def.get('name')}' does not match any value in templateswitch case.")

    def infer_dynamic_mappings(self, dynamic_mapper_fun_name: str, user_config_value: str) -> Optional[str]:
        """
        Infer dynamic mappings for a given FM property name.
        This is a placeholder method that should be implemented based on specific requirements.
        """
        if dynamic_mapper_fun_name and dynamic_mapper_fun_name== 'value.converter.reference.subject.name.strategy.mapper':
            sm_to_fm_mapping = {
                "io.confluent.kafka.serializers.subject.TopicNameStrategy": "TopicNameStrategy",
                "io.confluent.kafka.serializers.subject.RecordNameStrategy": "RecordNameStrategy",
                "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy": "TopicRecordNameStrategy"
            }
            return sm_to_fm_mapping.get(user_config_value, None)

        # Placeholder implementation - in real code, this would infer the mapping based on some logic
        self.logger.warning(f"Dynamic mapping inference not implemented for {dynamic_mapper_fun_name}. Returning None.")
        return None

    def _process_dynamic_mapper_case(
        self,
        connector_config_def: Dict[str, Any],
        user_config_value: str,
        user_configs: Dict[str, str],
        template_config_defs: List[Dict[str, Any]],
        fm_configs: Dict[str, str],
        warnings: List[str],
        semantic_match_list: set
    ):
        """Process dynamic mapper case (following Java pattern)"""
        connector_config_name = connector_config_def.get('name')

        # Check if the connector config def name is part of template config defs
        template_config_def = self._find_template_config_def_by_name(connector_config_name, template_config_defs)

        if template_config_def is not None:
            # If it exists in template config defs, add to fm_configs
            fm_configs[connector_config_name] = user_config_value
            return
        elif connector_config_def.get('dynamic.mapper') is not None and connector_config_def.get('dynamic.mapper').get('name') is not None :
            # Check if it has a dynamic mapper defined name is present in fm_template
            fm_template_def = self._find_template_config_def_by_name(connector_config_name, template_config_defs)

            if fm_template_def is not None:
                # If it has a fm template dynamic mapper defined and not present in fm_configs, try to infer the mapping
                dynamic_mapping_value = self.infer_dynamic_mappings(connector_config_def.get('dynamic.mapper').get('name'), user_config_value)
                if dynamic_mapping_value is not None:
                    # If dynamic mapping is found, add it to fm_configs
                    fm_configs[fm_template_def.get('name')] = dynamic_mapping_value
                    self.logger.info(f"Dynamic mapping for '{connector_config_name}' inferred as '{dynamic_mapping_value}'")
                    return
                elif fm_configs.get(fm_template_def.get('name')) is not None:
                    # If dynamic mapping is not found but fm_configs already has this config, skip
                    self.logger.info(f"Dynamic mapping for '{connector_config_name}' already exists in fm_configs, skipping inference.")
                    return

        # If not found in template config defs, add to semantic matching list
        semantic_match_list.add(connector_config_name)
        self.logger.warning(f"Dynamic mapper config '{connector_config_name}' not found in template configs. Will attempt semantic matching.")

    def _process_null_value_case(
        self,
        connector_config_def: Dict[str, Any],
        user_config_value: str,
        template_config_defs: List[Dict[str, Any]],
        fm_configs: Dict[str, str],
        warnings: List[str]
    ):
        """Process null value case (following Java pattern)"""
        # This is a simplified implementation - in the real Java code, this would handle null values
        # For now, we'll just add the value directly
        fm_configs[connector_config_def.get('name')] = user_config_value

    def _find_template_config_def_by_name(self, name: str, template_config_defs: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Find template config def by name (following Java pattern)"""
        for template_config_def in template_config_defs:
            if template_config_def.get('name') == name:
                return template_config_def
        return None

    def _find_referenced_keys(self, value: str, high_level_keys: Set[str]) -> Set[str]:
        """Find referenced keys in a value (following Java pattern)"""
        import re
        default_pattern = re.compile(r'\$\{([^}]+)\}')
        referenced_keys = set()

        for match in default_pattern.finditer(value):
            referenced_key = match.group(1)
            if referenced_key in high_level_keys:
                referenced_keys.add(referenced_key)

        return referenced_keys
    
    def _derive_connection_host(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive connection.host from user configs (e.g., from JDBC URL or MongoDB connection string)"""
        # Try to extract from JDBC URL
        if 'connection.url' in user_configs:
            jdbc_url = user_configs['connection.url']
            if jdbc_url.startswith('jdbc:'):
                parsed = self._parse_jdbc_url(jdbc_url)
                return parsed.get('host')

        # Try to extract from MongoDB connection string
        if 'connection.uri' in user_configs:
            mongo_uri = user_configs['connection.uri']
            parsed = self._parse_mongodb_connection_string(mongo_uri)
            return parsed.get('host')

        # Check for MongoDB-specific connection string configs
        for config_key in ['mongodb.connection.string', 'connection.string']:
            if config_key in user_configs:
                mongo_uri = user_configs[config_key]
                parsed = self._parse_mongodb_connection_string(mongo_uri)
                return parsed.get('host')

        return None
    
    def _derive_connection_port(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive connection.port from user configs (e.g., from JDBC URL)"""
        # Try to extract from JDBC URL
        if 'connection.url' in user_configs:
            jdbc_url = user_configs['connection.url']
            if jdbc_url.startswith('jdbc:'):
                parsed = self._parse_jdbc_url(jdbc_url)
                return parsed.get('port')
        return None
    def _derive_connection_user(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive connection.user from user configs (e.g., from JDBC URL or MongoDB connection string)"""
        # Try to extract from JDBC URL
        if 'connection.url' in user_configs:
            jdbc_url = user_configs['connection.url']
            if jdbc_url.startswith('jdbc:'):
                parsed = self._parse_jdbc_url(jdbc_url)
                return parsed.get('user')

        # Try to extract from MongoDB connection string
        if 'connection.uri' in user_configs:
            mongo_uri = user_configs['connection.uri']
            parsed = self._parse_mongodb_connection_string(mongo_uri)
            return parsed.get('user')

        # Check for MongoDB-specific connection string configs
        for config_key in ['mongodb.connection.string', 'connection.string']:
            if config_key in user_configs:
                mongo_uri = user_configs[config_key]
                parsed = self._parse_mongodb_connection_string(mongo_uri)
                return parsed.get('user')

        return None
    
    def _derive_connection_password(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive connection.password from user configs (e.g., from JDBC URL or MongoDB connection string)"""
        # Try to extract from JDBC URL
        if 'connection.url' in user_configs:
            jdbc_url = user_configs['connection.url']
            if jdbc_url.startswith('jdbc:'):
                parsed = self._parse_jdbc_url(jdbc_url)
                return parsed.get('password')

        # Try to extract from MongoDB connection string
        if 'connection.uri' in user_configs:
            mongo_uri = user_configs['connection.uri']
            parsed = self._parse_mongodb_connection_string(mongo_uri)
            return parsed.get('password')

        # Check for MongoDB-specific connection string configs
        for config_key in ['mongodb.connection.string', 'connection.string']:
            if config_key in user_configs:
                mongo_uri = user_configs[config_key]
                parsed = self._parse_mongodb_connection_string(mongo_uri)
                return parsed.get('password')

        return None

    def _derive_connection_database(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive connection.database from user configs (e.g., from JDBC URL)"""
        # Try to extract from JDBC URL
        if 'connection.url' in user_configs:
            jdbc_url = user_configs['connection.url']
            if jdbc_url.startswith('jdbc:'):
                parsed = self._parse_jdbc_url(jdbc_url)
                # The _parse_jdbc_url method returns 'db.name', not 'database'
                return parsed.get('db.name')
        return None

    def _derive_db_name(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:

        """Derive db.name from user configs (e.g., from JDBC URL)"""
        # Try to extract from JDBC URL
        if 'connection.url' in user_configs:
            jdbc_url = user_configs['connection.url']
            if jdbc_url.startswith('jdbc:'):
                parsed = self._parse_jdbc_url(jdbc_url)
                # The _parse_jdbc_url method returns 'db.name', not 'database'
                return parsed.get('db.name')

        # Try to extract from MongoDB connection string
        if 'connection.uri' in user_configs:
            mongo_uri = user_configs['connection.uri']
            parsed = self._parse_mongodb_connection_string(mongo_uri)
            return parsed.get('database')

        # Check for MongoDB-specific connection string configs
        for config_key in ['mongodb.connection.string', 'connection.string']:
            if config_key in user_configs:
                mongo_uri = user_configs[config_key]
                parsed = self._parse_mongodb_connection_string(mongo_uri)
                return parsed.get('database')

        # Check for direct db.name config
        if 'db.name' in user_configs:
            return user_configs['db.name']

        # Check for database config
        if 'database' in user_configs:
            return user_configs['database']

        return None

    def _derive_db_connection_type(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive db.connection.type from user configs (e.g., from JDBC URL)"""
        # Try to extract from JDBC URL
        if 'connection.url' in user_configs:
            jdbc_url = user_configs['connection.url']
            if jdbc_url.startswith('jdbc:'):
                parsed = self._parse_jdbc_url(jdbc_url)
                return parsed.get('db.connection.type')

        # Check for direct db.connection.type config
        if 'db.connection.type' in user_configs:
            return user_configs['db.connection.type']

        # Default fallback
        return None

    def _derive_ssl_server_cert_dn(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive ssl.server.cert.dn from user configs (e.g., from JDBC URL)"""
        # Try to extract from JDBC URL
        if 'connection.url' in user_configs:
            jdbc_url = user_configs['connection.url']
            if jdbc_url.startswith('jdbc:'):
                parsed = self._parse_jdbc_url(jdbc_url)
                return parsed.get('ssl.server.cert.dn')

        # Check for direct ssl.server.cert.dn config
        if 'ssl.server.cert.dn' in user_configs:
            return user_configs['ssl.server.cert.dn']

        # Default fallback
        return None

    def _derive_database_server_name(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive database.server.name from user configs (e.g., from JDBC URL)"""
        # Try to extract from JDBC URL
        if 'connection.url' in user_configs:
            jdbc_url = user_configs['connection.url']
            if jdbc_url.startswith('jdbc:'):
                parsed = self._parse_jdbc_url(jdbc_url)
                return parsed.get('host')

        # Check for direct database.server.name config
        if 'database.server.name' in user_configs:
            return user_configs['database.server.name']

        # Check for server name config
        if 'server.name' in user_configs:
            return user_configs['server.name']

    
    def _derive_input_key_format(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
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
            return format_key.upper()

        # Try to infer from schema registry configs
        if 'key.converter.schemas.enable' in user_configs:
            return 'JSON_SR'
        
        # Try to get default from template if available
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'input.key.format')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default.upper()
        
        # Default fallback
        return 'JSON'
    
    def _derive_input_data_format(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:

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
            return format_key.upper()

        # Try to infer from schema registry configs
        if 'value.converter.schemas.enable' in user_configs:
            return 'JSON_SR'
        
        # Try to get default from template if available
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'input.data.format')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default.upper()
        
        # Default fallback
        return 'JSON'
    
    def _derive_output_key_format(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:

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
            return format_key.upper()
        
        # Try to get default from template if available
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'output.key.format')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default.upper()
        
        # Default fallback
        return 'JSON'
    
    def _derive_output_data_format(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:

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
                return resolved_default.upper()
        
        # Default fallback
        return 'JSON'
    
    def _derive_output_data_key_format(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:

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
            return format_key.upper()

        # Try to infer from output key format if already derived
        if 'output.key.format' in fm_configs:
            return fm_configs['output.key.format']
        
        # Try to get default from template if available
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'output.data.key.format')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default.upper()
        
        # Default fallback
        return 'JSON'
    
    def _derive_output_data_value_format(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
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
            return format_key.upper()

        # Try to infer from output data format if already derived
        if 'output.data.format' in fm_configs:
            return fm_configs['output.data.format']
        
        # Try to get default from template if available
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'output.data.value.format')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default.upper()
        
        # Default fallback
        return 'JSON'
    
    def _derive_authentication_method(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:

        """Derive authentication.method from user configs"""
        # Check for various authentication-related configs
        auth_configs = [
            'security.protocol',
            'sasl.mechanism',
            'authentication.type',
            'auth.method'
        ]

        for auth_config in auth_configs:
            if auth_config in user_configs:
                auth_value = user_configs[auth_config].lower()
                if 'plain' in auth_value:
                    return 'PLAIN'
                elif 'scram' in auth_value:
                    return 'SCRAM'
                elif 'oauth' in auth_value or 'bearer' in auth_value:
                    return 'OAUTHBEARER'
                elif 'ssl' in auth_value or 'tls' in auth_value:
                    return 'SSL'
                else:
                    return auth_value.upper()
        
        # Try to get default from template if available
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'authentication.method')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default
        
        # Default fallback
        return 'PLAIN'
    
    def _derive_csfle_enabled(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:

        """Derive csfle.enabled from user configs"""
        # Try to get default from template if available
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'csfle.enabled')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default
        
        # Default fallback
        return 'false'
    
    def _derive_csfle_on_failure(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:

        """Derive csfle.onFailure from user configs"""
        # Try to get default from template if available
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'csfle.onFailure')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default
        
        # Default fallback
        return 'FAIL'
    
    def _derive_ssl_mode(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:

        """Derive ssl.mode from user configs"""
        # Check for direct ssl.mode config first
        if 'ssl.mode' in user_configs:
            value = user_configs['ssl.mode'].lower()
            # Map common SSL mode values
            if value in ['prefer', 'preferred']:
                return 'prefer'
            elif value in ['require', 'required']:
                return 'require'
            elif value in ['verify-ca', 'verifyca', 'verify_ca']:
                return 'verify-ca'
            elif value in ['verify-full', 'verifyfull', 'verify_full']:
                return 'verify-full'
            elif value in ['disabled', 'disable', 'false', 'none']:
                return 'disabled'

        # Check for database-specific SSL mode configs
        for config_key in [
            'connection.sslmode',  # PostgreSQL
            'connection.sslMode',  # MySQL
            'database.ssl.mode',   # MySQL CDC
            'redis.ssl.mode',      # Redis
            'ssl.enabled',         # Generic
            'use.ssl',             # Generic
            'ssl.use'              # Generic
        ]:
            if config_key in user_configs:
                value = user_configs[config_key].lower()
                # Map boolean values
                if value in ['true', 'yes', '1', 'enabled']:
                    return 'require'  # Default to require when SSL is enabled
                elif value in ['false', 'no', '0', 'disabled']:
                    return 'disabled'
                # Map string values
                elif value in ['prefer', 'preferred']:
                    return 'prefer'
                elif value in ['require', 'required']:
                    return 'require'
                elif value in ['verify-ca', 'verifyca', 'verify_ca']:
                    return 'verify-ca'
                elif value in ['verify-full', 'verifyfull', 'verify_full']:
                    return 'verify-full'

        # Check for SSL-related configs that might indicate SSL usage
        ssl_indicators = [
            'ssl.truststorefile', 'ssl.truststorepassword', 'ssl.rootcertfile',
            'connection.javax.net.ssl.trustStore', 'connection.javax.net.ssl.trustStorePassword',
            'ssl.truststore.file', 'ssl.truststore.password', 'ssl.cert.file',
            'ssl.key.file', 'ssl.ca.file', 'ssl.certificate.file'
        ]

        for indicator in ssl_indicators:
            if indicator in user_configs and user_configs[indicator]:
                # If SSL certificates/truststores are provided, likely need verify-ca or verify-full
                cert_value = user_configs[indicator].lower()
                if 'verify' in cert_value or 'cert' in cert_value:
                    return 'verify-ca'  # Default to verify-ca when certificates are provided
                else:
                    return 'require'  # Default to require when SSL files are provided

        # Check for connection URL that might indicate SSL
        if 'connection.url' in user_configs:
            url = user_configs['connection.url'].lower()
            if 'ssl=true' in url or 'sslmode=' in url or 'useSSL=true' in url:
                # Extract SSL mode from URL if present
                if 'sslmode=prefer' in url:
                    return 'prefer'
                elif 'sslmode=require' in url:
                    return 'require'
                elif 'sslmode=verify-ca' in url:
                    return 'verify-ca'
                elif 'sslmode=verify-full' in url:
                    return 'verify-full'
                elif 'sslmode=disable' in url or 'sslmode=disabled' in url:
                    return 'disabled'
                else:
                    return 'require'  # Default to require when SSL is enabled in URL
        
        # Try to get default from template if available
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'ssl.mode')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default
        
        # Default to prefer if no SSL configuration is found
        return 'prefer'
    
    def _derive_redis_hostname(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:

        """Derive redis.hostname from user configs"""
        # Check for direct redis.hostname config first
        if 'redis.hostname' in user_configs:
            return user_configs['redis.hostname']

        # Check for common Redis host configurations
        for config_key in [
            'redis.host', 'redis.server', 'redis.address', 'redis.endpoint',
            'host', 'server', 'address', 'endpoint'
        ]:
            if config_key in user_configs:
                value = user_configs[config_key]
                # If it's a host:port format, extract just the host
                if ':' in value:
                    host = value.split(':')[0]
                    return host
                return value

        # Check for redis.hosts config (format: host:port)
        if 'redis.hosts' in user_configs:
            hosts_value = user_configs['redis.hosts']
            if ':' in hosts_value:
                host = hosts_value.split(':')[0]
                return host

        # Check for connection URL that might contain Redis host
        for config_key in ['connection.url', 'connection.uri', 'redis.connection.url']:
            if config_key in user_configs:
                url = user_configs[config_key].lower()
                if 'redis://' in url:
                    # Extract host from Redis URL
                    # Format: redis://host:port/db
                    redis_part = url.replace('redis://', '')
                    if '@' in redis_part:
                        # Handle authentication: redis://user:pass@host:port/db
                        auth_part, host_part = redis_part.split('@', 1)
                        host = host_part.split('/')[0].split(':')[0]
                        return host
                    else:
                        host = redis_part.split('/')[0].split(':')[0]
                        return host

        return None
    
    def _derive_redis_portnumber(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:

        """Derive redis.portnumber from user configs"""
        # Check for direct redis.portnumber config first
        if 'redis.portnumber' in user_configs:
            return user_configs['redis.portnumber']

        # Check for common Redis port configurations
        for config_key in [
            'redis.port', 'redis.server.port', 'port', 'server.port'
        ]:
            if config_key in user_configs:
                return user_configs[config_key]

        # Check for redis.hosts config (format: host:port)
        if 'redis.hosts' in user_configs:
            hosts_value = user_configs['redis.hosts']
            if ':' in hosts_value:
                port = hosts_value.split(':')[1]
                # Remove any additional path or query parameters
                if '/' in port:
                    port = port.split('/')[0]
                return port

        # Check for connection URL that might contain Redis port
        for config_key in ['connection.url', 'connection.uri', 'redis.connection.url']:
            if config_key in user_configs:
                url = user_configs[config_key].lower()
                if 'redis://' in url:
                    # Extract port from Redis URL
                    # Format: redis://host:port/db
                    redis_part = url.replace('redis://', '')
                    if '@' in redis_part:
                        # Handle authentication: redis://user:pass@host:port/db
                        auth_part, host_part = redis_part.split('@', 1)
                        host_port = host_part.split('/')[0]
                        if ':' in host_port:
                            port = host_port.split(':')[1]
                            return port
                    else:
                        host_port = redis_part.split('/')[0]
                        if ':' in host_port:
                            port = host_port.split(':')[1]
                            return port
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'redis.portnumber')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default
        # Default Redis port
        return '6379'
    
    def _derive_redis_ssl_mode(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:

        """Derive redis.ssl.mode from user configs"""
        # Check for direct redis.ssl.mode config first
        if 'redis.ssl.mode' in user_configs:
            value = user_configs['redis.ssl.mode'].lower()
            # Map common SSL mode values to Redis SSL mode values
            if value in ['disabled', 'disable', 'false', 'none', 'off']:
                return 'disabled'
            elif value in ['enabled', 'enable', 'true', 'on']:
                return 'enabled'
            elif value in ['server', 'server-only', 'verify-server']:
                return 'server'
            elif value in ['server+client', 'server+client', 'mutual', 'two-way']:
                return 'server+client'
            else:
                # Return as-is if it's already a valid Redis SSL mode
                return user_configs['redis.ssl.mode']

        # Check for Redis SSL enabled flag
        for config_key in ['redis.ssl.enabled', 'redis.ssl', 'ssl.enabled', 'use.ssl']:
            if config_key in user_configs:
                value = user_configs[config_key].lower()
                if value in ['true', 'yes', '1', 'enabled', 'on']:
                    return 'enabled'
                elif value in ['false', 'no', '0', 'disabled', 'off']:
                    return 'disabled'

        # Check for SSL-related configs that might indicate SSL usage
        ssl_indicators = [
            'redis.ssl.keystore.file', 'redis.ssl.keystore.password',
            'redis.ssl.truststore.file', 'redis.ssl.truststore.password',
            'redis.ssl.cert.file', 'redis.ssl.key.file', 'redis.ssl.ca.file'
        ]

        for indicator in ssl_indicators:
            if indicator in user_configs and user_configs[indicator]:
                # If SSL certificates/keystores are provided, determine the mode
                cert_value = user_configs[indicator].lower()
                if 'client' in cert_value or 'keystore' in indicator:
                    return 'server+client'  # Client certificates indicate mutual auth
                else:
                    return 'server'  # Server certificates only

        # Check for connection URL that might indicate SSL
        for config_key in ['connection.url', 'connection.uri', 'redis.connection.url']:
            if config_key in user_configs:
                url = user_configs[config_key].lower()
                if 'rediss://' in url:  # Redis with SSL
                    return 'enabled'
                elif 'redis://' in url and 'ssl=true' in url:
                    return 'enabled'
                elif 'redis://' in url and 'ssl=false' in url:
                    return 'disabled'
        
        # Try to get default from template if available
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'redis.ssl.mode')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default

        # Default to disabled if no SSL configuration is found
        return 'disabled'

    def _derive_servicebus_namespace(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> str:
        if 'azure.servicebus.namespace' in user_configs:
            return user_configs['azure.servicebus.namespace']

        # Try to extract from connection string if present
        conn_str = user_configs.get('azure.servicebus.connection.string')
        if conn_str:
            match = re.search(r'Endpoint=sb://([^.]+)\.servicebus\.windows\.net/', conn_str)
            if match:
                return match.group(1)
        return user_configs.get('azure.servicebus.namespace')

    def _derive_azure_servicebus_sas_keyname(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> str:
        if 'azure.servicebus.sas.keyname' in user_configs:
            return user_configs['azure.servicebus.sas.keyname']

        # Try to extract from connection string if present
        conn_str = user_configs.get('azure.servicebus.connection.string')
        if conn_str:
            match = re.search(r'SharedAccessKeyName=([^;]+)', conn_str)
            if match:
                return match.group(1)
        return user_configs.get('azure.servicebus.sas.keyname')

    def _derive_azure_servicebus_sas_key(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> str:
        if 'azure.servicebus.sas.key' in user_configs:
            return user_configs['azure.servicebus.sas.key']

        # Try to extract from connection string if present
        conn_str = user_configs.get('azure.servicebus.connection.string')
        if conn_str:
            match = re.search(r'SharedAccessKey=([^;]+)', conn_str)
            if match:
                return match.group(1)
        return user_configs.get('azure.servicebus.sas.key')

    def _derive_azure_servicebus_entity_name(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> str:
        if 'azure.servicebus.entity.name' in user_configs:
            return user_configs['azure.servicebus.entity.name']

        # Try to extract from connection string if present
        conn_str = user_configs.get('azure.servicebus.connection.string')
        if conn_str:
            match = re.search(r'EntityPath=([^;]+)', conn_str)
            if match:
                return match.group(1)
        return user_configs.get('azure.servicebus.entity.name')

    def _derive_subject_name_strategy(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive subject name strategy from user configs by extracting recommended values from template config def"""
        
        # Get recommended values from template config definition for the specific config
        recommended_strategies = []
        if template_config_defs and config_name:
            for template_config_def in template_config_defs:
                if isinstance(template_config_def, dict) and template_config_def.get('name') == config_name:
                    recommended_values = template_config_def.get('recommended_values', [])
                    if recommended_values:
                        recommended_strategies.extend(recommended_values)
                        break
        
        # Fallback to common recommended values if not found in template
        if not recommended_strategies:
            recommended_strategies = [
                "TopicNameStrategy",
                "RecordNameStrategy", 
                "TopicRecordNameStrategy"
            ]
        
        # Look for the specific config in user configs
        if config_name and config_name in user_configs:
            config_value = user_configs[config_name]
            # Extract config value by finding last . and get string after that
            if '.' in config_value:
                config_value = config_value.split('.')[-1]
            # Check if any recommended strategy is contained in the config value
            for strategy in recommended_strategies:
                if strategy.lower() == config_value.lower():
                    return strategy
        
        return None

    def _derive_reference_subject_name_strategy(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive reference subject name strategy from user configs by extracting recommended values from template config def"""
        
        # Get recommended values from template config definition for the specific config
        recommended_strategies = []
        if template_config_defs and config_name:
            for template_config_def in template_config_defs:
                if isinstance(template_config_def, dict) and template_config_def.get('name') == config_name:
                    recommended_values = template_config_def.get('recommended_values', [])
                    if recommended_values:
                        recommended_strategies.extend(recommended_values)
                        break
        
        # Fallback to common recommended values if not found in template
        if not recommended_strategies:
            recommended_strategies = [
                "DefaultReferenceSubjectNameStrategy",
                "QualifiedReferenceSubjectNameStrategy"
            ]
        
        # Look for the specific config in user configs
        if config_name and config_name in user_configs:
            config_value = user_configs[config_name]
            # Extract config value by finding last . and get string after that
            if '.' in config_value:
                config_value = config_value.split('.')[-1]
            # Check if any recommended strategy is contained in the config value
            for strategy in recommended_strategies:
                if strategy.lower() == config_value.lower():
                    return strategy
        
        return None

    def _apply_reverse_switch(self, switch_mapping: Dict[str, str], user_value: str) -> Optional[str]:
        """Apply reverse switch (following Java pattern)"""
        for switch_key, switch_value in switch_mapping.items():
            if switch_value == user_value:
                # If the matched key is "default", return None
                if switch_key == "default":
                    return None
                return switch_key
        return None

    def _do_semantic_matching(self, fm_configs: Dict[str, str], semantic_match_list: set, user_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]], sm_template: Dict[str, Any]):
        """
        Perform semantic matching for configs that are not found in the template.

        Args:
            fm_configs: Dictionary of FM configurations
            semantic_match_list: Set of config names that need semantic matching
            user_configs: Dictionary of user configurations
            template_config_defs: List of template config definitions
        """
        if not semantic_match_list:
            self.logger.info("No configs require semantic matching")
            return

        self.logger.info(f"Performing semantic matching for {len(semantic_match_list)} configs: {', '.join(semantic_match_list)}")

        # Log SM template information
        self.logger.info(f"SM Template info for semantic matching:")
        if sm_template:
            self.logger.info(f"  - SM template type: {type(sm_template)}")
            self.logger.info(f"  - SM template keys: {list(sm_template.keys()) if isinstance(sm_template, dict) else 'Not a dict'}")
            if isinstance(sm_template, dict) and 'config_defs' in sm_template:
                self.logger.info(f"  - SM config_defs count: {len(sm_template['config_defs'])}")
            if isinstance(sm_template, dict) and 'sections' in sm_template:
                self.logger.info(f"  - SM sections count: {len(sm_template['sections'])}")
        else:
            self.logger.info(f"  - SM template is None/empty")

        # Get FM properties for matching (as dictionary)
        fm_properties_dict = {}
        for template_config_def in template_config_defs:
            fm_properties_dict[template_config_def.get('name')] = template_config_def

        self.logger.info(f"FM properties available for matching: {len(fm_properties_dict)}")

        # Perform semantic matching for each config in the list
        for config_name in semantic_match_list:
            if config_name in user_configs:
                user_value = user_configs[config_name]
                self.logger.info(f"Processing semantic match for '{config_name}' = '{user_value}'")



                # Get SM property from SM template
                sm_prop = self._get_sm_property_from_template(config_name, sm_template)

                if not sm_prop:
                    # Fallback to generic property if not found in SM template
                    self.logger.info(f"SM property '{config_name}' not found in template, using fallback")
                    sm_prop = {
                        'name': config_name,
                        'description': f"User config: {config_name}",
                        'type': 'STRING',
                        'section': 'General'
                    }
                else:
                    self.logger.info(f"Found SM property '{config_name}' in template: {sm_prop}")

                # Find best match using semantic matching with threshold
                result = self.semantic_matcher.find_best_match(sm_prop, fm_properties_dict, semantic_threshold=0.7)

                if result and result.matched_fm_property:
                    # Find the property name from the matched property info
                    fm_prop_name = None
                    for prop_name, prop_info in fm_properties_dict.items():
                        if prop_info == result.matched_fm_property:
                            fm_prop_name = prop_name
                            break

                    if fm_prop_name and fm_prop_name not in fm_configs:
                        fm_configs[fm_prop_name] = user_value
                        self.logger.info(f"Semantic match: {config_name} -> {fm_prop_name} (score: {result.similarity_score:.3f})")
                    elif fm_prop_name in fm_configs:
                        self.logger.debug(f"Skipping semantic mapping for {config_name} -> {fm_prop_name} as {fm_prop_name} is already mapped")
                    else:
                        self.logger.warning(f"Could not determine property name for matched property: {config_name}")
                else:
                    self.logger.warning(f"No semantic match found for config: {config_name}")
            else:
                self.logger.warning(f"Config {config_name} not found in user configs for semantic matching")

        self.logger.info(f"Semantic matching completed for {len(semantic_match_list)} configs")

    def _check_required_configs(self, fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]], errors: List[str]):
        """
        Check for required configs that are missing from FM configs after semantic matching.

        Args:
            fm_configs: Dictionary of FM configurations
            template_config_defs: List of template config definitions
            errors: List to add error messages to
        """
        self.logger.info("Checking for required configs that are missing from FM configs")

        for template_config_def in template_config_defs:
            config_name = template_config_def.get('name')
            required_value = template_config_def.get('required', False)
            is_internal = template_config_def.get('internal', False)

            # Handle both string and boolean values for 'required' field
            is_required = False
            if isinstance(required_value, bool):
                is_required = required_value
            elif isinstance(required_value, str):
                is_required = required_value.lower() == 'true'



            # Skip internal configs as they are handled automatically
            if is_internal:
                continue

            # Check if this required config is missing from FM configs
            if is_required and config_name not in fm_configs:
                # Check if this config has a default value
                default_value = template_config_def.get('default_value')
                if default_value is not None:
                    # Use the default value for this required config
                    fm_configs[config_name] = str(default_value)
                    self.logger.info(f"Required config '{config_name}' missing but has default value '{default_value}' - using default")
                else:
                    # No default value available, add error
                    error_msg = f"Required FM Config '{config_name}' could not be derived from given configs."
                    # Check if this error message is already in the errors list to prevent duplicates
                    if error_msg not in errors:
                        errors.append(error_msg)
                        self.logger.warning(f"Required config '{config_name}' missing from fm_configs and no default value available. Available keys: {list(fm_configs.keys())}")
                    else:
                        self.logger.debug(f"Duplicate error message for '{config_name}' already exists, skipping")
            elif is_required and config_name in fm_configs:
                self.logger.info(f"Required config '{config_name}' found in fm_configs with value: {fm_configs[config_name]}")

            # Check if FM config value is part of recommended values (if config exists and has recommended values)
            if config_name in fm_configs:
                fm_config_value = fm_configs[config_name]
                recommended_values = template_config_def.get('recommended_values', [])

                if recommended_values and fm_config_value not in recommended_values:
                    # Try case-insensitive matching for enum-like values
                    fm_config_value_lower = fm_config_value.lower() if isinstance(fm_config_value, str) else str(fm_config_value).lower()
                    recommended_values_lower = [str(v).lower() for v in recommended_values]
                    
                    if fm_config_value_lower not in recommended_values_lower:
                        error_msg = f"FM Config '{config_name}' value '{fm_config_value}' is not in the recommended values list: {recommended_values}"
                        # Check if this error message is already in the errors list to prevent duplicates
                        if error_msg not in errors:
                            errors.append(error_msg)
                            self.logger.warning(f"Value '{fm_config_value}' for '{config_name}' not in recommended values (case-insensitive check also failed)")
                        else:
                            self.logger.debug(f"Duplicate error message for '{config_name}' recommended values already exists, skipping")
                    else:
                        # Case-insensitive match found - log this for debugging
                        self.logger.info(f"Case-insensitive match found for '{config_name}': '{fm_config_value}' matches one of {recommended_values}")
                else:
                    # Value is in recommended values (case-sensitive match)
                    self.logger.debug(f"Value '{fm_config_value}' for '{config_name}' is in recommended values")

    def _get_sm_property_from_template(self, config_name: str, sm_template: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Get SM property definition from SM template.

        Args:
            config_name: Name of the config to find
            sm_template: SM template dictionary

        Returns:
            SM property definition or None if not found
        """
        self.logger.debug(f"Looking for SM property '{config_name}' in template")

        if not sm_template:
            self.logger.debug(f"SM template is empty or None")
            return None

        # Handle different SM template structures
        available_keys = list(sm_template.keys()) if isinstance(sm_template, dict) else []
        self.logger.debug(f"SM template keys: {available_keys}")

        # Try configs (newer format)
        if 'configs' in sm_template:
            self.logger.debug(f"Searching in 'configs' (count: {len(sm_template['configs'])})")
            for config_def in sm_template['configs']:
                if isinstance(config_def, dict) and config_def.get('name') == config_name:
                    self.logger.debug(f"Found SM property '{config_name}' in configs")
                    self.logger.debug(f"Property details: {config_def}")
                    return config_def

        # Try groups (newer format)
        if 'groups' in sm_template:
            groups = sm_template['groups']
            self.logger.debug(f"Searching in 'groups' (count: {len(groups)})")
            for group in groups:
                if isinstance(group, dict):
                    group_name = group.get('name', 'Unknown')
                    group_configs = group.get('configs', [])
                    self.logger.debug(f"Searching in group '{group_name}' (configs: {len(group_configs)})")

                    for config_def in group_configs:
                        if isinstance(config_def, dict) and config_def.get('name') == config_name:
                            self.logger.debug(f"Found SM property '{config_name}' in group '{group_name}'")
                            self.logger.debug(f"Property details: {config_def}")
                            return config_def

        # Try sections (older format)
        if 'sections' in sm_template:
            sections = sm_template['sections']
            self.logger.debug(f"Searching in 'sections' (count: {len(sections)})")
            for section in sections:
                if isinstance(section, dict):
                    section_name = section.get('name', 'Unknown')
                    section_configs = section.get('config_defs', [])
                    self.logger.debug(f"Searching in section '{section_name}' (configs: {len(section_configs)})")

                    for config_def in section_configs:
                        if isinstance(config_def, dict) and config_def.get('name') == config_name:
                            self.logger.debug(f"Found SM property '{config_name}' in section '{section_name}'")
                            self.logger.debug(f"Property details: {config_def}")
                            return config_def

        self.logger.debug(f"SM property '{config_name}' not found in template")
        return None

    def _load_semantic_matcher_from_path(self):
        """Load semantic matcher class from the specified path"""
        if not self.semantic_matcher_path:
            return

        try:
            semantic_matcher_file = Path(self.semantic_matcher_path)
            if not semantic_matcher_file.exists():
                self.logger.warning(f"Semantic matcher file not found: {self.semantic_matcher_path}")
                return

            self.logger.info(f"Loading semantic matcher from: {self.semantic_matcher_path}")

            # Import the custom semantic matcher module
            import importlib.util
            spec = importlib.util.spec_from_file_location("custom_semantic_matcher", semantic_matcher_file)
            custom_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(custom_module)

            # Get the SemanticMatcher class from the custom module
            if hasattr(custom_module, 'SemanticMatcher'):
                CustomSemanticMatcher = custom_module.SemanticMatcher
                self.semantic_matcher = CustomSemanticMatcher()
                self.logger.info(f"Successfully loaded custom semantic matcher from {self.semantic_matcher_path}")
            else:
                self.logger.error(f"SemanticMatcher class not found in {self.semantic_matcher_path}")

        except Exception as e:
            self.logger.error(f"Error loading semantic matcher from {self.semantic_matcher_path}: {e}")
            # Continue with default semantic matcher

    def _is_placeholder(self, value: str) -> bool:
        """Check if a value is a placeholder like ${xxxx}"""
        return value.startswith('${')
    
    def _extract_placeholder_name(self, placeholder: str) -> str:
        """Extract the placeholder name from ${xxxx} format"""
        if placeholder.startswith('${'):
            # Find the closing } or use the whole string after ${
            end_pos = placeholder.find('}', 2)
            if end_pos != -1:
                return placeholder[2:end_pos]  # Remove ${ and }
            else:
                return placeholder[2:]  # Remove ${ only
        return placeholder
    
    def _resolve_template_default(self, template_default: str, fm_configs: Dict[str, str]) -> str:
        """Resolve template default value, handling placeholders like ${xxxx}"""
        if self._is_placeholder(template_default):
            placeholder_name = self._extract_placeholder_name(template_default)
            if placeholder_name in fm_configs:
                resolved_value = fm_configs[placeholder_name]
                return resolved_value
            else:
                self.logger.warning(f"   ⚠️ Placeholder '{placeholder_name}' not found in fm_configs")
        return template_default

    def _get_template_default_value(self, template_config_defs: List[Dict[str, Any]], config_name: str) -> Optional[str]:
        """Extract default value for a configuration from template definitions"""
        for template_config_def in template_config_defs:
            if template_config_def.get('name') == config_name:
                default_value = template_config_def.get('default_value')
                if default_value is not None:
                    return str(default_value)
        return None

    def _derive_connection_url(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None) -> Optional[str]:
        """Derive connection.url from user configs specifically for Snowflake connectors"""
        
        
        # Check for JDBC URL patterns that might contain Snowflake URLs
        jdbc_patterns = [
            'connection.url'
        ]
        
        for pattern in jdbc_patterns:
            if pattern in user_configs:
                jdbc_url = user_configs[pattern]
                if jdbc_url and 'jdbc:snowflake://' in jdbc_url:
                    # Extract Snowflake connection string by removing jdbc:snowflake:// prefix
                    snowflake_connection_string = jdbc_url.replace('jdbc:snowflake://', '')
                    return snowflake_connection_string.strip()
                elif jdbc_url and jdbc_url.startswith('jdbc:'):
                    # For non-Snowflake JDBC URLs, return null
                    return None
        
        # Try to get default from template if available
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'connection.url')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default
        
        return None
    

    