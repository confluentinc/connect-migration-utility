import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
import re
from datetime import datetime
from semantic_matcher import SemanticMatcher, Property
import base64
import requests

class ConnectorComparator:
    def __init__(self, input_file: Path, output_dir: Path, sm_template_dir: Path = None, fm_template_dir: Path = None):
        self.logger = logging.getLogger(__name__)
        self.input_file = input_file
        self.output_dir = output_dir
        self.fm_configs_dir = output_dir / 'fm_configs'
        self.semantic_matcher = SemanticMatcher()
        
        # Template directories
        self.sm_template_dir = sm_template_dir
        self.fm_template_dir = fm_template_dir
        
        # Template path cache
        self.template_path_cache = {}
        
        # User selected templates cache
        self.user_selected_templates = {}
        
        # Load template files
        self.sm_templates = self._load_templates(sm_template_dir) if sm_template_dir else {}
        self.fm_templates = self._load_templates(fm_template_dir) if fm_template_dir else {}
        
        # Build connector.class to template mapping
        self.connector_class_to_template = self._build_connector_class_mapping()
        
        # Database type mappings
        self.jdbc_database_types = {
            'mysql': {
                'url_patterns': ['mysql', 'mariadb'],
                'default_port': '3306',
                'property_mappings': {
                    'connection.host': 'host',
                    'connection.port': 'port',
                    'connection.user': 'user',
                    'connection.password': 'password',
                    'db.name': 'db_name'
                }
            },
            'oracle': {
                'url_patterns': ['oracle', 'oracle:thin'],
                'default_port': '1521',
                'property_mappings': {
                    'connection.host': 'host',
                    'connection.port': 'port',
                    'connection.user': 'user',
                    'connection.password': 'password',
                    'db.name': 'service_name'
                }
            },
            'sqlserver': {
                'url_patterns': ['sqlserver', 'mssql'],
                'default_port': '1433',
                'property_mappings': {
                    'connection.host': 'host',
                    'connection.port': 'port',
                    'connection.user': 'user',
                    'connection.password': 'password',
                    'db.name': 'databaseName'
                }
            },
            'postgresql': {
                'url_patterns': ['postgresql', 'postgres'],
                'default_port': '5432',
                'property_mappings': {
                    'connection.host': 'host',
                    'connection.port': 'port',
                    'connection.user': 'user',
                    'connection.password': 'password',
                    'db.name': 'db_name'
                }
            }
        }

    def encode_to_base64(input_string):
        # Convert the input string to bytes
        byte_data = input_string.encode('utf-8')
        # Encode the bytes to base64
        base64_bytes = base64.b64encode(byte_data)
        # Convert the base64 bytes back to string
        base64_string = base64_bytes.decode('utf-8')
        return base64_string

    def extract_transforms_config(config_dict):
        # Extract all keys starting with "transforms"
        return {k: v for k, v in config_dict.items() if k.startswith("transforms")}

    def extract_recommended_transform_types(response_json):
        configs = response_json.get("configs", [])
        for config in configs:
            value = config.get("value", {})
            if value.get("name") == "transforms.transform_0.type":
                return value.get("recommended_values", [])
        return []

    def get_FM_SMT(self, plugin_type) -> set[str]:
        env_id = "env-yodk3k"
        lkc_id = "lkc-nww1j6"
        bearer_token = "G42O4SU5RQG5QP4T:xAqK3hQ9RZdss2zEr7G3St0Hviv+WnPAao+Ni3cay+n6TNfChmE/o36tZSuJv0ER"

        url = (
            f"https://confluent.cloud/api/internal/accounts/env-yodk3k/clusters/lkc-nww1j6/connector-plugins/DatagenSource/config/validate"
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
            "Authorization": f"Basic {encode_to_base64(bearer_token)}"
        }
        response = requests.put(url, params=params, json=data, headers=headers)
        response.raise_for_status()
        recommended_values = extract_recommended_transform_types(response.json())
        return recommended_values

    def get_transforms_config(self, config: Dict[str, Any], plugin_type: str) -> Dict[str, Dict[str, Any]]:

        fm_smt = get_FM_SMT(plugin_type)

        return classify_transform_configs_with_full_chain(config, fm_smt)

    def classify_transform_configs_with_full_chain(
        config: Dict[str, Any],
        allowed_transform_types: set
    ) -> Dict[str, Dict[str, Any]]:
        result: Dict[str, Dict[str, Any]] = {
            'allowed': {},
            'disallowed': {}
        }

        transform_chain = config.get("transforms", "")
        aliases = [alias.strip() for alias in transform_chain.split(",") if alias.strip()]

        allowed_aliases = []
        disallowed_aliases = []

        for alias in aliases:
            type_key = f"transforms.{alias}.type"
            transform_type = config.get(type_key)

            if not transform_type:
                disallowed_aliases.append(alias)
                for k, v in config.items():
                    if k.startswith(f"transforms.{alias}."):
                        result['disallowed'][k] = v
                continue

            if transform_type in allowed_transform_types:
                allowed_aliases.append(alias)
                for k, v in config.items():
                    if k.startswith(f"transforms.{alias}."):
                        result['allowed'][k] = v
            else:
                disallowed_aliases.append(alias)
                for k, v in config.items():
                    if k.startswith(f"transforms.{alias}."):
                        result['disallowed'][k] = v

        if allowed_aliases:
            result['allowed']["transforms"] = ", ".join(allowed_aliases)
        if disallowed_aliases:
            result['disallowed']["transforms"] = ", ".join(disallowed_aliases)

        return result

    def _build_connector_class_mapping(self) -> Dict[str, Dict[str, List[str]]]:
        """Build mapping of connector.class to template paths"""
        mapping = {}
        
        # Map SM templates
        if self.sm_template_dir:
            for template_file in self.sm_template_dir.glob('*.json'):
                try:
                    with open(template_file, 'r') as f:
                        template_data = json.load(f)
                        if 'connector.class' in template_data:
                            connector_class = template_data['connector.class']
                            if connector_class not in mapping:
                                mapping[connector_class] = {
                                    'sm_templates': [],
                                    'fm_templates': []
                                }
                            mapping[connector_class]['sm_templates'].append(str(template_file))
                except Exception as e:
                    self.logger.error(f"Error reading template {template_file}: {str(e)}")
        
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
                                    'sm_templates': [],
                                    'fm_templates': []
                                }
                            mapping[connector_class]['fm_templates'].append(str(template_file))
                except Exception as e:
                    self.logger.error(f"Error reading template {template_file}: {str(e)}")
        
        return mapping

    def _get_user_template_selection(self, connector_class: str, template_type: str, template_paths: List[str]) -> str:
        """Ask user to select a template when multiple options are available"""
        cache_key = f"{connector_class}_{template_type}"
        
        # Check if user has already selected this template
        if cache_key in self.user_selected_templates:
            return self.user_selected_templates[cache_key]
        
        print(f"\nMultiple {template_type.upper()} templates found for connector class: {connector_class}")
        print("Available templates:")
        for i, path in enumerate(template_paths, 1):
            print(f"{i}. {path}")
        
        while True:
            try:
                choice = int(input(f"\nPlease select a {template_type.upper()} template (1-{len(template_paths)}): "))
                if 1 <= choice <= len(template_paths):
                    selected_path = template_paths[choice - 1]
                    self.user_selected_templates[cache_key] = selected_path
                    return selected_path
                else:
                    print(f"Please enter a number between 1 and {len(template_paths)}")
            except ValueError:
                print("Please enter a valid number")

    def _get_manual_template_path(self, connector_class: str, template_type: str) -> Optional[str]:
        """Ask user to manually enter template path when no templates are found"""
        cache_key = f"{connector_class}_{template_type}"
        
        # Check if user has already provided a path
        if cache_key in self.user_selected_templates:
            return self.user_selected_templates[cache_key]
        
        print(f"\nNo {template_type.upper()} templates found for connector class: {connector_class}")
        while True:
            path = input(f"Please enter the path to the {template_type.upper()} template file (or press Enter to skip): ").strip()
            if not path:  # User pressed Enter
                return None
            if Path(path).exists():
                self.user_selected_templates[cache_key] = path
                return path
            print(f"File not found: {path}")

    def _get_templates_for_connector(self, connector_class: str) -> tuple[Dict[str, Any], Dict[str, Any]]:
        """Get SM and FM templates for a connector class"""
        # Check cache first
        if connector_class in self.template_path_cache:
            sm_path, fm_path = self.template_path_cache[connector_class]
            self.logger.info(f"Using cached templates for {connector_class}:")
            self.logger.info(f"  SM Template: {sm_path}")
            self.logger.info(f"  FM Template: {fm_path}")
            return self.sm_templates.get(sm_path, {}), self.fm_templates.get(fm_path, {})
        
        # Get template paths from mapping
        template_info = self.connector_class_to_template.get(connector_class, {})
        sm_templates = template_info.get('sm_templates', [])
        fm_templates = template_info.get('fm_templates', [])
        
        # Handle SM templates
        if not sm_templates:
            self.logger.warning(f"No SM templates found for connector class: {connector_class}")
            sm_template_path = self._get_manual_template_path(connector_class, 'sm')
            if sm_template_path:
                # Load the manually specified template
                try:
                    with open(sm_template_path, 'r') as f:
                        self.sm_templates[sm_template_path] = json.load(f)
                except Exception as e:
                    self.logger.error(f"Error loading manual SM template {sm_template_path}: {str(e)}")
                    sm_template_path = None
        else:
            sm_template_path = sm_templates[0] if len(sm_templates) == 1 else self._get_user_template_selection(connector_class, 'sm', sm_templates)
        
        # Handle FM templates
        if not fm_templates:
            self.logger.warning(f"No FM templates found for connector class: {connector_class}")
            fm_template_path = self._get_manual_template_path(connector_class, 'fm')
            if fm_template_path:
                # Load the manually specified template
                try:
                    with open(fm_template_path, 'r') as f:
                        self.fm_templates[fm_template_path] = json.load(f)
                except Exception as e:
                    self.logger.error(f"Error loading manual FM template {fm_template_path}: {str(e)}")
                    fm_template_path = None
        else:
            fm_template_path = fm_templates[0] if len(fm_templates) == 1 else self._get_user_template_selection(connector_class, 'fm', fm_templates)
        
        # If either template is missing, return empty templates
        if not sm_template_path or not fm_template_path:
            self.logger.error(f"Missing required templates for {connector_class}")
            return {}, {}
        
        # Log template selection
        self.logger.info(f"Selected templates for {connector_class}:")
        self.logger.info(f"  SM Template: {sm_template_path}")
        self.logger.info(f"  FM Template: {fm_template_path}")
        
        # Cache the paths
        self.template_path_cache[connector_class] = (sm_template_path, fm_template_path)
        
        # Return templates
        return self.sm_templates.get(sm_template_path, {}), self.fm_templates.get(fm_template_path, {})

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
        if 'connection.url' in config:
            url = config['connection.url'].lower()
            for db_type, info in self.jdbc_database_types.items():
                if any(pattern in url for pattern in info['url_patterns']):
                    return db_type
                    
        # Check specific database type config if available
        if 'database.type' in config:
            return config['database.type'].lower()
            
        return 'unknown'

    def _parse_jdbc_url(self, url: str) -> Dict[str, str]:
        """Parse JDBC URL to extract connection details from template-style URL"""
        url = url.lower()
        connection_info = {}
        
        self.logger.debug(f"Parsing JDBC URL: {url}")
        
        # Extract host
        host_match = re.search(r'//(\${connection\.host}|[^:/]+)', url)
        if host_match:
            host = host_match.group(1)
            if host.startswith('${') and host.endswith('}'):
                connection_info['host'] = host[2:-1]  # Remove ${ and }
            else:
                connection_info['host'] = host
            self.logger.debug(f"Extracted host: {connection_info['host']}")
        
        # Extract port
        port_match = re.search(r':(\${connection\.port}|\d+)', url)
        if port_match:
            port = port_match.group(1)
            if port.startswith('${') and port.endswith('}'):
                connection_info['port'] = port[2:-1]  # Remove ${ and }
            else:
                connection_info['port'] = port
            self.logger.debug(f"Extracted port: {connection_info['port']}")
        
        # Extract database name
        db_match = re.search(r'/([^/?]+)(?:\?|$)', url)
        if db_match:
            db_name = db_match.group(1)
            if db_name.startswith('${') and db_name.endswith('}'):
                connection_info['db_name'] = db_name[2:-1]  # Remove ${ and }
            else:
                connection_info['db_name'] = db_name
            self.logger.debug(f"Extracted db_name: {connection_info['db_name']}")
        
        return connection_info

    def _map_jdbc_properties(self, config: Dict[str, Any], db_type: str) -> Dict[str, Any]:
        """Map JDBC properties to database-specific properties"""
        self.logger.debug(f"Mapping JDBC properties for config: {config}")
        
        # Get database-specific property mappings
        db_info = self.jdbc_database_types.get(db_type, {})
        property_mappings = db_info.get('property_mappings', {})
        
        # Parse JDBC URL and map properties
        if 'connection.url' in config:
            connection_info = self._parse_jdbc_url(config['connection.url'])
            
            # Map connection details to database-specific properties
            mapped_config = {}
            for fm_prop, jdbc_prop in property_mappings.items():
                if jdbc_prop in connection_info:
                    mapped_config[fm_prop] = connection_info[jdbc_prop]
                    self.logger.debug(f"Mapped {jdbc_prop} to {fm_prop}")
            
            return mapped_config
        
        return {}

    def _get_required_properties(self, fm_template: Dict[str, Any]) -> Dict[str, Any]:
        """Extract required properties from FM template"""
        required_props = {}
        for prop_name, prop_info in fm_template.get('properties', {}).items():
            if prop_info.get('required', False):
                required_props[prop_name] = prop_info
        return required_props

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
        sm_template, fm_template = self._get_templates_for_connector(connector_class)
        
        # Initialize mapped config with name
        mapped_config = {'name': name}
        mapping_errors = []
        unmapped_configs = []
        
        # Map properties based on connector type
        if 'connection.url' in config and config['connection.url'].startswith('jdbc:'):
            connector_type = self._get_database_type(config)
            jdbc_mapped = self._map_jdbc_properties(config, connector_type)
            mapped_config.update(jdbc_mapped)
        
        # Apply template mappings if available
        if sm_template and fm_template:
            self.logger.debug(f"Applying template mappings for {connector_class}")
            
            # Get required properties from FM template
            required_props = self._get_required_properties(fm_template)
            plugin_type = fm_template.get('template_id', {})
            
            # First handle required properties
            for prop_name, prop_info in required_props.items():
                # Skip connector.class and name as they're already handled
                if prop_name in ['connector.class', 'name']:
                    continue
                
                # Check if property is in input config
                if prop_name in config:
                    mapped_config[prop_name] = config[prop_name]
                    self.logger.info(f"Using input value for required property: {prop_name}")
                # Check if property has a default value
                elif prop_info.get('default') is not None:
                    mapped_config[prop_name] = prop_info['default']
                    self.logger.info(f"Using default value for required property: {prop_name}")
                else:
                    # Required property with no default value and not in input
                    error_msg = f"Required property '{prop_name}' needs a value but none was provided in input config or default value"
                    mapping_errors.append(error_msg)
                    self.logger.error(error_msg)
            transforms_data = get_transforms_config(config, plugin_type)
            mapped_config.update(transforms_data['allowed'])
            for key, value in transforms_data['disallowed'].items():
                if key.endswith(".type"):
                    alias = key.split(".")[1]
                    error_msg = f"Transform not allowed in Cloud '{alias}:{value}'. Potentially Custom SMT can be used."
                    mapping_errors.append(error_msg)
            # Then map properties that exist in the input config
            for sm_prop_name, sm_prop_value in config.items():
                try:
                    # Skip connector.class and name as they're already handled
                    if sm_prop_name in ['connector.class', 'name']:
                        continue
                    
                    # Skip if we already handled this property as a required property
                    if sm_prop_name in mapped_config:
                        continue
                    
                    self.logger.debug(f"\nProcessing property: {sm_prop_name}")
                    self.logger.debug(f"Property value: {sm_prop_value}")
                    
                    # Step 1: Try exact name match first
                    if sm_prop_name in fm_template.get('properties', {}):
                        # Direct match found
                        mapped_config[sm_prop_name] = sm_prop_value
                        self.logger.info(f"Direct match found for property: {sm_prop_name}")
                    else:
                        # Step 2: Try semantic matching
                        sm_prop = {
                            'name': sm_prop_name,
                            'description': sm_template.get('documentation', ''),
                            'type': sm_template.get('type', 'STRING'),
                            'section': sm_template.get('group', 'General')
                        }
                        
                        # Create property object for semantic matching
                        sm_property = Property(
                            name=sm_prop_name,
                            description=sm_template.get('documentation', ''),
                            type=sm_template.get('type', 'STRING'),
                            section=sm_template.get('group', 'General')
                        )
                        
                        # Get FM properties for matching
                        fm_properties = []
                        for prop_name, prop_info in fm_template.get('properties', {}).items():
                            fm_properties.append(Property(
                                name=prop_name,
                                description=prop_info.get('description', ''),
                                type=prop_info.get('type', 'STRING'),
                                section=prop_info.get('section', 'General')
                            ))
                        
                        # Find best match using semantic matching
                        result = self.semantic_matcher.find_best_match(sm_property, fm_properties)
                        
                        if result and result.matched_fm_property:
                            fm_prop_name = result.matched_fm_property.name
                            mapped_config[fm_prop_name] = sm_prop_value
                            self.logger.info(f"Successfully mapped {sm_prop_name} to {fm_prop_name} using {result.match_type} matching")
                        else:
                            error_msg = f"No match found for property: {sm_prop_name}"
                            mapping_errors.append(error_msg)
                            unmapped_configs.append(sm_prop_name)
                            self.logger.warning(f"Failed to map property '{sm_prop_name}'")
                except Exception as e:
                    error_msg = f"Error mapping {sm_prop_name}: {str(e)}"
                    mapping_errors.append(error_msg)
                    self.logger.error(f"Error mapping property '{sm_prop_name}': {e}")
        
        # Log summary of mapping results
        if unmapped_configs:
            self.logger.warning(f"Connector has {len(unmapped_configs)} unmapped configurations: {', '.join(unmapped_configs)}")
        else:
            self.logger.info("All configurations were successfully mapped")
        
        self.logger.info(f"Mapping completed with {len(mapped_config)} properties mapped and {len(mapping_errors)} errors")
        
        return {
            'name': name,
            'config': mapped_config,
            'mapping_metadata': {
                'mapping_date': datetime.now().isoformat(),
                'connector_class': connector_class
            },
            'mapping_errors': mapping_errors,
            'unmapped_configs': unmapped_configs
        }

    def process_connectors(self):
        """Process all connectors and generate FM configurations"""
        # Read input file
        with open(self.input_file, 'r') as f:
            connectors = json.load(f)
        
        # Process each connector
        fm_configs = []
        for connector in connectors:
            try:
                fm_config = self._generate_fm_config(connector)
                fm_configs.append(fm_config)
                
                # Save individual FM config
                config_file = self.fm_configs_dir / f"{connector['name']}.json"
                with open(config_file, 'w') as f:
                    json.dump(fm_config, f, indent=2)
                
                self.logger.info(f"Saved FM config for {connector['name']} to {config_file}")
                
            except Exception as e:
                self.logger.error(f"Error processing connector {connector['name']}: {str(e)}")
        
        # Save all FM configs
        all_configs_file = self.output_dir / 'all_fm_configs.json'
        with open(all_configs_file, 'w') as f:
            json.dump(fm_configs, f, indent=2)
        
        self.logger.info(f"Saved {len(fm_configs)} FM configurations to {all_configs_file}") 