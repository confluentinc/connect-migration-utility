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
    def __init__(self, input_file: Path, output_dir: Path, worker_urls: List[str] = None):
        self.logger = logging.getLogger(__name__)
        self.input_file = input_file
        self.output_dir = output_dir
        self.fm_configs_dir = output_dir / 'fm_configs'
        self.semantic_matcher = SemanticMatcher()
        
        # Worker URLs for fetching SM templates
        self.worker_urls = worker_urls or []
        

        
        # Load template files - hardcoded FM template directory
        self.fm_template_dir = Path("templates/fm")
        self.fm_templates = self._load_templates(self.fm_template_dir) if self.fm_template_dir.exists() else {}
        
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
        return {k: v for k, v in config_dict.items() if k.startswith("transforms")}

    def extract_recommended_transform_types(self, response_json):
        configs = response_json.get("configs", [])
        for config in configs:
            value = config.get("value", {})
            if value.get("name") == "transforms.transform_0.type":
                return value.get("recommended_values", [])
        return []

    def get_SM_template(self, connector_class: str) -> Dict[str, Any]:
        """Fetch SM template for a connector class via HTTP PUT call to worker URLs"""
        if not self.worker_urls:
            self.logger.error("No worker URLs provided for fetching SM templates")
            return {}
        
        # Try each worker URL until we get a successful response
        for worker_url in self.worker_urls:
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
                response = requests.put(url, json=data, headers=headers)
                response.raise_for_status()
                
                template_data = response.json()
                self.logger.info(f"Successfully fetched SM template for {connector_class} from {worker_url}")
                return template_data
                
            except requests.exceptions.RequestException as e:
                self.logger.warning(f"Failed to fetch SM template for {connector_class} from {worker_url}: {str(e)}")
                continue
        
        self.logger.error(f"Failed to fetch SM template for {connector_class} from any worker URL")
        return {}

    def get_FM_SMT(self, plugin_type) -> set[str]:
        env_id = "env-yodk3k"
        lkc_id = "lkc-nww1j6"
        bearer_token = "G42O4SU5RQG5QP4T:xAqK3hQ9RZdss2zEr7G3St0Hviv+WnPAao+Ni3cay+n6TNfChmE/o36tZSuJv0ER"

        url = (
            f"https://confluent.cloud/api/internal/accounts/env-yodk3k/clusters/lkc-nww1j6/connector-plugins/{plugin_type}/config/validate"
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
            "Authorization": f"Basic {self.encode_to_base64(bearer_token)}"
        }
        response = requests.put(url, params=params, json=data, headers=headers)
        response.raise_for_status()
        recommended_values = self.extract_recommended_transform_types(response.json())
        return recommended_values

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

    def _find_fm_template_by_connector_class(self, connector_class: str, connector_name: str = None) -> Optional[str]:
        """Find FM template file that contains the specified connector.class"""
        if not self.fm_template_dir or not self.fm_template_dir.exists():
            self.logger.error(f"FM template directory does not exist: {self.fm_template_dir}")
            return None
        
        matching_templates = []
        template_info = []
        
        # Search through all JSON files in the FM template directory
        for template_file in self.fm_template_dir.glob('*.json'):
            try:
                with open(template_file, 'r') as f:
                    template_data = json.load(f)
                    
                # Check if this template has the matching connector.class
                # Handle both direct connector.class and nested templates structure
                found_connector_class = None
                
                # Check direct connector.class
                if template_data.get('connector.class') == connector_class:
                    found_connector_class = template_data.get('connector.class')
                # Check nested templates structure
                elif 'templates' in template_data:
                    for template in template_data['templates']:
                        if template.get('connector.class') == connector_class:
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
            # Multiple templates found, ask user to pick
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

    def _get_templates_for_connector(self, connector_class: str, connector_name: str = None) -> tuple[Dict[str, Any], Dict[str, Any]]:
        """Get SM and FM templates for a connector class"""
        # Fetch SM template via HTTP
        self.logger.info(f"Fetching SM template for {connector_class} via HTTP...")
        sm_template = self.get_SM_template(connector_class)
        
        # Handle FM templates - find by connector.class
        fm_template_path = self._find_fm_template_by_connector_class(connector_class, connector_name)
        if fm_template_path:
            try:
                with open(fm_template_path, 'r') as f:
                    self.fm_templates[fm_template_path] = json.load(f)
                self.logger.info(f"Loaded FM template by connector.class: {fm_template_path}")
            except Exception as e:
                self.logger.error(f"Error loading FM template {fm_template_path}: {str(e)}")
                fm_template_path = None
        else:
            self.logger.error(f"No FM template found for connector.class: {connector_class}")
            fm_template_path = None
        
        # If FM template is missing, return empty templates
        if not fm_template_path:
            self.logger.error(f"Missing required FM template for {connector_class}")
            return sm_template, {}
        
        # Log template selection
        self.logger.info(f"Selected templates for {connector_class}:")
        self.logger.info(f"  SM Template: Fetched via HTTP")
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
        
        # Look for properties in config_defs within templates
        if 'templates' in fm_template:
            for template in fm_template['templates']:
                if 'config_defs' in template:
                    for config_def in template['config_defs']:
                        # Skip internal properties as they are handled by the Cloud platform
                        if config_def.get('required', False) and not config_def.get('internal', False):
                            required_props[config_def['name']] = config_def
        
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
        sm_template, fm_template = self._get_templates_for_connector(connector_class, name)
        
        # Initialize mapped config with name
        mapped_config = {'name': name}
        
        # Always include connector.class as it's required
        if connector_class:
            mapped_config['connector.class'] = connector_class
        
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
            transforms_data = self.get_transforms_config(config, plugin_type)
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
                    property_found = False
                    if 'templates' in fm_template:
                        for template in fm_template['templates']:
                            # Check config_defs first
                            if 'config_defs' in template:
                                for config_def in template['config_defs']:
                                    if config_def['name'] == sm_prop_name:
                                        # Direct match found
                                        mapped_config[sm_prop_name] = sm_prop_value
                                        self.logger.info(f"Direct match found for property: {sm_prop_name}")
                                        property_found = True
                                        break
                            if property_found:
                                break
                    
                    if not property_found:
                        # Step 2: Try semantic matching
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
                        
                        # Find best match using semantic matching
                        result = self.semantic_matcher.find_best_match(sm_prop, fm_properties_dict)
                        
                        if result and result.matched_fm_property:
                            # result.matched_fm_property is now a dictionary, so we need to get the property name
                            # The find_best_match method returns the property info, but we need the property name
                            # We'll need to find the property name by matching the property info
                            fm_prop_name = None
                            for prop_name, prop_info in fm_properties_dict.items():
                                if prop_info == result.matched_fm_property:
                                    fm_prop_name = prop_name
                                    break
                            
                            if fm_prop_name:
                                mapped_config[fm_prop_name] = sm_prop_value
                                self.logger.info(f"Successfully mapped {sm_prop_name} to {fm_prop_name} using {result.match_type} matching")
                            else:
                                error_msg = f"Could not determine property name for matched property: {sm_prop_name}"
                                mapping_errors.append(error_msg)
                                unmapped_configs.append(sm_prop_name)
                                self.logger.warning(f"Failed to map property '{sm_prop_name}' - could not determine property name")
                        else:
                            error_msg = f"no FM Config found for {sm_prop_name}"
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