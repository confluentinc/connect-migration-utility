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
    def __init__(self, input_file: Path, output_dir: Path, worker_urls: List[str] = None, 
                 env_id: str = None, lkc_id: str = None, bearer_token: str = None, disable_ssl_verify: bool = False):
        self.logger = logging.getLogger(__name__)
        self.input_file = input_file
        self.output_dir = output_dir
        self.fm_configs_dir = output_dir / 'fm_configs'
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
            self.logger.warning("SSL certificate verification is DISABLED - this reduces security")
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

    def get_SM_template(self, connector_class: str, worker_url: str = None) -> Dict[str, Any]:
        """Get SM template for a connector class using the specified worker URL"""
        if not worker_url:
            self.logger.info(f"No worker URL provided - skipping SM template fetch for {connector_class}")
            return {}
        
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
        
        # Map database types to template names
        db_to_template_mapping = {
            'mysql': ['MySqlSource', 'MySqlSink'],
            'postgresql': ['PostgresSource', 'PostgresSink'],
            'oracle': ['OracleDatabaseSource', 'OracleDatabaseSink'],
            'sqlserver': ['MicrosoftSqlServerSource', 'MicrosoftSqlServerSink']
        }
        
        # Get expected template names for this database type
        expected_templates = db_to_template_mapping.get(db_type, [])
        
        # Find matching template
        for template in template_info:
            template_id = template['template_id']
            if template_id in expected_templates:
                selected_path = template['path']
                self.logger.info(f"Auto-selected JDBC template for {connector_display}: {template_id} (File: {template['filename']})")
                return selected_path
        
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
            for db_type, info in self.jdbc_database_types.items():
                if any(pattern in url for pattern in info['url_patterns']):
                    return db_type
                    
        # Check specific database type config if available
        if 'database.type' in config:
            return config['database.type'].lower()
            
        return 'unknown'

    def _parse_jdbc_url(self, url: str) -> Dict[str, str]:
        """Parse JDBC URL to extract connection details from real JDBC URLs"""
        original_url = url
        url = url.lower()
        connection_info = {}
        
        self.logger.debug(f"Parsing JDBC URL: {original_url}")
        
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
            connection_info['db_name'] = db_name
            self.logger.debug(f"Extracted db_name: {connection_info['db_name']}")
        
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
                        fm_property = config['name']
                        
                        # Handle template variables like ${cleanup.policy}
                        if isinstance(value, str) and value.startswith('${') and value.endswith('}'):
                            # Extract the property name from the template variable
                            fm_property_name = value[2:-1]  # Remove ${ and }
                            # SM property (fm_property) maps to FM property (fm_property_name)
                            mappings[fm_property] = fm_property_name
                        else:
                            # Direct value mapping
                            mappings[value] = fm_property
                    # If the config has a 'switch' field, handle switch mappings
                    elif 'switch' in config:
                        fm_property = config['name']
                        switch_config = config['switch']
                        
                        # Extract template variables from switch values
                        for switch_key, switch_values in switch_config.items():
                            if isinstance(switch_values, dict):
                                for condition, switch_value in switch_values.items():
                                    if isinstance(switch_value, str) and switch_value.startswith('${') and switch_value.endswith('}'):
                                        # Extract the property name from the template variable
                                        fm_property_name = switch_value[2:-1]  # Remove ${ and }
                                        # SM property (fm_property) maps to FM property (fm_property_name)
                                        mappings[fm_property] = fm_property_name
                                        break  # Use the first template variable found
                    # If no 'value' or 'switch' field, it's a direct name mapping (same name in SM and FM)
                    else:
                        fm_property = config['name']
                        mappings[fm_property] = fm_property
        
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

    def process_connectors(self):
        """Process all connectors and generate FM configurations"""
        # Read input file
        with open(self.input_file, 'r') as f:
            data = json.load(f)
        
        # Handle different input file structures
        if isinstance(data, dict) and 'connectors' in data:
            # Structure: {"connectors": {"connector_name": connector_data, ...}}
            connectors_dict = data['connectors']
            connectors = list(connectors_dict.values())
        elif isinstance(data, list):
            # Structure: [connector_data, ...]
            connectors = data
        else:
            self.logger.error(f"Unexpected input file structure: {type(data)}")
            return
        
        # Ensure connectors is a list
        if not isinstance(connectors, list):
            self.logger.error(f"Expected list of connectors, got {type(connectors)}")
            return
        
        # Process each connector
        fm_configs = []
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
                
                fm_config = self._generate_fm_config(connector)
                fm_configs.append(fm_config)
                
                # Save individual FM config
                config_file = self.fm_configs_dir / f"{connector['name']}.json"
                with open(config_file, 'w') as f:
                    json.dump(fm_config, f, indent=2)
                
                self.logger.info(f"Saved FM config for {connector['name']} to {config_file}")
                
            except Exception as e:
                connector_name = connector.get('name', f'connector_{i}') if isinstance(connector, dict) else f'connector_{i}'
                self.logger.error(f"Error processing connector {connector_name}: {str(e)}")
        
        # Save all FM configs
        all_configs_file = self.output_dir / 'all_fm_configs.json'
        with open(all_configs_file, 'w') as f:
            json.dump(fm_configs, f, indent=2)
        
        self.logger.info(f"Saved {len(fm_configs)} FM configurations to {all_configs_file}") 