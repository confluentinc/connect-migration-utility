"""
Apache Connect Migration Utility
Copyright 2024-2025 The Apache Software Foundation

This product includes software developed at The Apache Software Foundation.

ConfigMapperMixin: database-type detection, JDBC/MongoDB URL parsing,
JDBC property mapping, FM-template direct/fixed/recommended value extraction,
the legacy _generate_fm_config path, and the connector_config_def processing
machinery (value/switch/dynamic-mapper cases) used by transformSMToFm.

This is a mixin for ConnectorComparator (see connector_comparator.py) and is
not meant to be instantiated on its own; it relies on attributes/methods
provided by the composed class.
"""

import re
from typing import Dict, Any, List, Optional, Set, Tuple


class ConfigMapperMixin:

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
            self.logger.debug("Detected Oracle complex format with DESCRIPTION block.")
            # Extract HOST (case-insensitive to handle Host, HOST, host, etc.)
            host_match = re.search(r'\(HOST=([^\)]+)\)', original_url, re.IGNORECASE)
            if host_match:
                connection_info['host'] = host_match.group(1)
                self.logger.debug(f"[ORACLE] Extracted host: {connection_info['host']}")
            # Extract PORT (case-insensitive to handle Port, PORT, port, etc.)
            port_match = re.search(r'\(PORT=([^\)]+)\)', original_url, re.IGNORECASE)
            if port_match:
                connection_info['port'] = port_match.group(1)
                self.logger.debug(f"[ORACLE] Extracted port: {connection_info['port']}")
            # Extract db.connection.type and db.name (fix group assignments)
            # Handle both SERVICE_NAME and SID, case-insensitive
            dbtype_dbname_match = re.search(r'\(CONNECT_DATA=\(([^=\)]+)=([^\)]+)\)\)', original_url, re.IGNORECASE)
            if dbtype_dbname_match:
                connection_info['db.connection.type'] = dbtype_dbname_match.group(1)
                connection_info['db.name'] = dbtype_dbname_match.group(2)
                self.logger.debug(f"[ORACLE] Extracted db.connection.type: {connection_info['db.connection.type']}")
                self.logger.debug(f"[ORACLE] Extracted db.name: {connection_info['db.name']}")
            # Extract ssl.server.cert.dn (case-insensitive to handle ssl_server_cert_dn, SSL_SERVER_CERT_DN, etc.)
            ssl_cert_dn_match = re.search(r'SSL_SERVER_CERT_DN=\\?"?([^\)\"]+)\\?"?\)', original_url, re.IGNORECASE)
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
        """Extract template config definitions from FM template (following Java pattern)
        When multiple templates define the same config, uses the first definition encountered."""
        template_config_defs = []
        seen_config_names = set()  # Track config names we've already seen

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
                        for config_def in template['config_defs']:
                            if not isinstance(config_def, dict) or 'name' not in config_def:
                                continue
                            
                            config_name = config_def['name']
                            
                            # Only add if we haven't seen this config name before (use first definition)
                            if config_name not in seen_config_names:
                                template_config_defs.append(config_def)
                                seen_config_names.add(config_name)
                                self.logger.debug(f"Added config '{config_name}' from template {i} (first definition)")
                            else:
                                self.logger.debug(f"Skipping duplicate config '{config_name}' from template {i} (using first definition)")
                    else:
                        self.logger.warning(f"Expected config_defs to be a list, got {type(template['config_defs'])}: {template['config_defs']}")
                        # Skip this template's config_defs
                        continue
        else:
            self.logger.warning("No 'templates' key found in fm_template")

        self.logger.debug(f"Extracted {len(template_config_defs)} unique config definitions (using first definition for duplicates)")
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
