"""
Template Manager Module
Handles loading, caching, and finding FM/SM connector templates.
"""

import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple


class TemplateManager:
    """Manages connector templates for migration."""
    
    # Debezium v1 to v2 connector class mapping
    DEBEZIUM_V1_TO_V2_MAPPING = {
        'io.debezium.connector.mysql.MySqlConnector': 'io.debezium.connector.v2.mysql.MySqlConnectorV2',
        'io.debezium.connector.postgresql.PostgresConnector': 'io.debezium.connector.v2.postgresql.PostgresConnectorV2',
        'io.debezium.connector.sqlserver.SqlServerConnector': 'io.debezium.connector.v2.sqlserver.SqlServerConnectorV2',
        'io.debezium.connector.mariadb.MariaDbConnector': 'io.debezium.connector.v2.mariadb.MariaDbConnector'
    }
    
    # JDBC Database type mappings
    JDBC_DATABASE_TYPES = {
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
    
    def __init__(self, fm_template_dir: Path = None, debezium_version: str = 'v2', 
                 logger: Optional[logging.Logger] = None):
        """
        Initialize the TemplateManager.
        
        Args:
            fm_template_dir: Directory containing FM templates
            debezium_version: Debezium version for CDC template selection ('v1' or 'v2')
            logger: Optional logger instance
        """
        self.logger = logger or logging.getLogger(__name__)
        self.fm_template_dir = fm_template_dir or Path("templates/fm")
        self.debezium_version = debezium_version.lower()
        
        if self.debezium_version not in ['v1', 'v2']:
            self.logger.warning(f"Invalid debezium_version '{debezium_version}', defaulting to 'v2'")
            self.debezium_version = 'v2'
        
        # Build reverse mapping
        self.debezium_v2_to_v1_mapping = {v: k for k, v in self.DEBEZIUM_V1_TO_V2_MAPPING.items()}
        
        # Load templates
        self.fm_templates = self._load_templates(self.fm_template_dir) if self.fm_template_dir.exists() else {}
        self.connector_class_to_template = self._build_connector_class_mapping()
    
    def _load_templates(self, template_dir: Path) -> Dict[str, Dict[str, Any]]:
        """Load all JSON template files from a directory"""
        templates = {}
        if template_dir.exists():
            for template_file in template_dir.glob('*.json'):
                try:
                    with open(template_file, 'r') as f:
                        templates[template_file.stem] = json.load(f)
                    self.logger.debug(f"Loaded template: {template_file.name}")
                except Exception as e:
                    self.logger.error(f"Error loading template {template_file}: {str(e)}")
        self.logger.info(f"Loaded {len(templates)} templates from {template_dir}")
        return templates
    
    def _build_connector_class_mapping(self) -> Dict[str, Dict[str, List[str]]]:
        """Build mapping of connector.class to template paths"""
        mapping = {}

        if self.fm_template_dir and self.fm_template_dir.exists():
            for template_file in self.fm_template_dir.glob('*_resolved_templates.json'):
                try:
                    with open(template_file, 'r') as f:
                        template_data = json.load(f)
                        if 'connector.class' in template_data:
                            connector_class = template_data['connector.class']
                            if connector_class not in mapping:
                                mapping[connector_class] = {'fm_templates': []}
                            mapping[connector_class]['fm_templates'].append(str(template_file))
                except Exception as e:
                    self.logger.error(f"Error reading template {template_file}: {str(e)}")

        return mapping
    
    def get_database_type(self, config: Dict[str, Any]) -> str:
        """Determine database type from JDBC connector config"""
        if 'connection.url' in config and isinstance(config['connection.url'], str):
            url = config['connection.url'].lower()
            self.logger.debug(f"Analyzing JDBC URL for database type: {url}")

            # Precise pattern matching
            for db_type, info in self.JDBC_DATABASE_TYPES.items():
                for pattern in info['url_patterns']:
                    if f'jdbc:{pattern}://' in url:
                        self.logger.debug(f"Detected database type '{db_type}'")
                        return db_type

            # Fallback pattern matching
            for db_type, info in self.JDBC_DATABASE_TYPES.items():
                if any(pattern in url for pattern in info['url_patterns']):
                    self.logger.debug(f"Detected database type '{db_type}' using fallback")
                    return db_type

            self.logger.warning(f"No database type detected for URL: {url}")

        if 'database.type' in config:
            db_type = config['database.type'].lower()
            self.logger.debug(f"Using database type from config: {db_type}")
            return db_type

        return 'unknown'
    
    def find_fm_template(self, connector_class: str, connector_name: str = None, 
                         config: Dict[str, Any] = None) -> Optional[str]:
        """Find FM template file for a connector class"""
        if not self.fm_template_dir or not self.fm_template_dir.exists():
            self.logger.error(f"FM template directory does not exist: {self.fm_template_dir}")
            return None

        matching_templates = []
        template_info = []

        # Handle Debezium version mapping
        target_connector_class = connector_class
        if self.debezium_version == 'v2' and connector_class in self.DEBEZIUM_V1_TO_V2_MAPPING:
            target_connector_class = self.DEBEZIUM_V1_TO_V2_MAPPING[connector_class]
            self.logger.info(f"Migrating v1 to v2: {connector_class} -> {target_connector_class}")
        elif self.debezium_version == 'v1' and connector_class in self.debezium_v2_to_v1_mapping:
            target_connector_class = self.debezium_v2_to_v1_mapping[connector_class]
            self.logger.info(f"Migrating v2 to v1: {connector_class} -> {target_connector_class}")

        # Search all JSON files
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
                    template_id = template_data.get('template_id', 'Unknown')
                    if 'templates' in template_data and len(template_data['templates']) > 0:
                        template_id = template_data['templates'][0].get('template_id', template_id)

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
            self.logger.info(f"Using single FM template: {matching_templates[0]}")
            return matching_templates[0]
        else:
            # Multiple templates - filter by CDC version
            filtered_templates = self._filter_cdc_templates_by_version(connector_class, template_info)
            
            if len(filtered_templates) == 1:
                return filtered_templates[0]['path']
            elif filtered_templates:
                template_info = filtered_templates

            # For JDBC, auto-select based on database type
            if connector_class in ['io.confluent.connect.jdbc.JdbcSourceConnector', 
                                   'io.confluent.connect.jdbc.JdbcSinkConnector'] and config:
                return self._auto_select_jdbc_template(connector_class, template_info, config, connector_name)
            
            # Default to first matching template
            self.logger.info(f"Multiple templates found, using first: {template_info[0]['path']}")
            return template_info[0]['path']
    
    def _filter_cdc_templates_by_version(self, connector_class: str, 
                                         template_info: List[Dict[str, str]]) -> List[Dict[str, str]]:
        """Filter CDC templates based on debezium_version setting"""
        cdc_connector_classes = [
            'io.debezium.connector.mysql.MySqlConnector',
            'io.debezium.connector.postgresql.PostgresConnector',
            'io.debezium.connector.sqlserver.SqlServerConnector',
            'io.debezium.connector.mariadb.MariaDbConnector',
            'io.debezium.connector.v2.mysql.MySqlConnectorV2',
            'io.debezium.connector.v2.postgresql.PostgresConnectorV2',
            'io.debezium.connector.v2.sqlserver.SqlServerConnectorV2',
            'io.debezium.connector.v2.mariadb.MariaDbConnector'
        ]
        
        if connector_class not in cdc_connector_classes:
            return template_info
        
        filtered = []
        for template in template_info:
            template_id = template.get('template_id', '').lower()
            if self.debezium_version == 'v2':
                if 'v2' in template_id or 'cdc' not in template_id:
                    filtered.append(template)
            else:
                if 'v2' not in template_id:
                    filtered.append(template)
        
        return filtered if filtered else template_info
    
    def _auto_select_jdbc_template(self, connector_class: str, template_info: List[Dict[str, str]], 
                                    config: Dict[str, Any], connector_name: str = None) -> Optional[str]:
        """Auto-select JDBC template based on database type from connection URL"""
        db_type = self.get_database_type(config)
        self.logger.info(f"Detected database type: {db_type}")
        
        # Special handling for Snowflake
        if db_type == 'snowflake':
            for template in template_info:
                if 'snowflake' in template.get('template_id', '').lower():
                    self.logger.info(f"Selected Snowflake template: {template['path']}")
                    return template['path']
        
        # Try to match template by database type
        for template in template_info:
            template_id = template.get('template_id', '').lower()
            if db_type in template_id:
                self.logger.info(f"Selected template for {db_type}: {template['path']}")
                return template['path']
        
        # Default to first template
        if template_info:
            self.logger.info(f"Using default JDBC template: {template_info[0]['path']}")
            return template_info[0]['path']
        
        return None
    
    def get_template_by_path(self, template_path: str) -> Optional[Dict[str, Any]]:
        """Load a template from a file path"""
        try:
            with open(template_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Error loading template {template_path}: {str(e)}")
            return None
    
    def get_fm_template(self, template_name: str) -> Optional[Dict[str, Any]]:
        """Get a cached FM template by name"""
        return self.fm_templates.get(template_name)


# Singleton instance
_template_manager_instance = None


def get_template_manager(fm_template_dir: Path = None, debezium_version: str = 'v2',
                         logger: Optional[logging.Logger] = None) -> TemplateManager:
    """Get or create a TemplateManager instance."""
    global _template_manager_instance
    if _template_manager_instance is None:
        _template_manager_instance = TemplateManager(fm_template_dir, debezium_version, logger)
    return _template_manager_instance

