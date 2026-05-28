"""Derive FM connection fields (host, port, user, password, db.name, ...) from SM config."""

import re
from typing import Any, Dict, List, Optional

from connect_migrate.mapper.properties.derivations.base import DerivationGroup


class ConnectionFieldDeriver(DerivationGroup):
    DERIVATIONS: Dict[str, str] = {
        'connection.url': '_derive_connection_url',
        'connection.host': '_derive_connection_host',
        'connection.port': '_derive_connection_port',
        'connection.user': '_derive_connection_user',
        'connection.password': '_derive_connection_password',
        'connection.database': '_derive_connection_database',
        'db.name': '_derive_db_name',
        'db.connection.type': '_derive_db_connection_type',
        'ssl.server.cert.dn': '_derive_ssl_server_cert_dn',
    }

    def _derive_connection_url(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: Optional[List[Dict[str, Any]]] = None, config_name: Optional[str] = None) -> Optional[str]:
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

    def _derive_connection_host(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive connection.host from user configs (e.g., from JDBC URL or MongoDB connection string)"""
        # Try to extract from JDBC URL
        if 'connection.url' in user_configs:
            jdbc_url = user_configs['connection.url']
            if jdbc_url.startswith('jdbc:'):
                parsed = self.jdbc_url_parser.parse_jdbc_url(jdbc_url)
                return parsed.get('host')

        # Try to extract from MongoDB connection string
        if 'connection.uri' in user_configs:
            mongo_uri = user_configs['connection.uri']
            parsed = self.jdbc_url_parser.parse_mongodb_url(mongo_uri)
            return parsed.get('host')

        # Check for MongoDB-specific connection string configs
        for config_key in ['mongodb.connection.string', 'connection.string']:
            if config_key in user_configs:
                mongo_uri = user_configs[config_key]
                parsed = self.jdbc_url_parser.parse_mongodb_url(mongo_uri)
                return parsed.get('host')

        return None

    def _derive_connection_port(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive connection.port from user configs (e.g., from JDBC URL)"""
        # Try to extract from JDBC URL
        if 'connection.url' in user_configs:
            jdbc_url = user_configs['connection.url']
            if jdbc_url.startswith('jdbc:'):
                parsed = self.jdbc_url_parser.parse_jdbc_url(jdbc_url)
                return parsed.get('port')
        return None

    def _derive_connection_user(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive connection.user from user configs (e.g., from JDBC URL or MongoDB connection string)"""
        # Try to extract from JDBC URL
        if 'connection.url' in user_configs:
            jdbc_url = user_configs['connection.url']
            if jdbc_url.startswith('jdbc:'):
                parsed = self.jdbc_url_parser.parse_jdbc_url(jdbc_url)
                return parsed.get('user')

        # Try to extract from MongoDB connection string
        if 'connection.uri' in user_configs:
            mongo_uri = user_configs['connection.uri']
            parsed = self.jdbc_url_parser.parse_mongodb_url(mongo_uri)
            return parsed.get('user')

        # Check for MongoDB-specific connection string configs
        for config_key in ['mongodb.connection.string', 'connection.string']:
            if config_key in user_configs:
                mongo_uri = user_configs[config_key]
                parsed = self.jdbc_url_parser.parse_mongodb_url(mongo_uri)
                return parsed.get('user')

        return None

    def _derive_connection_password(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive connection.password from user configs (e.g., from JDBC URL or MongoDB connection string)"""
        # Try to extract from JDBC URL
        if 'connection.url' in user_configs:
            jdbc_url = user_configs['connection.url']
            if jdbc_url.startswith('jdbc:'):
                parsed = self.jdbc_url_parser.parse_jdbc_url(jdbc_url)
                return parsed.get('password')

        # Try to extract from MongoDB connection string
        if 'connection.uri' in user_configs:
            mongo_uri = user_configs['connection.uri']
            parsed = self.jdbc_url_parser.parse_mongodb_url(mongo_uri)
            return parsed.get('password')

        # Check for MongoDB-specific connection string configs
        for config_key in ['mongodb.connection.string', 'connection.string']:
            if config_key in user_configs:
                mongo_uri = user_configs[config_key]
                parsed = self.jdbc_url_parser.parse_mongodb_url(mongo_uri)
                return parsed.get('password')

        return None

    def _derive_connection_database(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive connection.database from user configs (e.g., from JDBC URL)"""
        # Try to extract from JDBC URL
        if 'connection.url' in user_configs:
            jdbc_url = user_configs['connection.url']
            if jdbc_url.startswith('jdbc:'):
                parsed = self.jdbc_url_parser.parse_jdbc_url(jdbc_url)
                # The _parse_jdbc_url method returns 'db.name', not 'database'
                return parsed.get('db.name')
        return None

    def _derive_db_name(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:

        """Derive db.name from user configs (e.g., from JDBC URL)"""
        # Try to extract from JDBC URL
        if 'connection.url' in user_configs:
            jdbc_url = user_configs['connection.url']
            if jdbc_url.startswith('jdbc:'):
                parsed = self.jdbc_url_parser.parse_jdbc_url(jdbc_url)
                # The _parse_jdbc_url method returns 'db.name', not 'database'
                return parsed.get('db.name')

        # Try to extract from MongoDB connection string
        if 'connection.uri' in user_configs:
            mongo_uri = user_configs['connection.uri']
            parsed = self.jdbc_url_parser.parse_mongodb_url(mongo_uri)
            return parsed.get('database')

        # Check for MongoDB-specific connection string configs
        for config_key in ['mongodb.connection.string', 'connection.string']:
            if config_key in user_configs:
                mongo_uri = user_configs[config_key]
                parsed = self.jdbc_url_parser.parse_mongodb_url(mongo_uri)
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
                parsed = self.jdbc_url_parser.parse_jdbc_url(jdbc_url)
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
                parsed = self.jdbc_url_parser.parse_jdbc_url(jdbc_url)
                return parsed.get('ssl.server.cert.dn')

        # Check for direct ssl.server.cert.dn config
        if 'ssl.server.cert.dn' in user_configs:
            return user_configs['ssl.server.cert.dn']

        # Default fallback
        return None

