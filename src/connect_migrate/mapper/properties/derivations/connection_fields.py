"""Derive FM connection fields (host, port, user, password, db.name, ...) from SM config."""

from typing import Any, Dict, List, Optional

from connect_migrate.mapper.properties.derivations.base import DerivationGroup


_MONGO_URI_KEYS = ("connection.uri", "mongodb.connection.string", "connection.string")


class ConnectionFieldDeriver(DerivationGroup):
    DERIVATIONS: Dict[str, str] = {
        'connection.url': '_derive_snowflake_connection_url',
        'connection.host': '_derive_connection_host',
        'connection.port': '_derive_connection_port',
        'connection.user': '_derive_connection_user',
        'connection.password': '_derive_connection_password',
        'connection.database': '_derive_connection_database',
        'db.name': '_derive_db_name',
        'db.connection.type': '_derive_db_connection_type',
        'ssl.server.cert.dn': '_derive_ssl_server_cert_dn',
    }

    def _mongo_field(self, user_configs: Dict[str, str], field: str) -> Optional[str]:
        """Return ``parsed[field]`` from the first present MongoDB URI config."""
        for key in _MONGO_URI_KEYS:
            if key in user_configs:
                return self.jdbc_url_parser.parse_mongodb_url(user_configs[key]).get(field)
        return None

    def _derive_snowflake_connection_url(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: Optional[List[Dict[str, Any]]] = None, config_name: Optional[str] = None) -> Optional[str]:
        """Derive ``connection.url`` from SM config. Snowflake-specific: strips the
        ``jdbc:snowflake://`` prefix. Non-Snowflake JDBC URLs return ``None`` so the
        direct-mapping pass copies the URL through unchanged.
        """
        if 'connection.url' in user_configs:
            jdbc_url = user_configs['connection.url']
            if jdbc_url and 'jdbc:snowflake://' in jdbc_url:
                return jdbc_url.replace('jdbc:snowflake://', '').strip()
            if jdbc_url and jdbc_url.startswith('jdbc:'):
                return None

        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'connection.url')
            if template_default:
                return self._resolve_template_default(template_default, fm_configs)

        return None

    def _derive_connection_host(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: Optional[List[Dict[str, Any]]] = None, config_name: Optional[str] = None) -> Optional[str]:
        """Derive connection.host from a JDBC URL or MongoDB connection string."""
        jdbc_url = user_configs.get('connection.url')
        if jdbc_url and jdbc_url.startswith('jdbc:'):
            return self.jdbc_url_parser.parse_jdbc_url(jdbc_url).get('host')
        return self._mongo_field(user_configs, 'host')

    def _derive_connection_port(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: Optional[List[Dict[str, Any]]] = None, config_name: Optional[str] = None) -> Optional[str]:
        """Derive connection.port from a JDBC URL."""
        jdbc_url = user_configs.get('connection.url')
        if jdbc_url and jdbc_url.startswith('jdbc:'):
            return self.jdbc_url_parser.parse_jdbc_url(jdbc_url).get('port')
        return None

    def _derive_connection_user(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: Optional[List[Dict[str, Any]]] = None, config_name: Optional[str] = None) -> Optional[str]:
        """Derive connection.user from a JDBC URL or MongoDB connection string."""
        jdbc_url = user_configs.get('connection.url')
        if jdbc_url and jdbc_url.startswith('jdbc:'):
            return self.jdbc_url_parser.parse_jdbc_url(jdbc_url).get('user')
        return self._mongo_field(user_configs, 'user')

    def _derive_connection_password(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: Optional[List[Dict[str, Any]]] = None, config_name: Optional[str] = None) -> Optional[str]:
        """Derive connection.password from a JDBC URL or MongoDB connection string."""
        jdbc_url = user_configs.get('connection.url')
        if jdbc_url and jdbc_url.startswith('jdbc:'):
            return self.jdbc_url_parser.parse_jdbc_url(jdbc_url).get('password')
        return self._mongo_field(user_configs, 'password')

    def _derive_connection_database(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: Optional[List[Dict[str, Any]]] = None, config_name: Optional[str] = None) -> Optional[str]:
        """Derive connection.database from a JDBC URL (parse returns 'db.name')."""
        jdbc_url = user_configs.get('connection.url')
        if jdbc_url and jdbc_url.startswith('jdbc:'):
            return self.jdbc_url_parser.parse_jdbc_url(jdbc_url).get('db.name')
        return None

    def _derive_db_name(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: Optional[List[Dict[str, Any]]] = None, config_name: Optional[str] = None) -> Optional[str]:
        """Derive db.name from JDBC, MongoDB URI, or a direct ``db.name``/``database`` config."""
        jdbc_url = user_configs.get('connection.url')
        if jdbc_url and jdbc_url.startswith('jdbc:'):
            return self.jdbc_url_parser.parse_jdbc_url(jdbc_url).get('db.name')

        mongo_db = self._mongo_field(user_configs, 'database')
        if mongo_db is not None or any(k in user_configs for k in _MONGO_URI_KEYS):
            return mongo_db

        return user_configs.get('db.name') or user_configs.get('database')

    def _derive_db_connection_type(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: Optional[List[Dict[str, Any]]] = None, config_name: Optional[str] = None) -> Optional[str]:
        """Derive db.connection.type from a JDBC URL or a direct config."""
        jdbc_url = user_configs.get('connection.url')
        if jdbc_url and jdbc_url.startswith('jdbc:'):
            return self.jdbc_url_parser.parse_jdbc_url(jdbc_url).get('db.connection.type')
        return user_configs.get('db.connection.type')

    def _derive_ssl_server_cert_dn(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: Optional[List[Dict[str, Any]]] = None, config_name: Optional[str] = None) -> Optional[str]:
        """Derive ssl.server.cert.dn from a JDBC URL (Oracle) or a direct config."""
        jdbc_url = user_configs.get('connection.url')
        if jdbc_url and jdbc_url.startswith('jdbc:'):
            return self.jdbc_url_parser.parse_jdbc_url(jdbc_url).get('ssl.server.cert.dn')
        return user_configs.get('ssl.server.cert.dn')