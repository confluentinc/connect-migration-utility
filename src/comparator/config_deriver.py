"""
Apache Connect Migration Utility
Copyright 2024-2025 The Apache Software Foundation

This product includes software developed at The Apache Software Foundation.

ConfigDeriverMixin: per-config derivation methods (_derive_*), reverse-switch
application, semantic matching, required-config validation, SM property lookup,
custom semantic-matcher loading, and placeholder/template-default helpers.

This is a mixin for ConnectorComparator (see connector_comparator.py) and is
not meant to be instantiated on its own; it relies on attributes/methods
provided by the composed class.
"""

import re
from pathlib import Path
from typing import Dict, Any, List, Optional


class ConfigDeriverMixin:

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
            return format_key

        # Try to infer from schema registry configs
        if 'key.converter.schemas.enable' in user_configs:
            return 'JSON_SR'

        # Try to get default from template if available
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'input.key.format')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default

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
            return format_key

        # Try to infer from schema registry configs
        if 'value.converter.schemas.enable' in user_configs:
            return 'JSON_SR'

        # Try to get default from template if available
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'input.data.format')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default

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
            return format_key

        # Try to get default from template if available
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'output.key.format')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default

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
                return resolved_default

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
            return format_key

        # Try to infer from output key format if already derived
        if 'output.key.format' in fm_configs:
            return fm_configs['output.key.format']

        # Try to get default from template if available
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'output.data.key.format')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default

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
            return format_key

        # Try to infer from output data format if already derived
        if 'output.data.format' in fm_configs:
            return fm_configs['output.data.format']

        # Try to get default from template if available
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'output.data.value.format')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default

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
                    return auth_value

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
                self.logger.warning(f"Placeholder '{placeholder_name}' not found in fm_configs")
                return None
        else:
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
