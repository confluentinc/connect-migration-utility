"""
Derivation Methods Module
Contains mixin class with all _derive_* methods for config transformation.
"""

import re
import logging
from typing import Dict, Any, List, Optional


class DerivationMethodsMixin:
    """
    Mixin class containing all derivation methods for connector config transformation.
    These methods derive FM config values from SM (user) configs.
    """
    
    # Reverse format mapping from converter class to format key
    REVERSE_FORMAT_MAPPING = {
        "io.confluent.connect.avro.AvroConverter": "AVRO",
        "io.confluent.connect.json.JsonSchemaConverter": "JSON_SR",
        "io.confluent.connect.protobuf.ProtobufConverter": "PROTOBUF",
        "org.apache.kafka.connect.converters.ByteArrayConverter": "BYTES",
        "org.apache.kafka.connect.json.JsonConverter": "JSON",
        "org.apache.kafka.connect.storage.StringConverter": "STRING"
    }
    
    def _is_placeholder(self, value: str) -> bool:
        """Check if a value is a placeholder like ${xxxx}"""
        return value.startswith('${')

    def _extract_placeholder_name(self, placeholder: str) -> str:
        """Extract the placeholder name from ${xxxx} format"""
        if placeholder.startswith('${'):
            end_pos = placeholder.find('}', 2)
            if end_pos != -1:
                return placeholder[2:end_pos]
            else:
                return placeholder[2:]
        return placeholder

    def _resolve_template_default(self, template_default: str, fm_configs: Dict[str, str]) -> str:
        """Resolve template default value, handling placeholders like ${xxxx}"""
        if self._is_placeholder(template_default):
            placeholder_name = self._extract_placeholder_name(template_default)
            if placeholder_name in fm_configs:
                return fm_configs[placeholder_name]
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

    def _derive_connection_host(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], 
                                 template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive connection.host from user configs (e.g., from JDBC URL or MongoDB connection string)"""
        if 'connection.url' in user_configs:
            jdbc_url = user_configs['connection.url']
            if jdbc_url.startswith('jdbc:'):
                parsed = self._parse_jdbc_url(jdbc_url)
                return parsed.get('host')

        if 'connection.uri' in user_configs:
            mongo_uri = user_configs['connection.uri']
            parsed = self._parse_mongodb_connection_string(mongo_uri)
            return parsed.get('host')

        for config_key in ['mongodb.connection.string', 'connection.string']:
            if config_key in user_configs:
                mongo_uri = user_configs[config_key]
                parsed = self._parse_mongodb_connection_string(mongo_uri)
                return parsed.get('host')

        return None

    def _derive_connection_port(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], 
                                 template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive connection.port from user configs (e.g., from JDBC URL)"""
        if 'connection.url' in user_configs:
            jdbc_url = user_configs['connection.url']
            if jdbc_url.startswith('jdbc:'):
                parsed = self._parse_jdbc_url(jdbc_url)
                return parsed.get('port')
        return None

    def _derive_connection_user(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], 
                                 template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive connection.user from user configs"""
        if 'connection.url' in user_configs:
            jdbc_url = user_configs['connection.url']
            if jdbc_url.startswith('jdbc:'):
                parsed = self._parse_jdbc_url(jdbc_url)
                return parsed.get('user')

        if 'connection.uri' in user_configs:
            mongo_uri = user_configs['connection.uri']
            parsed = self._parse_mongodb_connection_string(mongo_uri)
            return parsed.get('user')

        for config_key in ['mongodb.connection.string', 'connection.string']:
            if config_key in user_configs:
                mongo_uri = user_configs[config_key]
                parsed = self._parse_mongodb_connection_string(mongo_uri)
                return parsed.get('user')

        return None

    def _derive_connection_password(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], 
                                     template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive connection.password from user configs"""
        if 'connection.url' in user_configs:
            jdbc_url = user_configs['connection.url']
            if jdbc_url.startswith('jdbc:'):
                parsed = self._parse_jdbc_url(jdbc_url)
                return parsed.get('password')

        if 'connection.uri' in user_configs:
            mongo_uri = user_configs['connection.uri']
            parsed = self._parse_mongodb_connection_string(mongo_uri)
            return parsed.get('password')

        for config_key in ['mongodb.connection.string', 'connection.string']:
            if config_key in user_configs:
                mongo_uri = user_configs[config_key]
                parsed = self._parse_mongodb_connection_string(mongo_uri)
                return parsed.get('password')

        return None

    def _derive_connection_database(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], 
                                     template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive connection.database from user configs (e.g., from JDBC URL)"""
        if 'connection.url' in user_configs:
            jdbc_url = user_configs['connection.url']
            if jdbc_url.startswith('jdbc:'):
                parsed = self._parse_jdbc_url(jdbc_url)
                return parsed.get('db.name')
        return None

    def _derive_db_name(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], 
                         template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive db.name from user configs"""
        if 'connection.url' in user_configs:
            jdbc_url = user_configs['connection.url']
            if jdbc_url.startswith('jdbc:'):
                parsed = self._parse_jdbc_url(jdbc_url)
                return parsed.get('db.name')

        if 'connection.uri' in user_configs:
            mongo_uri = user_configs['connection.uri']
            parsed = self._parse_mongodb_connection_string(mongo_uri)
            return parsed.get('database')

        for config_key in ['mongodb.connection.string', 'connection.string']:
            if config_key in user_configs:
                mongo_uri = user_configs[config_key]
                parsed = self._parse_mongodb_connection_string(mongo_uri)
                return parsed.get('database')

        if 'db.name' in user_configs:
            return user_configs['db.name']

        if 'database' in user_configs:
            return user_configs['database']

        return None

    def _derive_db_connection_type(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], 
                                    template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive db.connection.type from user configs (e.g., from JDBC URL)"""
        if 'connection.url' in user_configs:
            jdbc_url = user_configs['connection.url']
            if jdbc_url.startswith('jdbc:'):
                parsed = self._parse_jdbc_url(jdbc_url)
                return parsed.get('db.connection.type')

        if 'db.connection.type' in user_configs:
            return user_configs['db.connection.type']

        return None

    def _derive_ssl_server_cert_dn(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], 
                                    template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive ssl.server.cert.dn from user configs (e.g., from JDBC URL)"""
        if 'connection.url' in user_configs:
            jdbc_url = user_configs['connection.url']
            if jdbc_url.startswith('jdbc:'):
                parsed = self._parse_jdbc_url(jdbc_url)
                return parsed.get('ssl.server.cert.dn')

        if 'ssl.server.cert.dn' in user_configs:
            return user_configs['ssl.server.cert.dn']

        return None

    def _derive_database_server_name(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], 
                                      template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive database.server.name from user configs"""
        if 'connection.url' in user_configs:
            jdbc_url = user_configs['connection.url']
            if jdbc_url.startswith('jdbc:'):
                parsed = self._parse_jdbc_url(jdbc_url)
                return parsed.get('host')

        if 'database.server.name' in user_configs:
            return user_configs['database.server.name']

        if 'server.name' in user_configs:
            return user_configs['server.name']
        
        return None

    def _derive_input_key_format(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], 
                                  template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive input.key.format from user configs using reverse format mapping"""
        if 'key.converter' in user_configs:
            converter_class = user_configs['key.converter']
            if converter_class in self.REVERSE_FORMAT_MAPPING:
                return self.REVERSE_FORMAT_MAPPING[converter_class]
            return converter_class

        format_key = user_configs.get('key.format') or user_configs.get('input.key.format')
        if format_key:
            return format_key

        if 'key.converter.schemas.enable' in user_configs:
            return 'JSON_SR'

        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'input.key.format')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default

        return 'JSON'

    def _derive_input_data_format(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], 
                                   template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive input.data.format from user configs using reverse format mapping"""
        if 'value.converter' in user_configs:
            converter_class = user_configs['value.converter']
            if converter_class in self.REVERSE_FORMAT_MAPPING:
                return self.REVERSE_FORMAT_MAPPING[converter_class]
            return converter_class

        format_key = user_configs.get('value.format') or user_configs.get('input.data.format')
        if format_key:
            return format_key

        if 'value.converter.schemas.enable' in user_configs:
            return 'JSON_SR'

        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'input.data.format')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default

        return 'JSON'

    def _derive_output_key_format(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], 
                                   template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive output.key.format from user configs using reverse format mapping"""
        if 'key.converter' in user_configs:
            converter_class = user_configs['key.converter']
            if converter_class in self.REVERSE_FORMAT_MAPPING:
                return self.REVERSE_FORMAT_MAPPING[converter_class]
            return converter_class

        format_key = user_configs.get('output.key.format') or user_configs.get('key.format')
        if format_key:
            return format_key

        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'output.key.format')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default

        return 'JSON'

    def _derive_output_data_format(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], 
                                    template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive output.data.format from user configs using reverse format mapping"""
        if 'value.converter' in user_configs:
            converter_class = user_configs['value.converter']
            if converter_class in self.REVERSE_FORMAT_MAPPING:
                return self.REVERSE_FORMAT_MAPPING[converter_class]
            return converter_class

        format_key = user_configs.get('output.data.format') or user_configs.get('value.format')
        if format_key:
            return format_key

        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'output.data.format')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default

        return 'JSON'

    def _derive_output_data_key_format(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], 
                                        template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive output.data.key.format from user configs"""
        if 'key.converter' in user_configs:
            converter_class = user_configs['key.converter']
            if converter_class in self.REVERSE_FORMAT_MAPPING:
                return self.REVERSE_FORMAT_MAPPING[converter_class]
            return converter_class

        format_key = user_configs.get('output.data.key.format') or user_configs.get('key.format')
        if format_key:
            return format_key

        if 'output.key.format' in fm_configs:
            return fm_configs['output.key.format']

        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'output.data.key.format')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default

        return 'JSON'

    def _derive_output_data_value_format(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], 
                                          template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive output.data.value.format from user configs"""
        if 'value.converter' in user_configs:
            converter_class = user_configs['value.converter']
            if converter_class in self.REVERSE_FORMAT_MAPPING:
                return self.REVERSE_FORMAT_MAPPING[converter_class]
            return converter_class

        format_key = user_configs.get('output.data.value.format') or user_configs.get('value.format')
        if format_key:
            return format_key

        if 'output.data.format' in fm_configs:
            return fm_configs['output.data.format']

        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'output.data.value.format')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default

        return 'JSON'

    def _derive_authentication_method(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], 
                                        template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive authentication.method from user configs"""
        auth_configs = ['security.protocol', 'sasl.mechanism', 'authentication.type', 'auth.method']

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

        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'authentication.method')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default

        return 'PLAIN'

    def _derive_csfle_enabled(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], 
                               template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive csfle.enabled from user configs"""
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'csfle.enabled')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default
        return 'false'

    def _derive_csfle_on_failure(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], 
                                  template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive csfle.onFailure from user configs"""
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'csfle.onFailure')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default
        return 'FAIL'

    def _derive_ssl_mode(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], 
                          template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive ssl.mode from user configs"""
        if 'ssl.mode' in user_configs:
            value = user_configs['ssl.mode'].lower()
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

        ssl_config_keys = [
            'connection.sslmode', 'connection.sslMode', 'database.ssl.mode',
            'redis.ssl.mode', 'ssl.enabled', 'use.ssl', 'ssl.use'
        ]
        for config_key in ssl_config_keys:
            if config_key in user_configs:
                value = user_configs[config_key].lower()
                if value in ['true', 'yes', '1', 'enabled']:
                    return 'require'
                elif value in ['false', 'no', '0', 'disabled']:
                    return 'disabled'
                elif value in ['prefer', 'preferred']:
                    return 'prefer'
                elif value in ['require', 'required']:
                    return 'require'
                elif value in ['verify-ca', 'verifyca', 'verify_ca']:
                    return 'verify-ca'
                elif value in ['verify-full', 'verifyfull', 'verify_full']:
                    return 'verify-full'

        ssl_indicators = [
            'ssl.truststorefile', 'ssl.truststorepassword', 'ssl.rootcertfile',
            'connection.javax.net.ssl.trustStore', 'connection.javax.net.ssl.trustStorePassword',
            'ssl.truststore.file', 'ssl.truststore.password', 'ssl.cert.file',
            'ssl.key.file', 'ssl.ca.file', 'ssl.certificate.file'
        ]

        for indicator in ssl_indicators:
            if indicator in user_configs and user_configs[indicator]:
                cert_value = user_configs[indicator].lower()
                if 'verify' in cert_value or 'cert' in cert_value:
                    return 'verify-ca'
                else:
                    return 'require'

        if 'connection.url' in user_configs:
            url = user_configs['connection.url'].lower()
            if 'ssl=true' in url or 'sslmode=' in url or 'useSSL=true' in url:
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
                    return 'require'

        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'ssl.mode')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default

        return 'prefer'

    def _derive_redis_hostname(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], 
                                template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive redis.hostname from user configs"""
        if 'redis.hostname' in user_configs:
            return user_configs['redis.hostname']

        redis_host_keys = ['redis.host', 'redis.server', 'redis.address', 'redis.endpoint',
                           'host', 'server', 'address', 'endpoint']
        for config_key in redis_host_keys:
            if config_key in user_configs:
                value = user_configs[config_key]
                if ':' in value:
                    return value.split(':')[0]
                return value

        if 'redis.hosts' in user_configs:
            hosts_value = user_configs['redis.hosts']
            if ':' in hosts_value:
                return hosts_value.split(':')[0]

        for config_key in ['connection.url', 'connection.uri', 'redis.connection.url']:
            if config_key in user_configs:
                url = user_configs[config_key].lower()
                if 'redis://' in url:
                    redis_part = url.replace('redis://', '')
                    if '@' in redis_part:
                        auth_part, host_part = redis_part.split('@', 1)
                        return host_part.split('/')[0].split(':')[0]
                    else:
                        return redis_part.split('/')[0].split(':')[0]

        return None

    def _derive_redis_portnumber(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], 
                                  template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive redis.portnumber from user configs"""
        if 'redis.portnumber' in user_configs:
            return user_configs['redis.portnumber']

        redis_port_keys = ['redis.port', 'redis.server.port', 'port', 'server.port']
        for config_key in redis_port_keys:
            if config_key in user_configs:
                return user_configs[config_key]

        if 'redis.hosts' in user_configs:
            hosts_value = user_configs['redis.hosts']
            if ':' in hosts_value:
                port = hosts_value.split(':')[1]
                if '/' in port:
                    port = port.split('/')[0]
                return port

        for config_key in ['connection.url', 'connection.uri', 'redis.connection.url']:
            if config_key in user_configs:
                url = user_configs[config_key].lower()
                if 'redis://' in url:
                    redis_part = url.replace('redis://', '')
                    if '@' in redis_part:
                        auth_part, host_part = redis_part.split('@', 1)
                        host_port = host_part.split('/')[0]
                        if ':' in host_port:
                            return host_port.split(':')[1]
                    else:
                        host_port = redis_part.split('/')[0]
                        if ':' in host_port:
                            return host_port.split(':')[1]

        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'redis.portnumber')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default

        return '6379'

    def _derive_redis_ssl_mode(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], 
                                template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive redis.ssl.mode from user configs"""
        if 'redis.ssl.mode' in user_configs:
            value = user_configs['redis.ssl.mode'].lower()
            if value in ['disabled', 'disable', 'false', 'none', 'off']:
                return 'disabled'
            elif value in ['enabled', 'enable', 'true', 'on']:
                return 'enabled'
            elif value in ['server', 'server-only', 'verify-server']:
                return 'server'
            elif value in ['server+client', 'mutual', 'two-way']:
                return 'server+client'
            else:
                return user_configs['redis.ssl.mode']

        ssl_enabled_keys = ['redis.ssl.enabled', 'redis.ssl', 'ssl.enabled', 'use.ssl']
        for config_key in ssl_enabled_keys:
            if config_key in user_configs:
                value = user_configs[config_key].lower()
                if value in ['true', 'yes', '1', 'enabled', 'on']:
                    return 'enabled'
                elif value in ['false', 'no', '0', 'disabled', 'off']:
                    return 'disabled'

        ssl_indicators = [
            'redis.ssl.keystore.file', 'redis.ssl.keystore.password',
            'redis.ssl.truststore.file', 'redis.ssl.truststore.password',
            'redis.ssl.cert.file', 'redis.ssl.key.file', 'redis.ssl.ca.file'
        ]

        for indicator in ssl_indicators:
            if indicator in user_configs and user_configs[indicator]:
                cert_value = user_configs[indicator].lower()
                if 'client' in cert_value or 'keystore' in indicator:
                    return 'server+client'
                else:
                    return 'server'

        for config_key in ['connection.url', 'connection.uri', 'redis.connection.url']:
            if config_key in user_configs:
                url = user_configs[config_key].lower()
                if 'rediss://' in url:
                    return 'enabled'
                elif 'redis://' in url and 'ssl=true' in url:
                    return 'enabled'
                elif 'redis://' in url and 'ssl=false' in url:
                    return 'disabled'

        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'redis.ssl.mode')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default

        return 'disabled'

    def _derive_servicebus_namespace(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], 
                                      template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> str:
        """Derive azure.servicebus.namespace from user configs"""
        if 'azure.servicebus.namespace' in user_configs:
            return user_configs['azure.servicebus.namespace']

        conn_str = user_configs.get('azure.servicebus.connection.string')
        if conn_str:
            match = re.search(r'Endpoint=sb://([^.]+)\.servicebus\.windows\.net/', conn_str)
            if match:
                return match.group(1)
        return user_configs.get('azure.servicebus.namespace')

    def _derive_azure_servicebus_sas_keyname(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], 
                                              template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> str:
        """Derive azure.servicebus.sas.keyname from user configs"""
        if 'azure.servicebus.sas.keyname' in user_configs:
            return user_configs['azure.servicebus.sas.keyname']

        conn_str = user_configs.get('azure.servicebus.connection.string')
        if conn_str:
            match = re.search(r'SharedAccessKeyName=([^;]+)', conn_str)
            if match:
                return match.group(1)
        return user_configs.get('azure.servicebus.sas.keyname')

    def _derive_azure_servicebus_sas_key(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], 
                                          template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> str:
        """Derive azure.servicebus.sas.key from user configs"""
        if 'azure.servicebus.sas.key' in user_configs:
            return user_configs['azure.servicebus.sas.key']

        conn_str = user_configs.get('azure.servicebus.connection.string')
        if conn_str:
            match = re.search(r'SharedAccessKey=([^;]+)', conn_str)
            if match:
                return match.group(1)
        return user_configs.get('azure.servicebus.sas.key')

    def _derive_azure_servicebus_entity_name(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], 
                                              template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> str:
        """Derive azure.servicebus.entity.name from user configs"""
        if 'azure.servicebus.entity.name' in user_configs:
            return user_configs['azure.servicebus.entity.name']

        conn_str = user_configs.get('azure.servicebus.connection.string')
        if conn_str:
            match = re.search(r'EntityPath=([^;]+)', conn_str)
            if match:
                return match.group(1)
        return user_configs.get('azure.servicebus.entity.name')

    def _derive_subject_name_strategy(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], 
                                       template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive subject name strategy from user configs"""
        recommended_strategies = []
        if template_config_defs and config_name:
            for template_config_def in template_config_defs:
                if isinstance(template_config_def, dict) and template_config_def.get('name') == config_name:
                    recommended_values = template_config_def.get('recommended_values', [])
                    if recommended_values:
                        recommended_strategies.extend(recommended_values)
                        break

        if not recommended_strategies:
            recommended_strategies = ["TopicNameStrategy", "RecordNameStrategy", "TopicRecordNameStrategy"]

        if config_name and config_name in user_configs:
            config_value = user_configs[config_name]
            if '.' in config_value:
                config_value = config_value.split('.')[-1]
            for strategy in recommended_strategies:
                if strategy.lower() == config_value.lower():
                    return strategy

        return None

    def _derive_reference_subject_name_strategy(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], 
                                                 template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive reference subject name strategy from user configs"""
        recommended_strategies = []
        if template_config_defs and config_name:
            for template_config_def in template_config_defs:
                if isinstance(template_config_def, dict) and template_config_def.get('name') == config_name:
                    recommended_values = template_config_def.get('recommended_values', [])
                    if recommended_values:
                        recommended_strategies.extend(recommended_values)
                        break

        if not recommended_strategies:
            recommended_strategies = ["DefaultReferenceSubjectNameStrategy", "QualifiedReferenceSubjectNameStrategy"]

        if config_name and config_name in user_configs:
            config_value = user_configs[config_name]
            if '.' in config_value:
                config_value = config_value.split('.')[-1]
            for strategy in recommended_strategies:
                if strategy.lower() == config_value.lower():
                    return strategy

        return None

    def _apply_reverse_switch(self, switch_mapping: Dict[str, str], user_value: str) -> Optional[str]:
        """Apply reverse switch (following Java pattern)"""
        for switch_key, switch_value in switch_mapping.items():
            if switch_value == user_value:
                if switch_key == "default":
                    return None
                return switch_key
        return None

    def _derive_connection_url(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], 
                                template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive connection.url from user configs specifically for Snowflake connectors"""
        jdbc_patterns = ['connection.url']

        for pattern in jdbc_patterns:
            if pattern in user_configs:
                jdbc_url = user_configs[pattern]
                # Check if it's a Snowflake URL
                if 'snowflake' in jdbc_url.lower():
                    # Extract the Snowflake URL part
                    return jdbc_url
        return None

