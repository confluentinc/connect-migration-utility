"""Derive FM Redis-connector fields (hostname, port, ssl mode)."""

import re
from typing import Any, Dict, List, Optional

from connect_migrate.mapper.properties.derivations.base import DerivationGroup


class RedisFieldDeriver(DerivationGroup):
    DERIVATIONS: Dict[str, str] = {
        'redis.hostname': '_derive_redis_hostname',
        'redis.portnumber': '_derive_redis_portnumber',
        'redis.ssl.mode': '_derive_redis_ssl_mode',
    }

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
