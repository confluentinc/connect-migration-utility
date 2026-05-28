"""Derive FM authentication and SSL-mode fields."""

import re
from typing import Any, Dict, List, Optional

from connect_migrate.mapper.properties.derivations.base import DerivationGroup


class AuthFieldDeriver(DerivationGroup):
    DERIVATIONS: Dict[str, str] = {
        'ssl.mode': '_derive_ssl_mode',
    }

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
