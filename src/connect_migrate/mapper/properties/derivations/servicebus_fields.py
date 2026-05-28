"""Derive FM Azure ServiceBus connector fields (namespace, SAS keys, entity name)."""

import re
from typing import Any, Dict, List, Optional, Pattern

from connect_migrate.mapper.properties.derivations.base import DerivationGroup


# Maps FM field name -> regex to extract that field from
# ``azure.servicebus.connection.string`` when the user doesn't supply the
# FM field directly.
_CONN_STRING_PATTERNS: Dict[str, Pattern[str]] = {
    'azure.servicebus.namespace': re.compile(
        r'Endpoint=sb://([^.]+)\.servicebus\.windows\.net/'
    ),
    'azure.servicebus.sas.keyname': re.compile(r'SharedAccessKeyName=([^;]+)'),
    'azure.servicebus.sas.key': re.compile(r'SharedAccessKey=([^;]+)'),
    'azure.servicebus.entity.name': re.compile(r'EntityPath=([^;]+)'),
}


class ServiceBusFieldDeriver(DerivationGroup):
    DERIVATIONS: Dict[str, str] = {
        field: '_derive_from_connection_string' for field in _CONN_STRING_PATTERNS
    }

    def _derive_from_connection_string(
        self,
        user_configs: Dict[str, str],
        fm_configs: Dict[str, str],
        template_config_defs: Optional[List[Dict[str, Any]]] = None,
        config_name: Optional[str] = None,
    ) -> Optional[str]:
        """Return ``user_configs[config_name]`` if set; otherwise extract via
        the regex registered for ``config_name`` against the connection string.
        """
        if config_name and config_name in user_configs:
            return user_configs[config_name]

        conn_str = user_configs.get('azure.servicebus.connection.string')
        if conn_str and config_name in _CONN_STRING_PATTERNS:
            match = _CONN_STRING_PATTERNS[config_name].search(conn_str)
            if match:
                return match.group(1)

        return None