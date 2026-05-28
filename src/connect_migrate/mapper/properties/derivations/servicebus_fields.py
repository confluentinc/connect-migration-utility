"""Derive FM Azure ServiceBus connector fields (namespace, SAS keys, entity name)."""

import re
from typing import Any, Dict, List, Optional

from connect_migrate.mapper.properties.derivations.base import DerivationGroup


class ServiceBusFieldDeriver(DerivationGroup):
    DERIVATIONS: Dict[str, str] = {
        'azure.servicebus.namespace': '_derive_servicebus_namespace',
        'azure.servicebus.sas.keyname': '_derive_azure_servicebus_sas_keyname',
        'azure.servicebus.sas.key': '_derive_azure_servicebus_sas_key',
        'azure.servicebus.entity.name': '_derive_azure_servicebus_entity_name',
    }

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
