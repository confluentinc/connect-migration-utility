"""Dispatch logic for deriving FM config values from user input.

Routes each connector-config-def to one of the four "case" handlers (value,
switch, dynamic-mapper, null), and resolves switch / dynamic mappings. Lives
separately from the orchestrator so the case-handling logic can evolve
without touching transformSMToFm.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Set


class FmDerivationDispatchMixin:

    def _get_config_derivation_method(self, template_config_name: str, template_config_def: Dict[str, Any]):
        """
        Get the method to derive a template config from user configs.
        This maps template config names to their derivation methods.
        """
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
        semantic_match_list: set,
    ):
        """Process a user config that is present in connector config def (following Java pattern)"""

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
        semantic_match_list: set,
    ):
        """Process value case (following Java pattern)"""
        value = connector_config_def.get('value')
        config_name = connector_config_def.get('name')

        if not isinstance(value, str):
            fm_configs[config_name] = str(value).lower() if isinstance(value, bool) else str(value)
            return

        # ${...} pattern that references an internal/platform-managed value
        if value is not None and (
            'org.apache.kafka.common.security.plain.PlainLoginModule' in value or
            '/mnt/secrets/connect-sr' in value or
            ('{{.logicalClusterId}}' in value and not '/mnt/secrets/connect-external-secrets' in value)
        ):
            warnings.append(f"{connector_config_def.get('name')} is internal. User given value will be ignored.")
            return

        default_pattern = re.compile(r'\$\{([^}]+)\}')
        matcher = default_pattern.search(value)

        if matcher:
            # It's not a true constant, it's a combination of multiple high level keys
            referenced_keys = self._find_referenced_keys(value, {td.get('name') for td in template_config_defs})

            for referenced_key in referenced_keys:
                config_name = connector_config_def.get('name')
                if fm_configs.get(config_name) is not None:
                    return
                if referenced_key in user_configs and user_configs[referenced_key].strip():
                    fm_configs[config_name] = user_config_value
                    return

                referenced_template_config_def = self._find_template_config_def_by_name(referenced_key, template_config_defs)

                if referenced_template_config_def is not None and not referenced_template_config_def.get('internal', False):
                    derivation_method = self._get_config_derivation_method(referenced_key, referenced_template_config_def)
                    if derivation_method:
                        # If there's a derivation method, return early — handled by the orchestrator later
                        return

                    if value == f"${{{referenced_key}}}":
                        fm_configs[referenced_key] = user_config_value
                elif referenced_template_config_def is not None and referenced_template_config_def.get('internal', False):
                    warnings.append(f"The transformed FM config is internal and will be inferred. User given value will be ignored.")
                else:
                    semantic_match_list.add(config_name)
                    self.logger.warning(f"'{config_name}' : Config transform not present in template for '{referenced_key}' which was referenced by connector config '{connector_config_def.get('name')}'. Will attempt a semantic match.")
            return
        else:
            config_name = connector_config_def.get('name')
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
        semantic_match_list: set,
    ):
        """Process switch case (following Java pattern)"""
        switch_cases = connector_config_def.get('switch', {})

        for template_config_key, switch_mapping in switch_cases.items():
            if fm_configs.get(template_config_key) is not None:
                return
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
        semantic_match_list: set,
    ):
        """Process non-internal switch case (following Java pattern)"""
        default_pattern = re.compile(r'\$\{([^}]+)\}')

        has_matchers = any(
            value is not None and default_pattern.search(value)
            for value in switch_mapping.values()
        )

        if has_matchers:
            template_config_name = template_config_def.get('name')
            derivation_method = self._get_config_derivation_method(template_config_name, template_config_def)
            if derivation_method:
                return
            # If no derivation method, fall back to semantic matching
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
        """Infer dynamic mappings for a given FM property name."""
        if dynamic_mapper_fun_name and dynamic_mapper_fun_name == 'value.converter.reference.subject.name.strategy.mapper':
            sm_to_fm_mapping = {
                "io.confluent.kafka.serializers.subject.TopicNameStrategy": "TopicNameStrategy",
                "io.confluent.kafka.serializers.subject.RecordNameStrategy": "RecordNameStrategy",
                "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy": "TopicRecordNameStrategy",
            }
            return sm_to_fm_mapping.get(user_config_value, None)

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
        semantic_match_list: set,
    ):
        """Process dynamic mapper case (following Java pattern)"""
        connector_config_name = connector_config_def.get('name')

        template_config_def = self._find_template_config_def_by_name(connector_config_name, template_config_defs)

        if template_config_def is not None:
            fm_configs[connector_config_name] = user_config_value
            return
        elif connector_config_def.get('dynamic.mapper') is not None and connector_config_def.get('dynamic.mapper').get('name') is not None:
            fm_template_def = self._find_template_config_def_by_name(connector_config_name, template_config_defs)

            if fm_template_def is not None:
                dynamic_mapping_value = self.infer_dynamic_mappings(connector_config_def.get('dynamic.mapper').get('name'), user_config_value)
                if dynamic_mapping_value is not None:
                    fm_configs[fm_template_def.get('name')] = dynamic_mapping_value
                    self.logger.info(f"Dynamic mapping for '{connector_config_name}' inferred as '{dynamic_mapping_value}'")
                    return
                elif fm_configs.get(fm_template_def.get('name')) is not None:
                    self.logger.info(f"Dynamic mapping for '{connector_config_name}' already exists in fm_configs, skipping inference.")
                    return

        semantic_match_list.add(connector_config_name)
        self.logger.warning(f"Dynamic mapper config '{connector_config_name}' not found in template configs. Will attempt semantic matching.")

    def _process_null_value_case(
        self,
        connector_config_def: Dict[str, Any],
        user_config_value: str,
        template_config_defs: List[Dict[str, Any]],
        fm_configs: Dict[str, str],
        warnings: List[str],
    ):
        """Process null value case (following Java pattern)"""
        fm_configs[connector_config_def.get('name')] = user_config_value

    def _apply_reverse_switch(self, switch_mapping: Dict[str, str], user_value: str) -> Optional[str]:
        """Apply reverse switch (following Java pattern)"""
        for switch_key, switch_value in switch_mapping.items():
            if switch_value == user_value:
                # If the matched key is "default", return None
                if switch_key == "default":
                    return None
                return switch_key
        return None
