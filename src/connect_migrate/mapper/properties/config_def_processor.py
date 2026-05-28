"""Walks ``connector_configs`` in an FM template and resolves each user value.

For each entry in an FM template's ``connector_configs`` array, we have four
possible shapes the entry can take, handled in turn by:

1. :meth:`_process_value_case`            — entry has a constant or
   ``${reference}`` ``value`` field.
2. :meth:`_process_switch_case`           — entry has a ``switch`` mapping
   that picks an FM value based on a user-supplied value.
3. :meth:`_process_dynamic_mapper_case`   — entry has a ``dynamic.mapper``
   spec for per-name lookup.
4. :meth:`_process_null_value_case`       — entry has no ``value`` (treated
   as direct copy of the user value).

Field-level *derivations* (e.g. extracting ``connection.host`` from a JDBC
URL) are delegated to an injected ``derivation_resolver`` callable so this
processor stays free of connector-specific logic.
"""

import logging
import re
from typing import Any, Callable, Dict, List, Optional, Set


# Matches ``${some.config.key}`` references inside template values.
_DEFAULT_PATTERN = re.compile(r"\$\{([^}]+)\}")


class ConfigDefProcessor:
    def __init__(
        self,
        derivation_resolver: Callable[
            [str, Dict[str, Any]], Optional[Callable[..., Optional[str]]]
        ],
        logger: Optional[logging.Logger] = None,
    ):
        self._resolve_derivation = derivation_resolver
        self.logger = logger or logging.getLogger(__name__)

    # ----------------------------------------------------------------- public

    def extract_connector_config_defs(
        self,
        fm_template: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Flatten ``connector_configs`` across every template entry."""
        connector_config_defs: List[Dict[str, Any]] = []

        if "templates" in fm_template:
            if not isinstance(fm_template["templates"], (list, tuple)):
                self.logger.error(
                    f"fm_template['templates'] is not a list, got "
                    f"{type(fm_template['templates'])}: {fm_template['templates']}"
                )
                return connector_config_defs

            for i, template in enumerate(fm_template["templates"]):
                self.logger.debug(f"Processing template {i}: {type(template)}")
                if not isinstance(template, dict):
                    self.logger.warning(f"Template {i} is not a dict: {type(template)}")
                    continue

                if "connector_configs" in template:
                    self.logger.debug(
                        f"Template {i} has connector_configs: {type(template['connector_configs'])}"
                    )
                    if isinstance(template["connector_configs"], (list, tuple)):
                        connector_config_defs.extend(template["connector_configs"])
                    else:
                        self.logger.warning(
                            f"Expected connector_configs to be a list, got "
                            f"{type(template['connector_configs'])}: {template['connector_configs']}"
                        )
                        continue
        else:
            self.logger.warning("No 'templates' key found in fm_template")

        return connector_config_defs

    def extract_template_config_defs(
        self,
        fm_template: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Flatten unique ``config_defs`` entries across every template entry.

        When the same config name appears in multiple template entries, the
        first occurrence wins.
        """
        template_config_defs: List[Dict[str, Any]] = []
        seen_config_names: Set[str] = set()

        if "templates" in fm_template:
            if not isinstance(fm_template["templates"], (list, tuple)):
                self.logger.error(
                    f"fm_template['templates'] is not a list, got "
                    f"{type(fm_template['templates'])}: {fm_template['templates']}"
                )
                return template_config_defs

            for i, template in enumerate(fm_template["templates"]):
                self.logger.debug(
                    f"Processing template {i} for config_defs: {type(template)}"
                )
                if not isinstance(template, dict):
                    self.logger.warning(f"Template {i} is not a dict: {type(template)}")
                    continue

                if "config_defs" in template:
                    self.logger.debug(
                        f"Template {i} has config_defs: {type(template['config_defs'])}"
                    )
                    if isinstance(template["config_defs"], (list, tuple)):
                        for config_def in template["config_defs"]:
                            if not isinstance(config_def, dict) or "name" not in config_def:
                                continue

                            config_name = config_def["name"]
                            if config_name not in seen_config_names:
                                template_config_defs.append(config_def)
                                seen_config_names.add(config_name)
                                self.logger.debug(
                                    f"Added config '{config_name}' from template {i} "
                                    f"(first definition)"
                                )
                            else:
                                self.logger.debug(
                                    f"Skipping duplicate config '{config_name}' from "
                                    f"template {i} (using first definition)"
                                )
                    else:
                        self.logger.warning(
                            f"Expected config_defs to be a list, got "
                            f"{type(template['config_defs'])}: {template['config_defs']}"
                        )
                        continue
        else:
            self.logger.warning("No 'templates' key found in fm_template")

        self.logger.debug(
            f"Extracted {len(template_config_defs)} unique config definitions "
            f"(using first definition for duplicates)"
        )
        return template_config_defs

    def process_user_config(
        self,
        connector_config_def: Dict[str, Any],
        user_config_value: str,
        template_config_defs: List[Dict[str, Any]],
        fm_configs: Dict[str, str],
        warnings: List[str],
        errors: List[str],
        user_configs: Dict[str, str],
        semantic_match_list: Set[str],
    ) -> None:
        """Dispatch a single ``connector_configs`` entry to its case handler."""
        if connector_config_def.get("value") is not None:
            self._process_value_case(
                connector_config_def,
                user_config_value,
                template_config_defs,
                fm_configs,
                warnings,
                user_configs,
                semantic_match_list,
            )
            return

        if connector_config_def.get("switch") is not None:
            self._process_switch_case(
                connector_config_def,
                user_configs,
                template_config_defs,
                fm_configs,
                warnings,
                errors,
                semantic_match_list,
            )
            return

        if connector_config_def.get("dynamic.mapper") is not None:
            self._process_dynamic_mapper_case(
                connector_config_def,
                user_config_value,
                user_configs,
                template_config_defs,
                fm_configs,
                warnings,
                semantic_match_list,
            )
            return

        if connector_config_def.get("value") is None:
            self._process_null_value_case(
                connector_config_def,
                user_config_value,
                template_config_defs,
                fm_configs,
                warnings,
            )
            return

    def infer_dynamic_mappings(
        self,
        dynamic_mapper_fun_name: str,
        user_config_value: str,
    ) -> Optional[str]:
        """Resolve a known dynamic-mapper lookup; returns None for unknown mappers."""
        if (
            dynamic_mapper_fun_name
            and dynamic_mapper_fun_name == "value.converter.reference.subject.name.strategy.mapper"
        ):
            sm_to_fm_mapping = {
                "io.confluent.kafka.serializers.subject.TopicNameStrategy": "TopicNameStrategy",
                "io.confluent.kafka.serializers.subject.RecordNameStrategy": "RecordNameStrategy",
                "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy": "TopicRecordNameStrategy",
            }
            return sm_to_fm_mapping.get(user_config_value, None)

        self.logger.warning(
            f"Dynamic mapping inference not implemented for {dynamic_mapper_fun_name}. "
            f"Returning None."
        )
        return None

    def find_template_config_def_by_name(
        self,
        name: str,
        template_config_defs: List[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        for template_config_def in template_config_defs:
            if template_config_def.get("name") == name:
                return template_config_def
        return None

    # ---------------------------------------------------------------- private

    def _process_value_case(
        self,
        connector_config_def: Dict[str, Any],
        user_config_value: str,
        template_config_defs: List[Dict[str, Any]],
        fm_configs: Dict[str, str],
        warnings: List[str],
        user_configs: Dict[str, str],
        semantic_match_list: Set[str],
    ) -> None:
        value = connector_config_def.get("value")
        config_name = connector_config_def.get("name")

        if not isinstance(value, str):
            fm_configs[config_name] = (
                str(value).lower() if isinstance(value, bool) else str(value)
            )
            return

        if value is not None and (
            "org.apache.kafka.common.security.plain.PlainLoginModule" in value
            or "/mnt/secrets/connect-sr" in value
            or (
                "{{.logicalClusterId}}" in value
                and "/mnt/secrets/connect-external-secrets" not in value
            )
        ):
            warnings.append(
                f"{connector_config_def.get('name')} is internal. "
                f"User given value will be ignored."
            )
            return

        matcher = _DEFAULT_PATTERN.search(value)

        if matcher:
            referenced_keys = self._find_referenced_keys(
                value, {td.get("name") for td in template_config_defs}
            )

            for referenced_key in referenced_keys:
                config_name = connector_config_def.get("name")
                if fm_configs.get(config_name) is not None:
                    return
                if referenced_key in user_configs and user_configs[referenced_key].strip():
                    fm_configs[config_name] = user_config_value
                    return

                referenced_template_config_def = self.find_template_config_def_by_name(
                    referenced_key, template_config_defs
                )

                if referenced_template_config_def is not None and not referenced_template_config_def.get(
                    "internal", False
                ):
                    derivation_method = self._resolve_derivation(
                        referenced_key, referenced_template_config_def
                    )
                    if derivation_method:
                        return

                    if value == f"${{{referenced_key}}}":
                        fm_configs[referenced_key] = user_config_value
                elif (
                    referenced_template_config_def is not None
                    and referenced_template_config_def.get("internal", False)
                ):
                    warnings.append(
                        f"The transformed FM config is internal and will be inferred. "
                        f"User given value will be ignored."
                    )
                else:
                    semantic_match_list.add(config_name)
                    self.logger.warning(
                        f"'{config_name}' : Config transform not present in template for "
                        f"'{referenced_key}' which was referenced by connector config "
                        f"'{connector_config_def.get('name')}'. Will attempt a semantic match."
                    )
            return

        config_name = connector_config_def.get("name")
        if value != user_config_value:
            warnings.append(
                f"{config_name} : FM config has constant value '{value}' but user "
                f"provided '{user_config_value}'. User given value will be ignored."
            )
        else:
            fm_configs[connector_config_def.get("name")] = user_config_value
        return

    def _process_switch_case(
        self,
        connector_config_def: Dict[str, Any],
        user_configs: Dict[str, str],
        template_config_defs: List[Dict[str, Any]],
        fm_configs: Dict[str, str],
        warnings: List[str],
        errors: List[str],
        semantic_match_list: Set[str],
    ) -> None:
        switch_cases = connector_config_def.get("switch", {})

        for template_config_key, switch_mapping in switch_cases.items():
            if fm_configs.get(template_config_key) is not None:
                return
            template_config_def = self.find_template_config_def_by_name(
                template_config_key, template_config_defs
            )

            if template_config_def is not None:
                if template_config_def.get("internal", False):
                    warnings.append(
                        "The transformed FM config is internal and will be inferred. "
                        "User given value will be ignored."
                    )
                else:
                    self._process_non_internal_switch_case(
                        connector_config_def,
                        template_config_def,
                        switch_mapping,
                        user_configs,
                        fm_configs,
                        warnings,
                        semantic_match_list,
                    )
            else:
                self.logger.error(
                    f"Switch case key '{template_config_key}' for config "
                    f"'{connector_config_def.get('name')}' is not part of template configs."
                )

    def _process_non_internal_switch_case(
        self,
        connector_config_def: Dict[str, Any],
        template_config_def: Dict[str, Any],
        switch_mapping: Dict[str, str],
        user_configs: Dict[str, str],
        fm_configs: Dict[str, str],
        warnings: List[str],
        semantic_match_list: Set[str],
    ) -> None:
        has_matchers = any(
            value is not None and _DEFAULT_PATTERN.search(value)
            for value in switch_mapping.values()
        )

        if has_matchers:
            template_config_name = template_config_def.get("name")
            derivation_method = self._resolve_derivation(
                template_config_name, template_config_def
            )
            if derivation_method:
                return
            config_name = connector_config_def.get("name")
            semantic_match_list.add(config_name)
            self.logger.error(
                f"'{config_name}' : Switch case has matchers but no derivation method "
                f"for '{template_config_name}'. Complex matcher logic not implemented. "
                f"Will attempt a semantic match."
            )
        else:
            user_value = user_configs.get(connector_config_def.get("name"))
            if user_value is not None:
                high_level_value = self._apply_reverse_switch(switch_mapping, user_value)
                if high_level_value is not None:
                    fm_configs[template_config_def.get("name")] = high_level_value
                else:
                    warnings.append(
                        f"User value '{user_value}' for "
                        f"'{connector_config_def.get('name')}' does not match any value "
                        f"in templateswitch case."
                    )

    def _process_dynamic_mapper_case(
        self,
        connector_config_def: Dict[str, Any],
        user_config_value: str,
        user_configs: Dict[str, str],
        template_config_defs: List[Dict[str, Any]],
        fm_configs: Dict[str, str],
        warnings: List[str],
        semantic_match_list: Set[str],
    ) -> None:
        connector_config_name = connector_config_def.get("name")

        template_config_def = self.find_template_config_def_by_name(
            connector_config_name, template_config_defs
        )

        if template_config_def is not None:
            fm_configs[connector_config_name] = user_config_value
            return
        elif (
            connector_config_def.get("dynamic.mapper") is not None
            and connector_config_def.get("dynamic.mapper").get("name") is not None
        ):
            fm_template_def = self.find_template_config_def_by_name(
                connector_config_name, template_config_defs
            )

            if fm_template_def is not None:
                dynamic_mapping_value = self.infer_dynamic_mappings(
                    connector_config_def.get("dynamic.mapper").get("name"),
                    user_config_value,
                )
                if dynamic_mapping_value is not None:
                    fm_configs[fm_template_def.get("name")] = dynamic_mapping_value
                    self.logger.info(
                        f"Dynamic mapping for '{connector_config_name}' inferred as "
                        f"'{dynamic_mapping_value}'"
                    )
                    return
                elif fm_configs.get(fm_template_def.get("name")) is not None:
                    self.logger.info(
                        f"Dynamic mapping for '{connector_config_name}' already exists in "
                        f"fm_configs, skipping inference."
                    )
                    return

        semantic_match_list.add(connector_config_name)
        self.logger.warning(
            f"Dynamic mapper config '{connector_config_name}' not found in template "
            f"configs. Will attempt semantic matching."
        )

    def _process_null_value_case(
        self,
        connector_config_def: Dict[str, Any],
        user_config_value: str,
        template_config_defs: List[Dict[str, Any]],
        fm_configs: Dict[str, str],
        warnings: List[str],
    ) -> None:
        fm_configs[connector_config_def.get("name")] = user_config_value

    def _find_referenced_keys(self, value: str, high_level_keys: Set[str]) -> Set[str]:
        referenced_keys: Set[str] = set()
        for match in _DEFAULT_PATTERN.finditer(value):
            referenced_key = match.group(1)
            if referenced_key in high_level_keys:
                referenced_keys.add(referenced_key)
        return referenced_keys

    def _apply_reverse_switch(
        self,
        switch_mapping: Dict[str, str],
        user_value: str,
    ) -> Optional[str]:
        for switch_key, switch_value in switch_mapping.items():
            if switch_value == user_value:
                if switch_key == "default":
                    return None
                return switch_key
        return None
