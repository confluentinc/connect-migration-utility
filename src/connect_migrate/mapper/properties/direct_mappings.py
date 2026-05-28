"""Direct (template-driven) property mappings from SM config to FM config.

Each FM template's ``connector_configs`` section can describe one of three
shapes per property:

* ``value: "${<sm_property>}"``  — direct template-variable mapping; the SM
  property's user-supplied value flows into the FM property.
* ``value: "<literal>"``         — fixed value; if the user supplied a
  different value it's overridden (and an error recorded).
* ``switch: {...}``              — switch-mapping with template variables;
  the first template variable encountered becomes the FM property name.

Plus two derived views over ``config_defs``:

* ``get_required_properties`` — required, non-internal property defs.
* ``get_recommended_values``  — per-property recommended-value lists used
  for validation.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple


class DirectMappings:
    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        self.logger = logger or logging.getLogger(__name__)

    def get_required_properties(self, fm_template: Dict[str, Any]) -> Dict[str, Any]:
        """Return ``{name: config_def}`` for non-internal, required properties."""
        required_props: Dict[str, Any] = {}

        if "templates" in fm_template:
            for template in fm_template["templates"]:
                if "config_defs" in template:
                    for config_def in template["config_defs"]:
                        is_required = config_def.get("required", False)
                        if isinstance(is_required, str):
                            is_required = is_required.lower() == "true"
                        elif not isinstance(is_required, bool):
                            is_required = False

                        if is_required and not config_def.get("internal", False):
                            required_props[config_def["name"]] = config_def

        return required_props

    def is_source_connector(self, fm_template: Dict[str, Any]) -> bool:
        """Return True if the FM template describes a source connector."""
        if not fm_template:
            return True

        if fm_template.get("connector_type"):
            return fm_template["connector_type"] == "SOURCE"

        if "templates" in fm_template:
            for template in fm_template["templates"]:
                if template.get("connector_type"):
                    return template["connector_type"] == "SOURCE"

        connector_class = fm_template.get("connector.class", "")
        if (
            not connector_class
            and "templates" in fm_template
            and len(fm_template["templates"]) > 0
        ):
            connector_class = fm_template["templates"][0].get("connector.class", "")

        source_indicators = ["Source", "CDC", "XStream"]
        sink_indicators = ["Sink"]

        for indicator in source_indicators:
            if indicator in connector_class:
                return True

        for indicator in sink_indicators:
            if indicator in connector_class:
                return False

        return True

    def create_from_template(self, fm_template: Dict[str, Any]) -> Dict[str, str]:
        """Return ``{sm_property: fm_property}`` direct-mapping dict from the template."""
        mappings: Dict[str, str] = {}

        if not fm_template or "templates" not in fm_template:
            return mappings

        for template in fm_template["templates"]:
            if "connector_configs" in template:
                for config in template["connector_configs"]:
                    if "value" in config:
                        value = config["value"]
                        sm_property_template = config["name"]

                        if (
                            isinstance(value, str)
                            and value.startswith("${")
                            and value.endswith("}")
                        ):
                            fm_property_name = value[2:-1]
                            mappings[sm_property_template] = fm_property_name
                        else:
                            mappings[value] = sm_property_template
                    elif "switch" in config:
                        sm_property_template = config["name"]
                        switch_config = config["switch"]

                        for switch_key, switch_values in switch_config.items():
                            if isinstance(switch_values, dict):
                                for condition, switch_value in switch_values.items():
                                    if (
                                        isinstance(switch_value, str)
                                        and switch_value.startswith("${")
                                        and switch_value.endswith("}")
                                    ):
                                        fm_property_name = switch_value[2:-1]
                                        mappings[sm_property_template] = fm_property_name
                                        break
                    else:
                        sm_property_template = config["name"]
                        mappings[sm_property_template] = sm_property_template

        return mappings

    def get_fixed_values(self, fm_template: Dict[str, Any]) -> Dict[str, str]:
        """Return ``{fm_property: fixed_value}`` for properties with literal template values."""
        fixed_values: Dict[str, str] = {}

        if not fm_template or "templates" not in fm_template:
            return fixed_values

        for template in fm_template["templates"]:
            if "connector_configs" in template:
                for config in template["connector_configs"]:
                    if "value" in config:
                        value = config["value"]
                        fm_property = config["name"]

                        if not (
                            isinstance(value, str)
                            and value.startswith("${")
                            and value.endswith("}")
                        ):
                            fixed_values[fm_property] = value

        return fixed_values

    def get_recommended_values(self, fm_template: Dict[str, Any]) -> Dict[str, List[str]]:
        """Return ``{config_name: [recommended_values]}`` from the template's ``config_defs``."""
        recommended_values: Dict[str, List[str]] = {}

        if not fm_template or "templates" not in fm_template:
            return recommended_values

        for template in fm_template["templates"]:
            if "config_defs" in template:
                for config_def in template["config_defs"]:
                    if "recommended_values" in config_def:
                        recommended_values[config_def["name"]] = config_def["recommended_values"]

        return recommended_values

    def apply_to_config(
        self,
        config: Dict[str, Any],
        fm_template: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], List[str]]:
        """Map an SM config through the FM template's direct mappings.

        Returns ``(mapped_config, mapping_errors)``. Fixed-value mismatches
        produce errors; values outside the recommended-values list cause the
        property to be skipped and an error recorded.
        """
        mapped_config: Dict[str, Any] = {}
        mapping_errors: List[str] = []
        direct_mappings = self.create_from_template(fm_template)
        fixed_values = self.get_fixed_values(fm_template)
        recommended_values = self.get_recommended_values(fm_template)

        self.logger.info(f"Created {len(direct_mappings)} direct mappings from template")
        self.logger.info(f"Found {len(fixed_values)} fixed values from template")
        self.logger.info(
            f"Found {len(recommended_values)} properties with recommended values"
        )

        for sm_property, fm_property in direct_mappings.items():
            if sm_property in config:
                if fm_property in fixed_values:
                    template_value = fixed_values[fm_property]
                    sm_value = config[sm_property]
                    if str(sm_value) != str(template_value):
                        mapped_config[fm_property] = template_value
                        error_msg = (
                            f"Property '{sm_property}' value '{sm_value}' overridden by "
                            f"template fixed value '{template_value}' for '{fm_property}'"
                        )
                        mapping_errors.append(error_msg)
                        self.logger.warning(
                            f"Fixed value override: {sm_property}='{sm_value}' -> "
                            f"{fm_property}='{template_value}'"
                        )
                    else:
                        mapped_config[fm_property] = sm_value
                        self.logger.info(
                            f"Direct template mapping (values match): {sm_property} -> {fm_property}"
                        )
                else:
                    sm_value = config[sm_property]
                    mapped_config[fm_property] = sm_value

                    if fm_property in recommended_values:
                        allowed_values = recommended_values[fm_property]
                        if str(sm_value) not in allowed_values:
                            error_msg = (
                                f"Property '{sm_property}' value '{sm_value}' is not in "
                                f"recommended values {allowed_values} for '{fm_property}'"
                            )
                            mapping_errors.append(error_msg)
                            self.logger.error(
                                f"Value validation failed: {sm_property}='{sm_value}' "
                                f"not in {allowed_values}"
                            )
                            continue
                        self.logger.info(
                            f"Direct template mapping (validated): {sm_property} -> {fm_property}"
                        )

        for sm_property, value in config.items():
            if sm_property not in mapped_config and sm_property in direct_mappings.values():
                mapped_config[sm_property] = value
                self.logger.info(f"Same-name mapping: {sm_property}")

        return mapped_config, mapping_errors
