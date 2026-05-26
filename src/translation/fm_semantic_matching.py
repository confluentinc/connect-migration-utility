"""Semantic matching and required-config validation for FM translation.

After the deterministic dispatch in fm_derivation_dispatch fails to place a
user config, we fall back to embedding-based matching against FM template
config_defs. This module also enforces the required-config and
recommended-values contracts after matching is complete.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any, Dict, List, Optional


class FmSemanticMatchingMixin:

    def _do_semantic_matching(
        self,
        fm_configs: Dict[str, str],
        semantic_match_list: set,
        user_configs: Dict[str, str],
        template_config_defs: List[Dict[str, Any]],
        sm_template: Dict[str, Any],
    ):
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

        fm_properties_dict = {}
        for template_config_def in template_config_defs:
            fm_properties_dict[template_config_def.get('name')] = template_config_def

        self.logger.info(f"FM properties available for matching: {len(fm_properties_dict)}")

        for config_name in semantic_match_list:
            if config_name in user_configs:
                user_value = user_configs[config_name]
                self.logger.info(f"Processing semantic match for '{config_name}' = '{user_value}'")

                sm_prop = self._get_sm_property_from_template(config_name, sm_template)

                if not sm_prop:
                    self.logger.info(f"SM property '{config_name}' not found in template, using fallback")
                    sm_prop = {
                        'name': config_name,
                        'description': f"User config: {config_name}",
                        'type': 'STRING',
                        'section': 'General',
                    }
                else:
                    self.logger.info(f"Found SM property '{config_name}' in template: {sm_prop}")

                result = self.semantic_matcher.find_best_match(sm_prop, fm_properties_dict, semantic_threshold=0.7)

                if result and result.matched_fm_property:
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

    def _check_required_configs(
        self,
        fm_configs: Dict[str, str],
        template_config_defs: List[Dict[str, Any]],
        errors: List[str],
    ):
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

            if is_internal:
                continue

            if is_required and config_name not in fm_configs:
                default_value = template_config_def.get('default_value')
                if default_value is not None:
                    fm_configs[config_name] = str(default_value)
                    self.logger.info(f"Required config '{config_name}' missing but has default value '{default_value}' - using default")
                else:
                    error_msg = f"Required FM Config '{config_name}' could not be derived from given configs."
                    if error_msg not in errors:
                        errors.append(error_msg)
                        self.logger.warning(f"Required config '{config_name}' missing from fm_configs and no default value available. Available keys: {list(fm_configs.keys())}")
                    else:
                        self.logger.debug(f"Duplicate error message for '{config_name}' already exists, skipping")
            elif is_required and config_name in fm_configs:
                self.logger.info(f"Required config '{config_name}' found in fm_configs with value: {fm_configs[config_name]}")

            if config_name in fm_configs:
                fm_config_value = fm_configs[config_name]
                recommended_values = template_config_def.get('recommended_values', [])

                if recommended_values and fm_config_value not in recommended_values:
                    # Try case-insensitive matching for enum-like values
                    fm_config_value_lower = fm_config_value.lower() if isinstance(fm_config_value, str) else str(fm_config_value).lower()
                    recommended_values_lower = [str(v).lower() for v in recommended_values]

                    if fm_config_value_lower not in recommended_values_lower:
                        error_msg = f"FM Config '{config_name}' value '{fm_config_value}' is not in the recommended values list: {recommended_values}"
                        if error_msg not in errors:
                            errors.append(error_msg)
                            self.logger.warning(f"Value '{fm_config_value}' for '{config_name}' not in recommended values (case-insensitive check also failed)")
                        else:
                            self.logger.debug(f"Duplicate error message for '{config_name}' recommended values already exists, skipping")
                    else:
                        self.logger.info(f"Case-insensitive match found for '{config_name}': '{fm_config_value}' matches one of {recommended_values}")
                else:
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

            spec = importlib.util.spec_from_file_location("custom_semantic_matcher", semantic_matcher_file)
            custom_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(custom_module)

            if hasattr(custom_module, 'SemanticMatcher'):
                CustomSemanticMatcher = custom_module.SemanticMatcher
                self.semantic_matcher = CustomSemanticMatcher()
                self.logger.info(f"Successfully loaded custom semantic matcher from {self.semantic_matcher_path}")
            else:
                self.logger.error(f"SemanticMatcher class not found in {self.semantic_matcher_path}")

        except Exception as e:
            self.logger.error(f"Error loading semantic matcher from {self.semantic_matcher_path}: {e}")
            # Continue with default semantic matcher
