"""
User-config -> FM-template-config matching passes.

Pulled out of the SM->FM pipeline so the two related passes
(``apply_user_config_mapping`` + ``direct_match_pass``) and their shared
consumer/producer-override normalization helper live together.
"""

import logging
from typing import Any, Dict, List, Optional

from connect_migrate.mapper.properties.config_def_processor import ConfigDefProcessor


class UserConfigMatcher:
    def __init__(
        self,
        config_def_processor: ConfigDefProcessor,
        logger: Optional[logging.Logger] = None,
    ):
        self._config_def_processor = config_def_processor
        self.logger = logger or logging.getLogger(__name__)

    def apply_user_config_mapping(
        self,
        config_dict: Dict[str, str],
        connector_config_defs: List[Dict[str, Any]],
        template_config_defs: List[Dict[str, Any]],
        fm_configs: Dict[str, Any],
        warnings: List[str],
        errors: List[str],
        transforms_configs: Dict[str, str],
        semantic_match_list: set,
    ) -> None:
        """Map each user config into ``fm_configs`` via connector/template config-defs.

        Builds two indexes once (O(n)) instead of doing two nested scans per user
        config (O(user_configs × config_defs)).
        """
        # First match wins: master iterated connector_config_defs and broke on
        # first matching name. A naive dict comprehension would let later
        # entries overwrite earlier ones — e.g. the s3-sink template defines
        # `partitioner.class` once as a switch (reverse-maps user's FQN to the
        # short recommended-values form) and again later as a constant FQN; the
        # switch must win.
        connector_def_by_name: Dict[str, Dict[str, Any]] = {}
        for cd in connector_config_defs:
            if isinstance(cd, dict) and 'name' in cd:
                connector_def_by_name.setdefault(cd['name'], cd)
        template_names: set = {
            cd['name']
            for cd in template_config_defs
            if isinstance(cd, dict) and 'name' in cd
        }

        user_config_key = None
        try:
            for user_config_key, user_config_value in config_dict.items():
                if user_config_key.startswith('connector.class') or user_config_key.startswith('name'):
                    continue
                if user_config_key.startswith('transforms') or user_config_key.startswith('predicates'):
                    transforms_configs[user_config_key] = user_config_value
                    continue

                matching_connector_config_def = connector_def_by_name.get(user_config_key)
                if matching_connector_config_def is not None:
                    self._config_def_processor.process_user_config(
                        matching_connector_config_def,
                        user_config_value,
                        template_config_defs,
                        fm_configs,
                        warnings,
                        errors,
                        config_dict,
                        semantic_match_list,
                    )
                    continue

                normalized = self._normalize_consumer_producer_override(user_config_key)
                if user_config_key in template_names or (
                    normalized is not None and normalized in template_names
                ):
                    continue

                warning_msg = (
                    f"Unused connector config '{user_config_key}'. Given value will be ignored. "
                    f"Default value will be used if any."
                )
                warnings.append(warning_msg)
                self.logger.warning(warning_msg)
        except Exception as e:
            self.logger.error(f"Error processing user configs: {str(e)}")
            errors.append(f"Error processing user configs {user_config_key}: {str(e)}")

    def direct_match_pass(
        self,
        config_dict: Dict[str, str],
        template_config_defs: List[Dict[str, Any]],
        fm_configs: Dict[str, Any],
        semantic_match_list: set,
    ) -> None:
        """For each user config not yet in fm_configs, fill it if its name (or its
        consumer/producer-normalized form) matches a template-config-def name."""
        for user_config_key, user_config_value in config_dict.items():
            if user_config_key in fm_configs:
                if user_config_key in semantic_match_list:
                    semantic_match_list.remove(user_config_key)
                continue

            original_user_config_key = user_config_key
            if user_config_key.startswith('producer.override') or user_config_key.startswith('consumer.override'):
                user_config_key = user_config_key.replace('consumer.override.', 'consumer.')
                user_config_key = user_config_key.replace('producer.override.', 'producer.')
            elif user_config_key.startswith('producer.') or user_config_key.startswith('consumer.'):
                user_config_key = user_config_key.replace('producer.', 'producer.override.')
                user_config_key = user_config_key.replace('consumer.', 'consumer.override.')

            try:
                for template_config_def in template_config_defs:
                    if isinstance(template_config_def, dict):
                        template_config_name = template_config_def.get("name")

                        if template_config_name == user_config_key:
                            fm_configs[user_config_key] = user_config_value
                            self.logger.info(f"Direct match found: {user_config_key} = {user_config_value}")
                            if user_config_key in semantic_match_list or original_user_config_key in semantic_match_list:
                                semantic_match_list.remove(user_config_key)
                                semantic_match_list.remove(original_user_config_key)
                            break
                        if template_config_name == original_user_config_key:
                            fm_configs[original_user_config_key] = user_config_value
                            self.logger.info(
                                f"Direct match found: {original_user_config_key} = {user_config_value}"
                            )
                            if user_config_key in semantic_match_list or original_user_config_key in semantic_match_list:
                                semantic_match_list.remove(user_config_key)
                                semantic_match_list.remove(original_user_config_key)
                            break
            except Exception as e:
                self.logger.error(f"Error checking template config match for {user_config_key}: {str(e)}")
                self.logger.error(
                    f"template_config_defs type: {type(template_config_defs)}, content: {template_config_defs}"
                )

    @staticmethod
    def _normalize_consumer_producer_override(key: str) -> Optional[str]:
        """Return the consumer/producer override-form (or non-override-form) of ``key``.

        ``consumer.x`` <-> ``consumer.override.x`` and similarly for producer. Returns
        ``None`` if ``key`` doesn't carry a consumer/producer prefix at all.
        """
        if key.startswith('consumer.override.'):
            return 'consumer.' + key[len('consumer.override.'):]
        if key.startswith('producer.override.'):
            return 'producer.' + key[len('producer.override.'):]
        if key.startswith('consumer.'):
            return 'consumer.override.' + key[len('consumer.'):]
        if key.startswith('producer.'):
            return 'producer.override.' + key[len('producer.'):]
        return None