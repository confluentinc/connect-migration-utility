"""SM -> FM translation orchestrator and the composed ConnectorComparator class.

Owns the public entry point `transformSMToFm` (on SmToFmTranslatorMixin) and
the `ConnectorComparator` class that composes every mixin in this package
into the single object that callers use.

Heavy lifting lives in sibling mixins:
  - fm_template_helpers       — read FM template structure
  - fm_derivation_dispatch    — value/switch/dynamic-mapper case handlers
  - fm_semantic_matching      — embedding-based fallback + required-config check
  - api_translation           — /translate API client + Debezium v1->v2 post-step
  - transforms                — SMT/predicate handling
  - template_discovery        — pick the SM and FM templates for a connector
  - connector_batch           — file-of-connectors batch driver
  - tco_info                  — TCO reporting from worker URLs
  - config_parsing            — parse/normalize input connector configs
  - fm_field_derivers         — _derive_* family (connection.url, ssl.mode, etc.)
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List

from requests.auth import HTTPBasicAuth

from data.connector_packs import COMMERCIAL_PACK_CONNECTORS, PREMIUM_PACK_CONNECTORS
from data.debezium_mappings import DEBEZIUM_V1_TO_V2_MAPPING, DEBEZIUM_V2_TO_V1_MAPPING
from data.format_mappings import (
    CONVERTER_TO_FORMAT_MAPPINGS,
    STATIC_PROPERTY_MAPPINGS_SINK,
    STATIC_PROPERTY_MAPPINGS_SOURCE,
)
from data.jdbc_database_types import JDBC_DATABASE_TYPES
from translation.matching.semantic_matcher import SemanticMatcher
from translation.transformers.bigquery import BigQueryV1ToV2Transformer
from translation.transformers.debezium import DebeziumV1ToV2Translator
from translation.transformers.http import HttpV1ToV2Transformer


class SmToFmTranslatorMixin:

    def transformSMToFm(self, connector_name: str, user_configs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform Self-Managed (SM) connector configurations to Fully Managed (FM) configurations.

        Args:
            user_configs: Dictionary of configuration key-value pairs where:
                - Key: Configuration property name (string)
                - Value: Configuration property value (will be converted to string)
                Example: {"connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector", "connection.url": "jdbc:mysql://localhost:3306/mydb"}

        Returns:
            Dictionary containing:
                - 'fm_configs': List of successfully transformed FM configurations
                - 'warnings': List of warning messages
                - 'errors': List of error messages
        """
        result = {
            'fm_configs': [],
            'warnings': [],
            'errors': [],
        }

        # Validate input
        if not isinstance(user_configs, dict):
            result['errors'].append("Input must be a dictionary of configuration key-value pairs")
            return result

        if not user_configs:
            result['errors'].append("No configuration properties provided")
            return result

        # Convert all values to strings
        config_dict = {}
        for key, value in user_configs.items():
            config_dict[key] = str(value)

        if 'connector.class' not in config_dict:
            result['errors'].append("Missing required 'connector.class' configuration")
            return result

        # Store original connector class to detect if v1->v2 translation is needed after SM->FM
        original_connector_class = config_dict.get('connector.class', '')

        translation_done_via_api = False

        # STEP 0: Try /translate API when env_id and lkc_id are available.
        # Preferred path: uses Confluent Cloud's translation service.
        # Falls back to template-based translation on failure or exception.
        if self.env_id and self.lkc_id and self.bearer_token:
            self.logger.info(f"Attempting /translate API for connector '{connector_name}'")
            try:
                translate_result = self._translate_connector_config_via_api(connector_name, config_dict)

                if translate_result is not None:
                    self.logger.info(f"Successfully translated connector '{connector_name}' via /translate API")

                    fm_configs = translate_result.get('config', {})
                    warnings = translate_result.get('warnings', [])
                    errors = translate_result.get('errors', [])

                    if 'name' not in fm_configs:
                        fm_configs['name'] = connector_name

                    translation_done_via_api = True
                else:
                    self.logger.info(f"/translate API returned no result for connector '{connector_name}', falling back to SM-to-FM translation")
            except Exception as e:
                self.logger.warning(f"/translate API exception for connector '{connector_name}': {str(e)}, falling back to SM-to-FM translation")
        else:
            self.logger.debug(f"Skipping /translate API for connector '{connector_name}' - credentials not provided")

        # If API translation succeeded, apply post-translation steps and return
        if translation_done_via_api:
            fm_configs, warnings, errors = self._apply_debezium_v1_to_v2_if_needed(
                original_connector_class, fm_configs, warnings, errors,
            )

            result['fm_configs'] = fm_configs
            result['warnings'] = warnings
            result['errors'] = errors
            result['name'] = connector_name

            return result

        # FALLBACK: template-based translation
        template_id = config_dict.get('connector.class')
        sm_template, fm_template = self._get_templates_for_connector(template_id, connector_name, config_dict)

        if fm_template is None:
            result['errors'].append(f"No FM template found for connector class: {template_id}")
            result['fm_configs'] = {
                'connector.class': template_id,
                'name': connector_name,
            }
            return result

        # Extract template components (following Java TemplateEngine pattern)
        try:
            if not isinstance(fm_template, dict):
                raise ValueError(f"Expected fm_template to be a dict, got {type(fm_template)}")

            if 'templates' not in fm_template:
                raise ValueError("fm_template missing 'templates' key")

            if not isinstance(fm_template['templates'], (list, tuple)):
                raise ValueError(f"Expected fm_template['templates'] to be a list, got {type(fm_template['templates'])}")

            self.logger.debug(f"Template structure: {list(fm_template.keys())}")
            self.logger.debug(f"Number of templates: {len(fm_template['templates'])}")

            connector_config_defs = self._extract_connector_config_defs(fm_template)
            template_config_defs = self._extract_template_config_defs(fm_template)

            self.logger.debug(f"Extracted {len(connector_config_defs)} connector config defs and {len(template_config_defs)} template config defs")

        except Exception as e:
            self.logger.error(f"Error extracting template components: {str(e)}")
            result['errors'].append(f"Error extracting template components: {str(e)}")
            result['fm_configs'] = {
                'connector.class': template_id,
                'name': connector_name,
            }
            return result

        # Initialize FM configs and message lists (following Java pattern)
        fm_configs = {}
        transforms_configs = {}  # transforms and predicates handled separately
        warnings = []
        errors = []
        semantic_match_list = set()  # configs that fell through to semantic matching

        # Step 1: Handle connector.class and name (following Java pattern)
        if 'connector.class' in config_dict:
            template_id = None
            if 'templates' in fm_template and len(fm_template['templates']) > 0:
                template_id = fm_template['templates'][0].get('template_id')
                self.logger.info(f"Found template_id in FM template: {template_id}")
                self.logger.info(f"FM template structure: {list(fm_template.keys())}")
                if 'templates' in fm_template:
                    self.logger.info(f"Number of templates: {len(fm_template['templates'])}")
                    for i, template in enumerate(fm_template['templates']):
                        self.logger.info(f"Template {i}: {template.get('template_id', 'NO_TEMPLATE_ID')}")

            if template_id:
                fm_configs['connector.class'] = template_id
                self.logger.info(f"Set connector.class to template_id: {template_id}")
            else:
                fm_configs['connector.class'] = config_dict['connector.class']
                self.logger.info(f"Set connector.class to original value: {config_dict['connector.class']}")
        else:
            errors.append(f"connector.class property is required.")

        fm_configs['name'] = connector_name
        self.logger.info(f"Set name in fm_configs from connector_name parameter: {connector_name}")

        if 'tasks.max' in config_dict:
            fm_configs['tasks.max'] = config_dict['tasks.max']
        else:
            fm_configs['tasks.max'] = "1"

        # Step 2: Process user configs (following Java pattern)
        if not isinstance(template_config_defs, (list, tuple)):
            self.logger.error(f"template_config_defs is not a list, got {type(template_config_defs)}: {template_config_defs}")
            errors.append(f"Invalid template_config_defs type: {type(template_config_defs)}")
            return result

        if not isinstance(connector_config_defs, (list, tuple)):
            self.logger.error(f"connector_config_defs is not a list, got {type(connector_config_defs)}: {connector_config_defs}")
            errors.append(f"Invalid connector_config_defs type: {type(connector_config_defs)}")
            return result

        if not template_config_defs:
            self.logger.error("template_config_defs is empty or None")
            errors.append("template_config_defs is empty or None")
            return result

        try:
            for user_config_key, user_config_value in config_dict.items():
                if user_config_key.startswith('connector.class') or user_config_key.startswith('name'):
                    continue

                if user_config_key.startswith('transforms') or user_config_key.startswith('predicates'):
                    transforms_configs[user_config_key] = user_config_value
                    continue

                # Find if user config is present in Connector config def
                matching_connector_config_def = None
                if isinstance(connector_config_defs, (list, tuple)):
                    for connector_config_def in connector_config_defs:
                        if isinstance(connector_config_def, dict) and connector_config_def.get('name') == user_config_key:
                            matching_connector_config_def = connector_config_def
                            break
                else:
                    self.logger.warning(f"Expected connector_config_defs to be a list, got {type(connector_config_defs)}")
                    continue

                if matching_connector_config_def is not None:
                    self._process_user_config_in_connector_config_def(
                        matching_connector_config_def,
                        user_config_value,
                        template_config_defs,
                        fm_configs,
                        warnings,
                        errors,
                        config_dict,
                        semantic_match_list,
                    )
                else:
                    config_found_in_template = False

                    try:
                        for template_config_def in template_config_defs:
                            original_user_config_key = user_config_key
                            if (user_config_key.startswith('consumer.') or user_config_key.startswith('producer.')) and \
                                    not user_config_key.startswith('consumer.override.') and not user_config_key.startswith('producer.override.'):
                                user_config_key = user_config_key.replace('consumer.', 'consumer.override.')
                                user_config_key = user_config_key.replace('producer.', 'producer.override.')
                            elif user_config_key.startswith('consumer.override.') or user_config_key.startswith('producer.override.'):
                                user_config_key = user_config_key.replace('consumer.override.', 'consumer.')
                                user_config_key = user_config_key.replace('producer.override.', 'producer.')

                            if isinstance(template_config_def, dict) and (template_config_def.get('name') == user_config_key or template_config_def.get('name') == original_user_config_key):
                                config_found_in_template = True
                                break
                    except Exception as e:
                        self.logger.error(f"Error iterating over template_config_defs for {user_config_key}: {str(e)}")
                        self.logger.error(f"template_config_defs type: {type(template_config_defs)}, content: {template_config_defs}")
                        config_found_in_template = False

                    if not config_found_in_template:
                        warning_msg = f"Unused connector config '{user_config_key}'. Given value will be ignored. Default value will be used if any."
                        warnings.append(warning_msg)
                        self.logger.warning(warning_msg)

        except Exception as e:
            self.logger.error(f"Error processing user configs: {str(e)}")
            errors.append(f"Error processing user configs {user_config_key}: {str(e)}")

        # Step 3: Process template configs using config derivation methods
        self.logger.info(f"Processing {len(template_config_defs)} template config definitions")
        try:
            for template_config_def in template_config_defs:
                template_config_name = template_config_def.get("name")
                is_required = template_config_def.get("required", False)
                self.logger.debug(f"Processing template config: {template_config_name}, required: {is_required}")

                derivation_method = self._get_config_derivation_method(template_config_name, template_config_def)

                if derivation_method:
                    derived_value = derivation_method(config_dict, fm_configs, template_config_defs, template_config_name)
                    if derived_value is not None:
                        fm_configs[template_config_name] = derived_value
                        self.logger.debug(f"Derived value for {template_config_name}: {derived_value}")
                else:
                    self.logger.debug(f"No derivation method found for {template_config_name}")
        except Exception as e:
            self.logger.error(f"Error processing template configs: {str(e)}")
            errors.append(f"Error processing template configs: {str(e)}")

        # Step 4: Before semantic matching, check direct name matches against template config defs
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
                            self.logger.info(f"Direct match found: {original_user_config_key} = {user_config_value}")
                            if user_config_key in semantic_match_list or original_user_config_key in semantic_match_list:
                                semantic_match_list.remove(user_config_key)
                                semantic_match_list.remove(original_user_config_key)
                            break
            except Exception as e:
                self.logger.error(f"Error checking template config match for {user_config_key}: {str(e)}")
                self.logger.error(f"template_config_defs type: {type(template_config_defs)}, content: {template_config_defs}")

        # Step 5: Semantic matching for configs still unresolved
        self._do_semantic_matching(fm_configs, semantic_match_list, config_dict, template_config_defs, sm_template)

        # Step 6: Validate required configs are present and recommended values are respected
        self._check_required_configs(fm_configs, template_config_defs, errors)

        # Step 7: Process transforms / predicates
        plugin_type = "Unknown"
        if fm_template.get('template_id'):
            plugin_type = fm_template.get('template_id')
        elif 'templates' in fm_template and len(fm_template['templates']) > 0:
            plugin_type = fm_template['templates'][0].get('template_id', 'Unknown')

        transforms_data = self.get_transforms_config(config_dict, plugin_type)
        fm_configs.update(transforms_data['allowed'])

        if 'mapping_errors' in transforms_data:
            errors.extend(transforms_data['mapping_errors'])

        if 'connector.class' in fm_configs:
            self.logger.info(f"Final connector.class value: {fm_configs['connector.class']}")
        else:
            self.logger.warning("connector.class not found in final fm_configs")

        # Step 8: Apply Debezium v1->v2 translation if needed (common post-translation step)
        fm_configs, warnings, errors = self._apply_debezium_v1_to_v2_if_needed(
            original_connector_class, fm_configs, warnings, errors,
        )

        # Step 9: Translate HTTP v1 FM configs to v2 FM configs (auto-detect)
        if self.http_transformer.is_http_v1(original_connector_class):
            self.logger.info(f"Detected HTTP V1 config, translating FM configs to V2 format")
            fm_configs, v1_to_v2_warnings, v1_to_v2_errors = self.http_transformer.translate_v1_to_v2(fm_configs)
            for warning in v1_to_v2_warnings:
                warnings.append(f"[HTTP v1→v2 Translation] {warning}")
            for error in v1_to_v2_errors:
                errors.append(f"[HTTP v1→v2 Translation] {error}")
            self.logger.info(f"HTTP V1 to V2 FM translation complete. Connector class is now: {fm_configs.get('connector.class')}")

        # Step 10: Translate BigQuery v1 FM configs to v2 FM configs (auto-detect)
        if self.bigquery_transformer.is_bigquery_v1(original_connector_class):
            self.logger.info(f"Detected BigQuery V1 config, translating FM configs to V2 format")
            fm_configs, v1_to_v2_warnings, v1_to_v2_errors = self.bigquery_transformer.translate_v1_to_v2(fm_configs)
            for warning in v1_to_v2_warnings:
                warnings.append(f"[BigQuery v1→v2 Translation] {warning}")
            for error in v1_to_v2_errors:
                errors.append(f"[BigQuery v1→v2 Translation] {error}")
            self.logger.info(f"BigQuery V1 to V2 FM translation complete. Connector class is now: {fm_configs.get('connector.class')}")

        result = {
            "name": connector_name,
            "fm_configs": fm_configs,
            "warnings": warnings,
            "errors": errors,
        }

        return result


# ---------------------------------------------------------------------------
# Composition root: ConnectorComparator
# ---------------------------------------------------------------------------
# Imported here (not at module top) to avoid pulling sibling mixins into the
# SmToFmTranslatorMixin namespace — the mixin itself only depends on `self.*`.
# ---------------------------------------------------------------------------

from translation.api_translation import ApiTranslationMixin
from discovery.config_parsing import ConfigParsingMixin
from discovery.connector_batch import ConnectorBatchMixin
from translation.fm_derivation_dispatch import FmDerivationDispatchMixin
from translation.fm_field_derivers import FieldDerivationMixin
from translation.fm_semantic_matching import FmSemanticMatchingMixin
from translation.fm_template_helpers import FmTemplateHelpersMixin
from reporting.tco_info import TcoInfoMixin
from translation.template_discovery import TemplateDiscoveryMixin
from translation.transforms import TransformsMixin


class ConnectorComparator(
    TemplateDiscoveryMixin,
    ConfigParsingMixin,
    FieldDerivationMixin,
    FmTemplateHelpersMixin,
    FmDerivationDispatchMixin,
    FmSemanticMatchingMixin,
    SmToFmTranslatorMixin,
    TransformsMixin,
    ApiTranslationMixin,
    ConnectorBatchMixin,
    TcoInfoMixin,
):
    DISCOVERED_CONFIGS_DIR: Path = Path("discovered_configs")
    SUCCESSFUL_CONFIGS_SUBDIR: Path = Path("successful_configs")
    UNSUCCESSFUL_CONFIGS_SUBDIR: Path = Path("unsuccessful_configs_with_errors")

    def __init__(
        self,
        input_file: Path,
        output_dir: Path,
        worker_urls: List[str] = None,
        env_id: str = None,
        lkc_id: str = None,
        bearer_token: str = None,
        disable_ssl_verify: bool = False,
        worker_username: str = None,
        worker_password: str = None,
        debezium_version: str = 'v2',
    ):
        self.logger = logging.getLogger(__name__)
        self.input_file = input_file
        self.output_dir = output_dir
        self.semantic_matcher = SemanticMatcher()

        # Debezium version for CDC template selection (default: v2)
        self.debezium_version = debezium_version.lower()
        if self.debezium_version not in ['v1', 'v2']:
            self.logger.warning(f"Invalid debezium_version '{debezium_version}', defaulting to 'v2'")
            self.debezium_version = 'v2'
        self.logger.info(f"Using Debezium version: {self.debezium_version}")

        self.debezium_translator = DebeziumV1ToV2Translator(logger=self.logger)
        self.http_transformer = HttpV1ToV2Transformer(logger=self.logger)
        self.bigquery_transformer = BigQueryV1ToV2Transformer(logger=self.logger)

        self.debezium_v1_to_v2_mapping = dict(DEBEZIUM_V1_TO_V2_MAPPING)
        self.debezium_v2_to_v1_mapping = dict(DEBEZIUM_V2_TO_V1_MAPPING)

        # Worker URLs for fetching SM templates
        self.worker_urls = worker_urls or []

        # Confluent Cloud credentials for FM transforms (optional)
        self.env_id = env_id
        self.lkc_id = lkc_id
        self.bearer_token = bearer_token
        self.disable_ssl_verify = disable_ssl_verify

        # Basic auth for Connect worker API
        self.worker_username = worker_username
        self.worker_password = worker_password
        self.worker_auth = None
        if self.worker_username and self.worker_password:
            self.worker_auth = HTTPBasicAuth(self.worker_username, self.worker_password)
            self.logger.info("Basic authentication enabled for Connect worker API")
        elif self.worker_username or self.worker_password:
            self.logger.warning("Basic auth username or password provided but not both - authentication disabled")

        if self.disable_ssl_verify:
            self.logger.info("SSL certificate verification is DISABLED")
        else:
            self.logger.info("SSL certificate verification is ENABLED")

        if env_id and lkc_id and bearer_token:
            self.logger.info(f"Confluent Cloud credentials provided: env_id={env_id}, lkc_id={lkc_id}, bearer_token=[HIDDEN]")
        elif env_id or lkc_id or bearer_token:
            self.logger.warning("Partial Confluent Cloud credentials provided - HTTP calls for FM transforms will be skipped")
        else:
            self.logger.info("No Confluent Cloud credentials provided - will use fallback FM transforms only")

        # Load FM templates from the hardcoded template directory
        self.fm_template_dir = Path("templates/fm")
        self.fm_templates = self._load_templates(self.fm_template_dir) if self.fm_template_dir.exists() else {}

        self.connector_class_to_template = self._build_connector_class_mapping()
        self.fm_transforms_fallback = self._load_fm_transforms_fallback()

        # Static lookup tables (imported from data module)
        self.jdbc_database_types = JDBC_DATABASE_TYPES
        self.static_property_mappings_source = STATIC_PROPERTY_MAPPINGS_SOURCE
        self.static_property_mappings_sink = STATIC_PROPERTY_MAPPINGS_SINK
        self.converter_to_format_mappings = CONVERTER_TO_FORMAT_MAPPINGS

        self.premium_pack_connector_dict = dict(PREMIUM_PACK_CONNECTORS)
        self.commercial_pack_connector_dict = dict(COMMERCIAL_PACK_CONNECTORS)
