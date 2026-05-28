"""
SM->FM transformation pipeline.

``SmToFmPipeline.run`` is the single entrypoint; it runs as a sequence of
named ``_stage_*`` methods so each stage is independently testable. The
heavier user-config-matching and v1->v2 post-translation passes live in
sibling modules and are injected.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

from connect_migrate.mapper.cc_translate_client.translate_api_client import TranslateApiClient
from connect_migrate.mapper.properties.config_def_processor import ConfigDefProcessor
from connect_migrate.mapper.properties.derivations import FieldDeriver
from connect_migrate.mapper.semantic_match_runner import SemanticMatchRunner
from connect_migrate.mapper.smt.fm_smt_registry import FmSmtRegistry
from connect_migrate.mapper.smt.smt_classifier import SmtClassifier
from connect_migrate.mapper.template_resolver import TemplateResolver
from connect_migrate.mapper.user_config_matcher import UserConfigMatcher
from connect_migrate.mapper.v1_to_v2_post_translator import V1ToV2PostTranslator


class SmToFmPipeline:
    def __init__(
        self,
        env_id: Optional[str],
        lkc_id: Optional[str],
        bearer_token: Optional[str],
        template_resolver: TemplateResolver,
        config_def_processor: ConfigDefProcessor,
        field_deriver: FieldDeriver,
        translate_api_client: TranslateApiClient,
        smt_classifier: SmtClassifier,
        fm_smt_registry: FmSmtRegistry,
        user_config_matcher: UserConfigMatcher,
        semantic_match_runner: SemanticMatchRunner,
        v1_to_v2_post_translator: V1ToV2PostTranslator,
        logger: Optional[logging.Logger] = None,
    ):
        self.env_id = env_id
        self.lkc_id = lkc_id
        self.bearer_token = bearer_token
        self._template_resolver = template_resolver
        self._config_def_processor = config_def_processor
        self._field_deriver = field_deriver
        self._translate_api_client = translate_api_client
        self._smt_classifier = smt_classifier
        self._fm_smt_registry = fm_smt_registry
        self._user_config_matcher = user_config_matcher
        self._semantic_match_runner = semantic_match_runner
        self._v1_to_v2 = v1_to_v2_post_translator
        self.logger = logger or logging.getLogger(__name__)

    def run(self, connector_name: str, user_configs: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a Self-Managed connector config into a Fully-Managed config.

        Returns a dict with ``name``, ``fm_configs``, ``warnings``, ``errors``.
        """
        # Stage 1: Validate + normalize.
        config_dict, validation_error = self._stage_validate_and_normalize(user_configs)
        if validation_error is not None:
            return {'fm_configs': [], 'warnings': [], 'errors': [validation_error]}

        original_connector_class = config_dict.get('connector.class', '')

        # Stage 2: Try the /translate API fast-path.
        api_result = self._stage_translate_api(connector_name, config_dict, original_connector_class)
        if api_result is not None:
            return api_result

        # Stage 3: Resolve SM + FM templates.
        template_id = config_dict.get('connector.class')
        sm_template, fm_template = self._template_resolver.resolve(template_id, connector_name, config_dict)
        if fm_template is None:
            return {
                'fm_configs': {'connector.class': template_id, 'name': connector_name},
                'warnings': [],
                'errors': [f"No FM template found for connector class: {template_id}"],
            }

        # Stage 4: Extract config defs from the FM template.
        try:
            connector_config_defs, template_config_defs = self._stage_extract_config_defs(fm_template)
        except ValueError as e:
            self.logger.error(f"Error extracting template components: {e}")
            return {
                'fm_configs': {'connector.class': template_id, 'name': connector_name},
                'warnings': [],
                'errors': [f"Error extracting template components: {e}"],
            }

        fm_configs: Dict[str, Any] = {}
        transforms_configs: Dict[str, str] = {}
        warnings: List[str] = []
        errors: List[str] = []
        semantic_match_list: set = set()

        # Stage 5: Apply defaults (connector.class, name, tasks.max).
        self._stage_apply_defaults(connector_name, config_dict, fm_template, fm_configs, errors)

        # Stage 6: Re-validate the config-def shapes returned by stage 4.
        shape_error = self._stage_validate_config_def_shapes(template_config_defs, connector_config_defs)
        if shape_error is not None:
            errors.append(shape_error)
            return {'fm_configs': [], 'warnings': warnings, 'errors': errors}

        # Stage 7: Apply user-config -> FM-config mapping.
        self._user_config_matcher.apply_user_config_mapping(
            config_dict, connector_config_defs, template_config_defs,
            fm_configs, warnings, errors, transforms_configs, semantic_match_list,
        )

        # Stage 8: Field-derivation pass over template_config_defs.
        self._stage_apply_template_derivations(template_config_defs, config_dict, fm_configs, errors)

        # Stage 9: Direct-name-match pass.
        self._user_config_matcher.direct_match_pass(
            config_dict, template_config_defs, fm_configs, semantic_match_list,
        )

        # Stage 10: Semantic matching for everything we still haven't placed.
        self._semantic_match_runner.do_semantic_matching(
            fm_configs, semantic_match_list, config_dict, template_config_defs, sm_template,
        )

        # Stage 11: Required-config / default-value / recommended-value validation.
        self._semantic_match_runner.check_required_configs(fm_configs, template_config_defs, errors)

        # Stage 12: SMT classification.
        self._stage_classify_transforms(config_dict, fm_template, fm_configs, errors)

        if 'connector.class' in fm_configs:
            self.logger.info(f"Final connector.class value: {fm_configs['connector.class']}")
        else:
            self.logger.warning("connector.class not found in final fm_configs")

        # Stage 13: v1->v2 post-translation transformers.
        fm_configs, warnings, errors = self._v1_to_v2.apply_post_translations(
            original_connector_class, fm_configs, warnings, errors,
        )

        return {
            'name': connector_name,
            'fm_configs': fm_configs,
            'warnings': warnings,
            'errors': errors,
        }

    # ---- pipeline stages -------------------------------------------------

    def _stage_validate_and_normalize(
        self, user_configs: Dict[str, Any]
    ) -> Tuple[Dict[str, str], Optional[str]]:
        """Validate the input and stringify every value. Returns ``(config_dict, error)``."""
        if not isinstance(user_configs, dict):
            return {}, "Input must be a dictionary of configuration key-value pairs"
        if not user_configs:
            return {}, "No configuration properties provided"

        config_dict = {key: str(value) for key, value in user_configs.items()}
        if 'connector.class' not in config_dict:
            return config_dict, "Missing required 'connector.class' configuration"
        return config_dict, None

    def _stage_translate_api(
        self,
        connector_name: str,
        config_dict: Dict[str, str],
        original_connector_class: str,
    ) -> Optional[Dict[str, Any]]:
        """Try the CC ``/translate`` API. Return a fully-formed result dict on success, else ``None``."""
        if not (self.env_id and self.lkc_id and self.bearer_token):
            self.logger.debug(
                f"Skipping /translate API for connector '{connector_name}' - credentials not provided"
            )
            return None

        self.logger.info(f"Attempting /translate API for connector '{connector_name}'")
        try:
            translate_result = self._translate_api_client.translate(connector_name, config_dict)
        except Exception as e:
            self.logger.warning(
                f"/translate API exception for connector '{connector_name}': {str(e)}, "
                f"falling back to SM-to-FM translation"
            )
            return None

        if translate_result is None:
            self.logger.info(
                f"/translate API returned no result for connector '{connector_name}', "
                f"falling back to SM-to-FM translation"
            )
            return None

        self.logger.info(f"Successfully translated connector '{connector_name}' via /translate API")
        fm_configs = translate_result.get('config', {})
        warnings = translate_result.get('warnings', [])
        errors = translate_result.get('errors', [])
        if 'name' not in fm_configs:
            fm_configs['name'] = connector_name

        fm_configs, warnings, errors = self._v1_to_v2.apply_debezium_v1_to_v2_if_needed(
            original_connector_class, fm_configs, warnings, errors,
        )
        return {
            'fm_configs': fm_configs,
            'warnings': warnings,
            'errors': errors,
            'name': connector_name,
        }

    def _stage_extract_config_defs(
        self, fm_template: Dict[str, Any]
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Validate the FM template structure and pull out the two config-def lists."""
        if not isinstance(fm_template, dict):
            raise ValueError(f"Expected fm_template to be a dict, got {type(fm_template)}")
        if 'templates' not in fm_template:
            raise ValueError("fm_template missing 'templates' key")
        if not isinstance(fm_template['templates'], (list, tuple)):
            raise ValueError(
                f"Expected fm_template['templates'] to be a list, got {type(fm_template['templates'])}"
            )

        self.logger.debug(f"Template structure: {list(fm_template.keys())}")
        self.logger.debug(f"Number of templates: {len(fm_template['templates'])}")

        connector_config_defs = self._config_def_processor.extract_connector_config_defs(fm_template)
        template_config_defs = self._config_def_processor.extract_template_config_defs(fm_template)

        self.logger.debug(
            f"Extracted {len(connector_config_defs)} connector config defs and "
            f"{len(template_config_defs)} template config defs"
        )
        return connector_config_defs, template_config_defs

    def _stage_apply_defaults(
        self,
        connector_name: str,
        config_dict: Dict[str, str],
        fm_template: Dict[str, Any],
        fm_configs: Dict[str, Any],
        errors: List[str],
    ) -> None:
        """Apply connector.class (from template_id), name, and tasks.max defaults to ``fm_configs``."""
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
            errors.append("connector.class property is required.")

        fm_configs['name'] = connector_name
        self.logger.info(f"Set name in fm_configs from connector_name parameter: {connector_name}")

        fm_configs['tasks.max'] = config_dict.get('tasks.max', "1")

    def _stage_validate_config_def_shapes(
        self,
        template_config_defs: Any,
        connector_config_defs: Any,
    ) -> Optional[str]:
        """Validate both config-def lists are non-empty lists. Return an error message or ``None``."""
        if not isinstance(template_config_defs, (list, tuple)):
            self.logger.error(
                f"template_config_defs is not a list, got {type(template_config_defs)}: {template_config_defs}"
            )
            return f"Invalid template_config_defs type: {type(template_config_defs)}"
        if not isinstance(connector_config_defs, (list, tuple)):
            self.logger.error(
                f"connector_config_defs is not a list, got {type(connector_config_defs)}: {connector_config_defs}"
            )
            return f"Invalid connector_config_defs type: {type(connector_config_defs)}"
        if not template_config_defs:
            self.logger.error("template_config_defs is empty or None")
            return "template_config_defs is empty or None"
        return None

    def _stage_apply_template_derivations(
        self,
        template_config_defs: List[Dict[str, Any]],
        config_dict: Dict[str, str],
        fm_configs: Dict[str, Any],
        errors: List[str],
    ) -> None:
        """For each template config def, dispatch to a field-deriver and fill ``fm_configs``."""
        self.logger.info(f"Processing {len(template_config_defs)} template config definitions")
        try:
            for template_config_def in template_config_defs:
                template_config_name = template_config_def.get("name")
                is_required = template_config_def.get("required", False)
                self.logger.debug(
                    f"Processing template config: {template_config_name}, required: {is_required}"
                )

                derivation_method = self._field_deriver.lookup(template_config_name, template_config_def)
                if derivation_method:
                    derived_value = derivation_method(
                        config_dict, fm_configs, template_config_defs, template_config_name
                    )
                    if derived_value is not None:
                        fm_configs[template_config_name] = derived_value
                        self.logger.debug(f"Derived value for {template_config_name}: {derived_value}")
                else:
                    self.logger.debug(f"No derivation method found for {template_config_name}")
        except Exception as e:
            self.logger.error(f"Error processing template configs: {str(e)}")
            errors.append(f"Error processing template configs: {str(e)}")

    def _stage_classify_transforms(
        self,
        config_dict: Dict[str, str],
        fm_template: Dict[str, Any],
        fm_configs: Dict[str, Any],
        errors: List[str],
    ) -> None:
        """Resolve which transforms FM allows for this plugin and merge them into ``fm_configs``."""
        plugin_type = "Unknown"
        if fm_template.get('template_id'):
            plugin_type = fm_template.get('template_id')
        elif 'templates' in fm_template and len(fm_template['templates']) > 0:
            plugin_type = fm_template['templates'][0].get('template_id', 'Unknown')

        transforms_data = self._smt_classifier.classify(
            config_dict, self._fm_smt_registry.get_smts_for_plugin(plugin_type)
        )
        fm_configs.update(transforms_data['allowed'])
        if 'mapping_errors' in transforms_data:
            errors.extend(transforms_data['mapping_errors'])