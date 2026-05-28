"""
Apache Connect Migration Utility
Copyright 2024-2025 The Apache Software Foundation

This product includes software developed at The Apache Software Foundation.
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from requests.auth import HTTPBasicAuth

from connect_migrate.constants.connector_packs import (
    COMMERCIAL_PACK_CONNECTORS,
    PREMIUM_PACK_CONNECTORS,
    connector_pack_type,
)
from connect_migrate.constants.paths import (
    DEFAULT_FM_TEMPLATE_DIR,
    DISCOVERED_CONFIGS_DIR,
    FM_TRANSFORMS_FALLBACK_FILE,
    SUCCESSFUL_CONFIGS_SUBDIR,
    UNSUCCESSFUL_CONFIGS_SUBDIR,
)
from connect_migrate.mapper.cc_translate_client.translate_api_client import TranslateApiClient
from connect_migrate.mapper.connector_file_parser import parse_connector_file
from connect_migrate.mapper.jdbc.database_inferrer import DatabaseInferrer
from connect_migrate.mapper.jdbc.url_parser import JdbcUrlParser
from connect_migrate.mapper.properties.config_def_processor import ConfigDefProcessor
from connect_migrate.mapper.properties.derivations import FieldDeriver
from connect_migrate.mapper.properties.direct_mappings import DirectMappings
from connect_migrate.mapper.semantic_match_runner import SemanticMatchRunner
from connect_migrate.mapper.semantic_matching.property_matcher import SemanticPropertyMatcher
from connect_migrate.mapper.sm_to_fm_pipeline import SmToFmPipeline
from connect_migrate.mapper.smt.fm_smt_registry import FmSmtRegistry
from connect_migrate.mapper.smt.smt_classifier import SmtClassifier
from connect_migrate.mapper.tco_processor import TcoProcessor
from connect_migrate.mapper.template_resolver import TemplateResolver
from connect_migrate.mapper.templates.connector_class_index import ConnectorClassIndex
from connect_migrate.mapper.templates.sm_template_fetcher import SmTemplateFetcher
from connect_migrate.mapper.templates.template_loader import TemplateLoader
from connect_migrate.mapper.templates.template_selector import TemplateSelector
from connect_migrate.mapper.user_config_matcher import UserConfigMatcher
from connect_migrate.mapper.v1_to_v2.bigquery_transformer import BigQueryV1ToV2Transformer
from connect_migrate.mapper.v1_to_v2.debezium_translator import DebeziumV1ToV2Translator
from connect_migrate.mapper.v1_to_v2.http_transformer import HttpV1ToV2Transformer
from connect_migrate.mapper.v1_to_v2_post_translator import V1ToV2PostTranslator


class ConnectorMapper:
    # Re-exported for back-compat with callers (CLI) that already reference
    # these as class constants. Source of truth is connect_migrate.constants.paths.
    DISCOVERED_CONFIGS_DIR: Path = DISCOVERED_CONFIGS_DIR
    SUCCESSFUL_CONFIGS_SUBDIR: Path = SUCCESSFUL_CONFIGS_SUBDIR
    UNSUCCESSFUL_CONFIGS_SUBDIR: Path = UNSUCCESSFUL_CONFIGS_SUBDIR

    def __init__(self, input_file: Path, output_dir: Path, worker_urls: Optional[List[str]] = None,
                 env_id: Optional[str] = None, lkc_id: Optional[str] = None,
                 bearer_token: Optional[str] = None, disable_ssl_verify: bool = False,
                 worker_username: Optional[str] = None, worker_password: Optional[str] = None,
                 debezium_version: str = 'v2'):
        self.logger = logging.getLogger(__name__)
        self.input_file = input_file
        self.output_dir = output_dir
        self.semantic_matcher = SemanticPropertyMatcher()

        self.debezium_version = debezium_version.lower()
        if self.debezium_version not in ['v1', 'v2']:
            self.logger.warning(f"Invalid debezium_version '{debezium_version}', defaulting to 'v2'")
            self.debezium_version = 'v2'
        self.logger.info(f"Using Debezium version: {self.debezium_version}")

        self.debezium_translator = DebeziumV1ToV2Translator(logger=self.logger)
        self.http_transformer = HttpV1ToV2Transformer(logger=self.logger)
        self.bigquery_transformer = BigQueryV1ToV2Transformer(logger=self.logger)

        self.worker_urls: List[str] = list(worker_urls) if worker_urls else []

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

        # JDBC parsing + database family inference
        self._jdbc_url_parser = JdbcUrlParser(logger=self.logger)
        self._database_inferrer = DatabaseInferrer(self._jdbc_url_parser, logger=self.logger)

        # FM template handling
        self.fm_template_dir = DEFAULT_FM_TEMPLATE_DIR
        self._template_loader = TemplateLoader(self.fm_template_dir, logger=self.logger)
        self._template_selector = TemplateSelector(
            fm_template_dir=self.fm_template_dir,
            debezium_version=self.debezium_version,
            database_type_resolver=self._database_inferrer.infer_database_type,
            logger=self.logger,
        )
        self._connector_class_index = ConnectorClassIndex(self.fm_template_dir, logger=self.logger)
        self.fm_templates = (
            self._template_loader.load_all() if self.fm_template_dir.exists() else {}
        )
        self.connector_class_to_template = self._connector_class_index.build()

        # SM template fetcher (worker REST)
        self._sm_template_fetcher = SmTemplateFetcher(
            disable_ssl_verify=self.disable_ssl_verify,
            worker_auth=self.worker_auth,
            logger=self.logger,
        )

        # SMT (Single Message Transform) classification + FM SMT registry
        self._smt_classifier = SmtClassifier(logger=self.logger)
        self._fm_smt_registry = FmSmtRegistry(
            env_id=env_id,
            lkc_id=lkc_id,
            bearer_token=bearer_token,
            fallback_file=FM_TRANSFORMS_FALLBACK_FILE,
            classifier=self._smt_classifier,
            logger=self.logger,
        )

        # Confluent Cloud /translate API client
        self._translate_api_client = TranslateApiClient(
            env_id=env_id,
            lkc_id=lkc_id,
            bearer_token=bearer_token,
            plugin_name_resolver=self._template_selector.get_plugin_name,
            disable_ssl_verify=self.disable_ssl_verify,
            logger=self.logger,
        )

        # Per-field FM-config derivers (split into 7 grouped classes + facade)
        self._field_deriver = FieldDeriver(self._jdbc_url_parser, logger=self.logger)

        # Per-property mapping helpers
        self._direct_mappings = DirectMappings(logger=self.logger)
        self._config_def_processor = ConfigDefProcessor(
            derivation_resolver=self._field_deriver.lookup,
            logger=self.logger,
        )

        # Static property mappings to prevent incorrect semantic matching
        self.static_property_mappings_source = {
            'key.converter': 'output.key.format',
            'value.converter': 'output.data.format'
        }

        self.static_property_mappings_sink = {
            'key.converter': 'input.key.format',
            'value.converter': 'input.data.format'
        }

        # Converter class to format reverse mappings
        self.converter_to_format_mappings = {
            'io.confluent.connect.avro.AvroConverter': 'AVRO',
            'io.confluent.connect.json.JsonSchemaConverter': 'JSON_SR',
            'io.confluent.connect.protobuf.ProtobufConverter': 'PROTOBUF',
            'org.apache.kafka.connect.json.JsonConverter': 'JSON'
        }

        # Source-of-truth membership sets; re-exposed for back-compat.
        self.premium_pack_connectors: frozenset = PREMIUM_PACK_CONNECTORS
        self.commercial_pack_connectors: frozenset = COMMERCIAL_PACK_CONNECTORS

        # Helpers used by the SM->FM pipeline
        self._semantic_match_runner = SemanticMatchRunner(
            semantic_matcher=self.semantic_matcher, logger=self.logger,
        )
        self._user_config_matcher = UserConfigMatcher(
            config_def_processor=self._config_def_processor, logger=self.logger,
        )
        self._v1_to_v2_post_translator = V1ToV2PostTranslator(
            debezium_version=self.debezium_version,
            debezium_translator=self.debezium_translator,
            http_transformer=self.http_transformer,
            bigquery_transformer=self.bigquery_transformer,
            logger=self.logger,
        )

        self._template_resolver = TemplateResolver(
            worker_urls=self.worker_urls,
            fm_template_dir=self.fm_template_dir,
            fm_templates=self.fm_templates,
            sm_template_fetcher=self._sm_template_fetcher,
            template_selector=self._template_selector,
            logger=self.logger,
        )

        self._pipeline = SmToFmPipeline(
            env_id=self.env_id,
            lkc_id=self.lkc_id,
            bearer_token=self.bearer_token,
            template_resolver=self._template_resolver,
            config_def_processor=self._config_def_processor,
            field_deriver=self._field_deriver,
            translate_api_client=self._translate_api_client,
            smt_classifier=self._smt_classifier,
            fm_smt_registry=self._fm_smt_registry,
            user_config_matcher=self._user_config_matcher,
            semantic_match_runner=self._semantic_match_runner,
            v1_to_v2_post_translator=self._v1_to_v2_post_translator,
            logger=self.logger,
        )

    @staticmethod
    def parse_connector_file(file, all_connectors_dict, logger=None):
        """Back-compat shim. Source of truth is
        ``connect_migrate.mapper.connector_file_parser.parse_connector_file``."""
        parse_connector_file(file, all_connectors_dict, logger)

    def process_connectors(self) -> Optional[Dict[str, Any]]:
        """Process all connectors and generate FM configurations"""
        connectors_dict: Dict[str, Any] = {}
        parse_connector_file(self.input_file, connectors_dict, self.logger)

        if not connectors_dict:
            self.logger.error("No connectors found after parsing the input file.")
            return None
        connectors = list(connectors_dict.values())

        fm_configs: Dict[str, Any] = {}
        for i, connector in enumerate(connectors):
            try:
                if not isinstance(connector, dict):
                    self.logger.error(f"Connector at index {i} is not a dictionary: {type(connector)}")
                    continue

                if 'name' not in connector or 'config' not in connector:
                    self.logger.error(f"Connector at index {i} missing required fields 'name' or 'config'")
                    continue

                result = self.transform_sm_to_fm(connector['name'], connector['config'])

                fm_config = {
                    'name': connector['name'],
                    'sm_config': connector['config'],
                    'config': result['fm_configs'],
                    'mapping_errors': result['errors'],
                    'mapping_warnings': result['warnings'],
                }

                fm_configs[connector['name']] = fm_config

            except Exception as e:
                connector_name = connector.get('name', f'connector_{i}') if isinstance(connector, dict) else f'connector_{i}'
                self.logger.error(f"Error processing connector {connector_name}: {str(e)}")

        return fm_configs

    def connector_pack_type(self, connector_class: str) -> str:
        """Determine connector pack type based on connector class"""
        return connector_pack_type(connector_class)

    def process_tco_information(self) -> Optional[Dict[str, Union[int, Dict[str, Any]]]]:
        """Crawl workers and write a TCO summary into output_dir."""
        return TcoProcessor(
            output_dir=self.output_dir,
            worker_urls=self.worker_urls,
            disable_ssl_verify=self.disable_ssl_verify,
            worker_auth=self.worker_auth,
            logger=self.logger,
        ).process()

    def transform_sm_to_fm(self, connector_name: str, user_configs: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a Self-Managed connector config into a Fully-Managed config."""
        return self._pipeline.run(connector_name, user_configs)