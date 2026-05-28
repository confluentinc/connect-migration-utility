"""
v1->v2 post-translation glue.

Wraps the three v1->v2 transformers (Debezium, HTTP, BigQuery) behind a single
``apply_post_translations`` call that runs them in order, prefixing each
transformer's warnings/errors so callers can tell who produced what.

``apply_debezium_v1_to_v2_if_needed`` is exposed separately because the
/translate-API fast-path needs to run only the Debezium step (the other two
transformers are no-ops on translate-API output).
"""

import logging
from typing import Any, Dict, List, Optional, Tuple

from connect_migrate.mapper.v1_to_v2.bigquery_transformer import BigQueryV1ToV2Transformer
from connect_migrate.mapper.v1_to_v2.debezium_translator import DebeziumV1ToV2Translator
from connect_migrate.mapper.v1_to_v2.http_transformer import HttpV1ToV2Transformer


class V1ToV2PostTranslator:
    def __init__(
        self,
        debezium_version: str,
        debezium_translator: DebeziumV1ToV2Translator,
        http_transformer: HttpV1ToV2Transformer,
        bigquery_transformer: BigQueryV1ToV2Transformer,
        logger: Optional[logging.Logger] = None,
    ):
        self.debezium_version = debezium_version
        self.debezium_translator = debezium_translator
        self.http_transformer = http_transformer
        self.bigquery_transformer = bigquery_transformer
        self.logger = logger or logging.getLogger(__name__)

    def apply_debezium_v1_to_v2_if_needed(
        self,
        original_connector_class: str,
        fm_configs: Dict[str, Any],
        warnings: List[str],
        errors: List[str],
    ) -> Tuple[Dict[str, Any], List[str], List[str]]:
        """Apply Debezium v1->v2 translation when the customer asked for v1 and
        the original connector is a Debezium connector. Safe to call from either
        the /translate fast-path or the local-template path."""
        if self.debezium_version == 'v1' and self.debezium_translator.is_debezium_v1(original_connector_class):
            self.logger.info(f"Customer provided Debezium v1 config, translating FM configs to v2 format")
            fm_configs, v1_to_v2_warnings, v1_to_v2_errors = self.debezium_translator.translate_v1_to_v2(
                original_connector_class, fm_configs
            )
            for warning in v1_to_v2_warnings:
                warnings.append(f"[v1→v2 Translation] {warning}")
            for error in v1_to_v2_errors:
                errors.append(f"[v1→v2 Translation] {error}")
            self.logger.info(f"V1 to V2 FM translation complete. Connector class is now: {fm_configs.get('connector.class')}")

        return fm_configs, warnings, errors

    def apply_post_translations(
        self,
        original_connector_class: str,
        fm_configs: Dict[str, Any],
        warnings: List[str],
        errors: List[str],
    ) -> Tuple[Dict[str, Any], List[str], List[str]]:
        """Apply Debezium / HTTP / BigQuery v1->v2 post-translation transformers in order."""
        fm_configs, warnings, errors = self.apply_debezium_v1_to_v2_if_needed(
            original_connector_class, fm_configs, warnings, errors,
        )

        if self.http_transformer.is_http_v1(original_connector_class):
            self.logger.info("Detected HTTP V1 config, translating FM configs to V2 format")
            fm_configs, v1_to_v2_warnings, v1_to_v2_errors = self.http_transformer.translate_v1_to_v2(fm_configs)
            for warning in v1_to_v2_warnings:
                warnings.append(f"[HTTP v1→v2 Translation] {warning}")
            for error in v1_to_v2_errors:
                errors.append(f"[HTTP v1→v2 Translation] {error}")
            self.logger.info(
                f"HTTP V1 to V2 FM translation complete. Connector class is now: {fm_configs.get('connector.class')}"
            )

        if self.bigquery_transformer.is_bigquery_v1(original_connector_class):
            self.logger.info("Detected BigQuery V1 config, translating FM configs to V2 format")
            fm_configs, v1_to_v2_warnings, v1_to_v2_errors = self.bigquery_transformer.translate_v1_to_v2(fm_configs)
            for warning in v1_to_v2_warnings:
                warnings.append(f"[BigQuery v1→v2 Translation] {warning}")
            for error in v1_to_v2_errors:
                errors.append(f"[BigQuery v1→v2 Translation] {error}")
            self.logger.info(
                f"BigQuery V1 to V2 FM translation complete. Connector class is now: {fm_configs.get('connector.class')}"
            )

        return fm_configs, warnings, errors