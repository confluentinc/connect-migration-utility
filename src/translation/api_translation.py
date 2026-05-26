"""Debezium translation hook, FM API translation and base64 encoding."""

from __future__ import annotations

import base64
import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import requests
from requests.auth import HTTPBasicAuth
from translation.matching.semantic_matcher import Property, SemanticMatcher


class ApiTranslationMixin:

    def _apply_debezium_v1_to_v2_if_needed(
        self, 
        original_connector_class: str, 
        fm_configs: Dict[str, Any], 
        warnings: List[str], 
        errors: List[str]
    ) -> Tuple[Dict[str, Any], List[str], List[str]]:
        """
        Apply Debezium v1 to v2 translation if needed.
        
        This is a common post-translation step that should be applied after
        either API translation or SM-to-FM template translation.
        
        Args:
            original_connector_class: The original connector.class from the input config
            fm_configs: The translated FM configurations
            warnings: List of warnings to append to
            errors: List of errors to append to
            
        Returns:
            Tuple of (fm_configs, warnings, errors) with any v1→v2 translations applied
        """
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


    def _translate_connector_config_via_api(self, connector_name: str, config_dict: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Translate connector config using the Confluent Cloud /translate API endpoint.
        
        This method calls the API:
        PUT https://api.confluent.cloud/connect/v1/environments/{env_id}/clusters/{cluster_id}/connector-plugins/{plugin_name}/config/translate
        
        Args:
            connector_name: Name of the connector
            config_dict: Dictionary of connector configuration
            
        Returns:
            Dictionary with 'config' and 'warnings' on success, None on failure
        """
        if not self.env_id or not self.lkc_id or not self.bearer_token:
            self.logger.debug("Skipping /translate API call - missing env_id, lkc_id, or bearer_token")
            return None
        
        connector_class = config_dict.get('connector.class')
        if not connector_class:
            self.logger.warning("Cannot call /translate API - no connector.class in config")
            return None
        
        # Get plugin name from FM template (or JDBC-specific logic)
        plugin_name = self._get_plugin_name_for_connector(connector_class, config_dict)
        if not plugin_name:
            self.logger.warning(f"Cannot call /translate API - could not determine plugin name for connector class: {connector_class}")
            return None
        
        url = f"https://api.confluent.cloud/connect/v1/environments/{self.env_id}/clusters/{self.lkc_id}/connector-plugins/{plugin_name}/config/translate"
        
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Basic {self.encode_to_base64(self.bearer_token)}"
        }
        
        try:
            self.logger.info(f"Calling /translate API for connector '{connector_name}' with plugin '{plugin_name}'")
            self.logger.debug(f"Translate URL: {url}")
            
            response = requests.put(
                url, 
                json=config_dict, 
                headers=headers, 
                verify=not self.disable_ssl_verify
            )
            
            if response.status_code != 200:
                self.logger.warning(f"/translate API failed with status {response.status_code}: {response.text}")
                return None
            
            result = response.json()
            self.logger.info(f"Successfully translated connector '{connector_name}' via /translate API")
            
            # Parse warnings from response
            warnings = []
            if 'warnings' in result and result['warnings']:
                for warning in result['warnings']:
                    field = warning.get('field', 'unknown')
                    message = warning.get('message', '')
                    warnings.append(f"[Translate API] {field}: {message}")
                self.logger.info(f"Translation returned {len(warnings)} warnings")
            
            # Parse errors from response
            errors = []
            if 'errors' in result and result['errors']:
                for error in result['errors']:
                    field = error.get('field', 'unknown')
                    message = error.get('message', '')
                    errors.append(f"[Translate API] {field}: {message}")
                self.logger.info(f"Translation returned {len(errors)} errors")
            
            return {
                'config': result.get('config', {}),
                'warnings': warnings,
                'errors': errors
            }
            
        except requests.exceptions.RequestException as e:
            self.logger.warning(f"/translate API request failed: {str(e)}")
            return None
        except json.JSONDecodeError as e:
            self.logger.warning(f"/translate API returned invalid JSON: {str(e)}")
            return None
        except Exception as e:
            self.logger.warning(f"/translate API call failed with unexpected error: {str(e)}")
            return None


    def encode_to_base64(self, input_string):
        # Convert the input string to bytes
        byte_data = input_string.encode('utf-8')
        # Encode the bytes to base64
        base64_bytes = base64.b64encode(byte_data)
        # Convert the base64 bytes back to string
        base64_string = base64_bytes.decode('utf-8')
        return base64_string

