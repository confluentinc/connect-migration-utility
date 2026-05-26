"""Single Message Transform (SMT) discovery and classification."""

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


class TransformsMixin:

    def extract_transforms_config(config_dict):
        # Extract all keys starting with "transforms"
        return {k: v for k, v in config_dict.items() if isinstance(k, str) and k.startswith("transforms")}


    def extract_recommended_transform_types(self, response_json):
        configs = response_json.get("configs", [])
        for config in configs:
            value = config.get("value", {})
            if value.get("name") == "transforms.transform_0.type":
                return value.get("recommended_values", [])
        return []


    def get_FM_SMT(self, plugin_type) -> Set[str]:
        # First try to get transforms via HTTP call if credentials are provided
        if self.env_id and self.lkc_id and self.bearer_token:
            try:
                url = (
                    f"https://confluent.cloud/api/internal/accounts/{self.env_id}/clusters/{self.lkc_id}/connector-plugins/{plugin_type}/config/validate"
                )
                params = {
                    "extra_fields": "configs/metadata,configs/internal"
                }
                data = {
                    "transforms": "transform_0",
                    "connector.class": plugin_type
                }
                headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Basic {self.encode_to_base64(self.bearer_token)}"
                }
                response = requests.put(url, params=params, json=data, headers=headers)
                response.raise_for_status()
                recommended_values = self.extract_recommended_transform_types(response.json())
                if recommended_values:
                    self.logger.info(f"Successfully fetched {len(recommended_values)} transforms for {plugin_type} via HTTP")
                    return recommended_values
            except Exception as e:
                self.logger.warning(f"Failed to fetch FM transforms for {plugin_type} via HTTP: {str(e)}")
        else:
            self.logger.info(f"Skipping HTTP call for {plugin_type} - no Confluent Cloud credentials provided")

        # Fallback to local file
        if plugin_type in self.fm_transforms_fallback:
            transforms = self.fm_transforms_fallback[plugin_type]
            self.logger.info(f"Using fallback transforms for {plugin_type}: {len(transforms)} transforms")
            return set(transforms)

        self.logger.warning(f"No transforms found for {plugin_type} in HTTP call or fallback file")
        return set()


    def get_transforms_config(self, config: Dict[str, Any], plugin_type: str) -> Dict[str, Dict[str, Any]]:

        fm_smt = self.get_FM_SMT(plugin_type)

        return self.classify_transform_configs_with_full_chain(config, fm_smt)


    def classify_transform_configs_with_full_chain(
        self,
        config: Dict[str, Any],
        allowed_transform_types: set
    ) -> Dict[str, Dict[str, Any]]:
        result: Dict[str, Dict[str, Any]] = {
            'allowed': {},
            'disallowed': {},
            'mapping_errors': []
        }

        transform_chain = config.get("transforms", "")
        aliases = [alias.strip() for alias in transform_chain.split(",") if alias.strip()]

        allowed_aliases = []
        disallowed_aliases = []

        # Track which predicates are associated with disallowed transforms
        disallowed_predicates = set()

        for alias in aliases:
            type_key = f"transforms.{alias}.type"
            transform_type = config.get(type_key)

            if not transform_type:
                disallowed_aliases.append(alias)
                error_msg = f"Transform '{alias}' has no type specified"
                result['mapping_errors'].append(error_msg)
                self.logger.warning(error_msg)
                for k, v in config.items():
                    if isinstance(k, str) and k.startswith(f"transforms.{alias}."):
                        result['disallowed'][k] = v
                        # Check if this transform references a predicate
                        if k == f"transforms.{alias}.predicate":
                            disallowed_predicates.add(v)
                continue

            if transform_type in allowed_transform_types:
                allowed_aliases.append(alias)
                for k, v in config.items():
                    if isinstance(k, str) and k.startswith(f"transforms.{alias}."):
                        result['allowed'][k] = v
            else:
                disallowed_aliases.append(alias)
                error_msg = f"Transform '{alias}' of type '{transform_type}' is not supported in Fully Managed Connector. Potentially Custom SMT can be used."
                result['mapping_errors'].append(error_msg)
                self.logger.warning(error_msg)
                for k, v in config.items():
                    if isinstance(k, str) and k.startswith(f"transforms.{alias}."):
                        result['disallowed'][k] = v
                        # Check if this transform references a predicate
                        if k == f"transforms.{alias}.predicate":
                            disallowed_predicates.add(v)
                            self.logger.info(f"Transform {alias} of type {transform_type} is not supported, so its predicate {v} will also be filtered out")

        # Handle predicates
        predicates_chain = config.get("predicates", "")
        predicate_aliases = [alias.strip() for alias in predicates_chain.split(",") if alias.strip()]

        allowed_predicate_aliases = []
        disallowed_predicate_aliases = []

        for predicate_alias in predicate_aliases:
            # Check if this predicate is associated with a disallowed transform
            if predicate_alias in disallowed_predicates:
                disallowed_predicate_aliases.append(predicate_alias)
                predicate_error_msg = f"Predicate '{predicate_alias}' is filtered out because it's associated with an unsupported transform."
                result['mapping_errors'].append(predicate_error_msg)
                for k, v in config.items():
                    if isinstance(k, str) and k.startswith(f"predicates.{predicate_alias}."):
                        result['disallowed'][k] = v
                self.logger.info(f"Predicate {predicate_alias} is associated with a disallowed transform, so it will be filtered out")
            else:
                # This predicate is not associated with any disallowed transform, so it's allowed
                allowed_predicate_aliases.append(predicate_alias)
                for k, v in config.items():
                    if isinstance(k, str) and k.startswith(f"predicates.{predicate_alias}."):
                        result['allowed'][k] = v

        if allowed_aliases:
            result['allowed']["transforms"] = ", ".join(allowed_aliases)
        if disallowed_aliases:
            result['disallowed']["transforms"] = ", ".join(disallowed_aliases)

        if allowed_predicate_aliases:
            result['allowed']["predicates"] = ", ".join(allowed_predicate_aliases)
        if disallowed_predicate_aliases:
            result['disallowed']["predicates"] = ", ".join(disallowed_predicate_aliases)

        return result

