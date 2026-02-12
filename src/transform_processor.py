"""
Transform Processor Module
Handles Single Message Transform (SMT) processing and classification.
"""

import json
import logging
import base64
import requests
from pathlib import Path
from typing import Dict, Any, List, Set, Optional


class TransformProcessor:
    """Processes and classifies connector transforms (SMTs)."""
    
    def __init__(self, env_id: str = None, lkc_id: str = None, bearer_token: str = None,
                 disable_ssl_verify: bool = False, fm_transforms_file: Path = None,
                 logger: Optional[logging.Logger] = None):
        """
        Initialize the TransformProcessor.
        
        Args:
            env_id: Confluent Cloud environment ID
            lkc_id: Confluent Cloud LKC cluster ID
            bearer_token: Confluent Cloud bearer token (api_key:api_secret)
            disable_ssl_verify: Whether to disable SSL verification
            fm_transforms_file: Path to fallback FM transforms JSON file
            logger: Optional logger instance
        """
        self.logger = logger or logging.getLogger(__name__)
        self.env_id = env_id
        self.lkc_id = lkc_id
        self.bearer_token = bearer_token
        self.disable_ssl_verify = disable_ssl_verify
        
        # Load fallback transforms
        self.fm_transforms_fallback = self._load_fm_transforms_fallback(fm_transforms_file)
    
    def _encode_to_base64(self, input_string: str) -> str:
        """Encode string to Base64"""
        return base64.b64encode(input_string.encode('utf-8')).decode('utf-8')
    
    def _load_fm_transforms_fallback(self, fm_transforms_file: Path = None) -> Dict[str, List[str]]:
        """Load FM transforms from fallback JSON file"""
        if fm_transforms_file is None:
            fm_transforms_file = Path("fm_transforms_list.json")
        
        if not fm_transforms_file.exists():
            self.logger.warning(f"FM transforms fallback file not found: {fm_transforms_file}")
            return {}
        
        try:
            with open(fm_transforms_file, 'r') as f:
                data = json.load(f)
                self.logger.info(f"Loaded FM transforms fallback from {fm_transforms_file}")
                return data
        except Exception as e:
            self.logger.error(f"Error loading FM transforms fallback: {str(e)}")
            return {}
    
    def get_fm_smt(self, plugin_type: str) -> Set[str]:
        """
        Get supported FM SMT types for a plugin.
        
        First tries HTTP call to Confluent Cloud, falls back to local file.
        
        Args:
            plugin_type: Connector plugin type/class
            
        Returns:
            Set of supported transform types
        """
        # Try HTTP call if credentials are provided
        if self.env_id and self.lkc_id and self.bearer_token:
            try:
                url = (
                    f"https://confluent.cloud/api/internal/accounts/{self.env_id}/"
                    f"clusters/{self.lkc_id}/connector-plugins/{plugin_type}/config/validate"
                )
                params = {"extra_fields": "configs/metadata,configs/internal"}
                data = {"transforms": "transform_0", "connector.class": plugin_type}
                headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Basic {self._encode_to_base64(self.bearer_token)}"
                }
                
                response = requests.put(url, params=params, json=data, headers=headers,
                                        verify=not self.disable_ssl_verify, timeout=30)
                response.raise_for_status()
                
                recommended_values = self._extract_recommended_transform_types(response.json())
                if recommended_values:
                    self.logger.info(f"Fetched {len(recommended_values)} transforms for {plugin_type} via HTTP")
                    return recommended_values
            except Exception as e:
                self.logger.warning(f"Failed to fetch FM transforms for {plugin_type} via HTTP: {str(e)}")
        else:
            self.logger.debug(f"Skipping HTTP call for {plugin_type} - no Confluent Cloud credentials")

        # Fallback to local file
        if plugin_type in self.fm_transforms_fallback:
            transforms = self.fm_transforms_fallback[plugin_type]
            self.logger.info(f"Using fallback transforms for {plugin_type}: {len(transforms)} transforms")
            return set(transforms)

        self.logger.warning(f"No transforms found for {plugin_type}")
        return set()
    
    def _extract_recommended_transform_types(self, response_json: Dict[str, Any]) -> Set[str]:
        """Extract recommended transform types from API response"""
        recommended_values = set()
        
        configs = response_json.get('configs', [])
        for config in configs:
            if config.get('definition', {}).get('name') == 'transforms.transform_0.type':
                values = config.get('value', {}).get('recommended_values', [])
                recommended_values.update(values)
                break
        
        return recommended_values
    
    @staticmethod
    def extract_transforms_config(config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract transform-related configs from connector config"""
        transforms_config = {}
        
        transforms_chain = config.get("transforms", "")
        if transforms_chain:
            transforms_config["transforms"] = transforms_chain
            
            aliases = [alias.strip() for alias in transforms_chain.split(",") if alias.strip()]
            for alias in aliases:
                for key, value in config.items():
                    if isinstance(key, str) and key.startswith(f"transforms.{alias}."):
                        transforms_config[key] = value
        
        predicates_chain = config.get("predicates", "")
        if predicates_chain:
            transforms_config["predicates"] = predicates_chain
            
            aliases = [alias.strip() for alias in predicates_chain.split(",") if alias.strip()]
            for alias in aliases:
                for key, value in config.items():
                    if isinstance(key, str) and key.startswith(f"predicates.{alias}."):
                        transforms_config[key] = value
        
        return transforms_config
    
    def get_transforms_config(self, config: Dict[str, Any], plugin_type: str) -> Dict[str, Dict[str, Any]]:
        """Get classified transforms config for a connector"""
        fm_smt = self.get_fm_smt(plugin_type)
        return self.classify_transform_configs_with_full_chain(config, fm_smt)
    
    def classify_transform_configs_with_full_chain(
        self,
        config: Dict[str, Any],
        allowed_transform_types: Set[str]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Classify transforms into allowed and disallowed based on FM support.
        
        Args:
            config: Connector configuration dictionary
            allowed_transform_types: Set of transform types supported in FM
            
        Returns:
            Dictionary with 'allowed', 'disallowed', and 'mapping_errors' keys
        """
        result: Dict[str, Dict[str, Any]] = {
            'allowed': {},
            'disallowed': {},
            'mapping_errors': []
        }

        transform_chain = config.get("transforms", "")
        aliases = [alias.strip() for alias in transform_chain.split(",") if alias.strip()]

        allowed_aliases = []
        disallowed_aliases = []
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
                error_msg = (f"Transform '{alias}' of type '{transform_type}' is not supported in "
                           "Fully Managed Connector. Potentially Custom SMT can be used.")
                result['mapping_errors'].append(error_msg)
                self.logger.warning(error_msg)
                for k, v in config.items():
                    if isinstance(k, str) and k.startswith(f"transforms.{alias}."):
                        result['disallowed'][k] = v
                        if k == f"transforms.{alias}.predicate":
                            disallowed_predicates.add(v)

        # Handle predicates
        predicates_chain = config.get("predicates", "")
        predicate_aliases = [alias.strip() for alias in predicates_chain.split(",") if alias.strip()]

        allowed_predicate_aliases = []
        disallowed_predicate_aliases = []

        for predicate_alias in predicate_aliases:
            if predicate_alias in disallowed_predicates:
                disallowed_predicate_aliases.append(predicate_alias)
                error_msg = (f"Predicate '{predicate_alias}' filtered out because it's "
                           "associated with an unsupported transform.")
                result['mapping_errors'].append(error_msg)
                for k, v in config.items():
                    if isinstance(k, str) and k.startswith(f"predicates.{predicate_alias}."):
                        result['disallowed'][k] = v
            else:
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


# Singleton instance
_transform_processor_instance = None


def get_transform_processor(env_id: str = None, lkc_id: str = None, bearer_token: str = None,
                            disable_ssl_verify: bool = False, fm_transforms_file: Path = None,
                            logger: Optional[logging.Logger] = None) -> TransformProcessor:
    """Get or create a TransformProcessor instance."""
    global _transform_processor_instance
    if _transform_processor_instance is None:
        _transform_processor_instance = TransformProcessor(
            env_id, lkc_id, bearer_token, disable_ssl_verify, fm_transforms_file, logger
        )
    return _transform_processor_instance

