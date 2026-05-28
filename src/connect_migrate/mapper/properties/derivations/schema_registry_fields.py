"""Derive FM Schema Registry subject-name-strategy fields."""

import re
from typing import Any, Dict, List, Optional

from connect_migrate.mapper.properties.derivations.base import DerivationGroup


class SchemaRegistryFieldDeriver(DerivationGroup):
    DERIVATIONS: Dict[str, str] = {
        'key.converter.key.subject.name.strategy': '_derive_subject_name_strategy',
        'value.converter.value.subject.name.strategy': '_derive_subject_name_strategy',
        'key.subject.name.strategy': '_derive_subject_name_strategy',
        'subject.name.strategy': '_derive_subject_name_strategy',
        'value.subject.name.strategy': '_derive_subject_name_strategy',
        'value.converter.reference.subject.name.strategy': '_derive_reference_subject_name_strategy',
        'key.converter.reference.subject.name.strategy': '_derive_reference_subject_name_strategy',
    }

    def _derive_subject_name_strategy(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive subject name strategy from user configs by extracting recommended values from template config def"""
        
        # Get recommended values from template config definition for the specific config
        recommended_strategies = []
        if template_config_defs and config_name:
            for template_config_def in template_config_defs:
                if isinstance(template_config_def, dict) and template_config_def.get('name') == config_name:
                    recommended_values = template_config_def.get('recommended_values', [])
                    if recommended_values:
                        recommended_strategies.extend(recommended_values)
                        break
        
        # Fallback to common recommended values if not found in template
        if not recommended_strategies:
            recommended_strategies = [
                "TopicNameStrategy",
                "RecordNameStrategy", 
                "TopicRecordNameStrategy"
            ]
        
        # Look for the specific config in user configs
        if config_name and config_name in user_configs:
            config_value = user_configs[config_name]
            # Extract config value by finding last . and get string after that
            if '.' in config_value:
                config_value = config_value.split('.')[-1]
            # Check if any recommended strategy is contained in the config value
            for strategy in recommended_strategies:
                if strategy.lower() == config_value.lower():
                    return strategy
        
        return None

    def _derive_reference_subject_name_strategy(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive reference subject name strategy from user configs by extracting recommended values from template config def"""
        
        # Get recommended values from template config definition for the specific config
        recommended_strategies = []
        if template_config_defs and config_name:
            for template_config_def in template_config_defs:
                if isinstance(template_config_def, dict) and template_config_def.get('name') == config_name:
                    recommended_values = template_config_def.get('recommended_values', [])
                    if recommended_values:
                        recommended_strategies.extend(recommended_values)
                        break
        
        # Fallback to common recommended values if not found in template
        if not recommended_strategies:
            recommended_strategies = [
                "DefaultReferenceSubjectNameStrategy",
                "QualifiedReferenceSubjectNameStrategy"
            ]
        
        # Look for the specific config in user configs
        if config_name and config_name in user_configs:
            config_value = user_configs[config_name]
            # Extract config value by finding last . and get string after that
            if '.' in config_value:
                config_value = config_value.split('.')[-1]
            # Check if any recommended strategy is contained in the config value
            for strategy in recommended_strategies:
                if strategy.lower() == config_value.lower():
                    return strategy
        
        return None
