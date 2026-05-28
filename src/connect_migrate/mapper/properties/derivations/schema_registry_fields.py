"""Derive FM Schema Registry subject-name-strategy fields."""

from typing import Any, Dict, List, Optional

from connect_migrate.mapper.properties.derivations.base import DerivationGroup


_SUBJECT_STRATEGY_FALLBACKS: List[str] = [
    "TopicNameStrategy",
    "RecordNameStrategy",
    "TopicRecordNameStrategy",
]

_REFERENCE_STRATEGY_FALLBACKS: List[str] = [
    "DefaultReferenceSubjectNameStrategy",
    "QualifiedReferenceSubjectNameStrategy",
]


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

    def _derive_subject_name_strategy(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: Optional[List[Dict[str, Any]]] = None, config_name: Optional[str] = None) -> Optional[str]:
        return self._resolve_strategy(user_configs, template_config_defs, config_name, _SUBJECT_STRATEGY_FALLBACKS)

    def _derive_reference_subject_name_strategy(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: Optional[List[Dict[str, Any]]] = None, config_name: Optional[str] = None) -> Optional[str]:
        return self._resolve_strategy(user_configs, template_config_defs, config_name, _REFERENCE_STRATEGY_FALLBACKS)

    @staticmethod
    def _resolve_strategy(
        user_configs: Dict[str, str],
        template_config_defs: Optional[List[Dict[str, Any]]],
        config_name: Optional[str],
        fallback_strategies: List[str],
    ) -> Optional[str]:
        """Match the user's strategy value against the template's recommended_values for ``config_name``.

        Falls back to ``fallback_strategies`` if the template doesn't list any.
        """
        recommended_strategies: List[str] = []
        if template_config_defs and config_name:
            for template_config_def in template_config_defs:
                if isinstance(template_config_def, dict) and template_config_def.get('name') == config_name:
                    recommended_values = template_config_def.get('recommended_values', [])
                    if recommended_values:
                        recommended_strategies = list(recommended_values)
                    break
        if not recommended_strategies:
            recommended_strategies = fallback_strategies

        if not (config_name and config_name in user_configs):
            return None
        config_value = user_configs[config_name]
        if '.' in config_value:
            config_value = config_value.split('.')[-1]
        for strategy in recommended_strategies:
            if strategy.lower() == config_value.lower():
                return strategy
        return None
