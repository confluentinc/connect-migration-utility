"""Base class for grouped per-field FM-config derivers.

Each subclass declares its dispatch table as the class attribute
:attr:`DERIVATIONS` (mapping FM config name -> method name on the subclass).
The :meth:`derive` entry point looks up the dispatch and invokes the bound
method with the standard signature:

    _derive_X(user_configs, fm_configs, template_config_defs, config_name)
        -> Optional[str]

Shared helpers used by many derivations live here on the base so subclasses
just inherit them.
"""

import logging
from typing import Any, Dict, List, Optional


class DerivationGroup:
    DERIVATIONS: Dict[str, str] = {}

    def __init__(self, jdbc_url_parser, logger: Optional[logging.Logger] = None):
        self.jdbc_url_parser = jdbc_url_parser
        self.logger = logger or logging.getLogger(__name__)

    def handles(self, config_name: str) -> bool:
        return config_name in self.DERIVATIONS

    def derive(
        self,
        config_name: str,
        user_configs: Dict[str, str],
        fm_configs: Dict[str, str],
        template_config_defs: Optional[List[Dict[str, Any]]] = None,
    ) -> Optional[str]:
        method_name = self.DERIVATIONS.get(config_name)
        if not method_name:
            return None
        return getattr(self, method_name)(
            user_configs, fm_configs, template_config_defs, config_name
        )

    # -------- Shared template helpers (used by several derivation groups) --------

    @staticmethod
    def _is_placeholder(value: str) -> bool:
        return value.startswith("${")

    @staticmethod
    def _extract_placeholder_name(placeholder: str) -> str:
        if placeholder.startswith("${"):
            end_pos = placeholder.find("}", 2)
            if end_pos != -1:
                return placeholder[2:end_pos]
            return placeholder[2:]
        return placeholder

    def _resolve_template_default(
        self,
        template_default: str,
        fm_configs: Dict[str, str],
    ) -> Optional[str]:
        if self._is_placeholder(template_default):
            placeholder_name = self._extract_placeholder_name(template_default)
            if placeholder_name in fm_configs:
                return fm_configs[placeholder_name]
            self.logger.warning(
                f"Placeholder '{placeholder_name}' not found in fm_configs"
            )
            return None
        return template_default

    @staticmethod
    def _get_template_default_value(
        template_config_defs: List[Dict[str, Any]],
        config_name: str,
    ) -> Optional[str]:
        if not template_config_defs:
            return None
        for template_config_def in template_config_defs:
            if template_config_def.get("name") == config_name:
                default_value = template_config_def.get("default_value")
                if default_value is not None:
                    return str(default_value)
        return None
