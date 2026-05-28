"""Derive FM CSFLE (Client-Side Field-Level Encryption) on-failure policy fields."""

import re
from typing import Any, Dict, List, Optional

from connect_migrate.mapper.properties.derivations.base import DerivationGroup


class CsfleFieldDeriver(DerivationGroup):
    DERIVATIONS: Dict[str, str] = {}

    def _derive_csfle_enabled(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:

        """Derive csfle.enabled from user configs"""
        # Try to get default from template if available
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'csfle.enabled')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default

        # Default fallback
        return 'false'

    def _derive_csfle_on_failure(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:

        """Derive csfle.onFailure from user configs"""
        # Try to get default from template if available
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'csfle.onFailure')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default

        # Default fallback
        return 'FAIL'
