"""Field-derivation facade.

Per-field FM-config derivations are split across 7 small grouped classes
under this package, each extending
:class:`connect_migrate.mapper.properties.derivations.base.DerivationGroup`.

This module exposes a single :class:`FieldDeriver` facade that owns one
instance of each group and provides:

* :meth:`FieldDeriver.lookup` — for callers that want the bound method to
  invoke later (the same shape ``ConfigDefProcessor`` expects from a
  ``derivation_resolver`` callback).
* :meth:`FieldDeriver.derive` — for callers that want to dispatch and
  invoke in one call.
"""

import logging
from typing import Any, Callable, Dict, List, Optional

from connect_migrate.mapper.properties.derivations.auth_fields import AuthFieldDeriver
from connect_migrate.mapper.properties.derivations.base import DerivationGroup
from connect_migrate.mapper.properties.derivations.connection_fields import (
    ConnectionFieldDeriver,
)
from connect_migrate.mapper.properties.derivations.csfle_fields import CsfleFieldDeriver
from connect_migrate.mapper.properties.derivations.format_fields import FormatFieldDeriver
from connect_migrate.mapper.properties.derivations.redis_fields import RedisFieldDeriver
from connect_migrate.mapper.properties.derivations.schema_registry_fields import (
    SchemaRegistryFieldDeriver,
)
from connect_migrate.mapper.properties.derivations.servicebus_fields import (
    ServiceBusFieldDeriver,
)


class FieldDeriver:
    def __init__(self, jdbc_url_parser, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)
        self._groups: List[DerivationGroup] = [
            ConnectionFieldDeriver(jdbc_url_parser, self.logger),
            FormatFieldDeriver(jdbc_url_parser, self.logger),
            AuthFieldDeriver(jdbc_url_parser, self.logger),
            RedisFieldDeriver(jdbc_url_parser, self.logger),
            ServiceBusFieldDeriver(jdbc_url_parser, self.logger),
            CsfleFieldDeriver(jdbc_url_parser, self.logger),
            SchemaRegistryFieldDeriver(jdbc_url_parser, self.logger),
        ]

    def lookup(
        self,
        config_name: str,
        config_def: Optional[Dict[str, Any]] = None,
    ) -> Optional[Callable[..., Optional[str]]]:
        """Return the bound derivation method for ``config_name``, or ``None``.

        The returned callable has signature
        ``(user_configs, fm_configs, template_config_defs, config_name) -> Optional[str]``,
        matching the original ``_derive_*`` methods on ``ConnectorMapper``.
        """
        for group in self._groups:
            if group.handles(config_name):
                method_name = group.DERIVATIONS[config_name]
                return getattr(group, method_name)
        return None

    def derive(
        self,
        config_name: str,
        user_configs: Dict[str, str],
        fm_configs: Dict[str, str],
        template_config_defs: Optional[List[Dict[str, Any]]] = None,
    ) -> Optional[str]:
        method = self.lookup(config_name)
        if method is None:
            return None
        return method(user_configs, fm_configs, template_config_defs, config_name)
