"""Classifies a connector's SMT chain into FM-allowed vs FM-disallowed transforms."""

import logging
from typing import Any, Dict, List, Optional, Set


class SmtClassifier:
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)

    @staticmethod
    def extract_recommended_transform_types(response_json: Dict[str, Any]) -> List[str]:
        """Read ``transforms.transform_0.type`` recommended values from a CC validate response."""
        configs = response_json.get("configs", [])
        for config in configs:
            value = config.get("value", {})
            if value.get("name") == "transforms.transform_0.type":
                return value.get("recommended_values", [])
        return []

    def classify(
        self,
        config: Dict[str, Any],
        allowed_transform_types: Set[str],
    ) -> Dict[str, Dict[str, Any]]:
        """Split the ``transforms`` and ``predicates`` chains into allowed/disallowed.

        Predicates associated with disallowed transforms are also disallowed.
        Returns ``{'allowed': {...}, 'disallowed': {...}, 'mapping_errors': [...]}``.
        """
        result: Dict[str, Dict[str, Any]] = {
            "allowed": {},
            "disallowed": {},
            "mapping_errors": [],
        }

        transform_chain = config.get("transforms", "")
        aliases = [alias.strip() for alias in transform_chain.split(",") if alias.strip()]

        allowed_aliases: List[str] = []
        disallowed_aliases: List[str] = []
        disallowed_predicates: Set[str] = set()

        for alias in aliases:
            type_key = f"transforms.{alias}.type"
            transform_type = config.get(type_key)

            if not transform_type:
                disallowed_aliases.append(alias)
                error_msg = f"Transform '{alias}' has no type specified"
                result["mapping_errors"].append(error_msg)
                self.logger.warning(error_msg)
                for k, v in config.items():
                    if isinstance(k, str) and k.startswith(f"transforms.{alias}."):
                        result["disallowed"][k] = v
                        if k == f"transforms.{alias}.predicate":
                            disallowed_predicates.add(v)
                continue

            if transform_type in allowed_transform_types:
                allowed_aliases.append(alias)
                for k, v in config.items():
                    if isinstance(k, str) and k.startswith(f"transforms.{alias}."):
                        result["allowed"][k] = v
            else:
                disallowed_aliases.append(alias)
                error_msg = (
                    f"Transform '{alias}' of type '{transform_type}' is not supported in "
                    f"Fully Managed Connector. Potentially Custom SMT can be used."
                )
                result["mapping_errors"].append(error_msg)
                self.logger.warning(error_msg)
                for k, v in config.items():
                    if isinstance(k, str) and k.startswith(f"transforms.{alias}."):
                        result["disallowed"][k] = v
                        if k == f"transforms.{alias}.predicate":
                            disallowed_predicates.add(v)
                            self.logger.info(
                                f"Transform {alias} of type {transform_type} is not supported, "
                                f"so its predicate {v} will also be filtered out"
                            )

        predicates_chain = config.get("predicates", "")
        predicate_aliases = [
            alias.strip() for alias in predicates_chain.split(",") if alias.strip()
        ]

        allowed_predicate_aliases: List[str] = []
        disallowed_predicate_aliases: List[str] = []

        for predicate_alias in predicate_aliases:
            if predicate_alias in disallowed_predicates:
                disallowed_predicate_aliases.append(predicate_alias)
                predicate_error_msg = (
                    f"Predicate '{predicate_alias}' is filtered out because it's "
                    f"associated with an unsupported transform."
                )
                result["mapping_errors"].append(predicate_error_msg)
                for k, v in config.items():
                    if isinstance(k, str) and k.startswith(f"predicates.{predicate_alias}."):
                        result["disallowed"][k] = v
                self.logger.info(
                    f"Predicate {predicate_alias} is associated with a disallowed transform, "
                    f"so it will be filtered out"
                )
            else:
                allowed_predicate_aliases.append(predicate_alias)
                for k, v in config.items():
                    if isinstance(k, str) and k.startswith(f"predicates.{predicate_alias}."):
                        result["allowed"][k] = v

        if allowed_aliases:
            result["allowed"]["transforms"] = ", ".join(allowed_aliases)
        if disallowed_aliases:
            result["disallowed"]["transforms"] = ", ".join(disallowed_aliases)

        if allowed_predicate_aliases:
            result["allowed"]["predicates"] = ", ".join(allowed_predicate_aliases)
        if disallowed_predicate_aliases:
            result["disallowed"]["predicates"] = ", ".join(disallowed_predicate_aliases)

        return result
