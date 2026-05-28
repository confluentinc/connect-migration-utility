"""Redacts sensitive values from connector configurations."""

import logging
from typing import Any, Dict, Optional, Set

from connect_migrate.constants.sensitive_keys import (
    STATIC_SENSITIVE_CONFIG_KEYS,
    SENSITIVE_KEY_PATTERNS,
)


REDACTED_PLACEHOLDER = "********"


class SensitiveDataRedactor:
    """Decides which config keys are sensitive and redacts their values."""

    def __init__(
        self,
        sensitive_file: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.logger = logger or logging.getLogger(__name__)
        self.file_sensitive_keys: Set[str] = set()
        if sensitive_file:
            self.file_sensitive_keys = self._load_from_file(sensitive_file)
            self.logger.info(f"Loaded {len(self.file_sensitive_keys)} sensitive keys from file.")

    def _load_from_file(self, file_path: str) -> Set[str]:
        keys: Set[str] = set()
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    clean = line.strip()
                    if clean and not clean.startswith("#"):
                        keys.add(clean.lower())
        except Exception as e:
            self.logger.error(f"Failed to read sensitive config file: {e}")
        return keys

    def is_sensitive(self, key: str) -> bool:
        key_lower = key.lower()
        if key_lower in STATIC_SENSITIVE_CONFIG_KEYS:
            return True
        if key_lower in self.file_sensitive_keys:
            return True
        if any(static in key_lower for static in STATIC_SENSITIVE_CONFIG_KEYS):
            return True
        return any(pattern in key_lower for pattern in SENSITIVE_KEY_PATTERNS)

    def redact(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Return a deep copy of ``config`` with sensitive values replaced."""

        def _walk(d: Dict[str, Any]) -> Dict[str, Any]:
            redacted: Dict[str, Any] = {}
            for k, v in d.items():
                if isinstance(v, dict):
                    redacted[k] = _walk(v)
                elif isinstance(v, list):
                    redacted[k] = [_walk(i) if isinstance(i, dict) else i for i in v]
                elif self.is_sensitive(k):
                    redacted[k] = REDACTED_PLACEHOLDER
                else:
                    redacted[k] = v
            return redacted

        return _walk(config)
