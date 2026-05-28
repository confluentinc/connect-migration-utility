"""Client for the Confluent Cloud ``/translate`` API endpoint.

The endpoint converts a Self-Managed connector config dict into the
matching Fully-Managed config dict server-side, returning structured
warnings and errors.

This client is a fail-soft wrapper — any missing CC credential, missing
plugin name, or HTTP/JSON failure produces a ``None`` return rather than
raising; callers are expected to fall back to local template-based mapping.
"""

import base64
import json
import logging
from typing import Any, Callable, Dict, Optional

import requests


def encode_to_base64(input_string: str) -> str:
    """Encode a string to base64 (utf-8 in, utf-8 out)."""
    return base64.b64encode(input_string.encode("utf-8")).decode("utf-8")


class TranslateApiClient:
    def __init__(
        self,
        env_id: Optional[str],
        lkc_id: Optional[str],
        bearer_token: Optional[str],
        plugin_name_resolver: Callable[[str, Optional[Dict[str, Any]]], Optional[str]],
        disable_ssl_verify: bool = False,
        logger: Optional[logging.Logger] = None,
    ):
        self.env_id = env_id
        self.lkc_id = lkc_id
        self.bearer_token = bearer_token
        self._resolve_plugin_name = plugin_name_resolver
        self.disable_ssl_verify = disable_ssl_verify
        self.logger = logger or logging.getLogger(__name__)

    def translate(
        self,
        connector_name: str,
        config_dict: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """Call CC's ``/translate`` and return ``{config, warnings, errors}`` on success."""
        if not self.env_id or not self.lkc_id or not self.bearer_token:
            self.logger.debug("Skipping /translate API call - missing env_id, lkc_id, or bearer_token")
            return None

        connector_class = config_dict.get("connector.class")
        if not connector_class:
            self.logger.warning("Cannot call /translate API - no connector.class in config")
            return None

        plugin_name = self._resolve_plugin_name(connector_class, config_dict)
        if not plugin_name:
            self.logger.warning(
                f"Cannot call /translate API - could not determine plugin name for "
                f"connector class: {connector_class}"
            )
            return None

        url = (
            f"https://api.confluent.cloud/connect/v1/environments/{self.env_id}/"
            f"clusters/{self.lkc_id}/connector-plugins/{plugin_name}/config/translate"
        )
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Basic {encode_to_base64(self.bearer_token)}",
        }

        try:
            self.logger.info(
                f"Calling /translate API for connector '{connector_name}' "
                f"with plugin '{plugin_name}'"
            )
            self.logger.debug(f"Translate URL: {url}")

            response = requests.put(
                url,
                json=config_dict,
                headers=headers,
                verify=not self.disable_ssl_verify,
                timeout=(5, 30),
            )

            if response.status_code != 200:
                self.logger.warning(
                    f"/translate API failed with status {response.status_code}: {response.text}"
                )
                return None

            result = response.json()
            self.logger.info(
                f"Successfully translated connector '{connector_name}' via /translate API"
            )

            warnings = []
            if "warnings" in result and result["warnings"]:
                for warning in result["warnings"]:
                    field = warning.get("field", "unknown")
                    message = warning.get("message", "")
                    warnings.append(f"[Translate API] {field}: {message}")
                self.logger.info(f"Translation returned {len(warnings)} warnings")

            errors = []
            if "errors" in result and result["errors"]:
                for error in result["errors"]:
                    field = error.get("field", "unknown")
                    message = error.get("message", "")
                    errors.append(f"[Translate API] {field}: {message}")
                self.logger.info(f"Translation returned {len(errors)} errors")

            return {
                "config": result.get("config", {}),
                "warnings": warnings,
                "errors": errors,
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
