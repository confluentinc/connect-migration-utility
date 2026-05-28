"""Resolves the set of Fully-Managed SMTs available for a given FM plugin.

Tries the Confluent Cloud internal validate endpoint first when CC creds are
provided; falls back to a local ``fm_transforms_list.json`` data file shipped
with this repo.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from connect_migrate.mapper.smt.smt_classifier import SmtClassifier
from connect_migrate.utils.http_session import DEFAULT_HTTP_TIMEOUT, make_http_session


class FmSmtRegistry:
    def __init__(
        self,
        env_id: Optional[str],
        lkc_id: Optional[str],
        bearer_token: Optional[str],
        fallback_file: Path,
        classifier: SmtClassifier,
        logger: Optional[logging.Logger] = None,
    ):
        self.env_id = env_id
        self.lkc_id = lkc_id
        self.bearer_token = bearer_token
        self._classifier = classifier
        self.logger = logger or logging.getLogger(__name__)
        self._fallback = self._load_fallback(fallback_file)
        self._session = make_http_session(
            retries=2, retry_on_methods=("GET", "PUT")
        )

    def _load_fallback(self, fallback_file: Path) -> Dict[str, List[str]]:
        if fallback_file.exists():
            try:
                with open(fallback_file, "r") as f:
                    data = json.load(f)
                self.logger.info(f"Loaded FM transforms fallback with {len(data)} template IDs")
                return data
            except Exception as e:
                self.logger.warning(f"Failed to load FM transforms fallback: {str(e)}")
        else:
            self.logger.warning(f"FM transforms fallback file not found: {fallback_file}")
        return {}

    def get_smts_for_plugin(self, plugin_type: str) -> Set[str]:
        """Return the set of FM-supported transform types for ``plugin_type``."""
        if self.env_id and self.lkc_id and self.bearer_token:
            try:
                url = (
                    f"https://confluent.cloud/api/internal/accounts/{self.env_id}/clusters/"
                    f"{self.lkc_id}/connector-plugins/{plugin_type}/config/validate"
                )
                params = {"extra_fields": "configs/metadata,configs/internal"}
                data = {"transforms": "transform_0", "connector.class": plugin_type}
                headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Basic {_encode_to_base64(self.bearer_token)}",
                }
                response = self._session.put(url, params=params, json=data, headers=headers, timeout=DEFAULT_HTTP_TIMEOUT)
                response.raise_for_status()
                recommended = self._classifier.extract_recommended_transform_types(
                    response.json()
                )
                if recommended:
                    self.logger.info(
                        f"Successfully fetched {len(recommended)} transforms for "
                        f"{plugin_type} via HTTP"
                    )
                    return recommended
            except Exception as e:
                self.logger.warning(
                    f"Failed to fetch FM transforms for {plugin_type} via HTTP: {str(e)}"
                )
        else:
            self.logger.info(
                f"Skipping HTTP call for {plugin_type} - no Confluent Cloud credentials provided"
            )

        if plugin_type in self._fallback:
            transforms = self._fallback[plugin_type]
            self.logger.info(
                f"Using fallback transforms for {plugin_type}: {len(transforms)} transforms"
            )
            return set(transforms)

        self.logger.warning(
            f"No transforms found for {plugin_type} in HTTP call or fallback file"
        )
        return set()


def _encode_to_base64(input_string: str) -> str:
    # Local helper to avoid importing the shared base64 encoder cross-package
    # at module import time; identical behavior to translate_api_client's helper.
    import base64

    return base64.b64encode(input_string.encode("utf-8")).decode("utf-8")
