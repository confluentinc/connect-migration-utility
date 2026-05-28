"""HTTP client for talking to Kafka Connect worker REST APIs.

All HTTP methods are instance methods that reuse a single
:class:`requests.Session` (with retry adapter) held on the client, so
auth/SSL state and connection pooling are shared across calls.
"""

import logging
from typing import Any, Dict, List, Optional

import requests

from connect_migrate.utils.http_session import DEFAULT_HTTP_TIMEOUT, make_http_session


class WorkerRestClient:
    def __init__(
        self,
        disable_ssl_verify: bool = False,
        auth: Optional[Any] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.disable_ssl_verify = disable_ssl_verify
        self.auth = auth
        self.logger = logger or logging.getLogger(__name__)
        self._session = make_http_session(
            disable_ssl_verify=disable_ssl_verify, auth=auth, retries=3
        )

    def is_alive(self, worker_url: str) -> bool:
        """Ping ``<worker_url>/connectors`` and return whether the worker responds OK."""
        test_url = f"{worker_url}/connectors"
        try:
            response = self._session.get(test_url, timeout=(5, 10))
            if response.status_code == 200:
                return True
            self.logger.warning(
                f"Worker at {worker_url} responded with status: {response.status_code}"
            )
        except requests.RequestException as e:
            self.logger.warning(f"Worker at {worker_url} is not reachable: {e}")
        return False

    def get_json(self, url: str) -> Optional[Dict[str, Any]]:
        """Make an HTTP GET request and return the JSON body, or ``None`` on error."""
        try:
            response = self._session.get(url, timeout=DEFAULT_HTTP_TIMEOUT)
            response.raise_for_status()
            payload = response.json()
            self.logger.debug(f"Response from {url} ({len(response.content)} bytes)")
            return payload
        except requests.exceptions.RequestException as e:
            self.logger.error(f"HTTP error for {url}: {e}")
        except ValueError:
            self.logger.error(f"Invalid JSON response from {url}")
        return None

    def get_connector_configs(self, worker_url: str) -> List[Dict[str, Any]]:
        """Fetch all connector configs from a worker via the ``?expand=info`` endpoint."""
        try:
            expanded_url = f"{worker_url}/connectors?expand=info"
            self.logger.info(f"Fetching connector info from: {expanded_url}")
            connectors_data = self.get_json(expanded_url)
            if not connectors_data:
                self.logger.error(f"No data received from {expanded_url}")
                return []

            configs: List[Dict[str, Any]] = []
            for connector_name, connector_data in connectors_data.items():
                self.logger.info(f"Processing connector: {connector_name}")
                info = connector_data.get("info", {})
                config = info.get("config", {})
                connector_type = info.get("type", "unknown")
                configs.append(
                    {
                        "name": connector_name,
                        "worker": worker_url,
                        "type": connector_type,
                        "config": config,
                    }
                )
            return configs
        except Exception as e:
            self.logger.error(f"Error getting connector configs from {worker_url}: {str(e)}")
            return []

    def get_connector_statuses(self, worker_url: str) -> Dict[str, Any]:
        """Fetch connector + task statuses from a worker via the ``?expand=status`` endpoint."""
        try:
            expand_status_url = f"{worker_url}/connectors?expand=status"
            self.logger.info(f"Fetching connector statuses info from: {expand_status_url}")
            connectors_data = self.get_json(expand_status_url)
            if not connectors_data:
                self.logger.error(f"No data received from {expand_status_url}")
                return {}

            statuses_map: Dict[str, Any] = {}
            for connector_name, connector_data in connectors_data.items():
                if connector_name in statuses_map:
                    continue
                statuses_map[connector_name] = {}
                status = connector_data.get("status", {})
                statuses_map[connector_name]["connector_status"] = status.get("connector", {})
                statuses_map[connector_name]["tasks_status"] = status.get("tasks", [])
            return statuses_map
        except Exception as e:
            self.logger.error(f"Error getting connector statuses from {worker_url}: {str(e)}")
            return {}