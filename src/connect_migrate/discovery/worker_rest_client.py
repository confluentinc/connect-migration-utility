"""HTTP client for talking to Kafka Connect worker REST APIs.

The instance form holds shared state (auth, ssl flag, logger); the static
methods preserve the legacy call-shape used by other modules.
"""

import logging
from typing import Any, Dict, List, Optional

import requests


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

    def is_alive(self, worker_url: str) -> bool:
        """Ping ``<worker_url>/connectors`` and return whether the worker responds OK."""
        test_url = f"{worker_url}/connectors"
        try:
            response = requests.get(
                test_url,
                timeout=3,
                verify=not self.disable_ssl_verify,
                auth=self.auth,
            )
            if response.status_code == 200:
                return True
            self.logger.warning(
                f"Worker at {worker_url} responded with status: {response.status_code}"
            )
        except requests.RequestException as e:
            self.logger.warning(f"Worker at {worker_url} is not reachable: {e}")
        return False

    @staticmethod
    def get_json(
        url: str,
        disable_ssl_verify: bool = False,
        logger: Optional[logging.Logger] = None,
        auth: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        """Make an HTTP GET request and return the JSON body, or ``None`` on error."""
        if logger is None:
            logger = logging.getLogger("worker_rest_client_default")
        try:
            response = requests.get(url, timeout=5, verify=not disable_ssl_verify, auth=auth)
            response.raise_for_status()
            logger.info(f"Response from {url}: {response.json()}")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP error for {url}: {e}")
        except ValueError:
            logger.error(f"Invalid JSON response from {url}")
        return None

    @staticmethod
    def get_connector_configs(
        worker_url: str,
        disable_ssl_verify: bool = False,
        logger: Optional[logging.Logger] = None,
        auth: Optional[Any] = None,
    ) -> List[Dict[str, Any]]:
        """Fetch all connector configs from a worker via the ``?expand=info`` endpoint."""
        if logger is None:
            logger = logging.getLogger("worker_rest_client_default")
        try:
            expanded_url = f"{worker_url}/connectors?expand=info"
            logger.info(f"Fetching connector info from: {expanded_url}")
            connectors_data = WorkerRestClient.get_json(
                expanded_url, disable_ssl_verify, logger, auth=auth
            )
            if not connectors_data:
                logger.error(f"No data received from {expanded_url}")
                return []

            configs: List[Dict[str, Any]] = []
            for connector_name, connector_data in connectors_data.items():
                logger.info(f"Processing connector: {connector_name}")
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
            logger.error(f"Error getting connector configs from {worker_url}: {str(e)}")
            return []

    @staticmethod
    def get_connector_statuses(
        worker_url: str,
        disable_ssl_verify: bool = False,
        logger: Optional[logging.Logger] = None,
        auth: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Fetch connector + task statuses from a worker via the ``?expand=status`` endpoint."""
        if logger is None:
            logger = logging.getLogger("worker_rest_client_default")
        try:
            expand_status_url = f"{worker_url}/connectors?expand=status"
            logger.info(f"Fetching connector statuses info from: {expand_status_url}")
            connectors_data = WorkerRestClient.get_json(
                expand_status_url, disable_ssl_verify, logger, auth=auth
            )
            if not connectors_data:
                logger.error(f"No data received from {expand_status_url}")
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
            logger.error(f"Error getting connector statuses from {worker_url}: {str(e)}")
            return {}
