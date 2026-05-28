"""Back-compat facade over the discovery sub-modules.

Preserves the external surface used by the rest of the codebase
(``ConfigDiscovery(...)`` constructor, ``FM_CONFIGS_DIR`` class constant,
``discover_and_save()`` instance method, and the static helpers used by
:mod:`connect_migrate.cli.migrate_cli`,
:mod:`connect_migrate.migration.offset_manager`, and
:mod:`connect_migrate.mapper.connector_mapper`).

Internally the work is delegated to:

* :class:`connect_migrate.discovery.worker_rest_client.WorkerRestClient`
* :class:`connect_migrate.discovery.local_file_loader.LocalFileLoader`
* :class:`connect_migrate.utils.sensitive_data_redactor.SensitiveDataRedactor`
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from requests.auth import HTTPBasicAuth

from connect_migrate.constants.paths import (
    COMPILED_INPUT_SM_CONFIGS_FILE,
    FM_CONFIGS_SUBDIR,
)
from connect_migrate.discovery.local_file_loader import (
    LocalFileLoader,
    WORKER_CONFIG_PREFIXES,
)
from connect_migrate.discovery.worker_rest_client import WorkerRestClient
from connect_migrate.utils.sensitive_data_redactor import SensitiveDataRedactor
from connect_migrate.utils.json_files import write_json


class ConfigDiscovery:
    FM_CONFIGS_DIR = FM_CONFIGS_SUBDIR

    def __init__(
        self,
        worker_urls: Optional[str] = None,
        worker_urls_file: Optional[str] = None,
        redact: bool = False,
        output_dir: Path = Path("output"),
        sensitive_file: Optional[str] = None,
        worker_config_file: Optional[str] = None,
        disable_ssl_verify: bool = False,
        worker_username: Optional[str] = None,
        worker_password: Optional[str] = None,
    ):
        self.logger = logging.getLogger(__name__)
        self.redact = redact
        self.output_dir = output_dir
        self.worker_config_file = worker_config_file
        self.disable_ssl_verify = disable_ssl_verify

        self.worker_auth: Optional[HTTPBasicAuth] = None
        if worker_username and worker_password:
            self.worker_auth = HTTPBasicAuth(worker_username, worker_password)
            self.logger.info("Basic authentication enabled for Connect worker API")
        elif worker_username or worker_password:
            self.logger.warning(
                "Basic auth username or password provided but not both - authentication disabled"
            )

        if disable_ssl_verify:
            self.logger.info("SSL certificate verification is DISABLED")
        else:
            self.logger.info("SSL certificate verification is ENABLED")

        self._rest_client = WorkerRestClient(
            disable_ssl_verify=disable_ssl_verify,
            auth=self.worker_auth,
            logger=self.logger,
        )
        self._file_loader = LocalFileLoader(logger=self.logger)
        self._redactor = SensitiveDataRedactor(
            sensitive_file=sensitive_file,
            logger=self.logger,
        )

        self.worker_urls = self._resolve_worker_urls(worker_urls, worker_urls_file)

    def _resolve_worker_urls(
        self,
        urls: Optional[str],
        urls_file: Optional[str],
    ) -> List[str]:
        if urls_file:
            worker_urls = self._file_loader.extract_worker_urls_from_file(urls_file)
        elif urls:
            worker_urls = [u.strip().rstrip("/") for u in urls.split(",") if u.strip()]
            for i, url in enumerate(worker_urls):
                if not url.startswith("http://") and not url.startswith("https://"):
                    worker_urls[i] = "http://" + url
        else:
            raise ValueError("Either worker_urls or worker_urls_file must be provided")

        alive_workers = [u for u in worker_urls if self._rest_client.is_alive(u)]
        if not alive_workers:
            self.logger.error("No reachable Kafka Connect workers found.")
            return []
        self.logger.info(f"Found {len(alive_workers)} reachable workers: {alive_workers}")
        return alive_workers

    def discover_and_save(self) -> Path:
        """Discover connector configurations from all alive workers and save to JSON."""
        all_connectors: Dict[str, Any] = {}

        if self.worker_config_file:
            worker_configs = self._file_loader.load_configs_from_file(
                self.worker_config_file,
                allowed_prefixes=WORKER_CONFIG_PREFIXES,
            )
            self.logger.info(
                f"Loaded {len(worker_configs)} worker configs matching allowed prefixes."
            )
        else:
            worker_configs = {}

        for worker_url in self.worker_urls:
            self.logger.info(f"=== Processing worker: {worker_url} ===")
            configs = self._rest_client.get_connector_configs(worker_url)
            for config in configs:
                if self.redact:
                    all_connectors[config["name"]] = self._redactor.redact(config)
                else:
                    all_connectors[config["name"]] = config

        output_data = {"connectors": all_connectors, "worker_configs": worker_configs}
        output_file = self.output_dir / COMPILED_INPUT_SM_CONFIGS_FILE
        write_json(output_file, output_data)
        self.logger.info(
            f"Saved {len(all_connectors)} connector configurations to {output_file}"
        )
        return output_file

    @staticmethod
    def get_json_from_url(
        url: str,
        disable_ssl_verify: bool = False,
        logger: Optional[logging.Logger] = None,
        auth: Optional[Any] = None,
    ) -> Optional[Dict[str, Any]]:
        client = WorkerRestClient(disable_ssl_verify=disable_ssl_verify, auth=auth, logger=logger)
        return client.get_json(url)

    @staticmethod
    def get_connector_configs_from_worker(
        worker_url: str,
        disable_ssl_verify: bool = False,
        logger: Optional[logging.Logger] = None,
        auth: Optional[Any] = None,
    ) -> List[Dict[str, Any]]:
        client = WorkerRestClient(disable_ssl_verify=disable_ssl_verify, auth=auth, logger=logger)
        return client.get_connector_configs(worker_url)

    @staticmethod
    def get_connector_statuses_from_worker(
        worker_url: str,
        disable_ssl_verify: bool = False,
        logger: Optional[logging.Logger] = None,
        auth: Optional[Any] = None,
    ) -> Dict[str, Any]:
        client = WorkerRestClient(disable_ssl_verify=disable_ssl_verify, auth=auth, logger=logger)
        return client.get_connector_statuses(worker_url)
