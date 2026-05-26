"""Offset retrieval for source/sink connectors during migration."""

import logging
from typing import Any, Dict, List, Optional

from data.offset_supported import OFFSET_SUPPORTED_SOURCE_CONNECTOR_TYPES
from discovery.config_discovery import ConfigDiscovery


class OffsetManager:
    _instance = None
    offset_supported_source_connector_types = OFFSET_SUPPORTED_SOURCE_CONNECTOR_TYPES

    def __init__(self, logger):
        if OffsetManager._instance is not None:
            raise Exception("This class is a singleton! Use OffsetManager.get_instance(logger) instead.")
        self.logger = logger
        OffsetManager._instance = self

    @classmethod
    def get_instance(cls, logger=None):
        if cls._instance is None:
            if logger is None:
                logger = logging.getLogger("offset_manager_default_logger")
            cls._instance = cls(logger)
        return cls._instance

    def is_offset_supported_connector(self, connector_type: str, connector_class: str) -> bool:
        if connector_type and connector_type.lower() == "sink":
            return True
        if connector_type and connector_type.lower() == "source":
            if connector_class and connector_class in OffsetManager.offset_supported_source_connector_types:
                self.logger.info(f"Connector class {connector_class} supports offsets")
                return True
            self.logger.info(f"Connector class {connector_class} does NOT support offsets")
        return False

    def get_offsets_of_connector(
        self,
        config: Dict[str, Any],
        disable_ssl_verify: bool = False,
        auth=None,
    ) -> Optional[List[Any]]:
        if not OffsetManager.get_instance(self.logger).is_offset_supported_connector(
            config["type"], config["config"].get("connector.class", "")
        ):
            self.logger.info(f"Connector {config['name']} does not support offsets")
            return None

        offsets_url = f"{config['worker']}/connectors/{config['name']}/offsets"
        self.logger.info(f"Fetching offsets for connector '{config['name']}' from: {offsets_url}")
        try:
            offsets = ConfigDiscovery.get_json_from_url(
                offsets_url, disable_ssl_verify, self.logger, auth=auth
            )
            if not offsets or not offsets.get("offsets"):
                self.logger.error(
                    f"Invalid response for connector '{config['name']}' offsets call at {offsets_url}"
                )
                return None
            return offsets.get("offsets", [])
        except Exception as e:
            self.logger.error(
                f"Error getting offsets for connector '{config['name']}' from {offsets_url}: {str(e)}"
            )
            return None

    def get_connector_configs_offsets(
        self,
        worker_urls: List[str],
        disable_ssl_verify: bool = False,
        auth=None,
    ) -> List[Dict[str, Any]]:
        """Get connector configurations and offsets from all workers."""
        self.logger.info(f"Getting connector configs and offsets for workers: {worker_urls}")
        configs_with_offsets: List[Dict[str, Any]] = []
        for worker_url in worker_urls:
            self.logger.info(f"Getting connector configs for worker: {worker_url}")
            configs_with_offsets.extend(
                ConfigDiscovery.get_connector_configs_from_worker(
                    worker_url, disable_ssl_verify, self.logger, auth=auth
                )
            )

        for config in configs_with_offsets:
            offsets = self.get_offsets_of_connector(config, disable_ssl_verify, auth=auth)
            if offsets:
                config["offsets"] = offsets
                self.logger.info(f"Connector {config['name']} offsets: {offsets}")
            else:
                self.logger.info(f"Connector {config['name']} has no offsets")
        return configs_with_offsets