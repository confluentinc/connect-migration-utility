from typing import List, Optional, Dict, Any
import requests
from config_discovery import ConfigDiscovery


class OffsetManager:
    _instance = None
    offset_supported_source_connector_types = [
        'io.confluent.connect.jdbc.JdbcSourceConnector',
    ]

    def __init__(self, logger):
        if OffsetManager._instance is not None:
            raise Exception("This class is a singleton! Use OffsetManager.get_instance(logger) instead.")
        self.logger = logger
        OffsetManager._instance = self

    @classmethod
    def get_instance(cls, logger=None):
        if cls._instance is None:
            if logger is None:
                raise Exception("Logger must be provided for the first instantiation.")
            cls._instance = cls(logger)
        return cls._instance

    @staticmethod
    def _get_json_from_url(url: str, disable_ssl_verify: bool = False, logger=None) -> Optional[Dict[str, Any]]:
        """Makes an HTTP GET request and returns JSON data."""
        try:
            response = requests.get(url, timeout=5, verify=not disable_ssl_verify)
            response.raise_for_status()
            logger.info(f"Response from {url}: {response.json()}")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"HTTP error for {url}: {e}")
        except ValueError:
            logger.error(f"Invalid JSON response from {url}")
        return None

    @staticmethod
    def _get_connector_configs_from_worker(worker_url: str, disable_ssl_verify: bool = False, logger=None) -> List[Dict[str, Any]]:
        """Get connector configurations from a worker using expanded info endpoint"""
        try:
            # Use the expanded endpoint to get all connector info in one call
            expanded_url = f"{worker_url}/connectors?expand=info"
            logger.info(f"Fetching connector info from: {expanded_url}")

            connectors_data = OffsetManager._get_json_from_url(expanded_url, disable_ssl_verify, logger)
            if not connectors_data:
                logger.error(f"No data received from {expanded_url}")
                return []

            configs = []
            for connector_name, connector_data in connectors_data.items():
                logger.info(f"Processing connector: {connector_name}")

                # Get the info section which contains config, tasks, and type
                info = connector_data.get('info', {})
                config = info.get('config', {})
                tasks = info.get('tasks', [])
                connector_type = info.get('type', 'unknown')

                # Add connector config to list
                configs.append({
                    'name': connector_name,
                    'worker': worker_url,
                    'type': connector_type,
                    'config': config
                })

            return configs

        except Exception as e:
            logger.error(f"Error getting connector configs from {worker_url}: {str(e)}")
            return []


    def is_offset_supported_connector(self, connector_type: str, connector_class: str) -> bool:
        if connector_type and connector_type.lower() == 'sink':
            return True
        elif connector_type and connector_type.lower() == 'source':
            if connector_class and connector_class in OffsetManager.offset_supported_source_connector_types:
                self.logger.info(f"Connector class {connector_class} supports offsets")
                return True
            else:
                self.logger.info(f"Connector class {connector_class} does NOT support offsets")
        return False

    def get_offsets_of_connector(self, config: Dict[str, Any]) -> list[Any] | None:
        if not OffsetManager.get_instance(self.logger).is_offset_supported_connector(config['type'], config['config'].get('connector.class', '')):
            self.logger.info(f"Connector {config['name']} does not support offsets")
            return None

        """Get offsets for a connector from the first available worker URL."""
        offsets_url = f"{config['worker']}/connectors/{config['name']}/offsets"
        self.logger.info(f"Fetching offsets for connector '{config['name']}' from: {offsets_url}")
        try:
            #TODO: Handle disable_ssl_verify if needed
            offsets = OffsetManager._get_json_from_url(offsets_url, False, self.logger)
            if not offsets or not offsets.get('offsets'):
                self.logger.error(f"Invalid response for connector '{config['name']}' offsets call at {offsets_url}")
                return None
            return offsets.get('offsets', [])
        except Exception as e:
            self.logger.error(f"Error getting offsets for connector '{config['name']}' from {offsets_url}: {str(e)}")
            return None

    def get_connector_configs_offsets(self, worker_urls: List[str]) -> List[Dict[str, Any]]:
        """Get connector configurations from all workers"""
        self.logger.info(f"Getting connector configs and offsets for workers: {worker_urls}")
        configs_with_offsets = []
        for worker_url in worker_urls:
            self.logger.info(f"Getting connector configs for worker: {worker_url}")
            #TODO: Handle disable_ssl_verify per worker if needed
            configs_with_offsets.extend(OffsetManager._get_connector_configs_from_worker(worker_url, False, self.logger))

        # Get offsets for each connector
        for config in configs_with_offsets:
            offsets = self.get_offsets_of_connector(config)
            if offsets:
                config['offsets'] = offsets
                self.logger.info(f"Connector {config['name']} offsets: {offsets}")
            else:
                self.logger.info(f"Connector {config['name']} has no offsets")
        return configs_with_offsets
