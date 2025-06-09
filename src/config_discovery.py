import json
import logging
from pathlib import Path
from typing import List, Optional, Dict, Any
import requests
from urllib.parse import urljoin

class ConfigDiscovery:
    def __init__(
        self,
        worker_urls: Optional[str] = None,
        worker_urls_file: Optional[str] = None,
        redact: bool = False,
        output_dir: Path = Path('output')
    ):
        self.logger = logging.getLogger(__name__)
        self.redact = redact
        self.output_dir = output_dir
        self.worker_urls = self._get_worker_urls(worker_urls, worker_urls_file)

    def _get_worker_urls(self, urls: Optional[str], urls_file: Optional[str]) -> List[str]:
        """Get list of worker URLs from either string or file"""
        if urls:
            return [url.strip() for url in urls.split(',')]
        elif urls_file:
            with open(urls_file, 'r') as f:
                return [line.strip() for line in f if line.strip()]
        else:
            raise ValueError("Either worker_urls or worker_urls_file must be provided")

    def _redact_sensitive_info(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Redact sensitive information from connector config"""
        sensitive_keys = {'password', 'secret', 'key', 'token'}
        
        def redact_dict(d: Dict[str, Any]) -> Dict[str, Any]:
            redacted = {}
            for k, v in d.items():
                if isinstance(v, dict):
                    redacted[k] = redact_dict(v)
                elif isinstance(v, list):
                    redacted[k] = [redact_dict(i) if isinstance(i, dict) else i for i in v]
                elif any(sensitive in k.lower() for sensitive in sensitive_keys):
                    redacted[k] = '********'
                else:
                    redacted[k] = v
            return redacted

        return redact_dict(config)

    def _get_connector_configs(self, worker_url: str) -> List[Dict[str, Any]]:
        """Get connector configurations from a worker"""
        try:
            # Get list of connectors
            connectors_url = urljoin(worker_url, '/connectors')
            response = requests.get(connectors_url)
            response.raise_for_status()
            connector_names = response.json()

            # Get config for each connector
            configs = []
            for name in connector_names:
                config_url = urljoin(worker_url, f'/connectors/{name}/config')
                response = requests.get(config_url)
                response.raise_for_status()
                config = response.json()
                
                if self.redact:
                    config = self._redact_sensitive_info(config)
                
                configs.append({
                    'name': name,
                    'config': config
                })

            return configs

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error getting connector configs from {worker_url}: {str(e)}")
            return []

    def discover_and_save(self) -> Path:
        """Discover connector configurations from all workers and save to JSON"""
        all_configs = []
        
        for worker_url in self.worker_urls:
            self.logger.info(f"Discovering connectors from {worker_url}")
            configs = self._get_connector_configs(worker_url)
            all_configs.extend(configs)

        # Save to JSON file
        output_file = self.output_dir / 'connectors.json'
        with open(output_file, 'w') as f:
            json.dump(all_configs, f, indent=2)

        self.logger.info(f"Saved {len(all_configs)} connector configurations to {output_file}")
        return output_file 