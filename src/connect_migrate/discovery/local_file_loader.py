"""Load SM connector configurations and worker URLs from local files."""

import logging
from typing import Dict, List, Optional


WORKER_CONFIG_PREFIXES: List[str] = [
    "key.",
    "value.",
    "header.",
    "producer.",
    "consumer.",
    "reporter.",
    "error.",
    "confluent.",
    "offset.",
]


class LocalFileLoader:
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)

    def extract_worker_urls_from_file(self, file_path: str) -> List[str]:
        """Parse a Control Center properties file and return any discovered worker URLs."""
        urls = set()
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if (
                        line.startswith("confluent.controlcenter.connect.")
                        and ".cluster=" in line
                    ):
                        _, url_string = line.split("=", 1)
                        for raw_url in url_string.split(","):
                            raw_url = raw_url.strip().rstrip("/")
                            if raw_url.startswith("http://") or raw_url.startswith("https://"):
                                urls.add(raw_url)
        except Exception as e:
            self.logger.error(f"Error reading file '{file_path}': {e}")
        return list(urls)

    def load_configs_from_file(
        self,
        file_path: str,
        allowed_prefixes: Optional[List[str]] = None,
    ) -> Dict[str, str]:
        """Load ``key=value`` configs from a file, optionally filtered to prefix matches."""
        new_configs: Dict[str, str] = {}
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if "=" in line and not line.startswith("#"):
                        key, value = line.split("=", 1)
                        key = key.strip()
                        value = value.strip()
                        if not allowed_prefixes or any(
                            key.startswith(prefix) for prefix in allowed_prefixes
                        ):
                            new_configs[key] = value
        except Exception as e:
            self.logger.error(f"Error reading the worker configs file: {e}")
        return new_configs
