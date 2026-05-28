"""Fetches a Self-Managed connector template (config schema) from a Connect worker.

Used during mapping to obtain the SM connector's property descriptions and
groups so the semantic matcher can pair them against FM template properties.
"""

import json
import logging
from typing import Any, Dict, Optional

import requests


class SmTemplateFetcher:
    def __init__(
        self,
        disable_ssl_verify: bool = False,
        worker_auth: Optional[Any] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.disable_ssl_verify = disable_ssl_verify
        self.worker_auth = worker_auth
        self.logger = logger or logging.getLogger(__name__)

    def fetch(self, connector_class: str, worker_url: Optional[str] = None) -> Dict[str, Any]:
        if not worker_url:
            self.logger.info(
                f"No worker URL provided - skipping SM template fetch for {connector_class}"
            )
            return {}

        if not worker_url.startswith(("http://", "https://")):
            worker_url = f"http://{worker_url}"
            self.logger.info(f"Added http:// protocol to worker URL: {worker_url}")

        try:
            url = f"{worker_url}/connector-plugins/{connector_class}/config/validate"
            data = {"connector.class": connector_class}
            headers = {"Content-Type": "application/json"}

            self.logger.info(f"Fetching SM template for {connector_class} from {url}")
            self.logger.info(f"Request body: {json.dumps(data, indent=2)}")
            response = requests.put(
                url,
                json=data,
                headers=headers,
                verify=not self.disable_ssl_verify,
                auth=self.worker_auth,
                timeout=(5, 30),
            )
            response.raise_for_status()

            template_data = response.json()
            self.logger.info(
                f"Successfully fetched SM template for {connector_class} from {worker_url}"
            )
            self._log_sm_template_structure(connector_class, template_data)
            return template_data

        except requests.exceptions.RequestException as e:
            self.logger.error(
                f"Failed to fetch SM template for {connector_class} from {worker_url}: {str(e)}"
            )
            return {}

    def _log_sm_template_structure(
        self,
        connector_class: str,
        template_data: Any,
    ) -> None:
        self.logger.info(f"=== SM Template Analysis for {connector_class} ===")
        self.logger.info(f"Template data type: {type(template_data)}")

        if isinstance(template_data, dict):
            self.logger.info(f"Template keys: {list(template_data.keys())}")

            if "configs" in template_data:
                configs = template_data["configs"]
                self.logger.info(
                    f"Number of configs: {len(configs) if isinstance(configs, list) else 'Not a list'}"
                )
                if isinstance(configs, list) and configs:
                    self.logger.info("Sample configs (first 5):")
                    for i, config_def in enumerate(configs[:5]):
                        if isinstance(config_def, dict):
                            name = config_def.get("name", "Unknown")
                            config_type = config_def.get("type", "Unknown")
                            required = config_def.get("required", False)
                            default_value = config_def.get("default_value", "None")
                            self.logger.info(
                                f"  {i+1}. {name} (type: {config_type}, "
                                f"required: {required}, default: {default_value})"
                            )

            if "groups" in template_data:
                groups = template_data["groups"]
                self.logger.info(
                    f"Number of groups: {len(groups) if isinstance(groups, list) else 'Not a list'}"
                )
                if isinstance(groups, list):
                    for i, group in enumerate(groups):
                        if isinstance(group, dict):
                            group_name = group.get("name", "Unknown")
                            group_configs = group.get("configs", [])
                            self.logger.info(
                                f"  Group {i+1}: {group_name} ({len(group_configs)} configs)"
                            )
                            if group_configs:
                                self.logger.info(f"    Sample configs in {group_name}:")
                                for j, config_def in enumerate(group_configs[:3]):
                                    if isinstance(config_def, dict):
                                        name = config_def.get("name", "Unknown")
                                        config_type = config_def.get("type", "Unknown")
                                        self.logger.info(
                                            f"      {j+1}. {name} (type: {config_type})"
                                        )

            if "config" in template_data:
                config_defs = template_data["config"]
                self.logger.info(
                    f"Number of config definitions: "
                    f"{len(config_defs) if isinstance(config_defs, list) else 'Not a list'}"
                )
                if isinstance(config_defs, list) and config_defs:
                    self.logger.info("Sample config definitions (first 5):")
                    for i, config_def in enumerate(config_defs[:5]):
                        if isinstance(config_def, dict):
                            name = config_def.get("name", "Unknown")
                            config_type = config_def.get("type", "Unknown")
                            required = config_def.get("required", False)
                            default_value = config_def.get("default_value", "None")
                            self.logger.info(
                                f"  {i+1}. {name} (type: {config_type}, "
                                f"required: {required}, default: {default_value})"
                            )

            if "sections" in template_data:
                sections = template_data["sections"]
                self.logger.info(
                    f"Number of sections: "
                    f"{len(sections) if isinstance(sections, list) else 'Not a list'}"
                )
                if isinstance(sections, list):
                    for i, section in enumerate(sections):
                        if isinstance(section, dict):
                            section_name = section.get("name", "Unknown")
                            section_configs = section.get("config_defs", [])
                            self.logger.info(
                                f"  Section {i+1}: {section_name} ({len(section_configs)} configs)"
                            )
                            if section_configs:
                                self.logger.info(f"    Sample configs in {section_name}:")
                                for j, config_def in enumerate(section_configs[:3]):
                                    if isinstance(config_def, dict):
                                        name = config_def.get("name", "Unknown")
                                        config_type = config_def.get("type", "Unknown")
                                        self.logger.info(
                                            f"      {j+1}. {name} (type: {config_type})"
                                        )

            for key in ["name", "version", "type"]:
                if key in template_data:
                    self.logger.info(f"{key.capitalize()}: {template_data[key]}")

        self.logger.info(f"=== End SM Template Analysis for {connector_class} ===")
