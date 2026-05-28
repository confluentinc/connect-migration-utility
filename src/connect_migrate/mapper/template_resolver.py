"""
SM + FM template resolver for the SM->FM pipeline.

Given a connector class (and optionally the connector's config so we can pick
up a connector-local ``worker`` URL), fetches the SM template from a Connect
worker REST endpoint and loads the matching FM template JSON from disk. Also
handles the SftpCsvSourceConnector -> SftpSource template override.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from connect_migrate.mapper.templates.sm_template_fetcher import SmTemplateFetcher
from connect_migrate.mapper.templates.template_selector import TemplateSelector
from connect_migrate.utils.json_files import read_json


class TemplateResolver:
    def __init__(
        self,
        worker_urls: List[str],
        fm_template_dir: Path,
        fm_templates: Dict[str, Any],
        sm_template_fetcher: SmTemplateFetcher,
        template_selector: TemplateSelector,
        logger: Optional[logging.Logger] = None,
    ):
        self.worker_urls = worker_urls
        self.fm_template_dir = fm_template_dir
        self.fm_templates = fm_templates
        self._sm_template_fetcher = sm_template_fetcher
        self._template_selector = template_selector
        self.logger = logger or logging.getLogger(__name__)

    def resolve(
        self,
        connector_class: str,
        connector_name: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
        """Return ``(sm_template, fm_template)``; ``fm_template`` is ``None`` when no FM template applies."""
        worker_url = None
        if config and 'worker' in config:
            worker_url = config['worker']
            self.logger.info(f"Using worker URL from connector config: {worker_url}")
        elif self.worker_urls:
            worker_url = self.worker_urls[0]
            self.logger.info(f"Using first worker URL from global list: {worker_url}")

        if worker_url:
            self.logger.info(f"Fetching SM template for {connector_class} via HTTP from {worker_url}...")
            sm_template = self._sm_template_fetcher.fetch(connector_class, worker_url)
        else:
            self.logger.info(f"No worker URL available - skipping SM template fetch for {connector_class}")
            sm_template = {}

        fm_template_path = self._template_selector.find_template_path(connector_class, connector_name, config)

        # Special mapping for SFTP connectors
        if not fm_template_path and connector_class == 'io.confluent.connect.sftp.SftpCsvSourceConnector':
            sftp_template_path = self.fm_template_dir / 'SftpSource_resolved_templates.json'
            if sftp_template_path.exists():
                fm_template_path = str(sftp_template_path)
                self.logger.info(f"Mapped SFTP connector to SftpSource template: {fm_template_path}")
            else:
                self.logger.warning(f"SftpSource template not found at expected path: {sftp_template_path}")

        if fm_template_path:
            try:
                self.fm_templates[fm_template_path] = read_json(fm_template_path)
                self.logger.info(f"Loaded FM template by connector.class: {fm_template_path}")
                loaded_template = self.fm_templates[fm_template_path]
                if 'templates' in loaded_template and len(loaded_template['templates']) > 0:
                    template_id = loaded_template['templates'][0].get('template_id', 'NO_TEMPLATE_ID')
                    self.logger.info(f"Template ID from loaded template: {template_id}")
            except (OSError, json.JSONDecodeError) as e:
                self.logger.error(f"Error loading FM template {fm_template_path}: {str(e)}")
                fm_template_path = None
        else:
            self.logger.error(f"No FM template found for connector.class: {connector_class}")
            fm_template_path = None

        if not fm_template_path:
            self.logger.error(f"Missing required FM template for {connector_class}")
            return sm_template, None

        self.logger.info(f"Selected templates for {connector_class}:")
        if worker_url:
            self.logger.info(f"  SM Template: Fetched via HTTP from {worker_url}")
        else:
            self.logger.info(f"  SM Template: Not available (no worker URL)")
        self.logger.info(f"  FM Template: {fm_template_path}")

        return sm_template, self.fm_templates.get(fm_template_path, {})