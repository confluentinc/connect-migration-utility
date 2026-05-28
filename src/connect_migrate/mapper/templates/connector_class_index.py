"""Builds an index from ``connector.class`` values to their FM template files."""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional


class ConnectorClassIndex:
    """Scans the FM template directory and indexes by ``connector.class``.

    Only files matching ``*_resolved_templates.json`` are included — that's
    the naming convention for FM template files in this repo.
    """

    def __init__(self, fm_template_dir: Path, logger: Optional[logging.Logger] = None) -> None:
        self.fm_template_dir = fm_template_dir
        self.logger = logger or logging.getLogger(__name__)

    def build(self) -> Dict[str, Dict[str, List[str]]]:
        mapping: Dict[str, Dict[str, List[str]]] = {}
        if not self.fm_template_dir:
            return mapping

        for template_file in self.fm_template_dir.glob("*_resolved_templates.json"):
            try:
                with open(template_file, "r") as f:
                    template_data = json.load(f)
                if "connector.class" in template_data:
                    connector_class = template_data["connector.class"]
                    if connector_class not in mapping:
                        mapping[connector_class] = {"fm_templates": []}
                    mapping[connector_class]["fm_templates"].append(str(template_file))
            except Exception as e:
                self.logger.error(f"Error reading template {template_file}: {str(e)}")

        return mapping
