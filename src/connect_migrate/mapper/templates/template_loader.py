"""Loads Fully-Managed connector template JSON files from disk."""

import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional


class TemplateLoader:
    """Reads FM template files from a directory on disk.

    Two access modes are provided:

    * :meth:`load_all` reads every ``*.json`` in the directory at once
      (keyed by file *stem* — historical behavior used by callers that
      need a bulk-loaded index).
    * :meth:`load_one` reads a single template file by full path.
    """

    def __init__(self, fm_template_dir: Path, logger: Optional[logging.Logger] = None) -> None:
        self.fm_template_dir = fm_template_dir
        self.logger = logger or logging.getLogger(__name__)

    def load_all(self) -> Dict[str, Dict[str, Any]]:
        templates: Dict[str, Dict[str, Any]] = {}
        if self.fm_template_dir.exists():
            for template_file in self.fm_template_dir.glob("*.json"):
                try:
                    with open(template_file, "r") as f:
                        templates[template_file.stem] = json.load(f)
                    self.logger.info(f"Loaded template: {template_file.name}")
                except Exception as e:
                    self.logger.error(f"Error loading template {template_file}: {str(e)}")
        return templates

    def load_one(self, template_path: str) -> Optional[Dict[str, Any]]:
        try:
            with open(template_path, "r") as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Error loading FM template {template_path}: {str(e)}")
            return None
