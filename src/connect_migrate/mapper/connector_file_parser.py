"""
Connector-config JSON file parser.

Reads a single JSON file and populates a flat ``{connector_name: {"name": ...,
"config": ...}}`` dict in-place, handling the half-dozen historical container
shapes (single config, list of configs, dict-of-dicts, ``{"connectors": {...}}``,
worker /connectors REST shape with an ``"info"`` sub-key, etc.).
"""

import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional


def parse_connector_file(
    file: Path,
    all_connectors_dict: Dict[str, Any],
    logger: Optional[logging.Logger] = None,
) -> None:
    if not os.path.exists(file):
        raise FileNotFoundError(f"File not found: {file}")
    if logger is None:
        logger = logging.getLogger("config_parser")

    if not (file.suffix == '.json' and file.is_file()):
        return
    try:
        # Output -> all_connectors_dict = { "connector_name": {"name":"", "config":""}, ... }
        with open(file, 'r') as f:
            data = json.load(f)
            if isinstance(data, dict) and 'connectors' in data:
                # Structure: {"connectors": {"connector_name": {"name":"", "config":""}, ...}}
                all_connectors_dict.update(data['connectors'])
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and 'name' in item and 'config' in item:
                        # Structure: [ {"connector_name_02": {"name":"", "config":""} }, ... ]
                        all_connectors_dict[item['name']] = item
                    elif isinstance(item, dict):
                        # list of configs [ { "name":..., "config":{...} }, { "name":..., "config":{...} }, ... ]
                        for value in item.values():
                            if isinstance(value, dict) and 'name' in value and 'config' in value:
                                all_connectors_dict[value['name']] = value
                            else:
                                logger.warning(f"Skipping non-connector dict item in list in {file}: {value}")
            elif isinstance(data, dict) and 'name' in data and 'config' in data:
                # Structure: {"name": ..., "config": ...} (single connector config)
                connector_name = data['name']
                all_connectors_dict[connector_name] = data
            elif isinstance(data, dict):
                # Structure: {"connector1": {...}, "connector2": {...}} (or just one)
                for connector_name, connector_val in data.items():
                    if isinstance(connector_val, dict) and 'name' in connector_val and 'config' in connector_val:
                        # Structure: {"connector_name": {"name":"", "config":""}}
                        all_connectors_dict[connector_name] = connector_val
                    else:
                        info_key = next((k for k in connector_val if isinstance(k, str) and k.lower() == "info"), None)
                        info = connector_val.get(info_key) if info_key and isinstance(connector_val[info_key], dict) else None

                        if info and 'name' in info and 'config' in info:
                            all_connectors_dict[connector_name] = info
                        else:
                            logger.warning(
                                f"Skipping connector '{connector_name}' in {file}: missing 'name' and 'config'")
            else:
                logger.warning(f"Skipping unrecognized format in {file}")
    except Exception as e:
        logger.error(f"Failed to parse {file}: {e}")