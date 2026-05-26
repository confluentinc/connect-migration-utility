"""Batch driver for translating a file of SM connector configs.

`parse_connector_file` accepts the several JSON shapes the discovery tool can
emit and normalizes them into a {name: connector} dict. `process_connectors`
walks that dict and runs `transformSMToFm` on each entry, collecting per-
connector FM configs, mapping errors, and warnings.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Optional


class ConnectorBatchMixin:

    @staticmethod
    def parse_connector_file(file, all_connectors_dict, logger=None):
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
                            # list of configs [ { "name":..., "config":{...} }, ... ]
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
                    # Structure: {"connector1": {...}, "connector2": {...}}
                    for connector_name, connector_val in data.items():
                        if isinstance(connector_val, dict) and 'name' in connector_val and 'config' in connector_val:
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

    def process_connectors(self) -> Optional[Dict[str, Any]]:
        """Process all connectors and generate FM configurations"""
        connectors_dict = {}
        # parse_connector_file is a @staticmethod on this same mixin; reach it
        # via self.* so the dispatch follows the composed class's MRO.
        self.parse_connector_file(self.input_file, connectors_dict, self.logger)

        if not connectors_dict:
            self.logger.error("No connectors found after parsing the input file.")
            return None
        connectors = list(connectors_dict.values())

        fm_configs = {}
        for i, connector in enumerate(connectors):
            try:
                if not isinstance(connector, dict):
                    self.logger.error(f"Connector at index {i} is not a dictionary: {type(connector)}")
                    continue

                if 'name' not in connector or 'config' not in connector:
                    self.logger.error(f"Connector at index {i} missing required fields 'name' or 'config'")
                    continue

                original_sm_config = connector['config']

                # HTTP, BigQuery, and Debezium V1->V2 transformations happen inside transformSMToFm
                result = self.transformSMToFm(connector['name'], original_sm_config)

                fm_config = {
                    'name': connector['name'],
                    'sm_config': connector['config'],
                    'config': result['fm_configs'],
                    'mapping_errors': result['errors'],
                    'mapping_warnings': result['warnings'],
                }

                fm_configs[connector['name']] = fm_config

            except Exception as e:
                connector_name = connector.get('name', f'connector_{i}') if isinstance(connector, dict) else f'connector_{i}'
                self.logger.error(f"Error processing connector {connector_name}: {str(e)}")

        return fm_configs
