"""Total Cost of Ownership (TCO) reporting.

Walks the worker URLs configured on the comparator, gathers connector status
and config info, classifies each connector into commercial / premium /
non-commercial / unknown pack tiers, tallies task-per-worker distribution,
and writes the result to `<output_dir>/tco_info.json`.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional, Union

from discovery.config_discovery import ConfigDiscovery


class TcoInfoMixin:

    def connector_pack_type(self, connector_class: str) -> str:
        """Determine connector pack type based on connector class"""
        if connector_class in self.premium_pack_connector_dict:
            return 'premium_pack_connectors'
        elif connector_class in self.commercial_pack_connector_dict:
            return 'commercial_pack_connectors'
        elif connector_class == 'unknown':
            return 'unknown_pack_connectors'
        else:
            return 'non_commercial_pack_connectors'

    def process_tco_information(self) -> Optional[Dict[str, Union[int, Dict[str, Any]]]]:
        """Gather TCO information across all configured worker URLs and persist it."""
        connectors_dict = {}

        for worker_url in self.worker_urls:
            connector_statuses = ConfigDiscovery.get_connector_statuses_from_worker(
                worker_url, self.disable_ssl_verify, self.logger, auth=self.worker_auth,
            )
            connector_info_list = ConfigDiscovery.get_connector_configs_from_worker(
                worker_url, self.disable_ssl_verify, self.logger, auth=self.worker_auth,
            )
            connector_info_dict = {item['name']: item for item in connector_info_list if 'name' in item}

            for connector_name, connector_status in connector_statuses.items():
                if connector_name not in connectors_dict:
                    connectors_dict[connector_name] = {'name': connector_name, 'tasks': [], 'connector.class': {}}

                if connector_status and 'tasks_status' in connector_status:
                    connectors_dict[connector_name]['tasks'] = connector_status['tasks_status']
                else:
                    connectors_dict[connector_name]['tasks'] = []

                if connector_name in connector_info_dict and 'config' in connector_info_dict[connector_name]:
                    connectors_dict[connector_name]['connector.class'] = connector_info_dict[connector_name]['config'].get('connector.class', 'unknown')
                    connectors_dict[connector_name]['type'] = connector_info_dict[connector_name].get('type', 'unknown')
                else:
                    connectors_dict[connector_name]['connector.class'] = 'unknown'
                    connectors_dict[connector_name]['type'] = 'unknown'

        connectors = list(connectors_dict.values())

        tco_info = {
            'total_connectors': len(connectors),
            'total_tasks': 0,
            'worker_node_task_map': {},
            'worker_node_count': 0,
            'premium_pack_connectors': {},
            'commercial_pack_connectors': {},
            'non_commercial_pack_connectors': {},
            'unknown_pack_connectors': [],
        }
        for i, connector in enumerate(connectors):
            connector_name = connector.get('name', f'connector_{i}') if isinstance(connector, dict) else f'connector_{i}'
            try:
                if not isinstance(connector, dict):
                    self.logger.error(f"{connector_name} SM config is not a dictionary: {type(connector)}")
                    continue

                connector_class = connector.get('connector.class', 'unknown')
                connector_type = connector.get('type', 'unknown')
                connector_pack = self.connector_pack_type(connector_class)

                if connector_name not in tco_info[connector_pack]:
                    tco_info[connector_pack][connector_name] = {
                        'connector_class': connector_class.split('.')[-1],
                        'connector_type': connector_type,
                        'connector_count': 1,
                    }
                else:
                    tco_info[connector_pack][connector_name]['connector_count'] += 1

                if 'tasks' not in connector or not connector['tasks']:
                    self.logger.error(f"{connector_name} missing required fields 'tasks' or 'tasks' status data is empty")
                    continue

                tco_info['total_tasks'] += len(connector['tasks'])

                for task in connector['tasks']:
                    worker_id = task.get('worker_id', 'unknown_worker').split(':')[0]

                    if worker_id not in tco_info['worker_node_task_map']:
                        tco_info['worker_node_task_map'][worker_id] = {
                            "task_count": 0,
                            "task_list": [],
                        }
                    tco_info['worker_node_task_map'][worker_id]["task_list"].append(f"{connector_name} - task-{task.get('id', 'x')}")
                    tco_info['worker_node_task_map'][worker_id]["task_count"] += 1

            except Exception as e:
                self.logger.error(f"Error processing connector {connector_name} for TCO information: {str(e)}")

        tco_info['worker_node_count'] = len(tco_info['worker_node_task_map'])

        tco_info_file = self.output_dir / 'tco_info.json'
        with open(tco_info_file, 'w') as tco_file:
            json.dump(tco_info, tco_file, indent=2)
        self.logger.info(f"TCO information saved to {tco_info_file}")

        return tco_info
