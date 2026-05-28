"""
TCO (Total Cost of Ownership) processor.

Crawls the configured Connect worker(s), collects per-connector task/worker
information, buckets connectors by premium/commercial/non-commercial/unknown
pack, and writes a ``tco_info.json`` summary into the migration output dir.
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from requests.auth import HTTPBasicAuth

from connect_migrate.constants.connector_packs import connector_pack_type
from connect_migrate.discovery.config_discovery import ConfigDiscovery
from connect_migrate.utils.json_files import write_json


class TcoProcessor:
    def __init__(
        self,
        output_dir: Path,
        worker_urls: List[str],
        disable_ssl_verify: bool = False,
        worker_auth: Optional[HTTPBasicAuth] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.output_dir = output_dir
        self.worker_urls = worker_urls
        self.disable_ssl_verify = disable_ssl_verify
        self.worker_auth = worker_auth
        self.logger = logger or logging.getLogger(__name__)

    def process(self) -> Optional[Dict[str, Union[int, Dict[str, Any]]]]:
        """Process all connectors and generate FM configurations"""
        connectors_dict: Dict[str, Dict[str, Any]] = {}

        for worker_url in self.worker_urls:
            connector_statuses = ConfigDiscovery.get_connector_statuses_from_worker(
                worker_url, self.disable_ssl_verify, self.logger, auth=self.worker_auth
            )
            connector_info_list = ConfigDiscovery.get_connector_configs_from_worker(
                worker_url, self.disable_ssl_verify, self.logger, auth=self.worker_auth
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

        tco_info: Dict[str, Any] = {
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
                connector_pack = connector_pack_type(connector_class)

                if connector_name not in tco_info[connector_pack]:
                    tco_info[connector_pack][connector_name] = {
                        'connector_class': connector_class.split('.')[-1],
                        'connector_type': connector_type,
                        'connector_count': 1
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
                            "task_list": []
                        }
                    tco_info['worker_node_task_map'][worker_id]["task_list"].append(f"{connector_name} - task-{task.get('id', 'x')}")
                    tco_info['worker_node_task_map'][worker_id]["task_count"] += 1

            except Exception as e:
                self.logger.error(f"Error processing connector {connector_name} for TCO information: {str(e)}")

        tco_info['worker_node_count'] = len(tco_info['worker_node_task_map'])

        tco_info_file = self.output_dir / 'tco_info.json'
        write_json(tco_info_file, tco_info)
        self.logger.info(f"TCO information saved to {tco_info_file}")

        return tco_info