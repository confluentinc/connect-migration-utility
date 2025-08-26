import requests
import base64
from typing import Dict, Any, List, Optional, Tuple
import json
import os
import logging

def extract_connectors_configs_from_json(json_file_path: str) -> List[Dict[str, Any]]:
    """
    Loads a connector JSON file and returns a list of dicts, each with keys:
    "name": connector name,
    "config": config dictionary,
    "offsets": list of offset dictionaries (or None if not present).

    Accepts single or multiple connector formats.
    """
    if not os.path.exists(json_file_path):
        raise FileNotFoundError(f"File not found: {json_file_path}")
    with open(json_file_path, 'r') as f:
        data = json.load(f)

    results = []

    # Single connector format
    if isinstance(data, dict) and 'name' in data and 'config' in data:
        results.append({
            "name": data['name'],
            "config": data['config'],
            "offsets": data.get('offsets')
        })
    # Multiple connectors: {"connectors": { ... }}
    elif isinstance(data, dict) and 'connectors' in data:
        for conn in data['connectors'].values():
            if isinstance(conn, dict) and 'name' in conn and 'config' in conn:
                results.append({
                    "name": conn['name'],
                    "config": conn['config'],
                    "offsets": conn.get('offsets')
                })
    # Multiple connectors: list of dicts
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                for conn in item.values():
                    if isinstance(conn, dict) and 'name' in conn and 'config' in conn:
                        results.append({
                            "name": conn['name'],
                            "config": conn['config'],
                            "offsets": conn.get('offsets')
                        })
    # Special case: {"conn1": {"name":..., "config":...}, ...}
    elif isinstance(data, dict):
        for conn in data.values():
            if isinstance(conn, dict) and 'name' in conn and 'config' in conn:
                results.append({
                    "name": conn['name'],
                    "config": conn['config'],
                    "offsets": conn.get('offsets')
                })
            # Nested Info: {"special_connector1": {"Info": {"name":..., "config":...}}}
            elif isinstance(conn, dict) and 'Info' in conn:
                info = conn['Info']
                if isinstance(info, dict) and 'name' in info and 'config' in info:
                    results.append({
                        "name": info['name'],
                        "config": info['config'],
                        "offsets": info.get('offsets')
                    })
    if not results:
        raise ValueError(f"No valid connector 'name', 'config' and 'offsets' found in {json_file_path}")
    return results


class ConnectorCreator:
    def __init__(self, environment: str):
        self.logger = logging.getLogger(__name__)
        if environment == "prod":
            self.url_template = "https://api.confluent.cloud/connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors"
        elif environment == "stag":
            self.url_template = "https://stag.cpdev.cloud/api/connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors"
        elif environment == "devel":
            self.url_template = "https://devel.cpdev.cloud/api/connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors"
        else:
            raise ValueError(f"Unknown environment: {environment}")

    def encode_to_base64(self, bearer_token: str) -> str:
        """
        Encode the bearer token as per documentation: echo -n "bearer_token" | base64
        This encodes only the bearer_token string, not ":bearer_token".
        """
        return base64.b64encode(bearer_token.encode('utf-8')).decode('utf-8')

    def create_connector(
        self,
        environment_id: str,
        kafka_cluster_id: str,
        kafka_api_key: str,
        kafka_api_secret: str,
        json_file_path: str,
        bearer_token: str = None,
        disable_ssl_verify: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Create new connector(s) in Confluent Cloud from a JSON file.
        Returns a list of new connector information dicts (one per connector in the file).
        """
        self.logger.info(f"[INFO] Creating connector(s) from {json_file_path}")
        connector_entries = extract_connectors_configs_from_json(json_file_path)
        results = []
        url = self.url_template.format(environment_id=environment_id, kafka_cluster_id=kafka_cluster_id)
        headers = {
            "Content-Type": "application/json",
        }
        self.logger.info(f"[INFO] Bearer token: {bearer_token}")
        if bearer_token:
            headers["Authorization"] = f"Basic {self.encode_to_base64(bearer_token)}"
            # headers["Authorization"] = f"Basic {bearer_token}"

        self.logger.info(f"[INFO] Headers: {headers}")

        self.logger.info(f"[INFO] API URL: {url}")
        self.logger.info(f"[INFO] Headers: {{'Content-Type': 'application/json', 'Authorization': '***' if bearer_token else None}}")

        for name, config, offsets in connector_entries:
            # Ensure config is a dict
            config = dict(config) if config is not None else {}
            # Add required kafka fields
            config["kafka.api.key"] = kafka_api_key
            config["kafka.api.secret"] = kafka_api_secret
            config["kafka.auth.mode"] = "KAFKA_API_KEY"

            body = {
                "name": name,
                "config": config
            }
            if offsets is not None:
                body["offsets"] = offsets
            # Redact sensitive info in body for print
            redacted_body = body.copy()
            if 'config' in redacted_body:
                redacted_body['config'] = {k: ('***' if 'password' in k.lower() or 'secret' in k.lower() else v) for k, v in redacted_body['config'].items()}
            self.logger.info(f"[INFO] Request body for connector '{name}': {json.dumps(redacted_body, indent=2)}")
            # try:
            #     response = requests.post(url, json=body, headers=headers, verify=not disable_ssl_verify)
            #     print(f"[INFO] Response status code for '{name}': {response.status_code}")
            #     print(f"[INFO] Response body for '{name}': {response.text}")
            #     response.raise_for_status()
            # except Exception as e:
            #     print(f"[ERROR] Failed to create connector '{name}': {response.status_code if 'response' in locals() else 'N/A'} {response.text if 'response' in locals() else str(e)}")
            #     raise RuntimeError(f"Failed to create connector '{name}': {response.status_code if 'response' in locals() else 'N/A'} {response.text if 'response' in locals() else str(e)}") from e
            # results.append(response.json())
        return results
