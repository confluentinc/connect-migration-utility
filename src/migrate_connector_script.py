import argparse
from pathlib import Path
import sys
import requests
import base64
from typing import Dict, Any, List, Optional, Tuple
import json
import os
import logging

from offset_manager import OffsetManager

def setup_logging(output_dir: Path):
    """Setup logging configuration"""
    log_file = output_dir / 'migration.log'

    # Clear any existing handlers to avoid conflicts
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Create file handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)

    # Create console handler (explicitly for stdout)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    # Configure root logger
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

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

    def stop_cp_connector(self, worker_url: str, connector_name: str, disable_ssl_verify: bool = False) -> None:
        """Makes an HTTP GET request and returns JSON data."""
        url = f"{worker_url}/connectors/{connector_name}/stop"
        try:
            response = requests.put(url, timeout=5, verify=not disable_ssl_verify)
            if response.status_code == 202:
                self.logger.info(f"Response from {url}: status 202 Accepted")
                self.logger.info(f"Connector: {connector_name}, stop initiated successfully.")
                return None
            if not response.ok:
                raise RuntimeError(f"Non-2xx response from {url}: {response.status_code} {response.text}")
            self.logger.info(f"Response from {url}: status {response.status_code}")
            return None
        except requests.exceptions.RequestException as e:
            self.logger.error(f"HTTP error for {url}: {e}")
            raise

    def create_connector_api_call(
        self,
        url: str,
        name: str,
        body: Dict[str, Any],
        headers: Dict[str, str]
    ) -> Dict[str, Any]:
        response = None
        try:
            response = requests.post(url, json=body, headers=headers)
            self.logger.info(f"[INFO] Response status code for '{name}': {response.status_code}")
            self.logger.info(f"[INFO] Response body for '{name}': {response.text}")
            response.raise_for_status()
        except Exception as e:
            self.logger.error(f"[ERROR] Failed to create connector '{name}': {response.status_code if 'response' in locals() else 'N/A'} {response.text if 'response' in locals() else str(e)}")
            raise RuntimeError(
                f"Failed to create connector '{name}': {response.status_code if 'response' in locals() else 'N/A'} {response.text if 'response' in locals() else str(e)}") from e

        return response.json()

    def create_connector_from_config(
        self,
        environment_id: str,
        kafka_cluster_id: str,
        kafka_api_key: str,
        kafka_api_secret: str,
        fm_config: Dict[str, Any],
        bearer_token: str = None
    ) -> Dict[str, Any] | None:
        name = fm_config['name']
        config = fm_config['config']
        offsets = fm_config.get('offsets', None)
        self.logger.info(
            f"[INFO] Creating connector '{name}' with config keys: {list(config.keys())} and offsets: {'present' if offsets else 'not present'}")

        # Ensure config is a dict
        if config is None or not isinstance(config, dict):
            self.logger.error(f"[ERROR] Config for connector '{name}' is not a valid dictionary")
            return None

        config = dict(config)
        # Add required kafka fields
        config["kafka.api.key"] = kafka_api_key
        config["kafka.api.secret"] = kafka_api_secret
        config["kafka.auth.mode"] = "KAFKA_API_KEY"

        url = self.url_template.format(environment_id=environment_id, kafka_cluster_id=kafka_cluster_id)

        headers = {
            "Content-Type": "application/json",
        }
        self.logger.info(f"[INFO] Bearer token: {bearer_token}")
        headers["Authorization"] = f"Basic {self.encode_to_base64(bearer_token)}"

        body = {
            "name": name,
            "config": config
        }
        if offsets is not None:
            body["offsets"] = offsets

        # Redact sensitive info in body for print
        redacted_body = body.copy()
        if 'config' in redacted_body:
            redacted_body['config'] = {k: ('***' if 'password' in k.lower() or 'secret' in k.lower() else v) for k, v in
                                       redacted_body['config'].items()}
        self.logger.info(f"[INFO] Request body for connector '{name}': {json.dumps(redacted_body, indent=2)}")

        return self.create_connector_api_call(url, name, body, headers)

    def create_connector_from_json_file(
        self,
        environment_id: str,
        kafka_cluster_id: str,
        kafka_api_key: str,
        kafka_api_secret: str,
        json_file_path: str,
        bearer_token: str = None
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
            "Authorization": f"Basic {self.encode_to_base64(bearer_token)}"
        }

        self.logger.info(f"[INFO] API URL: {url}")
        self.logger.info(f"[INFO] Headers: {{'Content-Type': 'application/json', 'Authorization': '***'}}")

        #TODO: Create source connectors before sink connectors to ensure topics exist
        for entry in connector_entries:
            name = entry['name']
            config = entry['config']
            offsets = entry.get('offsets', None)
            self.logger.info(f"[INFO] Creating connector '{name}' with config keys: {list(config.keys())} and offsets: {'present' if offsets else 'not present'}")

            # Ensure config is a dict
            if config is None or not isinstance(config, dict):
                self.logger.error(f"[ERROR] Error creating connector. Config for connector '{name}' is not a valid dictionary")
                continue
            config = dict(config)
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
            response = self.create_connector_api_call(url, name, body, headers)
            results.append(response)
        return results


def main():
    parser = argparse.ArgumentParser(description="Create Confluent Cloud connectors from JSON file")
    '''
    --fm-config-dir output/discovered_configs/fm_configs
    --bearer_token
    --api_key, --api-secret or --service_account - This becomes optional if this is provided as part of the config
    --envId
    --clusterId
    --workerURL list
    Options
    --migration-mode
        --stop_create_latest_offset
        --create - workerURL is not needed.
        --create_latest_offset
    '''
    parser.add_argument('--fm-config-dir', type=str, required=True,
                        help='Directory containing FM connector JSON config files')
    parser.add_argument('--migration-output-dir', type=str, default='migration_output')
    parser.add_argument('--bearer-token', type=str, required=True,
                        help='Confluent Cloud bearer token (api_key:api_secret) (or use --prompt-bearer-token for secure input)')
    parser.add_argument('--kafka-api-key', type=str, help='Kafka API key for LKC cluster')
    parser.add_argument('--kafka-api-secret', type=str, help='Kafka API secret for LKC cluster')
    # parser.add_argument('--service-account', type=str, help='Confluent Cloud service account (optional, alternative to api_key/api_secret)')
    parser.add_argument('--environment-id', type=str, help='Confluent Cloud environment ID')
    parser.add_argument('--cluster-id', type=str, help='Confluent Cloud LKC cluster ID')
    parser.add_argument('--environment', type=str, choices=['prod', 'stag', 'devel'], default='prod',
                        help='Environment to create connectors in (choose from prod, stag, devel)')
    parser.add_argument('--worker-urls', type=str, help='Comma-separated list of worker URLs to fetch latest offsets from')
    parser.add_argument('--migration-mode', type=str, choices=['stop_create_latest_offset', 'create', 'create_latest_offset'],)
    parser.add_argument('--disable-ssl-verify', type= bool, default=False, action='store_true',
                        help='Disable SSL certificate verification for HTTPS requests')



    parser.add_argument('--prompt-bearer-token', action='store_true',
                        help='Prompt for bearer token securely (recommended)')
    parser.add_argument('--semantic-cache-folder', type=str,
                        help='Cache folder for sentence transformer models (default: auto-detected from pip installation)')

    args = parser.parse_args()

    fm_config_dir = Path(args.fm_config_dir)
    migration_output_dir = Path(args.migration_output_dir)
    migration_output_dir.mkdir(parents=True)

    # Setup logging
    setup_logging(migration_output_dir)
    logger = logging.getLogger(__name__)


    bearer_token = args.bearer_token
    kafka_api_key = args.kafka_api_key
    kafka_api_secret = args.kafka_api_secret
    # service_account = args.service_account
    env_id = getattr(args, 'environment_id', None)
    lkc_id = getattr(args, 'cluster_id', None)
    environment = args.environment
    worker_urls = args.worker_urls.split(',') if args.worker_urls else []

    disable_ssl_verify = getattr(args, 'disable_ssl_verify', False)
    migration_mode = getattr(args, 'migration_mode', None)

    creator = ConnectorCreator(environment)

    logger.info("Starting connector creation process")
    if migration_mode=='create':
        for json_file in fm_config_dir.glob("*.json"):

            try:
                #TODO: Try creating source connectors before sink connectors to ensure topics exist
                print(f"Creating connector(s) from file: {json_file}")
                created_connectors = creator.create_connector_from_json_file(
                    environment_id=env_id,
                    kafka_cluster_id=lkc_id,
                    kafka_api_key=kafka_api_key,
                    kafka_api_secret=kafka_api_secret,
                    json_file_path=str(json_file),
                    bearer_token=bearer_token
                )
                for conn in created_connectors:
                    print(f"Created connector: {conn.get('name')}")
            except Exception as e:
                print(f"Failed to create connectors from {json_file}: {str(e)}")
                continue
    elif migration_mode in  ['stop_create_latest_offset', 'create_latest_offset']:
        if not getattr(args, 'worker_urls', None):
            parser.error(f"--worker-urls is required for migration mode '{migration_mode}'")
            sys.exit(0)

        offset_manager = OffsetManager.get_instance(logger)

        connector_fm_configs = {}
        for json_file in fm_config_dir.glob("*.json"):
            try:
                connector_entries = extract_connectors_configs_from_json(json_file)
            except Exception as e:
                logger.error(f"Failed to extract connectors from {json_file}: {str(e)}")
                continue

            for entry in connector_entries:
                name = entry['name']
                connector_fm_configs[name] = entry

        # Get connector configs and offsets from workers
        worker_connector_configs = offset_manager.get_connector_configs_offsets(worker_urls)

        for connector_name in connector_fm_configs:
            fm_entry = connector_fm_configs[connector_name]
            fm_name = fm_entry['name']


            # Find matching connector from workers by name
            matching_worker_sm_config = next((wc for wc in worker_connector_configs if wc['name'] == fm_name), None)
            if not matching_worker_sm_config:
                logger.warning(f"No matching SM connector configs found in given worker URLs to fetch offsets for FM connector '{fm_name}'")
                continue
            sm_worker = matching_worker_sm_config.get('worker', None)

            worker_offsets = matching_worker_sm_config.get('offsets', None)
            if not worker_offsets:
                logger.info(f"No offsets found on workers for connector '{fm_name}'")
            else:
                logger.info(f"Found offsets on workers for connector '{fm_name}': {worker_offsets}")
                fm_entry['offsets'] = worker_offsets


            # Create new connector with chosen offsets
            try:
                #TODO: This should be default False, change it to True if want to test without validation
                config_validation = False
                #validation



                #stop connector
                if config_validation and migration_mode=='stop_create_latest_offset' and sm_worker is not None:
                    logger.info("Stopping CP connector after validation as per migration mode")
                    creator.stop_cp_connector(sm_worker, fm_name, disable_ssl_verify)


                #create connector
                logger.info(f"Creating connector '{fm_name}' with offsets from workers")
                created_connector = creator.create_connector_from_config(
                    environment_id=env_id,
                    kafka_cluster_id=lkc_id,
                    kafka_api_key=kafka_api_key,
                    kafka_api_secret=kafka_api_secret,
                    fm_config=fm_entry,
                    bearer_token=bearer_token
                )
                logger.info(f"Created connector: {created_connector.get('name')}")
            except Exception as e:
                logger.info(f"Failed to create connector '{fm_name}': {str(e)}")
                continue


if __name__ == "__main__":
    main()
