import argparse
from pathlib import Path
import sys
import requests
import base64
from typing import Dict, Any, List, Optional, Tuple
import json
import shutil
import logging

from offset_manager import OffsetManager
from connector_comparator import ConnectorComparator
from config_discovery import ConfigDiscovery


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

class KafkaAuth:
    def __init__(self, api_key=None, api_secret=None, service_account_id=None, auth_mode='KAFKA_API_KEY'):
        self.api_key = api_key
        self.api_secret = api_secret
        self.service_account_id = service_account_id
        self.auth_mode = auth_mode

    def verify_kafka_auth(self):
        if self.auth_mode == 'KAFKA_API_KEY':
            if self.api_key is None or self.api_secret is None:
                raise ValueError("Kafka API key and secret are required when using KAFKA_API_KEY authentication mode")
        elif self.auth_mode == 'SERVICE_ACCOUNT':
            if self.service_account_id is None:
                raise ValueError("Kafka service account ID is required when using SERVICE_ACCOUNT authentication mode")

    def assign_kafka_auth_to_config(self, config: Dict[str, Any]):
        if self.auth_mode == 'KAFKA_API_KEY':
            config["kafka.api.key"] = self.api_key
            config["kafka.api.secret"] = self.api_secret
            config["kafka.auth.mode"] = "KAFKA_API_KEY"
        elif self.auth_mode == 'SERVICE_ACCOUNT':
            config["kafka.service.account.id"] = self.service_account_id
            config["kafka.auth.mode"] = "SERVICE_ACCOUNT"

class ConnectorCreator:
    def __init__(self, environment: str):
        self.logger = logging.getLogger(__name__)
        if environment == "prod":
            self.url_template = "https://api.confluent.cloud/connect/v1/environments/{environment_id}/clusters/{kafka_cluster_id}/connectors"
        else:
            raise ValueError(f"Unknown environment: {environment}")

    @staticmethod
    def encode_to_base64(bearer_token: str) -> str:
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
        kafka_auth: KafkaAuth,
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
        kafka_auth.assign_kafka_auth_to_config(config)

        url = self.url_template.format(environment_id=environment_id, kafka_cluster_id=kafka_cluster_id)

        headers = {
            "Content-Type": "application/json",
        }
        self.logger.info(f"[INFO] Bearer token: {bearer_token}")
        headers["Authorization"] = f"Basic {ConnectorCreator.encode_to_base64(bearer_token)}"

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
        kafka_auth: KafkaAuth,
        json_file_path: str,
        bearer_token: str = None
    ) -> List[Dict[str, Any]]:
        """
        Create new connector(s) in Confluent Cloud from a JSON file.
        Returns a list of new connector information dicts (one per connector in the file).
        """
        self.logger.info(f"[INFO] Creating connector(s) from {json_file_path}")
        connectors_dict = {}
        ConnectorComparator.parse_connector_file(json_file_path, connectors_dict, self.logger)
        results = []
        url = self.url_template.format(environment_id=environment_id, kafka_cluster_id=kafka_cluster_id)
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Basic {ConnectorCreator.encode_to_base64(bearer_token)}"
        }

        self.logger.info(f"[INFO] API URL: {url}")
        self.logger.info(f"[INFO] Headers: {{'Content-Type': 'application/json', 'Authorization': '***'}}")

        for key, entry in connectors_dict.items():
            name = key
            config = entry.get('config', None)
            self.logger.info(f"[INFO] Creating connector '{name}' with config keys: {list(config.keys())} ")

            # Ensure config is a dict
            if config is None or not isinstance(config, dict):
                self.logger.error(f"[ERROR] Error creating connector. Config for connector '{name}' is not a valid dictionary")
                continue
            config = dict(config)
            # Assign kafka auth fields
            kafka_auth.assign_kafka_auth_to_config(config)

            body = {
                "name": name,
                "config": config
            }
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
    parser.add_argument('--fm-config-dir', type=str, required=True,
                        help='Directory containing FM connector JSON config files')
    parser.add_argument('--migration-output-dir', type=str, default='migration_output')
    parser.add_argument('--bearer-token', type=str, required=True,
                        help='Confluent Cloud bearer token (api_key:api_secret) (or use --prompt-bearer-token for secure input)')
    parser.add_argument('--kafka-api-key', type=str, help='Kafka API key for LKC cluster')
    parser.add_argument('--kafka-api-secret', type=str, help='Kafka API secret for LKC cluster')
    parser.add_argument('--kafka-service-account-id', type=str, help='Confluent Cloud service account (optional, alternative to api_key/api_secret)')
    parser.add_argument('--kafka-auth-mode', type=str, choices = ['SERVICE_ACCOUNT','KAFKA_API_KEY'], default='KAFKA_API_KEY', help='Kafka authentication mode (default: KAFKA_API_KEY)')

    parser.add_argument('--environment-id', type=str, required=True, help='Confluent Cloud environment ID')
    parser.add_argument('--cluster-id', type=str, required=True, help='Confluent Cloud LKC cluster ID')
    parser.add_argument('--worker-urls', type=str, help='Comma-separated list of worker URLs to fetch latest offsets from')
    parser.add_argument('--migration-mode', type=str, choices=['stop_create_latest_offset', 'create', 'create_latest_offset'], required=True)
    parser.add_argument('--disable-ssl-verify', action='store_true', help='Disable SSL certificate verification for HTTPS requests')



    parser.add_argument('--prompt-bearer-token', action='store_true',
                        help='Prompt for bearer token securely (recommended)')
    parser.add_argument('--semantic-cache-folder', type=str,
                        help='Cache folder for sentence transformer models (default: auto-detected from pip installation)')

    args = parser.parse_args()

    fm_config_dir = Path(args.fm_config_dir)
    migration_output_dir = Path(getattr(args, 'migration_output_dir', None) or "migration_output")
    if migration_output_dir.exists():
        # Remove existing directory and its contents
        shutil.rmtree(migration_output_dir)
    migration_output_dir.mkdir(parents=True)

    # Setup logging
    setup_logging(migration_output_dir)
    logger = logging.getLogger(__name__)


    bearer_token = args.bearer_token

    kafka_auth = KafkaAuth(
        api_key=getattr(args, 'kafka_api_key', None),
        api_secret=getattr(args, 'kafka_api_secret', None),
        service_account_id=getattr(args, 'kafka_service_account_id', None),
        auth_mode=getattr(args, 'kafka_auth_mode', 'KAFKA_API_KEY')
    )
    kafka_auth.verify_kafka_auth()

    env_id = args.environment_id
    lkc_id = args.cluster_id
    environment = "prod"
    worker_urls = getattr(args, 'worker_urls', None)
    if worker_urls:
        worker_urls = worker_urls.split(',')
    else:
        worker_urls = []

    disable_ssl_verify = getattr(args, 'disable_ssl_verify', False)
    migration_mode = args.migration_mode

    creator = ConnectorCreator(environment)

    logger.info("Starting connector creation process")
    successes = []
    failures = []
    if migration_mode=='create':
        for json_file in fm_config_dir.glob("*.json"):

            try:
                print(f"Creating connector(s) from file: {json_file}")
                created_connectors = creator.create_connector_from_json_file(
                    environment_id=env_id,
                    kafka_cluster_id=lkc_id,
                    kafka_auth=kafka_auth,
                    json_file_path=json_file,
                    bearer_token=bearer_token
                )
                for conn in created_connectors:
                    # Heuristic: error_code or status >= 400 means failure
                    if 'error_code' in conn or (isinstance(conn.get('status'), int) and conn['status'] >= 400):
                        failures.append(conn)
                    else:
                        successes.append(conn)
                    print(f"Created connector: {conn.get('name')}")
            except Exception as e:
                failures.append({"file": str(json_file), "error": str(e)})
                print(f"Failed to create connectors from {json_file}: {str(e)}")
                continue
    elif migration_mode in  ['stop_create_latest_offset', 'create_latest_offset']:
        if not getattr(args, 'worker_urls', None):
            parser.error(f"--worker-urls is required to fetch offsets for migration mode '{migration_mode}'")

        offset_manager = OffsetManager.get_instance(logger)

        connector_fm_configs = {}
        for json_file in fm_config_dir.glob("*.json"):
            try:
                ConnectorComparator.parse_connector_file(json_file, connector_fm_configs, logger)
            except Exception as e:
                logger.error(f"Failed to extract connectors from {json_file}: {str(e)}")
                continue

        connector_configs_from_worker = []
        for worker_url in worker_urls:
            logger.info(f"Getting connector configs for worker: {worker_url}")
            connector_configs_from_worker.extend(ConfigDiscovery.get_connector_configs_from_worker(worker_url, disable_ssl_verify, logger))

        for connector_name in connector_fm_configs:
            fm_entry = connector_fm_configs[connector_name]
            fm_name = fm_entry['name']
            matching_worker_sm_config = next((wc for wc in connector_configs_from_worker if wc['name'] == fm_name), None)
            if not matching_worker_sm_config:
                logger.warning(f"No matching SM connector configs found in given worker URLs to fetch offsets for FM connector '{fm_name}'")
                continue

            try:
                # stop connector
                if migration_mode == 'stop_create_latest_offset':
                    logger.info("Stopping CP connector after validation as per migration mode")
                    creator.stop_cp_connector(matching_worker_sm_config.get('worker', None), fm_name, disable_ssl_verify)

                offsets = offset_manager.get_offsets_of_connector(matching_worker_sm_config, disable_ssl_verify)
                if not offsets:
                    logger.info(f"No offsets found on workers for connector '{fm_name}'")
                else:
                    logger.info(f"Found offsets on workers for connector '{fm_name}': {offsets}")
                    fm_entry['offsets'] = offsets

                # create connector
                logger.info(f"Creating connector '{fm_name}' with offsets from workers")
                created_connector = creator.create_connector_from_config(
                    environment_id=env_id,
                    kafka_cluster_id=lkc_id,
                    kafka_auth=kafka_auth,
                    fm_config=fm_entry,
                    bearer_token=bearer_token
                )
                if created_connector is not None and not ('error_code' in created_connector or (isinstance(created_connector.get('status'), int) and created_connector['status'] >= 400)):
                    successes.append(created_connector)
                    logger.info(f"Created connector: {created_connector.get('name') if created_connector else fm_name}")
                else:
                    failures.append(created_connector)
                    logger.info(f"Failed to create connector '{fm_name}': {created_connector}")

            except Exception as e:
                failures.append({"connector": fm_name, "error": str(e)})
                logger.info(f"Failed to create connector '{fm_name}': {str(e)}")
                continue

    # Write results to files
    with open(migration_output_dir / "successful_migration.json", "w") as f:
        json.dump(successes, f, indent=2)
    with open(migration_output_dir / "unsuccessful_migration.json", "w") as f:
        json.dump(failures, f, indent=2)


if __name__ == "__main__":
    main()
