"""Domain types for creating Fully-Managed connectors in Confluent Cloud.

:class:`KafkaAuth` carries the Kafka-side credential choice and stamps it
onto an outgoing connector config. :class:`ConnectorCreator` issues the
HTTP calls to the Confluent Cloud Connect API.

Previously these lived inside ``connect_migrate.cli.migrate_cli``; they
were moved here so the CLI module is purely an argparse + main()
orchestrator and the domain classes can be reused / unit-tested without
importing the CLI.
"""

import json
import logging
from typing import Any, Dict, List, Optional

import requests

from connect_migrate.mapper.connector_mapper import ConnectorMapper
from connect_migrate.utils.encoding import encode_to_base64
from connect_migrate.utils.http_session import DEFAULT_HTTP_TIMEOUT, make_http_session
from connect_migrate.utils.sensitive_data_redactor import SensitiveDataRedactor


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
        self._session = make_http_session()
        self._redactor = SensitiveDataRedactor(logger=self.logger)

    def stop_cp_connector(self, worker_url: str, connector_name: str, disable_ssl_verify: bool = False, auth=None) -> None:
        """Makes an HTTP PUT request to stop a connector."""
        url = f"{worker_url}/connectors/{connector_name}/stop"
        try:
            response = requests.put(url, timeout=DEFAULT_HTTP_TIMEOUT, verify=not disable_ssl_verify, auth=auth)
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
            response = self._session.post(url, json=body, headers=headers, timeout=DEFAULT_HTTP_TIMEOUT)
            self.logger.info(f"[INFO] Response status code for '{name}': {response.status_code}")
            self.logger.debug(f"[DEBUG] Response body for '{name}' ({len(response.content)} bytes)")
            response.raise_for_status()
        except Exception as e:
            status = response.status_code if response is not None else 'N/A'
            body_text = response.text if response is not None else str(e)
            self.logger.error(f"[ERROR] Failed to create connector '{name}': {status} {body_text}")
            raise RuntimeError(f"Failed to create connector '{name}': {status} {body_text}") from e

        return response.json()

    def create_connector_from_config(
        self,
        environment_id: str,
        kafka_cluster_id: str,
        kafka_auth: KafkaAuth,
        fm_config: Dict[str, Any],
        bearer_token: str = None
    ) -> Optional[Dict[str, Any]]:
        name = fm_config['name']
        config = fm_config['config']
        offsets = fm_config.get('offsets', None)
        self.logger.info(
            f"[INFO] Creating connector '{name}' with config keys: {list(config.keys())} and offsets: {'present' if offsets else 'not present'}")

        if config is None or not isinstance(config, dict):
            self.logger.error(f"[ERROR] Config for connector '{name}' is not a valid dictionary")
            return None

        config = dict(config)
        kafka_auth.assign_kafka_auth_to_config(config)

        url = self.url_template.format(environment_id=environment_id, kafka_cluster_id=kafka_cluster_id)

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Basic {encode_to_base64(bearer_token)}",
        }

        body = {
            "name": name,
            "config": config
        }
        if offsets is not None:
            body["offsets"] = offsets

        redacted_body = dict(body)
        redacted_body['config'] = self._redactor.redact(body['config'])
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
        connectors_dict: Dict[str, Any] = {}
        ConnectorMapper.parse_connector_file(json_file_path, connectors_dict, self.logger)
        results = []
        url = self.url_template.format(environment_id=environment_id, kafka_cluster_id=kafka_cluster_id)
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Basic {encode_to_base64(bearer_token)}"
        }

        self.logger.info(f"[INFO] API URL: {url}")
        self.logger.info(f"[INFO] Headers: {{'Content-Type': 'application/json', 'Authorization': '***'}}")

        for name, entry in connectors_dict.items():
            config = entry.get('config', None)
            self.logger.info(f"[INFO] Creating connector '{name}' with config keys: {list(config.keys())} ")

            if config is None or not isinstance(config, dict):
                self.logger.error(f"[ERROR] Error creating connector. Config for connector '{name}' is not a valid dictionary")
                continue
            config = dict(config)
            kafka_auth.assign_kafka_auth_to_config(config)

            body = {
                "name": name,
                "config": config
            }
            redacted_body = dict(body)
            redacted_body['config'] = self._redactor.redact(body['config'])
            self.logger.info(f"[INFO] Request body for connector '{name}': {json.dumps(redacted_body, indent=2)}")
            response = self.create_connector_api_call(url, name, body, headers)
            results.append(response)
        return results