import json
import logging
import requests
from pathlib import Path
from typing import List, Optional, Dict, Any, Set
from urllib.parse import urljoin

class ConfigDiscovery:
    def __init__(
        self,
        worker_urls: Optional[str] = None,
        worker_urls_file: Optional[str] = None,
        redact: bool = False,
        output_dir: Path = Path('output'),
        sensitive_file: Optional[str] = None,
        worker_config_file: Optional[str] = None
    ):
        self.logger = logging.getLogger(__name__)
        self.redact = redact
        self.output_dir = output_dir
        self.sensitive_file = sensitive_file
        self.worker_config_file = worker_config_file
        
        # Initialize sensitive config handling
        self.static_sensitive_configs = [
            "activemq.password",
            "appdynamics.proxy.password",
            "aws.access.key.id",
            "aws.dynamodb.proxy.password",
            "aws.lambda.proxy.password",
            "aws.redshift.password",
            "aws.secret.access.key",
            "aws.secret.key.id",
            "azblob.account.key",
            "azblob.proxy.password",
            "azblob.sas.token",
            "azure.datalake.client.key",
            "azure.datalake.gen2.client.key",
            "azure.datalake.gen2.sas.key",
            "azure.eventhubs.sas.key",
            "azure.search.api.key",
            "azure.servicebus.connection.string",
            "azure.servicebus.sas.key",
            "azure.sql.dw.password",
            "basic.auth.user.info",
            "bearer.token",
            "cassandra.password",
            "cassandra.ssl.truststore.password",
            "confluent.topic.ssl.key.password",
            "confluent.topic.ssl.keystore.password",
            "confluent.topic.ssl.truststore.password",
            "connection.password",
            "database.password",
            "delta.lake.token",
            "diode.encryption.password",
            "diode.encryption.salt",
            "elastic.https.ssl.key.password",
            "elastic.https.ssl.keystore.password",
            "elastic.https.ssl.truststore.password",
            "ftps.password",
            "ftps.ssl.key.password",
            "ftps.ssl.keystore.password",
            "ftps.ssl.truststore.password",
            "function.key",
            "gcf.credentials.json",
            "gcp.bigtable.credentials.json",
            "gcp.dataproc.credentials.json",
            "gcp.firebase.credentials.json",
            "gcp.pubsub.credentials.json",
            "gcp.spanner.credentials.json",
            "gcp.spanner.proxy.password",
            "gcs.credentials.json",
            "gemfire.password",
            "http.proxy.password",
            "https.ssl.key.password",
            "https.ssl.keystore.password",
            "https.ssl.truststore.password",
            "impala.ldap.password",
            "influxdb.password",
            "java.naming.security.credentials",
            "kinesis.proxy.password",
            "mongodb.password",
            "mq.password",
            "mq.tls.key.password",
            "mq.tls.keystore.password",
            "mq.tls.truststore.password",
            "mqtt.password",
            "mqtt.ssl.key.password",
            "mqtt.ssl.key.store.password",
            "mqtt.ssl.trust.store.password",
            "oauth2.client.secret",
            "oauth2.jwt.keystore.password",
            "pagerduty.api.key",
            "pagerduty.proxy.password",
            "password",
            "prometheus.listener.basic.auth.password",
            "prometheus.listener.ssl.key.password",
            "prometheus.listener.ssl.keystore.password",
            "proxy.password",
            "rabbitmq.https.ssl.key.password",
            "rabbitmq.https.ssl.keystore.password",
            "rabbitmq.https.ssl.truststore.password",
            "redis.password",
            "redis.ssl.keystore.password",
            "redis.ssl.truststore.password",
            "s3.proxy.password",
            "s3.sse.customer.key",
            "salesforce.consumer.secret",
            "salesforce.jwt.keystore.password",
            "salesforce.password",
            "salesforce.password.token",
            "sasl.jaas.config",
            "schema.registry.ssl.key.password",
            "schema.registry.ssl.keystore.password",
            "schema.registry.ssl.truststore.password",
            "servicenow.password",
            "solace.password",
            "solace.ssl.keystore.password",
            "solace.ssl.truststore.password",
            "splunk.collector.authentication.tokens",
            "splunk.hec.ssl.trust.store.password",
            "splunk.hec.token",
            "splunk.s2s.ssl.key.password",
            "splunk.ssl.key.store.password",
            "sqs.proxy.password",
            "ssl.key.password",
            "ssl.keystore.certificate.chain",
            "ssl.keystore.key",
            "ssl.keystore.password",
            "ssl.truststore.certificates",
            "ssl.truststore.password",
            "staging.s3.access.key.id",
            "staging.s3.secret.access.key",
            "syslog.ssl.key.password",
            "teradata.password",
            "tibco.password",
            "tls.passphrase",
            "tls.private.key",
            "tls.public.key",
            "v3.$username.auth.password",
            "v3.$username.privacy.password",
            "vertica.password",
            "zendesk.password"
        ]
        
        self.sensitive_patterns = ["password", "token", "secret", "credential"]
        self.all_sensitive_configs = set(self.static_sensitive_configs)
        
        # Load sensitive keys from file if provided
        if self.sensitive_file:
            loaded_keys = self._load_sensitive_configs(self.sensitive_file)
            self.all_sensitive_configs.update(loaded_keys)
            self.logger.info(f"Loaded {len(loaded_keys)} sensitive keys from file.")
        
        # Worker config prefixes
        self.worker_configs = [
            "key.", 
            "value.",
            "header.",
            "producer.",
            "consumer.",
            "reporter.",
            "error.",
            "confluent.",
            "offset."
        ]
        
        # Get worker URLs
        self.worker_urls = self._get_worker_urls(worker_urls, worker_urls_file)

    def _load_sensitive_configs(self, file_path: str) -> Set[str]:
        """Load sensitive config keys from a file (one key per line)."""
        file_keys = set()
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    clean_line = line.strip()
                    if clean_line and not clean_line.startswith("#"):
                        file_keys.add(clean_line.lower())
        except Exception as e:
            self.logger.error(f"Failed to read sensitive config file: {e}")
        return file_keys

    def _sensitive_config(self, key: str) -> bool:
        """Checks if a config key is considered sensitive."""
        key_lower = key.lower()
        if key_lower in self.all_sensitive_configs:
            return True
        return any(pattern in key_lower for pattern in self.sensitive_patterns)

    def _extract_worker_urls_from_file(self, file_path: str) -> List[str]:
        """Extracts worker URLs with full schemes from config lines."""
        urls = set()
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("confluent.controlcenter.connect.") and ".cluster=" in line:
                        _, url_string = line.split("=", 1)
                        for raw_url in url_string.split(","):
                            raw_url = raw_url.strip().rstrip("/")
                            if raw_url.startswith("http://") or raw_url.startswith("https://"):
                                urls.add(raw_url)
        except Exception as e:
            self.logger.error(f"Error reading file '{file_path}': {e}")
        return list(urls)

    def _is_worker_alive(self, full_url: str) -> bool:
        """Checks if a Kafka Connect worker is alive by pinging /connectors."""
        test_url = f"{full_url}/connectors"
        try:
            response = requests.get(test_url, timeout=3, verify=False)
            if response.status_code == 200:
                return True
            else:
                self.logger.warning(f"Worker at {full_url} responded with status: {response.status_code}")
        except requests.RequestException as e:
            self.logger.warning(f"Worker at {full_url} is not reachable: {e}")
        return False

    def _get_worker_urls(self, urls: Optional[str], urls_file: Optional[str]) -> List[str]:
        """Get worker URLs from a file or comma-separated list."""
        if urls_file:
            worker_urls = self._extract_worker_urls_from_file(urls_file)
        elif urls:
            worker_urls = [url.strip().rstrip("/") for url in urls.split(",") if url.strip()]
            
            # Append 'http://' or 'https://' if missing
            for i, url in enumerate(worker_urls):
                if not url.startswith("http://") and not url.startswith("https://"):
                    worker_urls[i] = "http://" + url  # Default to http://
        else:
            raise ValueError("Either worker_urls or worker_urls_file must be provided")

        # Filter to only reachable workers
        alive_workers = [url for url in worker_urls if self._is_worker_alive(url)]

        if not alive_workers:
            self.logger.error("No reachable Kafka Connect workers found.")
            return []
        
        self.logger.info(f"Found {len(alive_workers)} reachable workers: {alive_workers}")
        return alive_workers

    def _load_configs_from_file(self, file_path: str, allowed_prefixes: Optional[List[str]] = None) -> Dict[str, str]:
        """
        Load key-value configurations from a file (key=value format),
        only return keys that start with one of the allowed prefixes.
        """
        new_configs = {}
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if '=' in line and not line.startswith("#"):
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()
                        if not allowed_prefixes or any(key.startswith(prefix) for prefix in allowed_prefixes):
                            new_configs[key] = value
        except Exception as e:
            self.logger.error(f"Error reading the worker configs file: {e}")
        return new_configs

    def _get_json_from_url(self, url: str) -> Optional[Dict[str, Any]]:
        """Makes an HTTP GET request and returns JSON data."""
        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"HTTP error for {url}: {e}")
        except ValueError:
            self.logger.error(f"Invalid JSON response from {url}")
        return None

    def _redact_sensitive_info(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Redact sensitive information from connector config"""
        def redact_dict(d: Dict[str, Any]) -> Dict[str, Any]:
            redacted = {}
            for k, v in d.items():
                if isinstance(v, dict):
                    redacted[k] = redact_dict(v)
                elif isinstance(v, list):
                    redacted[k] = [redact_dict(i) if isinstance(i, dict) else i for i in v]
                elif self._sensitive_config(k):
                    redacted[k] = '********'
                else:
                    redacted[k] = v
            return redacted

        return redact_dict(config)

    def _get_connector_configs(self, worker_url: str) -> List[Dict[str, Any]]:
        """Get connector configurations from a worker using expanded info endpoint"""
        try:
            # Use the expanded endpoint to get all connector info in one call
            expanded_url = f"{worker_url}/connectors?expand=info"
            self.logger.info(f"Fetching connector info from: {expanded_url}")
            
            connectors_data = self._get_json_from_url(expanded_url)
            if not connectors_data:
                self.logger.error(f"No data received from {expanded_url}")
                return []

            configs = []
            for connector_name, connector_data in connectors_data.items():
                self.logger.info(f"Processing connector: {connector_name}")
                
                # Get the info section which contains config, tasks, and type
                info = connector_data.get('info', {})
                config = info.get('config', {})
                tasks = info.get('tasks', [])
                connector_type = info.get('type', 'unknown')
                
                # Apply redaction if needed
                if self.redact:
                    config = self._redact_sensitive_info(config)

                configs.append({
                    'name': connector_name,
                    'worker': worker_url,
                    'type': connector_type,
                    'tasks': tasks,
                    'config': config
                })

            return configs

        except Exception as e:
            self.logger.error(f"Error getting connector configs from {worker_url}: {str(e)}")
            return []

    def discover_and_save(self) -> Path:
        """Discover connector configurations from all workers and save to JSON"""
        all_connectors = {}
        
        # Load existing worker configs from file (if provided)
        if self.worker_config_file:
            worker_configs = self._load_configs_from_file(self.worker_config_file, allowed_prefixes=self.worker_configs)
            self.logger.info(f"Loaded {len(worker_configs)} worker configs matching allowed prefixes.")
        else:
            worker_configs = {}
        
        for worker_url in self.worker_urls:
            self.logger.info(f"=== Processing worker: {worker_url} ===")
            configs = self._get_connector_configs(worker_url)
            
            # Convert list to dict format for consistency
            for config in configs:
                all_connectors[config['name']] = config

        # Create output data structure
        output_data = {
            "connectors": all_connectors,
            "worker_configs": worker_configs
        }

        # Save to JSON file
        output_file = self.output_dir / 'connectors.json'
        self.output_dir.mkdir(exist_ok=True)
        
        with open(output_file, 'w') as f:
            json.dump(output_data, f, indent=2)

        self.logger.info(f"Saved {len(all_connectors)} connector configurations to {output_file}")
        return output_file 