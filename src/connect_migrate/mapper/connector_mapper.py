"""
Apache Connect Migration Utility
Copyright 2024-2025 The Apache Software Foundation

This product includes software developed at The Apache Software Foundation.
"""

import json
import logging
import os

from pathlib import Path
from typing import Dict, Any, List, Optional, Set, Union, Tuple
import re
from connect_migrate.mapper.semantic_matching.property_matcher import SemanticPropertyMatcher, Property
import base64
import requests
from requests.auth import HTTPBasicAuth

from connect_migrate.discovery.config_discovery import ConfigDiscovery
from connect_migrate.mapper.v1_to_v2.http_transformer import HttpV1ToV2Transformer
from connect_migrate.mapper.v1_to_v2.bigquery_transformer import BigQueryV1ToV2Transformer
from connect_migrate.mapper.v1_to_v2.debezium_translator import DebeziumV1ToV2Translator
from connect_migrate.mapper.templates.template_loader import TemplateLoader
from connect_migrate.mapper.templates.template_selector import TemplateSelector
from connect_migrate.mapper.templates.connector_class_index import ConnectorClassIndex
from connect_migrate.mapper.templates.sm_template_fetcher import SmTemplateFetcher
from connect_migrate.mapper.jdbc.url_parser import JdbcUrlParser
from connect_migrate.mapper.jdbc.database_inferrer import DatabaseInferrer
from connect_migrate.mapper.smt.smt_classifier import SmtClassifier
from connect_migrate.mapper.smt.fm_smt_registry import FmSmtRegistry
from connect_migrate.mapper.cc_translate_client.translate_api_client import TranslateApiClient
from connect_migrate.mapper.properties.direct_mappings import DirectMappings
from connect_migrate.mapper.properties.config_def_processor import ConfigDefProcessor
from connect_migrate.mapper.properties.derivations import FieldDeriver
from connect_migrate.constants.paths import (
    DEFAULT_FM_TEMPLATE_DIR,
    DISCOVERED_CONFIGS_DIR,
    FM_TRANSFORMS_FALLBACK_FILE,
    SUCCESSFUL_CONFIGS_SUBDIR,
    UNSUCCESSFUL_CONFIGS_SUBDIR,
)
from connect_migrate.utils.json_files import read_json, write_json

class ConnectorMapper:
    # Re-exported for back-compat with callers (CLI) that already reference
    # these as class constants. Source of truth is connect_migrate.constants.paths.
    DISCOVERED_CONFIGS_DIR: Path = DISCOVERED_CONFIGS_DIR
    SUCCESSFUL_CONFIGS_SUBDIR: Path = SUCCESSFUL_CONFIGS_SUBDIR
    UNSUCCESSFUL_CONFIGS_SUBDIR: Path = UNSUCCESSFUL_CONFIGS_SUBDIR

    def __init__(self, input_file: Path, output_dir: Path, worker_urls: List[str] = None,
                 env_id: str = None, lkc_id: str = None, bearer_token: str = None, disable_ssl_verify: bool = False,
                 worker_username: str = None, worker_password: str = None, debezium_version: str = 'v2'):
        self.logger = logging.getLogger(__name__)
        self.input_file = input_file
        self.output_dir = output_dir
        # Use local model
        self.semantic_matcher = SemanticPropertyMatcher()
        
        # Debezium version for CDC template selection (default: v2)
        self.debezium_version = debezium_version.lower()
        if self.debezium_version not in ['v1', 'v2']:
            self.logger.warning(f"Invalid debezium_version '{debezium_version}', defaulting to 'v2'")
            self.debezium_version = 'v2'
        self.logger.info(f"Using Debezium version: {self.debezium_version}")
        
        # Initialize the Debezium v1 to v2 translator
        self.debezium_translator = DebeziumV1ToV2Translator(logger=self.logger)
        
        # Initialize the HTTP v1 to v2 transformer
        self.http_transformer = HttpV1ToV2Transformer(logger=self.logger)
        
        # Initialize the BigQuery v1 to v2 transformer
        self.bigquery_transformer = BigQueryV1ToV2Transformer(logger=self.logger)
        
        # Worker URLs for fetching SM templates
        self.worker_urls = worker_urls or []

        # Confluent Cloud credentials for FM transforms (optional)
        self.env_id = env_id
        self.lkc_id = lkc_id
        self.bearer_token = bearer_token
        self.disable_ssl_verify = disable_ssl_verify
        
        # Basic auth for Connect worker API
        self.worker_username = worker_username
        self.worker_password = worker_password
        self.worker_auth = None
        if self.worker_username and self.worker_password:
            self.worker_auth = HTTPBasicAuth(self.worker_username, self.worker_password)
            self.logger.info("Basic authentication enabled for Connect worker API")
        elif self.worker_username or self.worker_password:
            self.logger.warning("Basic auth username or password provided but not both - authentication disabled")

        # Log SSL verification status
        if self.disable_ssl_verify:
            self.logger.info("SSL certificate verification is DISABLED")
        else:
            self.logger.info("SSL certificate verification is ENABLED")

        # Log credential status (without exposing sensitive data)
        if env_id and lkc_id and bearer_token:
            self.logger.info(f"Confluent Cloud credentials provided: env_id={env_id}, lkc_id={lkc_id}, bearer_token=[HIDDEN]")
        elif env_id or lkc_id or bearer_token:
            self.logger.warning("Partial Confluent Cloud credentials provided - HTTP calls for FM transforms will be skipped")
        else:
            self.logger.info("No Confluent Cloud credentials provided - will use fallback FM transforms only")

        # JDBC parsing + database family inference
        self._jdbc_url_parser = JdbcUrlParser(logger=self.logger)
        self._database_inferrer = DatabaseInferrer(
            self._jdbc_url_parser, logger=self.logger
        )

        # FM template handling
        self.fm_template_dir = DEFAULT_FM_TEMPLATE_DIR
        self._template_loader = TemplateLoader(self.fm_template_dir, logger=self.logger)
        self._template_selector = TemplateSelector(
            fm_template_dir=self.fm_template_dir,
            debezium_version=self.debezium_version,
            database_type_resolver=self._database_inferrer.infer_database_type,
            logger=self.logger,
        )
        self._connector_class_index = ConnectorClassIndex(
            self.fm_template_dir, logger=self.logger
        )
        self.fm_templates = (
            self._template_loader.load_all() if self.fm_template_dir.exists() else {}
        )
        self.connector_class_to_template = self._connector_class_index.build()

        # SM template fetcher (worker REST)
        self._sm_template_fetcher = SmTemplateFetcher(
            disable_ssl_verify=self.disable_ssl_verify,
            worker_auth=self.worker_auth,
            logger=self.logger,
        )

        # SMT (Single Message Transform) classification + FM SMT registry
        self._smt_classifier = SmtClassifier(logger=self.logger)
        self._fm_smt_registry = FmSmtRegistry(
            env_id=env_id,
            lkc_id=lkc_id,
            bearer_token=bearer_token,
            fallback_file=FM_TRANSFORMS_FALLBACK_FILE,
            classifier=self._smt_classifier,
            logger=self.logger,
        )

        # Confluent Cloud /translate API client
        self._translate_api_client = TranslateApiClient(
            env_id=env_id,
            lkc_id=lkc_id,
            bearer_token=bearer_token,
            plugin_name_resolver=self._template_selector.get_plugin_name,
            disable_ssl_verify=self.disable_ssl_verify,
            logger=self.logger,
        )

        # Per-field FM-config derivers (split into 7 grouped classes + facade)
        self._field_deriver = FieldDeriver(self._jdbc_url_parser, logger=self.logger)

        # Per-property mapping helpers
        self._direct_mappings = DirectMappings(logger=self.logger)
        self._config_def_processor = ConfigDefProcessor(
            derivation_resolver=self._field_deriver.lookup,
            logger=self.logger,
        )

        # Static property mappings to prevent incorrect semantic matching
        # These will be determined dynamically based on connector type (source vs sink)
        self.static_property_mappings_source = {
            'key.converter': 'output.key.format',
            'value.converter': 'output.data.format'
        }

        self.static_property_mappings_sink = {
            'key.converter': 'input.key.format',
            'value.converter': 'input.data.format'
        }

        # Converter class to format reverse mappings
        self.converter_to_format_mappings = {
            'io.confluent.connect.avro.AvroConverter': 'AVRO',
            'io.confluent.connect.json.JsonSchemaConverter': 'JSON_SR',
            'io.confluent.connect.protobuf.ProtobufConverter': 'PROTOBUF',
            'org.apache.kafka.connect.json.JsonConverter': 'JSON'
        }

        self.premium_pack_connector_dict = {

            "io.confluent.connect.jms.IbmMqSinkConnector":{},
            "io.confluent.connect.ibm.mq.IbmMQSourceConnector": {},
            "io.confluent.connect.splunk.s2s.SplunkS2SSourceConnector": {},

            #Cloud connectors
            "io.confluent.connect.oracle.cdc.OracleCdcSourceConnector": {},
            "io.confluent.connect.oracle.xstream.cdc.OracleXStreamSourceConnector": {},
        }

        self.commercial_pack_connector_dict = {
            "io.confluent.connect.amps.AmpsSourceConnector": {},
            "io.confluent.connect.weblogic.WeblogicSourceConnector": {},
            "io.confluent.connect.ftps.FtpsSourceConnector": {},
            "io.confluent.connect.ftps.FtpsSinkConnector": {},
            "io.confluent.connect.jms.ActiveMqSinkConnector": {},
            "io.confluent.connect.kudu.KuduSourceConnector": {},
            "io.confluent.connect.kudu.KuduSinkConnector": {},
            "io.confluent.connect.appdynamics.metrics.AppDynamicsMetricsSinkConnector": {},
            "io.confluent.connect.cassandra.CassandraSinkConnector": {},
            "io.confluent.connect.diode.sink.DataDiodeSinkConnector": {},
            "io.confluent.connect.diode.source.DataDiodeSourceConnector": {},
            "io.confluent.connect.firebase.FirebaseSourceConnector": {},
            "io.confluent.connect.firebase.FirebaseSinkConnector": {},
            "io.confluent.connect.hbase.HBaseSinkConnector": {},
            "io.confluent.connect.hdfs2.Hdfs2SourceConnector": {},
            "io.confluent.connect.hdfs3.Hdfs3SinkConnector": {},
            "io.confluent.connect.hdfs3.Hdfs3SourceConnector": {},
            "io.confluent.connect.omnisci.OmnisciSinkConnector": {},
            "io.confluent.connect.jms.IbmMqSinkConnector": {},
            "io.confluent.influxdb.source.InfluxdbSourceConnector": {},
            "io.confluent.influxdb.InfluxDBSinkConnector": {},
            "io.confluent.connect.jms.JmsSinkConnector": {},
            "io.confluent.connect.jms.JmsSourceConnector": {},
            "io.confluent.connect.mapr.db.MapRDBSinkConnector": {},
            "io.confluent.connect.netezza.NetezzaSinkConnector": {},
            "io.confluent.connect.prometheus.PrometheusMetricsSinkConnector": {},
            "io.confluent.connect.snmp.SnmpTrapSourceConnector": {},
            "io.confluent.salesforce.SalesforcePushTopicSourceConnector": {},
            "io.confluent.salesforce.SalesforceSObjectSinkConnector": {},
            "io.confluent.salesforce.SalesforceCdcSourceConnector": {},
            "io.confluent.salesforce.SalesforcePlatformEventSourceConnector": {},
            "io.confluent.salesforce.SalesforcePlatformEventSinkConnector": {},
            "io.confluent.connect.salesforce.SalesforceBulkApiSourceConnector": {},
            "io.confluent.connect.salesforce.SalesforceBulkApiSinkConnector": {},
            "io.confluent.connect.solace.SolaceSourceConnector": {},
            "io.confluent.connect.SplunkHttpSourceConnector": {},
            "io.confluent.connect.syslog.SyslogSourceConnector": {},
            "io.confluent.connect.tibco.TibcoSourceConnector": {},
            "io.confluent.connect.jms.TibcoSinkConnector": {},
            "io.confluent.connect.pivotal.gemfire.PivotalGemfireSinkConnector": {},
            "io.confluent.vertica.VerticaSinkConnector": {},
            "io.confluent.connect.teradata.TeradataSourceConnector": {},
            "io.confluent.connect.teradata.TeradataSinkConnector": {},

            #Cloud connectors
            "io.confluent.connect.aws.lambda.AwsLambdaSinkConnector_sink": {},
            "io.confluent.connect.activemq.ActiveMQSourceConnector": {},
            "io.confluent.connect.aws.cloudwatch.AwsCloudWatchSourceConnector": {},
            "io.confluent.connect.aws.cloudwatch.metrics.AwsCloudWatchMetricsSinkConnector": {},
            "io.confluent.connect.aws.dynamodb.DynamoDbSinkConnector": {},
            "io.confluent.connect.kinesis.KinesisSourceConnector": {},
            "io.confluent.connect.aws.redshift.RedshiftSinkConnector": {},
            "io.confluent.connect.s3.source.S3SourceConnector": {},
            "io.confluent.connect.sqs.source.SqsSourceConnector": {},
            "io.confluent.connect.azure.blob.storage.AzureBlobStorageSourceConnector": {},
            "io.confluent.connect.azure.blob.AzureBlobStorageSinkConnector": {},
            "io.confluent.connect.azure.search.AzureSearchSinkConnector": {},
            "io.confluent.connect.azure.datalake.gen2.AzureDataLakeGen2SinkConnector": {},
            "io.confluent.connect.azure.eventhubs.EventHubsSourceConnector": {},
            "io.confluent.connect.azure.functions.AzureFunctionsSinkConnector": {},
            "io.confluent.connect.azure.servicebus.ServiceBusSourceConnector": {},
            "io.confluent.connect.azureloganalytics.AzureLogAnalyticsSinkConnector": {},
            "io.confluent.connect.databricks.deltalake.DatabricksDeltaLakeSinkConnector": {},
            "io.confluent.connect.datadog.metrics.DatadogMetricsSinkConnector": {},
            "io.confluent.connect.github.GithubSourceConnector": {},
            "io.confluent.connect.gcp.bigtable.BigtableSinkConnector": {},
            "io.confluent.connect.gcp.functions.GoogleCloudFunctionsSinkConnector": {},
            "io.confluent.connect.gcp.pubsub.PubSubSourceConnector": {},
            "io.confluent.connect.gcp.spanner.SpannerSinkConnector": {},
            "io.confluent.connect.gcs.GcsSourceConnector": {},
            "io.confluent.connect.gcs.GcsSinkConnector": {},
            "io.confluent.connect.gcp.dataproc.DataprocSinkConnector": {},
            "io.confluent.connect.http.HttpSourceConnector": {},
            "io.confluent.connect.http.HttpSinkConnector": {},
            "io.confluent.connect.ibm.mq.IbmMQSourceConnector": {},
            "io.confluent.connect.jira.JiraSourceConnector": {},
            "io.confluent.connect.mqtt.MqttSinkConnector": {},
            "io.confluent.connect.pagerduty.PagerDutySinkConnector": {},
            "io.confluent.connect.rabbitmq.RabbitMQSourceConnector": {},
            "io.confluent.connect.rabbitmq.sink.RabbitMQSinkConnector": {},
            "io.confluent.connect.sftp.SftpGenericSourceConnector": {},
            "io.confluent.connect.sftp.SftpSinkConnector": {},
            "io.confluent.connect.servicenow.ServiceNowSourceConnector": {},
            "io.confluent.connect.servicenow.ServiceNowSinkConnector": {},
            "io.confluent.connect.jms.SolaceSinkConnector": {},
            "io.confluent.connect.zendesk.ZendeskSourceConnector": {}
        }

    def _apply_debezium_v1_to_v2_if_needed(
        self, 
        original_connector_class: str, 
        fm_configs: Dict[str, Any], 
        warnings: List[str], 
        errors: List[str]
    ) -> Tuple[Dict[str, Any], List[str], List[str]]:
        """
        Apply Debezium v1 to v2 translation if needed.
        
        This is a common post-translation step that should be applied after
        either API translation or SM-to-FM template translation.
        
        Args:
            original_connector_class: The original connector.class from the input config
            fm_configs: The translated FM configurations
            warnings: List of warnings to append to
            errors: List of errors to append to
            
        Returns:
            Tuple of (fm_configs, warnings, errors) with any v1→v2 translations applied
        """
        if self.debezium_version == 'v1' and self.debezium_translator.is_debezium_v1(original_connector_class):
            self.logger.info(f"Customer provided Debezium v1 config, translating FM configs to v2 format")
            fm_configs, v1_to_v2_warnings, v1_to_v2_errors = self.debezium_translator.translate_v1_to_v2(
                original_connector_class, fm_configs
            )
            for warning in v1_to_v2_warnings:
                warnings.append(f"[v1→v2 Translation] {warning}")
            for error in v1_to_v2_errors:
                errors.append(f"[v1→v2 Translation] {error}")
            self.logger.info(f"V1 to V2 FM translation complete. Connector class is now: {fm_configs.get('connector.class')}")
        
        return fm_configs, warnings, errors

    def _get_templates_for_connector(self, connector_class: str, connector_name: str = None, config: Dict[str, Any] = None) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """Get SM and FM templates for a connector class"""
        # Get worker URL from connector config if available
        worker_url = None
        if config and 'worker' in config:
            worker_url = config['worker']
            self.logger.info(f"Using worker URL from connector config: {worker_url}")
        elif self.worker_urls and len(self.worker_urls) > 0:
            worker_url = self.worker_urls[0]
            self.logger.info(f"Using first worker URL from global list: {worker_url}")

        # Fetch SM template via HTTP (if worker URL available)
        if worker_url:
            self.logger.info(f"Fetching SM template for {connector_class} via HTTP from {worker_url}...")
            sm_template = self._sm_template_fetcher.fetch(connector_class, worker_url)
        else:
            self.logger.info(f"No worker URL available - skipping SM template fetch for {connector_class}")
            sm_template = {}

        # Handle FM templates - find by connector.class
        fm_template_path = self._template_selector.find_template_path(connector_class, connector_name, config)

        # Special mapping for SFTP connectors
        if not fm_template_path and connector_class == 'io.confluent.connect.sftp.SftpCsvSourceConnector':
            # Map to SftpSource template
            sftp_template_path = self.fm_template_dir / 'SftpSource_resolved_templates.json'
            if sftp_template_path.exists():
                fm_template_path = str(sftp_template_path)
                self.logger.info(f"Mapped SFTP connector to SftpSource template: {fm_template_path}")
            else:
                self.logger.warning(f"SftpSource template not found at expected path: {sftp_template_path}")

        if fm_template_path:
            try:
                with open(fm_template_path, 'r') as f:
                    self.fm_templates[fm_template_path] = json.load(f)
                self.logger.info(f"Loaded FM template by connector.class: {fm_template_path}")
                self.logger.info(f"Template file path: {fm_template_path}")
                # Log the template_id from the loaded template
                loaded_template = self.fm_templates[fm_template_path]
                if 'templates' in loaded_template and len(loaded_template['templates']) > 0:
                    template_id = loaded_template['templates'][0].get('template_id', 'NO_TEMPLATE_ID')
                    self.logger.info(f"Template ID from loaded template: {template_id}")
            except Exception as e:
                self.logger.error(f"Error loading FM template {fm_template_path}: {str(e)}")
                fm_template_path = None
        else:
            self.logger.error(f"No FM template found for connector.class: {connector_class}")
            fm_template_path = None

        # If FM template is missing, return empty templates
        if not fm_template_path:
            self.logger.error(f"Missing required FM template for {connector_class}")
            return sm_template, None  # Return None to indicate missing template

        # Log template selection
        self.logger.info(f"Selected templates for {connector_class}:")
        if worker_url:
            self.logger.info(f"  SM Template: Fetched via HTTP from {worker_url}")
        else:
            self.logger.info(f"  SM Template: Not available (no worker URL)")
        self.logger.info(f"  FM Template: {fm_template_path}")

        # Return templates
        return sm_template, self.fm_templates.get(fm_template_path, {})

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

    def process_connectors(self) -> Optional[Dict[str, Any]]:
        """Process all connectors and generate FM configurations"""
        connectors_dict = {}
        ConnectorMapper.parse_connector_file(self.input_file, connectors_dict, self.logger)

        if not connectors_dict:
            self.logger.error("No connectors found after parsing the input file.")
            return None
        connectors = list(connectors_dict.values())

        # Process each connector
        fm_configs = {}
        for i, connector in enumerate(connectors):
            try:
                # Handle case where connector might be a string or other type
                if not isinstance(connector, dict):
                    self.logger.error(f"Connector at index {i} is not a dictionary: {type(connector)}")
                    continue

                # Ensure connector has required fields
                if 'name' not in connector or 'config' not in connector:
                    self.logger.error(f"Connector at index {i} missing required fields 'name' or 'config'")
                    continue

                original_sm_config = connector['config']

                # Transform SM to FM using the new method
                # Note: HTTP, BigQuery, and Debezium V1 to V2 transformations are handled inside transformSMToFm
                result = self.transformSMToFm(connector['name'], original_sm_config)

                # Create FM config object in the expected format
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
        """Process all connectors and generate FM configurations"""
        connectors_dict = {}

        for worker_url in self.worker_urls:
            connector_statuses = ConfigDiscovery.get_connector_statuses_from_worker(worker_url, self.disable_ssl_verify, self.logger, auth=self.worker_auth)
            connector_info_list = ConfigDiscovery.get_connector_configs_from_worker(worker_url, self.disable_ssl_verify, self.logger, auth=self.worker_auth)
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

        # Process each connector
        tco_info = {
            'total_connectors': len(connectors),
            'total_tasks': 0,
            'worker_node_task_map': {},
            'worker_node_count': 0,
            'premium_pack_connectors': {},
            'commercial_pack_connectors': {},
            'non_commercial_pack_connectors': {},
            'unknown_pack_connectors' : [],
        }
        for i, connector in enumerate(connectors):
            connector_name = connector.get('name', f'connector_{i}') if isinstance(connector, dict) else f'connector_{i}'
            try:
                # Handle case where connector might be a string or other type
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
                        'connector_count': 1
                    }
                else:
                    tco_info[connector_pack][connector_name]['connector_count'] += 1

                # Ensure connector has tasks information
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

        # Save TCO information to a file
        tco_info_file = self.output_dir / 'tco_info.json'
        write_json(tco_info_file, tco_info)
        self.logger.info(f"TCO information saved to {tco_info_file}")

        return tco_info

    def transformSMToFm(self, connector_name:str, user_configs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform Self-Managed (SM) connector configurations to Fully Managed (FM) configurations.

        Args:
            user_configs: Dictionary of configuration key-value pairs where:
                - Key: Configuration property name (string)
                - Value: Configuration property value (will be converted to string)
                Example: {"connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector", "connection.url": "jdbc:mysql://localhost:3306/mydb"}

        Returns:
            Dictionary containing:
                - 'fm_configs': List of successfully transformed FM configurations
                - 'warnings': List of warning messages
                - 'errors': List of error messages
        """
        result = {
            'fm_configs': [],
            'warnings': [],
            'errors': []
        }

        # Validate input
        if not isinstance(user_configs, dict):
            result['errors'].append("Input must be a dictionary of configuration key-value pairs")
            return result

        if not user_configs:
            result['errors'].append("No configuration properties provided")
            return result

        # Convert all values to strings
        config_dict = {}
        for key, value in user_configs.items():
            config_dict[key] = str(value)

        # Check for required connector.class
        if 'connector.class' not in config_dict:
            result['errors'].append("Missing required 'connector.class' configuration")
            return result

        # Store original connector class to detect if v1 to v2 translation is needed after SM to FM
        original_connector_class = config_dict.get('connector.class', '')

        # Flag to track if translation was done via API
        translation_done_via_api = False
        
        # STEP 0: Try /translate API when env_id and lkc_id are available
        # This is the preferred method as it uses Confluent Cloud's translation service
        # Falls back to existing SM-to-FM template-based translation on failure or exception
        if self.env_id and self.lkc_id and self.bearer_token:
            self.logger.info(f"Attempting /translate API for connector '{connector_name}'")
            try:
                translate_result = self._translate_api_client.translate(connector_name, config_dict)
                
                if translate_result is not None:
                    # API translation succeeded - use the result
                    self.logger.info(f"Successfully translated connector '{connector_name}' via /translate API")
                    
                    fm_configs = translate_result.get('config', {})
                    warnings = translate_result.get('warnings', [])
                    errors = translate_result.get('errors', [])
                    
                    # Ensure name is set
                    if 'name' not in fm_configs:
                        fm_configs['name'] = connector_name
                    
                    translation_done_via_api = True
                else:
                    # API translation returned None - fall back to SM-to-FM translation
                    self.logger.info(f"/translate API returned no result for connector '{connector_name}', falling back to SM-to-FM translation")
            except Exception as e:
                # Any exception from translate API - fall back to SM-to-FM translation
                self.logger.warning(f"/translate API exception for connector '{connector_name}': {str(e)}, falling back to SM-to-FM translation")
        else:
            self.logger.debug(f"Skipping /translate API for connector '{connector_name}' - credentials not provided")
        
        # If API translation succeeded, apply post-translation steps and return
        if translation_done_via_api:
            # Apply Debezium v1 to v2 translation if needed (common post-translation step)
            fm_configs, warnings, errors = self._apply_debezium_v1_to_v2_if_needed(
                original_connector_class, fm_configs, warnings, errors
            )
            
            # Return the result
            result['fm_configs'] = fm_configs
            result['warnings'] = warnings
            result['errors'] = errors
            result['name'] = connector_name
            
            return result

        # FALLBACK: Use existing SM-to-FM template-based translation
        # Get template ID (connector class)
        template_id = config_dict.get('connector.class')

        sm_template, fm_template = self._get_templates_for_connector(template_id, connector_name, config_dict)

        if fm_template is None:
            result['errors'].append(f"No FM template found for connector class: {template_id}")
            # Continue without config processing - just return the basic structure
            result['fm_configs'] = {
                'connector.class': template_id,
                'name': connector_name,
            }
            return result

        # Extract template components (following Java TemplateEngine pattern)
        try:
            # Validate template structure
            if not isinstance(fm_template, dict):
                raise ValueError(f"Expected fm_template to be a dict, got {type(fm_template)}")

            if 'templates' not in fm_template:
                raise ValueError("fm_template missing 'templates' key")

            if not isinstance(fm_template['templates'], (list, tuple)):
                raise ValueError(f"Expected fm_template['templates'] to be a list, got {type(fm_template['templates'])}")

            # Log template structure for debugging
            self.logger.debug(f"Template structure: {list(fm_template.keys())}")
            self.logger.debug(f"Number of templates: {len(fm_template['templates'])}")

            connector_config_defs = self._config_def_processor.extract_connector_config_defs(fm_template)
            template_config_defs = self._config_def_processor.extract_template_config_defs(fm_template)

            self.logger.debug(f"Extracted {len(connector_config_defs)} connector config defs and {len(template_config_defs)} template config defs")

        except Exception as e:
            self.logger.error(f"Error extracting template components: {str(e)}")
            # Return basic structure with error
            result['errors'].append(f"Error extracting template components: {str(e)}")
            result['fm_configs'] = {
                'connector.class': template_id,
                'name': connector_name,
            }
            return result

        # Initialize FM configs and message lists (following Java pattern)
        fm_configs = {}
        transforms_configs = {}  # Separate dictionary for transforms and predicates
        warnings = []
        errors = []
        semantic_match_list = set()  # Track configs that need semantic matching

        # Step 1: Handle connector.class and name (following Java pattern)
        if 'connector.class' in config_dict:
            # Get template_id from the first template in the templates array
            template_id = None
            if 'templates' in fm_template and len(fm_template['templates']) > 0:
                template_id = fm_template['templates'][0].get('template_id')
                self.logger.info(f"Found template_id in FM template: {template_id}")
                self.logger.info(f"FM template structure: {list(fm_template.keys())}")
                if 'templates' in fm_template:
                    self.logger.info(f"Number of templates: {len(fm_template['templates'])}")
                    for i, template in enumerate(fm_template['templates']):
                        self.logger.info(f"Template {i}: {template.get('template_id', 'NO_TEMPLATE_ID')}")

            if template_id:
                fm_configs['connector.class'] = template_id
                self.logger.info(f"Set connector.class to template_id: {template_id}")
            else:
                # Fallback to the original connector.class if template_id is not found
                fm_configs['connector.class'] = config_dict['connector.class']
                self.logger.info(f"Set connector.class to original value: {config_dict['connector.class']}")
        else:
            errors.append(f"connector.class property is required.")

        # Always set the name from connector_name parameter
        fm_configs['name'] = connector_name
        self.logger.info(f"Set name in fm_configs from connector_name parameter: {connector_name}")

        if 'tasks.max' in config_dict:
            fm_configs['tasks.max'] = config_dict['tasks.max']
        else:
            fm_configs['tasks.max'] = "1"

        # Step 2: Process user configs (following Java pattern)
        # Validate template_config_defs is a list
        if not isinstance(template_config_defs, (list, tuple)):
            self.logger.error(f"template_config_defs is not a list, got {type(template_config_defs)}: {template_config_defs}")
            errors.append(f"Invalid template_config_defs type: {type(template_config_defs)}")
            return result

        # Validate connector_config_defs is a list
        if not isinstance(connector_config_defs, (list, tuple)):
            self.logger.error(f"connector_config_defs is not a list, got {type(connector_config_defs)}: {connector_config_defs}")
            errors.append(f"Invalid connector_config_defs type: {type(connector_config_defs)}")
            return result

        # Additional safety check - ensure template_config_defs is not empty
        if not template_config_defs:
            self.logger.error("template_config_defs is empty or None")
            errors.append("template_config_defs is empty or None")
            return result

        try:
            for user_config_key, user_config_value in config_dict.items():

                # Check if this is a transforms or predicates config
                if user_config_key.startswith('connector.class') or user_config_key.startswith('name'):
                    continue

                if user_config_key.startswith('transforms') or user_config_key.startswith('predicates'):
                    transforms_configs[user_config_key] = user_config_value
                    continue

              
                # Find if user config is present in Connector config def
                matching_connector_config_def = None
                if isinstance(connector_config_defs, (list, tuple)):
                    for connector_config_def in connector_config_defs:
                        if isinstance(connector_config_def, dict) and connector_config_def.get('name') == user_config_key:
                            matching_connector_config_def = connector_config_def
                            break
                        
                else:
                    self.logger.warning(f"Expected connector_config_defs to be a list, got {type(connector_config_defs)}")
                    continue

                if matching_connector_config_def is not None:
                    # User config present in Connector config def
                    self._config_def_processor.process_user_config(
                        matching_connector_config_def,
                        user_config_value,
                        template_config_defs,
                        fm_configs,
                        warnings,
                        errors,
                        config_dict,
                        semantic_match_list
                    )
                else:
                    # Check if this config is defined in template_config_defs
                    config_found_in_template = False

                    try:
                        for template_config_def in template_config_defs:
                            original_user_config_key = user_config_key
                            if (user_config_key.startswith('consumer.') or user_config_key.startswith('producer.')) and \
                            not user_config_key.startswith('consumer.override.') and not user_config_key.startswith('producer.override.'):
                                user_config_key = user_config_key.replace('consumer.', 'consumer.override.')
                                user_config_key = user_config_key.replace('producer.', 'producer.override.')
                            elif user_config_key.startswith('consumer.override.') or user_config_key.startswith('producer.override.'):
                                # Remove override. from config in template_config_def.get('name')
                                user_config_key = user_config_key.replace('consumer.override.', 'consumer.')
                                user_config_key = user_config_key.replace('producer.override.', 'producer.')

                            if isinstance(template_config_def, dict) and (template_config_def.get('name') == user_config_key or template_config_def.get('name') == original_user_config_key):
                                config_found_in_template = True
                                break
                    except Exception as e:
                        self.logger.error(f"Error iterating over template_config_defs for {user_config_key}: {str(e)}")
                        self.logger.error(f"template_config_defs type: {type(template_config_defs)}, content: {template_config_defs}")
                        config_found_in_template = False

                    if not config_found_in_template:
                        # User config not present in either Connector config def or template config defs - warn
                        warning_msg = f"Unused connector config '{user_config_key}'. Given value will be ignored. Default value will be used if any."
                        warnings.append(warning_msg)
                        self.logger.warning(warning_msg)

        except Exception as e:
            self.logger.error(f"Error processing user configs: {str(e)}")
            errors.append(f"Error processing user configs {user_config_key}: {str(e)}")
            # Continue with basic config

        # Step 3: Process template configs using config derivation methods
        self.logger.info(f"Processing {len(template_config_defs)} template config definitions")
        try:
            for template_config_def in template_config_defs:
                template_config_name = template_config_def.get("name")
                is_required = template_config_def.get("required", False)
                self.logger.debug(f"Processing template config: {template_config_name}, required: {is_required}")

                # Get the method to derive this config from user configs
                derivation_method = self._field_deriver.lookup(template_config_name, template_config_def)

                if derivation_method:
                    derived_value = derivation_method(config_dict, fm_configs, template_config_defs, template_config_name)
                    if derived_value is not None:
                        fm_configs[template_config_name] = derived_value
                        self.logger.debug(f"Derived value for {template_config_name}: {derived_value}")
                else:
                    self.logger.debug(f"No derivation method found for {template_config_name}")
        except Exception as e:
            self.logger.error(f"Error processing template configs: {str(e)}")
            errors.append(f"Error processing template configs: {str(e)}")

        # Step 4: Before semantic matching, check if user config keys directly match template config def names
        for user_config_key, user_config_value in config_dict.items():
            # Skip if already in fm_configs
            if user_config_key in fm_configs:
                if user_config_key in semantic_match_list:
                    semantic_match_list.remove(user_config_key)
                continue

            original_user_config_key = user_config_key
            if user_config_key.startswith('producer.override') or user_config_key.startswith('consumer.override'):
                user_config_key = user_config_key.replace('consumer.override.', 'consumer.')
                user_config_key = user_config_key.replace('producer.override.', 'producer.')
            elif user_config_key.startswith('producer.') or user_config_key.startswith('consumer.'):
                user_config_key = user_config_key.replace('producer.', 'producer.override.')
                user_config_key = user_config_key.replace('consumer.', 'consumer.override.')
                

            # Check if user config key matches any template config def name
            try:
                for template_config_def in template_config_defs:
                    if isinstance(template_config_def, dict):
                        template_config_name = template_config_def.get("name")
                    
                        if template_config_name == user_config_key:
                            # Direct match found - add to fm_configs
                            fm_configs[user_config_key] = user_config_value
                            self.logger.info(f"Direct match found: {user_config_key} = {user_config_value}")
                            if user_config_key in semantic_match_list or original_user_config_key in semantic_match_list:
                                semantic_match_list.remove(user_config_key)
                                semantic_match_list.remove(original_user_config_key)
                            break
                        if template_config_name == original_user_config_key:
                            # Direct match found - add to fm_configs
                            fm_configs[original_user_config_key] = user_config_value
                            self.logger.info(f"Direct match found: {original_user_config_key} = {user_config_value}")
                            if user_config_key in semantic_match_list or original_user_config_key in semantic_match_list:
                                semantic_match_list.remove(user_config_key)
                                semantic_match_list.remove(original_user_config_key)
                            break    
            except Exception as e:
                self.logger.error(f"Error checking template config match for {user_config_key}: {str(e)}")
                self.logger.error(f"template_config_defs type: {type(template_config_defs)}, content: {template_config_defs}")

        # Step 5: do semantic matching for the configs that are not present in the template
        self._do_semantic_matching(fm_configs, semantic_match_list, config_dict, template_config_defs, sm_template)

        # Check for required configs that are missing after semantic matching
        self._check_required_configs(fm_configs, template_config_defs, errors)

        # Process transforms configs
        # Extract template_id from the correct location
        plugin_type = "Unknown"
        if fm_template.get('template_id'):
            plugin_type = fm_template.get('template_id')
        elif 'templates' in fm_template and len(fm_template['templates']) > 0:
            plugin_type = fm_template['templates'][0].get('template_id', 'Unknown')

        transforms_data = self._smt_classifier.classify(config_dict, self._fm_smt_registry.get_smts_for_plugin(plugin_type))
        fm_configs.update(transforms_data['allowed'])

        # Add mapping errors from transforms processing
        if 'mapping_errors' in transforms_data:
            errors.extend(transforms_data['mapping_errors'])

        # Debug logging for final connector.class value
        if 'connector.class' in fm_configs:
            self.logger.info(f"Final connector.class value: {fm_configs['connector.class']}")
        else:
            self.logger.warning("connector.class not found in final fm_configs")

        # Step: Apply Debezium v1 to v2 translation if needed (common post-translation step)
        fm_configs, warnings, errors = self._apply_debezium_v1_to_v2_if_needed(
            original_connector_class, fm_configs, warnings, errors
        )

        # Step: Translate HTTP v1 FM configs to v2 FM configs
        # This happens AFTER SM to FM translation is complete (similar to Debezium pattern)
        # Auto-detect HTTP V1 connector and transform to V2
        if self.http_transformer.is_http_v1(original_connector_class):
            self.logger.info(f"Detected HTTP V1 config, translating FM configs to V2 format")
            fm_configs, v1_to_v2_warnings, v1_to_v2_errors = self.http_transformer.translate_v1_to_v2(fm_configs)
            # Add v1 to v2 translation warnings
            for warning in v1_to_v2_warnings:
                warnings.append(f"[HTTP v1→v2 Translation] {warning}")
            # Add v1 to v2 translation errors
            for error in v1_to_v2_errors:
                errors.append(f"[HTTP v1→v2 Translation] {error}")
            self.logger.info(f"HTTP V1 to V2 FM translation complete. Connector class is now: {fm_configs.get('connector.class')}")

        # Step: Translate BigQuery v1 FM configs to v2 FM configs
        # This happens AFTER SM to FM translation is complete (similar to Debezium pattern)
        # Auto-detect BigQuery V1 connector and transform to V2
        if self.bigquery_transformer.is_bigquery_v1(original_connector_class):
            self.logger.info(f"Detected BigQuery V1 config, translating FM configs to V2 format")
            fm_configs, v1_to_v2_warnings, v1_to_v2_errors = self.bigquery_transformer.translate_v1_to_v2(fm_configs)
            # Add v1 to v2 translation warnings
            for warning in v1_to_v2_warnings:
                warnings.append(f"[BigQuery v1→v2 Translation] {warning}")
            # Add v1 to v2 translation errors
            for error in v1_to_v2_errors:
                errors.append(f"[BigQuery v1→v2 Translation] {error}")
            self.logger.info(f"BigQuery V1 to V2 FM translation complete. Connector class is now: {fm_configs.get('connector.class')}")

        # Return the result in the required format
        result = {
            "name": connector_name,
            "fm_configs": fm_configs,
            "warnings": warnings,
            "errors": errors
        }

        return result

    def _do_semantic_matching(self, fm_configs: Dict[str, str], semantic_match_list: set, user_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]], sm_template: Dict[str, Any]):
        """
        Perform semantic matching for configs that are not found in the template.

        Args:
            fm_configs: Dictionary of FM configurations
            semantic_match_list: Set of config names that need semantic matching
            user_configs: Dictionary of user configurations
            template_config_defs: List of template config definitions
        """
        if not semantic_match_list:
            self.logger.info("No configs require semantic matching")
            return

        self.logger.info(f"Performing semantic matching for {len(semantic_match_list)} configs: {', '.join(semantic_match_list)}")

        # Log SM template information
        self.logger.info(f"SM Template info for semantic matching:")
        if sm_template:
            self.logger.info(f"  - SM template type: {type(sm_template)}")
            self.logger.info(f"  - SM template keys: {list(sm_template.keys()) if isinstance(sm_template, dict) else 'Not a dict'}")
            if isinstance(sm_template, dict) and 'config_defs' in sm_template:
                self.logger.info(f"  - SM config_defs count: {len(sm_template['config_defs'])}")
            if isinstance(sm_template, dict) and 'sections' in sm_template:
                self.logger.info(f"  - SM sections count: {len(sm_template['sections'])}")
        else:
            self.logger.info(f"  - SM template is None/empty")

        # Get FM properties for matching (as dictionary)
        fm_properties_dict = {}
        for template_config_def in template_config_defs:
            fm_properties_dict[template_config_def.get('name')] = template_config_def

        self.logger.info(f"FM properties available for matching: {len(fm_properties_dict)}")

        # Perform semantic matching for each config in the list
        for config_name in semantic_match_list:
            if config_name in user_configs:
                user_value = user_configs[config_name]
                self.logger.info(f"Processing semantic match for '{config_name}' = '{user_value}'")

                # Get SM property from SM template
                sm_prop = self._get_sm_property_from_template(config_name, sm_template)

                if not sm_prop:
                    # Fallback to generic property if not found in SM template
                    self.logger.info(f"SM property '{config_name}' not found in template, using fallback")
                    sm_prop = {
                        'name': config_name,
                        'description': f"User config: {config_name}",
                        'type': 'STRING',
                        'section': 'General'
                    }
                else:
                    self.logger.info(f"Found SM property '{config_name}' in template: {sm_prop}")

                # Find best match using semantic matching with threshold
                result = self.semantic_matcher.find_best_match(sm_prop, fm_properties_dict, semantic_threshold=0.7)

                if result and result.matched_fm_property:
                    # Find the property name from the matched property info
                    fm_prop_name = None
                    for prop_name, prop_info in fm_properties_dict.items():
                        if prop_info == result.matched_fm_property:
                            fm_prop_name = prop_name
                            break

                    if fm_prop_name and fm_prop_name not in fm_configs:
                        fm_configs[fm_prop_name] = user_value
                        self.logger.info(f"Semantic match: {config_name} -> {fm_prop_name} (score: {result.similarity_score:.3f})")
                    elif fm_prop_name in fm_configs:
                        self.logger.debug(f"Skipping semantic mapping for {config_name} -> {fm_prop_name} as {fm_prop_name} is already mapped")
                    else:
                        self.logger.warning(f"Could not determine property name for matched property: {config_name}")
                else:
                    self.logger.warning(f"No semantic match found for config: {config_name}")
            else:
                self.logger.warning(f"Config {config_name} not found in user configs for semantic matching")

        self.logger.info(f"Semantic matching completed for {len(semantic_match_list)} configs")

    def _check_required_configs(self, fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]], errors: List[str]):
        """
        Check for required configs that are missing from FM configs after semantic matching.

        Args:
            fm_configs: Dictionary of FM configurations
            template_config_defs: List of template config definitions
            errors: List to add error messages to
        """
        self.logger.info("Checking for required configs that are missing from FM configs")

        for template_config_def in template_config_defs:
            config_name = template_config_def.get('name')
            required_value = template_config_def.get('required', False)
            is_internal = template_config_def.get('internal', False)

            # Handle both string and boolean values for 'required' field
            is_required = False
            if isinstance(required_value, bool):
                is_required = required_value
            elif isinstance(required_value, str):
                is_required = required_value.lower() == 'true'

            # Skip internal configs as they are handled automatically
            if is_internal:
                continue

            # Check if this required config is missing from FM configs
            if is_required and config_name not in fm_configs:
                # Check if this config has a default value
                default_value = template_config_def.get('default_value')
                if default_value is not None:
                    # Use the default value for this required config
                    fm_configs[config_name] = str(default_value)
                    self.logger.info(f"Required config '{config_name}' missing but has default value '{default_value}' - using default")
                else:
                    # No default value available, add error
                    error_msg = f"Required FM Config '{config_name}' could not be derived from given configs."
                    # Check if this error message is already in the errors list to prevent duplicates
                    if error_msg not in errors:
                        errors.append(error_msg)
                        self.logger.warning(f"Required config '{config_name}' missing from fm_configs and no default value available. Available keys: {list(fm_configs.keys())}")
                    else:
                        self.logger.debug(f"Duplicate error message for '{config_name}' already exists, skipping")
            elif is_required and config_name in fm_configs:
                self.logger.info(f"Required config '{config_name}' found in fm_configs with value: {fm_configs[config_name]}")

            # Check if FM config value is part of recommended values (if config exists and has recommended values)
            if config_name in fm_configs:
                fm_config_value = fm_configs[config_name]
                recommended_values = template_config_def.get('recommended_values', [])

                if recommended_values and fm_config_value not in recommended_values:
                    # Try case-insensitive matching for enum-like values
                    fm_config_value_lower = fm_config_value.lower() if isinstance(fm_config_value, str) else str(fm_config_value).lower()
                    recommended_values_lower = [str(v).lower() for v in recommended_values]

                    if fm_config_value_lower not in recommended_values_lower:
                        error_msg = f"FM Config '{config_name}' value '{fm_config_value}' is not in the recommended values list: {recommended_values}"
                        # Check if this error message is already in the errors list to prevent duplicates
                        if error_msg not in errors:
                            errors.append(error_msg)
                            self.logger.warning(f"Value '{fm_config_value}' for '{config_name}' not in recommended values (case-insensitive check also failed)")
                        else:
                            self.logger.debug(f"Duplicate error message for '{config_name}' recommended values already exists, skipping")
                    else:
                        # Case-insensitive match found - log this for debugging
                        self.logger.info(f"Case-insensitive match found for '{config_name}': '{fm_config_value}' matches one of {recommended_values}")
                else:
                    # Value is in recommended values (case-sensitive match)
                    self.logger.debug(f"Value '{fm_config_value}' for '{config_name}' is in recommended values")

    def _get_sm_property_from_template(self, config_name: str, sm_template: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Get SM property definition from SM template.

        Args:
            config_name: Name of the config to find
            sm_template: SM template dictionary

        Returns:
            SM property definition or None if not found
        """
        self.logger.debug(f"Looking for SM property '{config_name}' in template")

        if not sm_template:
            self.logger.debug(f"SM template is empty or None")
            return None

        # Handle different SM template structures
        available_keys = list(sm_template.keys()) if isinstance(sm_template, dict) else []
        self.logger.debug(f"SM template keys: {available_keys}")

        # Try configs (newer format)
        if 'configs' in sm_template:
            self.logger.debug(f"Searching in 'configs' (count: {len(sm_template['configs'])})")
            for config_def in sm_template['configs']:
                if isinstance(config_def, dict) and config_def.get('name') == config_name:
                    self.logger.debug(f"Found SM property '{config_name}' in configs")
                    self.logger.debug(f"Property details: {config_def}")
                    return config_def

        # Try groups (newer format)
        if 'groups' in sm_template:
            groups = sm_template['groups']
            self.logger.debug(f"Searching in 'groups' (count: {len(groups)})")
            for group in groups:
                if isinstance(group, dict):
                    group_name = group.get('name', 'Unknown')
                    group_configs = group.get('configs', [])
                    self.logger.debug(f"Searching in group '{group_name}' (configs: {len(group_configs)})")

                    for config_def in group_configs:
                        if isinstance(config_def, dict) and config_def.get('name') == config_name:
                            self.logger.debug(f"Found SM property '{config_name}' in group '{group_name}'")
                            self.logger.debug(f"Property details: {config_def}")
                            return config_def

        # Try sections (older format)
        if 'sections' in sm_template:
            sections = sm_template['sections']
            self.logger.debug(f"Searching in 'sections' (count: {len(sections)})")
            for section in sections:
                if isinstance(section, dict):
                    section_name = section.get('name', 'Unknown')
                    section_configs = section.get('config_defs', [])
                    self.logger.debug(f"Searching in section '{section_name}' (configs: {len(section_configs)})")

                    for config_def in section_configs:
                        if isinstance(config_def, dict) and config_def.get('name') == config_name:
                            self.logger.debug(f"Found SM property '{config_name}' in section '{section_name}'")
                            self.logger.debug(f"Property details: {config_def}")
                            return config_def

        self.logger.debug(f"SM property '{config_name}' not found in template")
        return None

    def _load_semantic_matcher_from_path(self):
        """Load semantic matcher class from the specified path"""
        if not self.semantic_matcher_path:
            return

        try:
            semantic_matcher_file = Path(self.semantic_matcher_path)
            if not semantic_matcher_file.exists():
                self.logger.warning(f"Semantic matcher file not found: {self.semantic_matcher_path}")
                return

            self.logger.info(f"Loading semantic matcher from: {self.semantic_matcher_path}")

            # Import the custom semantic matcher module
            import importlib.util
            spec = importlib.util.spec_from_file_location("custom_semantic_matcher", semantic_matcher_file)
            custom_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(custom_module)

            # Get the SemanticMatcher class from the custom module
            if hasattr(custom_module, 'SemanticMatcher'):
                CustomSemanticMatcher = custom_module.SemanticMatcher
                self.semantic_matcher = CustomSemanticMatcher()
                self.logger.info(f"Successfully loaded custom semantic matcher from {self.semantic_matcher_path}")
            else:
                self.logger.error(f"SemanticMatcher class not found in {self.semantic_matcher_path}")

        except Exception as e:
            self.logger.error(f"Error loading semantic matcher from {self.semantic_matcher_path}: {e}")
            # Continue with default semantic matcher

