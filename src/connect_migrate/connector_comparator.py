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

class ConnectorComparator:
    DISCOVERED_CONFIGS_DIR: Path = Path("discovered_configs")
    SUCCESSFUL_CONFIGS_SUBDIR: Path = Path("successful_configs")
    UNSUCCESSFUL_CONFIGS_SUBDIR: Path = Path("unsuccessful_configs_with_errors")

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
        self.fm_template_dir = Path("templates/fm")
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
            fallback_file=Path("fm_transforms_list.json"),
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

        # Per-property mapping helpers
        self._direct_mappings = DirectMappings(logger=self.logger)
        self._config_def_processor = ConfigDefProcessor(
            derivation_resolver=self._get_config_derivation_method,
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

    def _generate_fm_config(self, connector: Dict[str, Any]) -> Dict[str, Any]:
        """Generate FM configuration for a connector"""
        name = connector['name']
        config = connector['config']

        self.logger.info(f"Generating FM config for connector: {name}")

        # Get connector class
        connector_class = config.get('connector.class')
        if not connector_class:
            self.logger.error(f"No connector.class found in config for connector: {name}")
            return {
                'name': name,
                'config': {},
                'mapping_errors': ['No connector.class found in config'],
                'unmapped_configs': list(config.keys())
            }

        # Get templates based on connector class
        # Create a config dict that includes the worker URL for template fetching
        config_with_worker = config.copy()
        if 'worker' in connector:
            config_with_worker['worker'] = connector['worker']

        sm_template, fm_template = self._get_templates_for_connector(connector_class, name, config_with_worker)

        # Initialize mapped config with name
        mapped_config = {'name': name}

        # Always include connector.class as it's required
        if connector_class:
            mapped_config['connector.class'] = connector_class

        mapping_errors = []
        unmapped_configs = []

        # Map properties based on connector type
        jdbc_mapped = {}
        if 'connection.url' in config and isinstance(config['connection.url'], str) and config['connection.url'].startswith('jdbc:'):
            connector_type = self._database_inferrer.infer_database_type(config)
            jdbc_mapped = self._database_inferrer.map_jdbc_properties(config, connector_type)
            mapped_config.update(jdbc_mapped)

        # Track all handled properties to avoid remapping
        handled_properties = set(mapped_config.keys())

        # Also exclude connection.url from semantic matching since it's handled by JDBC parsing
        if 'connection.url' in config:
            handled_properties.add('connection.url')

        # Apply template mappings if available
        if fm_template:  # Only require FM template, SM template is optional
            self.logger.debug(f"Applying template mappings for {connector_class}")

            # Get required properties from FM template
            required_props = self._direct_mappings.get_required_properties(fm_template)

            # Extract template_id from the correct location
            plugin_type = "Unknown"
            if fm_template.get('template_id'):
                plugin_type = fm_template.get('template_id')
            elif 'templates' in fm_template and len(fm_template['templates']) > 0:
                plugin_type = fm_template['templates'][0].get('template_id', 'Unknown')

            self.logger.info(f"Using template_id for transforms: {plugin_type}")

            # Update connector.class to use template_id
            if plugin_type != "Unknown":
                mapped_config['connector.class'] = plugin_type
                self.logger.info(f"Updated connector.class to template_id: {plugin_type}")

            # Instead of required property error logic, just try to map required properties if present
            for prop_name, prop_info in required_props.items():
                # Skip connector.class and name as they're already handled
                if prop_name in ['connector.class', 'name']:
                    continue

                # Check if property is in input config
                if prop_name in config:
                    mapped_config[prop_name] = config[prop_name]
                    handled_properties.add(prop_name)
                    self.logger.info(f"Using input value for required property: {prop_name}")
                # Check if property was mapped from JDBC URL
                elif prop_name in jdbc_mapped:
                    mapped_config[prop_name] = jdbc_mapped[prop_name]
                    handled_properties.add(prop_name)
                    self.logger.info(f"Using JDBC mapped value for required property: {prop_name}")
                # Check if property has a default value
                elif prop_info.get('default_value') is not None:
                    mapped_config[prop_name] = prop_info['default_value']
                    handled_properties.add(prop_name)
                    self.logger.info(f"Using default value for required property: {prop_name}")
            transforms_data = self._smt_classifier.classify(config, self._fm_smt_registry.get_smts_for_plugin(plugin_type))
            mapped_config.update(transforms_data['allowed'])

            # Add mapping errors from transforms processing
            if 'mapping_errors' in transforms_data:
                mapping_errors.extend(transforms_data['mapping_errors'])
            # Step 1: Try direct mappings from template connector_configs first
            direct_mapped, direct_mapping_errors = self._direct_mappings.apply_to_config(config, fm_template)
            for fm_prop_name, value in direct_mapped.items():
                if fm_prop_name not in handled_properties:
                    mapped_config[fm_prop_name] = value
                    handled_properties.add(fm_prop_name)
                    self.logger.info(f"Direct template mapping: {fm_prop_name}")

            # Add direct mapping errors to the main error list
            mapping_errors.extend(direct_mapping_errors)

            # Then map properties that exist in the input config
            for sm_prop_name, sm_prop_value in config.items():
                try:
                    # Skip connector.class and name as they're already handled
                    if sm_prop_name in ['connector.class', 'name']:
                        continue

                    # Skip if we already handled this property as a required property or via JDBC mapping
                    if sm_prop_name in handled_properties:
                        continue

                    # Skip transform properties as they are handled separately
                    if isinstance(sm_prop_name, str) and sm_prop_name.startswith('transforms'):
                        continue

                    self.logger.debug(f"\nProcessing property: {sm_prop_name}")
                    self.logger.debug(f"Property value: {sm_prop_value}")

                    # Step 2: Try exact name match first
                    property_found = False
                    if 'templates' in fm_template:
                        for template in fm_template['templates']:
                            # Check config_defs first
                            if 'config_defs' in template:
                                for config_def in template['config_defs']:
                                    if config_def['name'] == sm_prop_name:
                                        # Direct match found - only map if not already handled
                                        if sm_prop_name not in handled_properties:
                                            mapped_config[sm_prop_name] = sm_prop_value
                                            handled_properties.add(sm_prop_name)
                                            self.logger.info(f"Direct match found for property: {sm_prop_name}")
                                        else:
                                            self.logger.debug(f"Skipping direct match for {sm_prop_name} as it is already mapped")
                                        property_found = True
                                        break
                            if property_found:
                                break

                    if not property_found:
                        # Step 3: Check static mappings based on connector type
                        is_source = self._direct_mappings.is_source_connector(fm_template)
                        static_mappings = self.static_property_mappings_source if is_source else self.static_property_mappings_sink

                        if sm_prop_name in static_mappings:
                            fm_prop_name = static_mappings[sm_prop_name]

                            # Only map if the target property hasn't been handled yet
                            if fm_prop_name not in handled_properties:
                                # Apply reverse value mapping for converter properties
                                if sm_prop_name in ['key.converter', 'value.converter'] and sm_prop_value in self.converter_to_format_mappings:
                                    mapped_value = self.converter_to_format_mappings[sm_prop_value]
                                    mapped_config[fm_prop_name] = mapped_value
                                    connector_type = "source" if is_source else "sink"
                                    self.logger.info(f"Static mapping with value conversion ({connector_type}): {sm_prop_name}='{sm_prop_value}' -> {fm_prop_name}='{mapped_value}'")
                                else:
                                    mapped_config[fm_prop_name] = sm_prop_value
                                    connector_type = "source" if is_source else "sink"
                                    self.logger.info(f"Static mapping found ({connector_type}): {sm_prop_name} -> {fm_prop_name}")

                                handled_properties.add(fm_prop_name)
                                property_found = True
                            else:
                                self.logger.debug(f"Skipping static mapping for {sm_prop_name} -> {fm_prop_name} as {fm_prop_name} is already mapped")
                                property_found = True

                    if not property_found:
                        # Step 4: Try semantic matching
                        sm_prop = {
                            'name': sm_prop_name,
                            'description': sm_template.get('documentation', ''),
                            'type': sm_template.get('type', 'STRING'),
                            'section': sm_template.get('group', 'General')
                        }

                        # Get FM properties for matching (as dictionary)
                        fm_properties_dict = {}
                        if 'templates' in fm_template:
                            for template in fm_template['templates']:
                                # Add config_defs properties
                                if 'config_defs' in template:
                                    for config_def in template['config_defs']:
                                        fm_properties_dict[config_def['name']] = config_def

                        # Find best match using semantic matching with threshold
                        result = self.semantic_matcher.find_best_match(sm_prop, fm_properties_dict, semantic_threshold=0.7)

                        if result and result.matched_fm_property:
                            # result.matched_fm_property is now a dictionary, so we need to get the property name
                            # The find_best_match method returns the property info, but we need the property name
                            # We'll need to find the property name by matching the property info
                            fm_prop_name = None
                            for prop_name, prop_info in fm_properties_dict.items():
                                if prop_info == result.matched_fm_property:
                                    fm_prop_name = prop_name
                                    break

                            if fm_prop_name and fm_prop_name not in handled_properties:
                                mapped_config[fm_prop_name] = sm_prop_value
                                handled_properties.add(fm_prop_name)
                                self.logger.info(f"Successfully mapped {sm_prop_name} to {fm_prop_name} using {result.match_type} matching")
                            elif fm_prop_name in handled_properties:
                                self.logger.debug(f"Skipping semantic mapping for {sm_prop_name} -> {fm_prop_name} as {fm_prop_name} is already mapped")
                            else:
                                error_msg = f"Could not determine property name for matched property: {sm_prop_name}"
                                mapping_errors.append(error_msg)
                                unmapped_configs.append(sm_prop_name)
                                self.logger.warning(f"Failed to map property '{sm_prop_name}' - could not determine property name")
                        else:
                            error_msg = f"Config '{sm_prop_name}' not exposed for fully managed connector"
                            mapping_errors.append(error_msg)
                            unmapped_configs.append(sm_prop_name)
                            self.logger.warning(f"Failed to map property '{sm_prop_name}'")
                except Exception as e:
                    error_msg = f"Error mapping {sm_prop_name}: {str(e)}"
                    mapping_errors.append(error_msg)
                    self.logger.error(f"Error mapping property '{sm_prop_name}': {e}")
            # After all mapping, check for missing required properties
            for prop_name, prop_info in required_props.items():
                if prop_name in ['connector.class', 'name']:
                    continue
                if prop_name not in mapped_config:
                    error_msg = f"Required property '{prop_name}' needs a value but none was provided in input config or default value"
                    mapping_errors.append(error_msg)
                    self.logger.error(error_msg)

        # Filter mapped config to only include properties defined in config_defs
        filtered_config = {}
        filtered_out_properties = []
        if fm_template and 'templates' in fm_template:
            # Collect all config_def names from all templates
            config_def_names = set()
            for template in fm_template['templates']:
                if 'config_defs' in template:
                    for config_def in template['config_defs']:
                        config_def_names.add(config_def['name'])

            # Only keep properties that are in config_defs, but exclude transform properties
            for prop_name, prop_value in mapped_config.items():
                # Transform properties are separate from config_defs and should always be included
                if prop_name.startswith('transforms') or prop_name == 'transforms':
                    filtered_config[prop_name] = prop_value
                    self.logger.debug(f"Including transform property '{prop_name}' (not subject to config_defs filtering)")
                elif prop_name in config_def_names:
                    filtered_config[prop_name] = prop_value
                else:
                    filtered_out_properties.append(prop_name)
                    error_msg = f"Config '{prop_name}' not exposed for fully managed connector"
                    mapping_errors.append(error_msg)
                    unmapped_configs.append(prop_name)
                    self.logger.warning(error_msg)

            self.logger.info(f"Filtered config from {len(mapped_config)} to {len(filtered_config)} properties (only config_defs)")
            if filtered_out_properties:
                self.logger.warning(f"Filtered out {len(filtered_out_properties)} properties not in config_defs: {', '.join(filtered_out_properties)}")
        else:
            # If no FM template, keep all mapped config
            filtered_config = mapped_config
            self.logger.warning("No FM template available - keeping all mapped properties")

        # Log summary of mapping results
        if unmapped_configs:
            self.logger.warning(f"Connector has {len(unmapped_configs)} unmapped configurations: {', '.join(unmapped_configs)}")
        else:
            self.logger.info("All configurations were successfully mapped")

        self.logger.info(f"Mapping completed with {len(filtered_config)} properties mapped and {len(mapping_errors)} errors")

        return {
            'name': name,
            'sm_config': config,  # Include original SM config
            'config': filtered_config,
            'mapping_errors': mapping_errors,
            'unmapped_configs': unmapped_configs
        }

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
        ConnectorComparator.parse_connector_file(self.input_file, connectors_dict, self.logger)

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
        with open(tco_info_file, 'w') as tco_file:
            json.dump(tco_info, tco_file, indent=2)
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
                derivation_method = self._get_config_derivation_method(template_config_name, template_config_def)

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

    def _get_config_derivation_method(self, template_config_name: str, template_config_def: Dict[str, Any]):
        """
        Get the method to derive a template config from user configs.
        This maps template config names to their derivation methods.
        """
        # Map of template config names to their derivation methods
        config_derivation_methods = {
            # JDBC-related configs
            'connection.url': self._derive_connection_url,
            'connection.host': self._derive_connection_host,
            'connection.port': self._derive_connection_port,
            'connection.user': self._derive_connection_user,
            'connection.password': self._derive_connection_password,
            'connection.database': self._derive_connection_database,
            'db.name': self._derive_db_name,
            'db.connection.type': self._derive_db_connection_type,
            'ssl.server.cert.dn': self._derive_ssl_server_cert_dn,

            # Data format configs
            'input.key.format': self._derive_input_key_format,
            'input.data.format': self._derive_input_data_format,
            'output.key.format': self._derive_output_key_format,
            'output.data.format': self._derive_output_data_format,
            'output.data.key.format': self._derive_output_data_key_format,
            'output.data.value.format': self._derive_output_data_value_format,

            # SSL configs
            'ssl.mode': self._derive_ssl_mode,

            # Redis configs
            'redis.hostname': self._derive_redis_hostname,
            'redis.portnumber': self._derive_redis_portnumber,
            'redis.ssl.mode': self._derive_redis_ssl_mode,

            # Service Bus configs
            'azure.servicebus.namespace': self._derive_servicebus_namespace,
            'azure.servicebus.sas.keyname': self._derive_azure_servicebus_sas_keyname,
            'azure.servicebus.sas.key': self._derive_azure_servicebus_sas_key,
            'azure.servicebus.entity.name': self._derive_azure_servicebus_entity_name,
            
            # Subject name strategy configs
            'key.converter.key.subject.name.strategy': self._derive_subject_name_strategy,
            'value.converter.value.subject.name.strategy': self._derive_subject_name_strategy,
            'key.subject.name.strategy': self._derive_subject_name_strategy,
            'subject.name.strategy': self._derive_subject_name_strategy,
            'value.subject.name.strategy': self._derive_subject_name_strategy,
            'value.converter.reference.subject.name.strategy': self._derive_reference_subject_name_strategy,
            'key.converter.reference.subject.name.strategy': self._derive_reference_subject_name_strategy,
            # Add more mappings as needed
        }

        return config_derivation_methods.get(template_config_name)

    def _derive_connection_host(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive connection.host from user configs (e.g., from JDBC URL or MongoDB connection string)"""
        # Try to extract from JDBC URL
        if 'connection.url' in user_configs:
            jdbc_url = user_configs['connection.url']
            if jdbc_url.startswith('jdbc:'):
                parsed = self._jdbc_url_parser.parse_jdbc_url(jdbc_url)
                return parsed.get('host')

        # Try to extract from MongoDB connection string
        if 'connection.uri' in user_configs:
            mongo_uri = user_configs['connection.uri']
            parsed = self._jdbc_url_parser.parse_mongodb_url(mongo_uri)
            return parsed.get('host')

        # Check for MongoDB-specific connection string configs
        for config_key in ['mongodb.connection.string', 'connection.string']:
            if config_key in user_configs:
                mongo_uri = user_configs[config_key]
                parsed = self._jdbc_url_parser.parse_mongodb_url(mongo_uri)
                return parsed.get('host')

        return None
    
    def _derive_connection_port(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive connection.port from user configs (e.g., from JDBC URL)"""
        # Try to extract from JDBC URL
        if 'connection.url' in user_configs:
            jdbc_url = user_configs['connection.url']
            if jdbc_url.startswith('jdbc:'):
                parsed = self._jdbc_url_parser.parse_jdbc_url(jdbc_url)
                return parsed.get('port')
        return None
    def _derive_connection_user(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive connection.user from user configs (e.g., from JDBC URL or MongoDB connection string)"""
        # Try to extract from JDBC URL
        if 'connection.url' in user_configs:
            jdbc_url = user_configs['connection.url']
            if jdbc_url.startswith('jdbc:'):
                parsed = self._jdbc_url_parser.parse_jdbc_url(jdbc_url)
                return parsed.get('user')

        # Try to extract from MongoDB connection string
        if 'connection.uri' in user_configs:
            mongo_uri = user_configs['connection.uri']
            parsed = self._jdbc_url_parser.parse_mongodb_url(mongo_uri)
            return parsed.get('user')

        # Check for MongoDB-specific connection string configs
        for config_key in ['mongodb.connection.string', 'connection.string']:
            if config_key in user_configs:
                mongo_uri = user_configs[config_key]
                parsed = self._jdbc_url_parser.parse_mongodb_url(mongo_uri)
                return parsed.get('user')

        return None
    
    def _derive_connection_password(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive connection.password from user configs (e.g., from JDBC URL or MongoDB connection string)"""
        # Try to extract from JDBC URL
        if 'connection.url' in user_configs:
            jdbc_url = user_configs['connection.url']
            if jdbc_url.startswith('jdbc:'):
                parsed = self._jdbc_url_parser.parse_jdbc_url(jdbc_url)
                return parsed.get('password')

        # Try to extract from MongoDB connection string
        if 'connection.uri' in user_configs:
            mongo_uri = user_configs['connection.uri']
            parsed = self._jdbc_url_parser.parse_mongodb_url(mongo_uri)
            return parsed.get('password')

        # Check for MongoDB-specific connection string configs
        for config_key in ['mongodb.connection.string', 'connection.string']:
            if config_key in user_configs:
                mongo_uri = user_configs[config_key]
                parsed = self._jdbc_url_parser.parse_mongodb_url(mongo_uri)
                return parsed.get('password')

        return None

    def _derive_connection_database(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive connection.database from user configs (e.g., from JDBC URL)"""
        # Try to extract from JDBC URL
        if 'connection.url' in user_configs:
            jdbc_url = user_configs['connection.url']
            if jdbc_url.startswith('jdbc:'):
                parsed = self._jdbc_url_parser.parse_jdbc_url(jdbc_url)
                # The _parse_jdbc_url method returns 'db.name', not 'database'
                return parsed.get('db.name')
        return None

    def _derive_db_name(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:

        """Derive db.name from user configs (e.g., from JDBC URL)"""
        # Try to extract from JDBC URL
        if 'connection.url' in user_configs:
            jdbc_url = user_configs['connection.url']
            if jdbc_url.startswith('jdbc:'):
                parsed = self._jdbc_url_parser.parse_jdbc_url(jdbc_url)
                # The _parse_jdbc_url method returns 'db.name', not 'database'
                return parsed.get('db.name')

        # Try to extract from MongoDB connection string
        if 'connection.uri' in user_configs:
            mongo_uri = user_configs['connection.uri']
            parsed = self._jdbc_url_parser.parse_mongodb_url(mongo_uri)
            return parsed.get('database')

        # Check for MongoDB-specific connection string configs
        for config_key in ['mongodb.connection.string', 'connection.string']:
            if config_key in user_configs:
                mongo_uri = user_configs[config_key]
                parsed = self._jdbc_url_parser.parse_mongodb_url(mongo_uri)
                return parsed.get('database')

        # Check for direct db.name config
        if 'db.name' in user_configs:
            return user_configs['db.name']

        # Check for database config
        if 'database' in user_configs:
            return user_configs['database']

        return None

    def _derive_db_connection_type(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive db.connection.type from user configs (e.g., from JDBC URL)"""
        # Try to extract from JDBC URL
        if 'connection.url' in user_configs:
            jdbc_url = user_configs['connection.url']
            if jdbc_url.startswith('jdbc:'):
                parsed = self._jdbc_url_parser.parse_jdbc_url(jdbc_url)
                return parsed.get('db.connection.type')

        # Check for direct db.connection.type config
        if 'db.connection.type' in user_configs:
            return user_configs['db.connection.type']

        # Default fallback
        return None

    def _derive_ssl_server_cert_dn(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive ssl.server.cert.dn from user configs (e.g., from JDBC URL)"""
        # Try to extract from JDBC URL
        if 'connection.url' in user_configs:
            jdbc_url = user_configs['connection.url']
            if jdbc_url.startswith('jdbc:'):
                parsed = self._jdbc_url_parser.parse_jdbc_url(jdbc_url)
                return parsed.get('ssl.server.cert.dn')

        # Check for direct ssl.server.cert.dn config
        if 'ssl.server.cert.dn' in user_configs:
            return user_configs['ssl.server.cert.dn']

        # Default fallback
        return None

    def _derive_database_server_name(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive database.server.name from user configs (e.g., from JDBC URL)"""
        # Try to extract from JDBC URL
        if 'connection.url' in user_configs:
            jdbc_url = user_configs['connection.url']
            if jdbc_url.startswith('jdbc:'):
                parsed = self._jdbc_url_parser.parse_jdbc_url(jdbc_url)
                return parsed.get('host')

        # Check for direct database.server.name config
        if 'database.server.name' in user_configs:
            return user_configs['database.server.name']

        # Check for server name config
        if 'server.name' in user_configs:
            return user_configs['server.name']
    
    def _derive_input_key_format(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive input.key.format from user configs using reverse format mapping"""
        # Reverse data format mapping from template (converter class -> format key)
        reverse_format_mapping = {
            "io.confluent.connect.avro.AvroConverter": "AVRO",
            "io.confluent.connect.json.JsonSchemaConverter": "JSON_SR",
            "io.confluent.connect.protobuf.ProtobufConverter": "PROTOBUF",
            "org.apache.kafka.connect.converters.ByteArrayConverter": "BYTES",
            "org.apache.kafka.connect.json.JsonConverter": "JSON",
            "org.apache.kafka.connect.storage.StringConverter": "STRING"
        }

        # Try direct converter mapping first (reverse map)
        if 'key.converter' in user_configs:
            converter_class = user_configs['key.converter']
            if converter_class in reverse_format_mapping:
                return reverse_format_mapping[converter_class]
            return converter_class  # Return as-is if not in mapping

        # Try to get format from user configs (direct format key)
        format_key = user_configs.get('key.format') or user_configs.get('input.key.format')
        if format_key:
            return format_key

        # Try to infer from schema registry configs
        if 'key.converter.schemas.enable' in user_configs:
            return 'JSON_SR'

        # Try to get default from template if available
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'input.key.format')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default

        # Default fallback
        return 'JSON'
    
    def _derive_input_data_format(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:

        """Derive input.data.format from user configs using reverse format mapping"""
        # Reverse data format mapping from template (converter class -> format key)
        reverse_format_mapping = {
            "io.confluent.connect.avro.AvroConverter": "AVRO",
            "io.confluent.connect.json.JsonSchemaConverter": "JSON_SR",
            "io.confluent.connect.protobuf.ProtobufConverter": "PROTOBUF",
            "org.apache.kafka.connect.converters.ByteArrayConverter": "BYTES",
            "org.apache.kafka.connect.json.JsonConverter": "JSON",
            "org.apache.kafka.connect.storage.StringConverter": "STRING"
        }

        # Try direct converter mapping first (reverse map)
        if 'value.converter' in user_configs:
            converter_class = user_configs['value.converter']
            if converter_class in reverse_format_mapping:
                return reverse_format_mapping[converter_class]
            return converter_class  # Return as-is if not in mapping

        # Try to get format from user configs (direct format key)
        format_key = user_configs.get('value.format') or user_configs.get('input.data.format')
        if format_key:
            return format_key

        # Try to infer from schema registry configs
        if 'value.converter.schemas.enable' in user_configs:
            return 'JSON_SR'

        # Try to get default from template if available
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'input.data.format')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default

        # Default fallback
        return 'JSON'
    
    def _derive_output_key_format(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:

        """Derive output.key.format from user configs using reverse format mapping"""
        # Reverse data format mapping from template (converter class -> format key)
        reverse_format_mapping = {
            "io.confluent.connect.avro.AvroConverter": "AVRO",
            "io.confluent.connect.json.JsonSchemaConverter": "JSON_SR",
            "io.confluent.connect.protobuf.ProtobufConverter": "PROTOBUF",
            "org.apache.kafka.connect.converters.ByteArrayConverter": "BYTES",
            "org.apache.kafka.connect.json.JsonConverter": "JSON",
            "org.apache.kafka.connect.storage.StringConverter": "STRING"
        }

        # Try direct converter mapping first (reverse map)
        if 'key.converter' in user_configs:
            converter_class = user_configs['key.converter']
            if converter_class in reverse_format_mapping:
                return reverse_format_mapping[converter_class]
            return converter_class  # Return as-is if not in mapping

        # Try to get format from user configs (direct format key)
        format_key = user_configs.get('output.key.format') or user_configs.get('key.format')
        if format_key:
            return format_key

        # Try to get default from template if available
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'output.key.format')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default

        # Default fallback
        return 'JSON'
    
    def _derive_output_data_format(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:

        """Derive output.data.format from user configs using reverse format mapping"""
        # Reverse data format mapping from template (converter class -> format key)
        reverse_format_mapping = {
            "io.confluent.connect.avro.AvroConverter": "AVRO",
            "io.confluent.connect.json.JsonSchemaConverter": "JSON_SR",
            "io.confluent.connect.protobuf.ProtobufConverter": "PROTOBUF",
            "org.apache.kafka.connect.converters.ByteArrayConverter": "BYTES",
            "org.apache.kafka.connect.json.JsonConverter": "JSON",
            "org.apache.kafka.connect.storage.StringConverter": "STRING"
        }

        # Try direct converter mapping first (reverse map)
        if 'value.converter' in user_configs:
            converter_class = user_configs['value.converter']
            if converter_class in reverse_format_mapping:
                return reverse_format_mapping[converter_class]
            return converter_class  # Return as-is if not in mapping

        # Try to get format from user configs (direct format key)
        format_key = user_configs.get('output.data.format') or user_configs.get('value.format')
        if format_key:
          return format_key

        # Try to get default from template if available
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'output.data.format')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default

        # Default fallback
        return 'JSON'

    def _derive_output_data_key_format(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:

        """Derive output.data.key.format from user configs using reverse format mapping"""
        # Reverse data format mapping from template (converter class -> format key)
        reverse_format_mapping = {
            "io.confluent.connect.avro.AvroConverter": "AVRO",
            "io.confluent.connect.json.JsonSchemaConverter": "JSON_SR",
            "io.confluent.connect.protobuf.ProtobufConverter": "PROTOBUF",
            "org.apache.kafka.connect.converters.ByteArrayConverter": "BYTES",
            "org.apache.kafka.connect.json.JsonConverter": "JSON",
            "org.apache.kafka.connect.storage.StringConverter": "STRING"
        }

        # Try direct converter mapping first (reverse map)
        if 'key.converter' in user_configs:
            converter_class = user_configs['key.converter']
            if converter_class in reverse_format_mapping:
                return reverse_format_mapping[converter_class]
            return converter_class  # Return as-is if not in mapping

        # Try to get format from user configs (direct format key)
        format_key = user_configs.get('output.data.key.format') or user_configs.get('key.format')
        if format_key:
            return format_key

        # Try to infer from output key format if already derived
        if 'output.key.format' in fm_configs:
            return fm_configs['output.key.format']

        # Try to get default from template if available
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'output.data.key.format')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default

        # Default fallback
        return 'JSON'
    
    def _derive_output_data_value_format(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:

        """Derive output.data.value.format from user configs using reverse format mapping"""
        # Reverse data format mapping from template (converter class -> format key)
        reverse_format_mapping = {
            "io.confluent.connect.avro.AvroConverter": "AVRO",
            "io.confluent.connect.json.JsonSchemaConverter": "JSON_SR",
            "io.confluent.connect.protobuf.ProtobufConverter": "PROTOBUF",
            "org.apache.kafka.connect.converters.ByteArrayConverter": "BYTES",
            "org.apache.kafka.connect.json.JsonConverter": "JSON",
            "org.apache.kafka.connect.storage.StringConverter": "STRING"
        }

        # Try direct converter mapping first (reverse map)
        if 'value.converter' in user_configs:
            converter_class = user_configs['value.converter']
            if converter_class in reverse_format_mapping:
                return reverse_format_mapping[converter_class]
            return converter_class  # Return as-is if not in mapping

        # Try to get format from user configs (direct format key)
        format_key = user_configs.get('output.data.value.format') or user_configs.get('value.format')
        if format_key:
            return format_key

        # Try to infer from output data format if already derived
        if 'output.data.format' in fm_configs:
            return fm_configs['output.data.format']

        # Try to get default from template if available
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'output.data.value.format')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default

        # Default fallback
        return 'JSON'
    
    def _derive_authentication_method(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:

        """Derive authentication.method from user configs"""
        # Check for various authentication-related configs
        auth_configs = [
            'security.protocol',
            'sasl.mechanism',
            'authentication.type',
            'auth.method'
        ]

        for auth_config in auth_configs:
            if auth_config in user_configs:
                auth_value = user_configs[auth_config].lower()
                if 'plain' in auth_value:
                    return 'PLAIN'
                elif 'scram' in auth_value:
                    return 'SCRAM'
                elif 'oauth' in auth_value or 'bearer' in auth_value:
                    return 'OAUTHBEARER'
                elif 'ssl' in auth_value or 'tls' in auth_value:
                    return 'SSL'
                else:
                    return auth_value

        # Try to get default from template if available
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'authentication.method')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default

        # Default fallback
        return 'PLAIN'
    
    def _derive_csfle_enabled(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:

        """Derive csfle.enabled from user configs"""
        # Try to get default from template if available
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'csfle.enabled')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default

        # Default fallback
        return 'false'
    
    def _derive_csfle_on_failure(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:

        """Derive csfle.onFailure from user configs"""
        # Try to get default from template if available
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'csfle.onFailure')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default

        # Default fallback
        return 'FAIL'
    
    def _derive_ssl_mode(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:

        """Derive ssl.mode from user configs"""
        # Check for direct ssl.mode config first
        if 'ssl.mode' in user_configs:
            value = user_configs['ssl.mode'].lower()
            # Map common SSL mode values
            if value in ['prefer', 'preferred']:
                return 'prefer'
            elif value in ['require', 'required']:
                return 'require'
            elif value in ['verify-ca', 'verifyca', 'verify_ca']:
                return 'verify-ca'
            elif value in ['verify-full', 'verifyfull', 'verify_full']:
                return 'verify-full'
            elif value in ['disabled', 'disable', 'false', 'none']:
                return 'disabled'

        # Check for database-specific SSL mode configs
        for config_key in [
            'connection.sslmode',  # PostgreSQL
            'connection.sslMode',  # MySQL
            'database.ssl.mode',   # MySQL CDC
            'redis.ssl.mode',      # Redis
            'ssl.enabled',         # Generic
            'use.ssl',             # Generic
            'ssl.use'              # Generic
        ]:
            if config_key in user_configs:
                value = user_configs[config_key].lower()
                # Map boolean values
                if value in ['true', 'yes', '1', 'enabled']:
                    return 'require'  # Default to require when SSL is enabled
                elif value in ['false', 'no', '0', 'disabled']:
                    return 'disabled'
                # Map string values
                elif value in ['prefer', 'preferred']:
                    return 'prefer'
                elif value in ['require', 'required']:
                    return 'require'
                elif value in ['verify-ca', 'verifyca', 'verify_ca']:
                    return 'verify-ca'
                elif value in ['verify-full', 'verifyfull', 'verify_full']:
                    return 'verify-full'

        # Check for SSL-related configs that might indicate SSL usage
        ssl_indicators = [
            'ssl.truststorefile', 'ssl.truststorepassword', 'ssl.rootcertfile',
            'connection.javax.net.ssl.trustStore', 'connection.javax.net.ssl.trustStorePassword',
            'ssl.truststore.file', 'ssl.truststore.password', 'ssl.cert.file',
            'ssl.key.file', 'ssl.ca.file', 'ssl.certificate.file'
        ]

        for indicator in ssl_indicators:
            if indicator in user_configs and user_configs[indicator]:
                # If SSL certificates/truststores are provided, likely need verify-ca or verify-full
                cert_value = user_configs[indicator].lower()
                if 'verify' in cert_value or 'cert' in cert_value:
                    return 'verify-ca'  # Default to verify-ca when certificates are provided
                else:
                    return 'require'  # Default to require when SSL files are provided

        # Check for connection URL that might indicate SSL
        if 'connection.url' in user_configs:
            url = user_configs['connection.url'].lower()
            if 'ssl=true' in url or 'sslmode=' in url or 'useSSL=true' in url:
                # Extract SSL mode from URL if present
                if 'sslmode=prefer' in url:
                    return 'prefer'
                elif 'sslmode=require' in url:
                    return 'require'
                elif 'sslmode=verify-ca' in url:
                    return 'verify-ca'
                elif 'sslmode=verify-full' in url:
                    return 'verify-full'
                elif 'sslmode=disable' in url or 'sslmode=disabled' in url:
                    return 'disabled'
                else:
                    return 'require'  # Default to require when SSL is enabled in URL

        # Try to get default from template if available
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'ssl.mode')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default

        # Default to prefer if no SSL configuration is found
        return 'prefer'
    
    def _derive_redis_hostname(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:

        """Derive redis.hostname from user configs"""
        # Check for direct redis.hostname config first
        if 'redis.hostname' in user_configs:
            return user_configs['redis.hostname']

        # Check for common Redis host configurations
        for config_key in [
            'redis.host', 'redis.server', 'redis.address', 'redis.endpoint',
            'host', 'server', 'address', 'endpoint'
        ]:
            if config_key in user_configs:
                value = user_configs[config_key]
                # If it's a host:port format, extract just the host
                if ':' in value:
                    host = value.split(':')[0]
                    return host
                return value

        # Check for redis.hosts config (format: host:port)
        if 'redis.hosts' in user_configs:
            hosts_value = user_configs['redis.hosts']
            if ':' in hosts_value:
                host = hosts_value.split(':')[0]
                return host

        # Check for connection URL that might contain Redis host
        for config_key in ['connection.url', 'connection.uri', 'redis.connection.url']:
            if config_key in user_configs:
                url = user_configs[config_key].lower()
                if 'redis://' in url:
                    # Extract host from Redis URL
                    # Format: redis://host:port/db
                    redis_part = url.replace('redis://', '')
                    if '@' in redis_part:
                        # Handle authentication: redis://user:pass@host:port/db
                        auth_part, host_part = redis_part.split('@', 1)
                        host = host_part.split('/')[0].split(':')[0]
                        return host
                    else:
                        host = redis_part.split('/')[0].split(':')[0]
                        return host

        return None
    
    def _derive_redis_portnumber(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:

        """Derive redis.portnumber from user configs"""
        # Check for direct redis.portnumber config first
        if 'redis.portnumber' in user_configs:
            return user_configs['redis.portnumber']

        # Check for common Redis port configurations
        for config_key in [
            'redis.port', 'redis.server.port', 'port', 'server.port'
        ]:
            if config_key in user_configs:
                return user_configs[config_key]

        # Check for redis.hosts config (format: host:port)
        if 'redis.hosts' in user_configs:
            hosts_value = user_configs['redis.hosts']
            if ':' in hosts_value:
                port = hosts_value.split(':')[1]
                # Remove any additional path or query parameters
                if '/' in port:
                    port = port.split('/')[0]
                return port

        # Check for connection URL that might contain Redis port
        for config_key in ['connection.url', 'connection.uri', 'redis.connection.url']:
            if config_key in user_configs:
                url = user_configs[config_key].lower()
                if 'redis://' in url:
                    # Extract port from Redis URL
                    # Format: redis://host:port/db
                    redis_part = url.replace('redis://', '')
                    if '@' in redis_part:
                        # Handle authentication: redis://user:pass@host:port/db
                        auth_part, host_part = redis_part.split('@', 1)
                        host_port = host_part.split('/')[0]
                        if ':' in host_port:
                            port = host_port.split(':')[1]
                            return port
                    else:
                        host_port = redis_part.split('/')[0]
                        if ':' in host_port:
                            port = host_port.split(':')[1]
                            return port
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'redis.portnumber')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default
        # Default Redis port
        return '6379'
    
    def _derive_redis_ssl_mode(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:

        """Derive redis.ssl.mode from user configs"""
        # Check for direct redis.ssl.mode config first
        if 'redis.ssl.mode' in user_configs:
            value = user_configs['redis.ssl.mode'].lower()
            # Map common SSL mode values to Redis SSL mode values
            if value in ['disabled', 'disable', 'false', 'none', 'off']:
                return 'disabled'
            elif value in ['enabled', 'enable', 'true', 'on']:
                return 'enabled'
            elif value in ['server', 'server-only', 'verify-server']:
                return 'server'
            elif value in ['server+client', 'server+client', 'mutual', 'two-way']:
                return 'server+client'
            else:
                # Return as-is if it's already a valid Redis SSL mode
                return user_configs['redis.ssl.mode']

        # Check for Redis SSL enabled flag
        for config_key in ['redis.ssl.enabled', 'redis.ssl', 'ssl.enabled', 'use.ssl']:
            if config_key in user_configs:
                value = user_configs[config_key].lower()
                if value in ['true', 'yes', '1', 'enabled', 'on']:
                    return 'enabled'
                elif value in ['false', 'no', '0', 'disabled', 'off']:
                    return 'disabled'

        # Check for SSL-related configs that might indicate SSL usage
        ssl_indicators = [
            'redis.ssl.keystore.file', 'redis.ssl.keystore.password',
            'redis.ssl.truststore.file', 'redis.ssl.truststore.password',
            'redis.ssl.cert.file', 'redis.ssl.key.file', 'redis.ssl.ca.file'
        ]

        for indicator in ssl_indicators:
            if indicator in user_configs and user_configs[indicator]:
                # If SSL certificates/keystores are provided, determine the mode
                cert_value = user_configs[indicator].lower()
                if 'client' in cert_value or 'keystore' in indicator:
                    return 'server+client'  # Client certificates indicate mutual auth
                else:
                    return 'server'  # Server certificates only

        # Check for connection URL that might indicate SSL
        for config_key in ['connection.url', 'connection.uri', 'redis.connection.url']:
            if config_key in user_configs:
                url = user_configs[config_key].lower()
                if 'rediss://' in url:  # Redis with SSL
                    return 'enabled'
                elif 'redis://' in url and 'ssl=true' in url:
                    return 'enabled'
                elif 'redis://' in url and 'ssl=false' in url:
                    return 'disabled'

        # Try to get default from template if available
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'redis.ssl.mode')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default

        # Default to disabled if no SSL configuration is found
        return 'disabled'

    def _derive_servicebus_namespace(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> str:
        if 'azure.servicebus.namespace' in user_configs:
            return user_configs['azure.servicebus.namespace']

        # Try to extract from connection string if present
        conn_str = user_configs.get('azure.servicebus.connection.string')
        if conn_str:
            match = re.search(r'Endpoint=sb://([^.]+)\.servicebus\.windows\.net/', conn_str)
            if match:
                return match.group(1)
        return user_configs.get('azure.servicebus.namespace')

    def _derive_azure_servicebus_sas_keyname(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> str:
        if 'azure.servicebus.sas.keyname' in user_configs:
            return user_configs['azure.servicebus.sas.keyname']

        # Try to extract from connection string if present
        conn_str = user_configs.get('azure.servicebus.connection.string')
        if conn_str:
            match = re.search(r'SharedAccessKeyName=([^;]+)', conn_str)
            if match:
                return match.group(1)
        return user_configs.get('azure.servicebus.sas.keyname')

    def _derive_azure_servicebus_sas_key(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> str:
        if 'azure.servicebus.sas.key' in user_configs:
            return user_configs['azure.servicebus.sas.key']

        # Try to extract from connection string if present
        conn_str = user_configs.get('azure.servicebus.connection.string')
        if conn_str:
            match = re.search(r'SharedAccessKey=([^;]+)', conn_str)
            if match:
                return match.group(1)
        return user_configs.get('azure.servicebus.sas.key')

    def _derive_azure_servicebus_entity_name(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> str:
        if 'azure.servicebus.entity.name' in user_configs:
            return user_configs['azure.servicebus.entity.name']

        # Try to extract from connection string if present
        conn_str = user_configs.get('azure.servicebus.connection.string')
        if conn_str:
            match = re.search(r'EntityPath=([^;]+)', conn_str)
            if match:
                return match.group(1)
        return user_configs.get('azure.servicebus.entity.name')

    def _derive_subject_name_strategy(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive subject name strategy from user configs by extracting recommended values from template config def"""
        
        # Get recommended values from template config definition for the specific config
        recommended_strategies = []
        if template_config_defs and config_name:
            for template_config_def in template_config_defs:
                if isinstance(template_config_def, dict) and template_config_def.get('name') == config_name:
                    recommended_values = template_config_def.get('recommended_values', [])
                    if recommended_values:
                        recommended_strategies.extend(recommended_values)
                        break
        
        # Fallback to common recommended values if not found in template
        if not recommended_strategies:
            recommended_strategies = [
                "TopicNameStrategy",
                "RecordNameStrategy", 
                "TopicRecordNameStrategy"
            ]
        
        # Look for the specific config in user configs
        if config_name and config_name in user_configs:
            config_value = user_configs[config_name]
            # Extract config value by finding last . and get string after that
            if '.' in config_value:
                config_value = config_value.split('.')[-1]
            # Check if any recommended strategy is contained in the config value
            for strategy in recommended_strategies:
                if strategy.lower() == config_value.lower():
                    return strategy
        
        return None

    def _derive_reference_subject_name_strategy(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None, config_name: str = None) -> Optional[str]:
        """Derive reference subject name strategy from user configs by extracting recommended values from template config def"""
        
        # Get recommended values from template config definition for the specific config
        recommended_strategies = []
        if template_config_defs and config_name:
            for template_config_def in template_config_defs:
                if isinstance(template_config_def, dict) and template_config_def.get('name') == config_name:
                    recommended_values = template_config_def.get('recommended_values', [])
                    if recommended_values:
                        recommended_strategies.extend(recommended_values)
                        break
        
        # Fallback to common recommended values if not found in template
        if not recommended_strategies:
            recommended_strategies = [
                "DefaultReferenceSubjectNameStrategy",
                "QualifiedReferenceSubjectNameStrategy"
            ]
        
        # Look for the specific config in user configs
        if config_name and config_name in user_configs:
            config_value = user_configs[config_name]
            # Extract config value by finding last . and get string after that
            if '.' in config_value:
                config_value = config_value.split('.')[-1]
            # Check if any recommended strategy is contained in the config value
            for strategy in recommended_strategies:
                if strategy.lower() == config_value.lower():
                    return strategy
        
        return None

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

    def _is_placeholder(self, value: str) -> bool:
        """Check if a value is a placeholder like ${xxxx}"""
        return value.startswith('${')

    def _extract_placeholder_name(self, placeholder: str) -> str:
        """Extract the placeholder name from ${xxxx} format"""
        if placeholder.startswith('${'):
            # Find the closing } or use the whole string after ${
            end_pos = placeholder.find('}', 2)
            if end_pos != -1:
                return placeholder[2:end_pos]  # Remove ${ and }
            else:
                return placeholder[2:]  # Remove ${ only
        return placeholder

    def _resolve_template_default(self, template_default: str, fm_configs: Dict[str, str]) -> str:
        """Resolve template default value, handling placeholders like ${xxxx}"""
        if self._is_placeholder(template_default):
            placeholder_name = self._extract_placeholder_name(template_default)
            if placeholder_name in fm_configs:
                resolved_value = fm_configs[placeholder_name]
                return resolved_value
            else:
                self.logger.warning(f"Placeholder '{placeholder_name}' not found in fm_configs")
                return None
        else:
            return template_default

    def _get_template_default_value(self, template_config_defs: List[Dict[str, Any]], config_name: str) -> Optional[str]:
        """Extract default value for a configuration from template definitions"""
        for template_config_def in template_config_defs:
            if template_config_def.get('name') == config_name:
                default_value = template_config_def.get('default_value')
                if default_value is not None:
                    return str(default_value)
        return None

    def _derive_connection_url(self, user_configs: Dict[str, str], fm_configs: Dict[str, str], template_config_defs: List[Dict[str, Any]] = None) -> Optional[str]:
        """Derive connection.url from user configs specifically for Snowflake connectors"""

        # Check for JDBC URL patterns that might contain Snowflake URLs
        jdbc_patterns = [
            'connection.url'
        ]

        for pattern in jdbc_patterns:
            if pattern in user_configs:
                jdbc_url = user_configs[pattern]
                if jdbc_url and 'jdbc:snowflake://' in jdbc_url:
                    # Extract Snowflake connection string by removing jdbc:snowflake:// prefix
                    snowflake_connection_string = jdbc_url.replace('jdbc:snowflake://', '')
                    return snowflake_connection_string.strip()
                elif jdbc_url and jdbc_url.startswith('jdbc:'):
                    # For non-Snowflake JDBC URLs, return null
                    return None

        # Try to get default from template if available
        if template_config_defs:
            template_default = self._get_template_default_value(template_config_defs, 'connection.url')
            if template_default:
                resolved_default = self._resolve_template_default(template_default, fm_configs)
                return resolved_default

        return None
