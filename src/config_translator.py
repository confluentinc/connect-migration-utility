"""
Config Translator Module
Translates connector configurations using Confluent Cloud's translate API.
"""

import json
import logging
import base64
import requests
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass


@dataclass
class TranslationWarning:
    """Represents a warning from the translate API."""
    field: str
    message: str


@dataclass
class TranslationResult:
    """Result from the translate API call."""
    success: bool
    config: Optional[Dict[str, Any]]
    warnings: List[TranslationWarning]
    error: Optional[str]
    connector_name: str


class ConfigTranslator:
    """
    Translates connector configurations using Confluent Cloud's /translate API.
    
    This follows the same pattern as the MSK connector migrator in the KCP project.
    """
    
    TRANSLATE_API_URL_TEMPLATE = (
        "https://api.confluent.cloud/connect/v1/environments/{env_id}/clusters/{cluster_id}"
        "/connector-plugins/{plugin_name}/config/translate"
    )
    
    # Mapping from connector.class to plugin name
    CONNECTOR_CLASS_TO_PLUGIN = {
        # Source connectors
        "io.debezium.connector.mysql.MySqlConnector": "MySqlCdcSource",
        "io.debezium.connector.postgresql.PostgresConnector": "PostgresCdcSource",
        "io.debezium.connector.sqlserver.SqlServerConnector": "SqlServerCdcSource",
        "io.debezium.connector.mongodb.MongoDbConnector": "MongoDbCdcSource",
        "io.debezium.connector.oracle.OracleConnector": "OracleCdcSource",
        "io.debezium.connector.v2.mysql.MySqlConnectorV2": "MySqlCdcSourceV2",
        "io.debezium.connector.v2.postgresql.PostgresConnectorV2": "PostgresCdcSourceV2",
        "io.debezium.connector.v2.sqlserver.SqlServerConnectorV2": "SqlServerCdcSourceV2",
        "io.confluent.connect.jdbc.JdbcSourceConnector": "JdbcSource",
        "io.confluent.connect.s3.source.S3SourceConnector": "S3Source",
        "io.confluent.connect.gcs.GcsSourceConnector": "GcsSource",
        "io.confluent.connect.azure.blob.storage.AzureBlobStorageSourceConnector": "AzureBlobStorageSource",
        "io.confluent.connect.sftp.SftpSourceConnector": "SftpSource",
        "io.confluent.connect.http.HttpSourceConnector": "HttpSource",
        "io.confluent.connect.elasticsearch.ElasticsearchSourceConnector": "ElasticsearchSource",
        "io.confluent.connect.mongodb.MongoDbAtlasSource": "MongoDbAtlasSource",
        "io.confluent.connect.salesforce.SalesforceSource": "SalesforceSource",
        "io.confluent.connect.salesforce.SalesforcePushTopicSource": "SalesforcePushTopicSource",
        "io.confluent.connect.salesforce.SalesforcePlatformEventSource": "SalesforcePlatformEventSource",
        "io.confluent.connect.salesforce.SalesforceCdcSource": "SalesforceCdcSource",
        "io.confluent.connect.servicenow.ServiceNowSource": "ServiceNowSource",
        "io.confluent.connect.zendesk.ZendeskSource": "ZendeskSource",
        "io.confluent.connect.jira.JiraSource": "JiraSource",
        "io.confluent.connect.github.GithubSource": "GithubSource",
        "io.confluent.connect.datagen.DatagenSource": "DatagenSource",
        "io.confluent.kafka.connect.datagen.DatagenConnector": "DatagenSource",
        "com.snowflake.kafka.connector.SnowflakeSinkConnector": "SnowflakeSink",
        
        # Sink connectors
        "io.confluent.connect.jdbc.JdbcSinkConnector": "JdbcSink",
        "io.confluent.connect.s3.S3SinkConnector": "S3_SINK",
        "io.confluent.connect.gcs.GcsSinkConnector": "GcsSink",
        "io.confluent.connect.azure.blob.storage.AzureBlobStorageSinkConnector": "AzureBlobStorageSink",
        "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector": "ElasticsearchSink",
        "io.confluent.connect.http.HttpSinkConnector": "HttpSink",
        "io.confluent.connect.mongodb.MongoDbAtlasSink": "MongoDbAtlasSink",
        "io.confluent.connect.sftp.SftpSinkConnector": "SftpSink",
        "io.confluent.connect.splunk.SplunkSinkConnector": "SplunkSink",
        "io.confluent.connect.datadog.DatadogMetricsSinkConnector": "DatadogMetricsSink",
        "io.confluent.connect.salesforce.SalesforcePlatformEventSinkConnector": "SalesforcePlatformEventSink",
        "io.confluent.connect.redis.RedisSinkConnector": "RedisSink",
        "io.confluent.connect.bigquery.BigQuerySinkConnector": "BigQuerySink",
        "io.confluent.connect.dynamodb.DynamoDbSinkConnector": "DynamoDbSink",
    }
    
    def __init__(
        self,
        environment_id: str,
        cluster_id: str,
        api_key: str,
        api_secret: str,
        disable_ssl_verify: bool = False,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize the ConfigTranslator.
        
        Args:
            environment_id: Confluent Cloud environment ID (e.g., env-xxxxx)
            cluster_id: Confluent Cloud cluster ID (e.g., lkc-xxxxx)
            api_key: Confluent Cloud API key
            api_secret: Confluent Cloud API secret
            disable_ssl_verify: Whether to disable SSL verification
            logger: Optional logger instance
        """
        self.environment_id = environment_id
        self.cluster_id = cluster_id
        self.api_key = api_key
        self.api_secret = api_secret
        self.disable_ssl_verify = disable_ssl_verify
        self.logger = logger or logging.getLogger(__name__)
        
        # Validate required fields
        if not all([environment_id, cluster_id, api_key, api_secret]):
            raise ValueError("environment_id, cluster_id, api_key, and api_secret are all required")
    
    def _get_auth_header(self) -> str:
        """Generate the Basic auth header value."""
        credentials = f"{self.api_key}:{self.api_secret}"
        encoded = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
        return f"Basic {encoded}"
    
    def _infer_plugin_name(self, connector_class: str) -> Optional[str]:
        """
        Infer the plugin name from the connector class.
        
        Args:
            connector_class: The fully qualified connector class name
            
        Returns:
            The plugin name for the translate API, or None if not found
        """
        # Direct lookup first
        if connector_class in self.CONNECTOR_CLASS_TO_PLUGIN:
            return self.CONNECTOR_CLASS_TO_PLUGIN[connector_class]
        
        # Try to extract from class name (last part)
        # e.g., "io.confluent.connect.jdbc.JdbcSinkConnector" -> "JdbcSink"
        class_simple_name = connector_class.split('.')[-1]
        
        # Check if it ends with "Connector" and try to derive plugin name
        if class_simple_name.endswith('Connector'):
            base_name = class_simple_name[:-9]  # Remove "Connector"
            # Check for common patterns
            for key, value in self.CONNECTOR_CLASS_TO_PLUGIN.items():
                if value.lower().replace('_', '') == base_name.lower():
                    return value
        
        self.logger.warning(f"Could not infer plugin name for connector class: {connector_class}")
        return None
    
    def translate_config(
        self,
        connector_name: str,
        connector_config: Dict[str, Any],
        plugin_name: Optional[str] = None
    ) -> TranslationResult:
        """
        Translate a connector configuration using the Confluent Cloud translate API.
        
        Args:
            connector_name: Name of the connector
            connector_config: The connector configuration dictionary
            plugin_name: Optional plugin name override. If not provided, will be inferred.
            
        Returns:
            TranslationResult containing the translated config or error information
        """
        # Get connector class
        connector_class = connector_config.get('connector.class')
        if not connector_class:
            return TranslationResult(
                success=False,
                config=None,
                warnings=[],
                error="'connector.class' not found in config",
                connector_name=connector_name
            )
        
        # Infer plugin name if not provided
        if not plugin_name:
            plugin_name = self._infer_plugin_name(connector_class)
            if not plugin_name:
                return TranslationResult(
                    success=False,
                    config=None,
                    warnings=[],
                    error=f"Could not determine plugin name for connector class: {connector_class}",
                    connector_name=connector_name
                )
        
        self.logger.info(f"Translating connector '{connector_name}' using plugin '{plugin_name}'")
        
        # Build the URL
        url = self.TRANSLATE_API_URL_TEMPLATE.format(
            env_id=self.environment_id,
            cluster_id=self.cluster_id,
            plugin_name=plugin_name
        )
        
        # Prepare headers
        headers = {
            "Content-Type": "application/json",
            "Authorization": self._get_auth_header()
        }
        
        try:
            # Make the PUT request
            response = requests.put(
                url,
                json=connector_config,
                headers=headers,
                verify=not self.disable_ssl_verify,
                timeout=30
            )
            
            if response.status_code != 200:
                error_msg = f"API request failed with status {response.status_code}: {response.text}"
                self.logger.error(f"Translation failed for '{connector_name}': {error_msg}")
                return TranslationResult(
                    success=False,
                    config=None,
                    warnings=[],
                    error=error_msg,
                    connector_name=connector_name
                )
            
            # Parse response
            response_data = response.json()
            
            # Extract config and warnings
            translated_config = response_data.get('config', {})
            warnings_data = response_data.get('warnings', [])
            
            warnings = [
                TranslationWarning(
                    field=w.get('field', ''),
                    message=w.get('message', '')
                )
                for w in warnings_data
            ]
            
            if warnings:
                self.logger.info(f"Translation warnings for '{connector_name}': {len(warnings)} warning(s)")
                for w in warnings:
                    self.logger.warning(f"  - {w.field}: {w.message}")
            
            self.logger.info(f"Successfully translated config for '{connector_name}'")
            
            return TranslationResult(
                success=True,
                config=translated_config,
                warnings=warnings,
                error=None,
                connector_name=connector_name
            )
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Network error during translation: {str(e)}"
            self.logger.error(f"Translation failed for '{connector_name}': {error_msg}")
            return TranslationResult(
                success=False,
                config=None,
                warnings=[],
                error=error_msg,
                connector_name=connector_name
            )
        except json.JSONDecodeError as e:
            error_msg = f"Failed to parse API response: {str(e)}"
            self.logger.error(f"Translation failed for '{connector_name}': {error_msg}")
            return TranslationResult(
                success=False,
                config=None,
                warnings=[],
                error=error_msg,
                connector_name=connector_name
            )
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            self.logger.error(f"Translation failed for '{connector_name}': {error_msg}")
            return TranslationResult(
                success=False,
                config=None,
                warnings=[],
                error=error_msg,
                connector_name=connector_name
            )
    
    def translate_connectors(
        self,
        connectors: Dict[str, Dict[str, Any]]
    ) -> Tuple[List[TranslationResult], List[TranslationResult]]:
        """
        Translate multiple connector configurations.
        
        Args:
            connectors: Dictionary mapping connector names to their configs
            
        Returns:
            Tuple of (successful_translations, failed_translations)
        """
        successful = []
        failed = []
        
        for connector_name, connector_data in connectors.items():
            # Handle both formats: direct config or nested config
            if 'config' in connector_data:
                config = connector_data['config']
            else:
                config = connector_data
            
            result = self.translate_config(connector_name, config)
            
            if result.success:
                successful.append(result)
            else:
                failed.append(result)
        
        self.logger.info(
            f"Translation complete: {len(successful)} successful, {len(failed)} failed"
        )
        
        return successful, failed


# Convenience function for quick translation
def translate_connector_config(
    connector_name: str,
    connector_config: Dict[str, Any],
    environment_id: str,
    cluster_id: str,
    api_key: str,
    api_secret: str,
    disable_ssl_verify: bool = False
) -> TranslationResult:
    """
    Convenience function to translate a single connector configuration.
    
    Args:
        connector_name: Name of the connector
        connector_config: The connector configuration dictionary
        environment_id: Confluent Cloud environment ID
        cluster_id: Confluent Cloud cluster ID
        api_key: Confluent Cloud API key
        api_secret: Confluent Cloud API secret
        disable_ssl_verify: Whether to disable SSL verification
        
    Returns:
        TranslationResult containing the translated config or error information
    """
    translator = ConfigTranslator(
        environment_id=environment_id,
        cluster_id=cluster_id,
        api_key=api_key,
        api_secret=api_secret,
        disable_ssl_verify=disable_ssl_verify
    )
    return translator.translate_config(connector_name, connector_config)

