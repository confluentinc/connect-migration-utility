import logging
from typing import List, Optional, Dict, Any
import requests
from config_discovery import ConfigDiscovery


class OffsetManager:
    _instance = None
    offset_supported_source_connector_types = [
        'io.confluent.connect.jdbc.JdbcSourceConnector', # Amazon DynamoDB Source connector
        'io.confluent.connect.kinesis.KinesisSourceConnector', # Amazon Kinesis Source connector
        'io.confluent.connect.s3.source.S3SourceConnector', # Amazon S3 Source connector
        'io.confluent.connect.azure.blob.storage.AzureBlobStorageSourceConnector', # Azure Blob Storage Source connector
        'com.azure.cosmos.kafka.connect.source.CosmosDBSourceConnector', # Azure Cosmos DB Source connector
        'com.couchbase.connect.kafka.CouchbaseSourceConnector', # Couchbase Source connector.
        'io.confluent.connect.github.GithubSourceConnector', # GitHub Source connector
        'io.confluent.connect.gcs.GcsSourceConnector', # Google Cloud Storage (GCS) Source connector,
        'io.confluent.connect.http.source.GenericHttpSourceConnector', # HTTP Source V2 connector
        'io.confluent.influxdb.v2.source.InfluxDB2SourceConnector', # InfluxDB 2 Source connector
        'io.confluent.connect.jira.JiraSourceConnector', # Jira Source connector
        'io.debezium.connector.v2.mariadb.MariaDbConnector', # MariaDB CDC Source connector
        'io.debezium.connector.v2.sqlserver.SqlServerConnectorV2' # Microsoft SQL Server Change Data Capture (CDC) Source V2 (Debezium) connector
        'io.confluent.connect.jdbc.JdbcSourceConnector', #Microsoft SQL Server Source (JDBC)
        'com.mongodb.kafka.connect.MongoSourceConnector', # MongoDB Atlas Source connector
        'placeholder-mysql-cdc-source-debezium', # MySQL CDC Source (Debezium) [Legacy] connector,
        'io.debezium.connector.v2.mysql.MySqlConnectorV2', # MySQL CDC Source V2 (Debezium) connector
        'io.confluent.connect.jdbc.JdbcSourceConnector', # MySQL Source (JDBC)
        'io.confluent.connect.oracle.cdc.OracleCdcSourceConnector', # Oracle CDC Source connector
        'io.confluent.connect.oracle.xstream.cdc.OracleXStreamSourceConnector', # Oracle XStream CDC Source connector
        'io.confluent.connect.jdbc.JdbcSourceConnector', # Oracle Database Source (JDBC)
        'placeholder-postgresql-cdc-source-debezium', # PostgreSQL Change Data Capture (CDC) Source (Debezium) connector
        'io.debezium.connector.v2.postgresql.PostgresConnectorV2', # PostgreSQL Change Data Capture (CDC) Source V2 (Debezium) connector
        'io.confluent.connect.jdbc.JdbcSourceConnector', # PostgreSQL Source (JDBC)
        'io.confluent.connect.salesforce.SalesforceBulkApiSourceConnector', # Salesforce Bulk API 2.0 Source connector
        'io.confluent.salesforce.SalesforceCdcSourceConnector', # Salesforce Change Data Capture (CDC) Source connector
        'io.confluent.salesforce.SalesforcePlatformEventSourceConnector', # Salesforce Platform Event Source connector
        'io.confluent.salesforce.SalesforcePushTopicSourceConnector', # Salesforce PushTopic Source connector
        'io.confluent.connect.servicenow.ServiceNowSourceConnector', # ServiceNow Source [Legacy] connector
        'io.confluent.connect.http.source.GenericHttpSourceConnector', # ServiceNow Source V2 connector
        'io.confluent.connect.snowflake.jdbc.SnowflakeSourceConnector', # Snowflake Source connector
        'io.confluent.connect.zendesk.ZendeskSourceConnector', # Zendesk Source connector
    ]

    def __init__(self, logger):
        if OffsetManager._instance is not None:
            raise Exception("This class is a singleton! Use OffsetManager.get_instance(logger) instead.")
        self.logger = logger
        OffsetManager._instance = self

    @classmethod
    def get_instance(cls, logger=None):
        if cls._instance is None:
            if logger is None:
                logger = logging.getLogger("offset_manager_default_logger")
            cls._instance = cls(logger)
        return cls._instance


    def is_offset_supported_connector(self, connector_type: str, connector_class: str) -> bool:
        if connector_type and connector_type.lower() == 'sink':
            return True
        elif connector_type and connector_type.lower() == 'source':
            if connector_class and connector_class in OffsetManager.offset_supported_source_connector_types:
                self.logger.info(f"Connector class {connector_class} supports offsets")
                return True
            else:
                self.logger.info(f"Connector class {connector_class} does NOT support offsets")
        return False

    def get_offsets_of_connector(self, config: Dict[str, Any], disable_ssl_verify: bool = False) -> list[Any] | None:
        if not OffsetManager.get_instance(self.logger).is_offset_supported_connector(config['type'], config['config'].get('connector.class', '')):
            self.logger.info(f"Connector {config['name']} does not support offsets")
            return None

        """Get offsets for a connector from the first available worker URL."""
        offsets_url = f"{config['worker']}/connectors/{config['name']}/offsets"
        self.logger.info(f"Fetching offsets for connector '{config['name']}' from: {offsets_url}")
        try:
            offsets = ConfigDiscovery.get_json_from_url(offsets_url, disable_ssl_verify, self.logger)
            if not offsets or not offsets.get('offsets'):
                self.logger.error(f"Invalid response for connector '{config['name']}' offsets call at {offsets_url}")
                return None
            return offsets.get('offsets', [])
        except Exception as e:
            self.logger.error(f"Error getting offsets for connector '{config['name']}' from {offsets_url}: {str(e)}")
            return None

    def get_connector_configs_offsets(self, worker_urls: List[str], disable_ssl_verify: bool = False) -> List[Dict[str, Any]]:
        """Get connector configurations from all workers"""
        self.logger.info(f"Getting connector configs and offsets for workers: {worker_urls}")
        configs_with_offsets = []
        for worker_url in worker_urls:
            self.logger.info(f"Getting connector configs for worker: {worker_url}")
            configs_with_offsets.extend(ConfigDiscovery.get_connector_configs_from_worker(worker_url, disable_ssl_verify, self.logger))

        # Get offsets for each connector
        for config in configs_with_offsets:
            offsets = self.get_offsets_of_connector(config, disable_ssl_verify)
            if offsets:
                config['offsets'] = offsets
                self.logger.info(f"Connector {config['name']} offsets: {offsets}")
            else:
                self.logger.info(f"Connector {config['name']} has no offsets")
        return configs_with_offsets
