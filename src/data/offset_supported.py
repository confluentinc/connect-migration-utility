"""Source connector classes that support offset retrieval.

NOTE: this list preserves a pre-existing quirk from offset_manager.py where the
SqlServer entry was missing a trailing comma, causing Python implicit string
concatenation with the next entry. The concatenated form is retained verbatim
so behavior is unchanged after the refactor.
"""

OFFSET_SUPPORTED_SOURCE_CONNECTOR_TYPES = [
    "io.confluent.connect.jdbc.JdbcSourceConnector",  # Amazon DynamoDB Source connector
    "io.confluent.connect.kinesis.KinesisSourceConnector",  # Amazon Kinesis Source connector
    "io.confluent.connect.s3.source.S3SourceConnector",  # Amazon S3 Source connector
    "io.confluent.connect.azure.blob.storage.AzureBlobStorageSourceConnector",  # Azure Blob Storage Source connector
    "com.azure.cosmos.kafka.connect.source.CosmosDBSourceConnector",  # Azure Cosmos DB Source connector
    "com.couchbase.connect.kafka.CouchbaseSourceConnector",  # Couchbase Source connector
    "io.confluent.connect.github.GithubSourceConnector",  # GitHub Source connector
    "io.confluent.connect.gcs.GcsSourceConnector",  # Google Cloud Storage (GCS) Source connector
    "io.confluent.connect.http.source.GenericHttpSourceConnector",  # HTTP Source V2 connector
    "io.confluent.influxdb.v2.source.InfluxDB2SourceConnector",  # InfluxDB 2 Source connector
    "io.confluent.connect.jira.JiraSourceConnector",  # Jira Source connector
    "io.debezium.connector.v2.mariadb.MariaDbConnector",  # MariaDB CDC Source connector
    "io.debezium.connector.v2.sqlserver.SqlServerConnectorV2",
    "io.confluent.connect.jdbc.JdbcSourceConnector",  # Microsoft SQL Server CDC V2 / JDBC (concatenated entry)
    "com.mongodb.kafka.connect.MongoSourceConnector",  # MongoDB Atlas Source connector
    "io.debezium.connector.mysql.MySqlConnector",  # MySQL CDC Source (Debezium) [Legacy] connector
    "io.debezium.connector.v2.mysql.MySqlConnectorV2",  # MySQL CDC Source V2 (Debezium) connector
    "io.confluent.connect.jdbc.JdbcSourceConnector",  # MySQL Source (JDBC)
    "io.confluent.connect.oracle.cdc.OracleCdcSourceConnector",  # Oracle CDC Source connector
    "io.confluent.connect.oracle.xstream.cdc.OracleXStreamSourceConnector",  # Oracle XStream CDC Source connector
    "io.confluent.connect.jdbc.JdbcSourceConnector",  # Oracle Database Source (JDBC)
    "io.debezium.connector.postgresql.PostgresConnector",  # PostgreSQL CDC Source (Debezium)
    "io.debezium.connector.v2.postgresql.PostgresConnectorV2",  # PostgreSQL CDC Source V2 (Debezium)
    "io.confluent.connect.jdbc.JdbcSourceConnector",  # PostgreSQL Source (JDBC)
    "io.confluent.connect.salesforce.SalesforceBulkApiSourceConnector",  # Salesforce Bulk API 2.0
    "io.confluent.salesforce.SalesforceCdcSourceConnector",  # Salesforce CDC
    "io.confluent.salesforce.SalesforcePlatformEventSourceConnector",  # Salesforce Platform Event
    "io.confluent.salesforce.SalesforcePushTopicSourceConnector",  # Salesforce PushTopic
    "io.confluent.connect.servicenow.ServiceNowSourceConnector",  # ServiceNow [Legacy]
    "io.confluent.connect.http.source.GenericHttpSourceConnector",  # ServiceNow Source V2
    "io.confluent.connect.snowflake.jdbc.SnowflakeSourceConnector",  # Snowflake
    "io.confluent.connect.zendesk.ZendeskSourceConnector",  # Zendesk
]