"""Tests for src/data/offset_supported.py.

NOTE: the source data includes an intentional concatenated entry (a pre-existing
bug from offset_manager.py where a trailing comma was missing). We test for that
explicitly so any future fixer knows to update the test alongside the data.
"""

import pytest

from data.offset_supported import OFFSET_SUPPORTED_SOURCE_CONNECTOR_TYPES


class TestOffsetSupportedConnectorTypes:
    def test_is_a_list_of_strings(self):
        assert isinstance(OFFSET_SUPPORTED_SOURCE_CONNECTOR_TYPES, list)
        for entry in OFFSET_SUPPORTED_SOURCE_CONNECTOR_TYPES:
            assert isinstance(entry, str)
            assert entry

    def test_has_at_least_thirty_entries(self):
        assert len(OFFSET_SUPPORTED_SOURCE_CONNECTOR_TYPES) >= 30

    @pytest.mark.parametrize(
        "connector_class",
        [
            "io.confluent.connect.s3.source.S3SourceConnector",
            "io.confluent.connect.kinesis.KinesisSourceConnector",
            "com.mongodb.kafka.connect.MongoSourceConnector",
            "io.debezium.connector.mysql.MySqlConnector",
            "io.debezium.connector.v2.mysql.MySqlConnectorV2",
            "io.debezium.connector.postgresql.PostgresConnector",
            "io.debezium.connector.v2.postgresql.PostgresConnectorV2",
            "io.confluent.connect.snowflake.jdbc.SnowflakeSourceConnector",
            "io.confluent.connect.zendesk.ZendeskSourceConnector",
        ],
    )
    def test_well_known_connectors_in_list(self, connector_class):
        assert connector_class in OFFSET_SUPPORTED_SOURCE_CONNECTOR_TYPES

    def test_preserves_legacy_concatenated_sqlserver_jdbc_entry(self):
        """Pins the historical implicit-string-concat bug.

        Original offset_manager.py was missing a trailing comma on the
        SqlServerConnectorV2 line, causing Python to concatenate it with the
        following 'JdbcSourceConnector' entry into a single list element. We
        preserve that exact behavior so the migration is purely structural.
        """
        concatenated = (
            "io.debezium.connector.v2.sqlserver.SqlServerConnectorV2"
            "io.confluent.connect.jdbc.JdbcSourceConnector"
        )
        assert concatenated in OFFSET_SUPPORTED_SOURCE_CONNECTOR_TYPES

    def test_jdbc_source_appears_multiple_times(self):
        # The original list intentionally includes JdbcSourceConnector multiple
        # times (DynamoDB, MySQL, Oracle, PostgreSQL JDBC), so its frequency
        # should be > 1.
        jdbc = "io.confluent.connect.jdbc.JdbcSourceConnector"
        count = OFFSET_SUPPORTED_SOURCE_CONNECTOR_TYPES.count(jdbc)
        assert count >= 2
