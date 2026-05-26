"""Mappings between Debezium v1 and v2 connector class names."""

DEBEZIUM_V1_TO_V2_MAPPING = {
    "io.debezium.connector.mysql.MySqlConnector": "io.debezium.connector.v2.mysql.MySqlConnectorV2",
    "io.debezium.connector.postgresql.PostgresConnector": "io.debezium.connector.v2.postgresql.PostgresConnectorV2",
    "io.debezium.connector.sqlserver.SqlServerConnector": "io.debezium.connector.v2.sqlserver.SqlServerConnectorV2",
    "io.debezium.connector.mariadb.MariaDbConnector": "io.debezium.connector.v2.mariadb.MariaDbConnector",
}

DEBEZIUM_V2_TO_V1_MAPPING = {v: k for k, v in DEBEZIUM_V1_TO_V2_MAPPING.items()}