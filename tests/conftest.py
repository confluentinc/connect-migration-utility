"""Shared pytest fixtures for the connect-migration-utility test suite."""

import json
import logging
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Logger fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def silent_logger() -> logging.Logger:
    """A logger that swallows everything — useful for asserting on calls without noise."""
    logger = logging.getLogger("test_silent")
    logger.handlers = []
    logger.addHandler(logging.NullHandler())
    logger.setLevel(logging.CRITICAL + 1)
    return logger


@pytest.fixture
def mock_logger() -> MagicMock:
    """A MagicMock posing as a logger, so tests can assert on log calls."""
    return MagicMock(spec=logging.Logger)


# ---------------------------------------------------------------------------
# Sample config fixtures (factories so each test gets a fresh copy)
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_jdbc_source_config() -> Dict[str, Any]:
    return {
        "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
        "name": "mysql-source",
        "tasks.max": "1",
        "connection.url": "jdbc:mysql://db.example.com:3306/sales",
        "connection.user": "ksqluser",
        "connection.password": "supersecret",
        "topic.prefix": "mysql-",
        "table.whitelist": "orders,customers",
    }


@pytest.fixture
def sample_jdbc_sink_config() -> Dict[str, Any]:
    return {
        "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
        "name": "postgres-sink",
        "tasks.max": "1",
        "connection.url": "jdbc:postgresql://db.example.com:5432/analytics",
        "connection.user": "writer",
        "connection.password": "topsecret",
        "topics": "events",
    }


@pytest.fixture
def sample_debezium_mysql_v1_config() -> Dict[str, Any]:
    return {
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        "name": "mysql-cdc",
        "tasks.max": "1",
        "database.hostname": "mysql.example.com",
        "database.port": "3306",
        "database.user": "debezium",
        "database.password": "secret",
        "database.server.name": "mysqlcluster",
        "database.history.kafka.topic": "schema-history",
        "database.history.kafka.bootstrap.servers": "kafka:9092",
        "after.state.only": "false",
    }


@pytest.fixture
def sample_debezium_postgres_v1_config() -> Dict[str, Any]:
    return {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "name": "pg-cdc",
        "tasks.max": "1",
        "database.hostname": "pg.example.com",
        "database.port": "5432",
        "database.user": "debezium",
        "database.password": "secret",
        "database.dbname": "events",
        "database.server.name": "pgcluster",
        "plugin.name": "decoderbufs",
        "publication.name": "dbz_publication",
    }


@pytest.fixture
def sample_debezium_sqlserver_v1_config() -> Dict[str, Any]:
    return {
        "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
        "name": "mssql-cdc",
        "tasks.max": "1",
        "database.hostname": "mssql.example.com",
        "database.port": "1433",
        "database.user": "debezium",
        "database.password": "secret",
        "database.dbname": "orders",
        "database.server.name": "mssqlserver",
        "database.history.kafka.topic": "schema-history",
        "database.history.kafka.bootstrap.servers": "kafka:9092",
        "database.applicationIntent": "ReadOnly",
    }


@pytest.fixture
def sample_bigquery_v1_config() -> Dict[str, Any]:
    return {
        "connector.class": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
        "name": "bq-sink",
        "tasks.max": "1",
        "project": "my-project",
        "datasets": "my_dataset",
        "topics": "events",
        "keyfile": "{\"type\":\"service_account\"}",
        "sanitize.field.names": "true",
        "table.name.format": "${topic}_table",
        "bigquery.retry.count": "3",
    }


@pytest.fixture
def sample_http_v1_config() -> Dict[str, Any]:
    return {
        "connector.class": "io.confluent.connect.http.HttpSinkConnector",
        "name": "http-sink",
        "tasks.max": "1",
        "topics": "events",
        "http.api.url": "https://api.example.com/v1/events?source=kafka",
        "auth.type": "BASIC",
        "connection.user": "kafka",
        "connection.password": "secret",
        "request.method": "POST",
        "behavior.on.error": "ignore",
        "behavior.on.null.values": "delete",
        "request.body.format": "json",
        "report.errors.as": "http_response",
    }


# ---------------------------------------------------------------------------
# Sample worker responses
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_worker_expand_info_response() -> Dict[str, Any]:
    return {
        "mysql-source": {
            "info": {
                "type": "source",
                "name": "mysql-source",
                "config": {
                    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                    "connection.url": "jdbc:mysql://db.example.com:3306/sales",
                    "connection.password": "supersecret",
                    "name": "mysql-source",
                },
                "tasks": [{"connector": "mysql-source", "task": 0}],
            },
        },
        "http-sink": {
            "info": {
                "type": "sink",
                "name": "http-sink",
                "config": {
                    "connector.class": "io.confluent.connect.http.HttpSinkConnector",
                    "http.api.url": "https://api.example.com/v1/events",
                    "name": "http-sink",
                },
                "tasks": [{"connector": "http-sink", "task": 0}],
            },
        },
    }


@pytest.fixture
def sample_worker_expand_status_response() -> Dict[str, Any]:
    return {
        "mysql-source": {
            "status": {
                "connector": {"state": "RUNNING", "worker_id": "worker-1"},
                "tasks": [{"id": 0, "state": "RUNNING", "worker_id": "worker-1"}],
            },
        },
        "http-sink": {
            "status": {
                "connector": {"state": "FAILED", "worker_id": "worker-1"},
                "tasks": [{"id": 0, "state": "FAILED", "worker_id": "worker-1", "trace": "boom"}],
            },
        },
    }


# ---------------------------------------------------------------------------
# Filesystem helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def fixtures_dir() -> Path:
    return FIXTURES_DIR


@pytest.fixture
def tmp_output_dir(tmp_path: Path) -> Path:
    """A tmp directory laid out like the real output/ tree."""
    output_dir = tmp_path / "output"
    output_dir.mkdir()
    return output_dir


# ---------------------------------------------------------------------------
# Singleton hygiene
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _reset_offset_manager_singleton():
    """Reset OffsetManager._instance between tests so the singleton starts fresh each time."""
    from discovery.offset_manager import OffsetManager

    OffsetManager._instance = None
    yield
    OffsetManager._instance = None
