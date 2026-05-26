"""Exhaustive tests for src/transformers/debezium.py."""

from unittest.mock import MagicMock

import pytest

from translation.transformers.debezium import DebeziumV1ToV2Translator


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------


class TestDetectionMethods:
    @pytest.fixture
    def translator(self):
        return DebeziumV1ToV2Translator(logger=MagicMock())

    @pytest.mark.parametrize(
        "method,positive_class",
        [
            ("is_debezium_sqlserver_v1", "io.debezium.connector.sqlserver.SqlServerConnector"),
            ("is_debezium_mysql_v1", "io.debezium.connector.mysql.MySqlConnector"),
            ("is_debezium_postgresql_v1", "io.debezium.connector.postgresql.PostgresConnector"),
        ],
    )
    def test_positive_detection(self, translator, method, positive_class):
        assert getattr(translator, method)(positive_class) is True

    @pytest.mark.parametrize(
        "method",
        ["is_debezium_sqlserver_v1", "is_debezium_mysql_v1", "is_debezium_postgresql_v1"],
    )
    def test_unknown_class_is_not_detected(self, translator, method):
        assert getattr(translator, method)("io.confluent.connect.jdbc.JdbcSinkConnector") is False

    @pytest.mark.parametrize(
        "method",
        ["is_debezium_sqlserver_v1", "is_debezium_mysql_v1", "is_debezium_postgresql_v1"],
    )
    @pytest.mark.parametrize("falsy", ["", None])
    def test_empty_or_none_is_not_detected(self, translator, method, falsy):
        assert getattr(translator, method)(falsy) is False

    def test_is_debezium_v1_true_for_all_v1_classes(self, translator):
        for cls in (
            "io.debezium.connector.sqlserver.SqlServerConnector",
            "io.debezium.connector.mysql.MySqlConnector",
            "io.debezium.connector.postgresql.PostgresConnector",
        ):
            assert translator.is_debezium_v1(cls) is True

    def test_is_debezium_v1_false_for_non_debezium(self, translator):
        assert translator.is_debezium_v1("io.confluent.connect.jdbc.JdbcSourceConnector") is False


# ---------------------------------------------------------------------------
# translate_sqlserver_v1_to_v2
# ---------------------------------------------------------------------------


class TestTranslateSqlserverV1ToV2:
    @pytest.fixture
    def translator(self):
        return DebeziumV1ToV2Translator(logger=MagicMock())

    def test_connector_class_becomes_v2_template_id(self, translator, sample_debezium_sqlserver_v1_config):
        translated, _ = translator.translate_sqlserver_v1_to_v2(sample_debezium_sqlserver_v1_config)
        assert translated["connector.class"] == "SqlServerCdcSourceV2"

    def test_jdbc_url_constructed_from_hostname_port_dbname(
        self, translator, sample_debezium_sqlserver_v1_config
    ):
        translated, _ = translator.translate_sqlserver_v1_to_v2(sample_debezium_sqlserver_v1_config)
        assert translated["database.url"] == "jdbc:sqlserver://mssql.example.com:1433;databaseName=orders"

    def test_jdbc_url_without_port_uses_only_hostname(self, translator):
        cfg = {
            "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
            "database.hostname": "mssql.example.com",
            "database.port": "",
            "database.dbname": "orders",
            "database.server.name": "srv",
        }
        translated, _ = translator.translate_sqlserver_v1_to_v2(cfg)
        assert translated["database.url"].startswith("jdbc:sqlserver://mssql.example.com")

    def test_jdbc_url_skipped_when_no_hostname(self, translator):
        cfg = {
            "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
            "database.dbname": "orders",
            "database.server.name": "srv",
        }
        translated, _ = translator.translate_sqlserver_v1_to_v2(cfg)
        assert "database.url" not in translated

    def test_database_server_name_renamed_to_topic_prefix(
        self, translator, sample_debezium_sqlserver_v1_config
    ):
        translated, _ = translator.translate_sqlserver_v1_to_v2(sample_debezium_sqlserver_v1_config)
        assert translated["topic.prefix"] == "mssqlserver"

    def test_database_dbname_also_added_as_database_names(
        self, translator, sample_debezium_sqlserver_v1_config
    ):
        translated, _ = translator.translate_sqlserver_v1_to_v2(sample_debezium_sqlserver_v1_config)
        assert translated["database.names"] == "orders"

    def test_application_intent_renamed(self, translator, sample_debezium_sqlserver_v1_config):
        translated, _ = translator.translate_sqlserver_v1_to_v2(sample_debezium_sqlserver_v1_config)
        assert translated["driver.applicationIntent"] == "ReadOnly"

    def test_history_keys_renamed_to_schema_history_internal(self, translator):
        cfg = {
            "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
            "database.hostname": "h",
            "database.history.kafka.topic": "mytopic",
            "database.history.kafka.bootstrap.servers": "kafka:9092",
            "database.server.name": "srv",
            "database.dbname": "x",
        }
        translated, _ = translator.translate_sqlserver_v1_to_v2(cfg)
        assert translated["schema.history.internal.kafka.topic"] == "mytopic"
        assert translated["schema.history.internal.kafka.bootstrap.servers"] == "kafka:9092"

    def test_schema_history_topic_defaults_to_server_name_if_missing(self, translator):
        cfg = {
            "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
            "database.hostname": "h",
            "database.server.name": "srv",
            "database.dbname": "x",
        }
        translated, _ = translator.translate_sqlserver_v1_to_v2(cfg)
        assert translated["schema.history.internal.kafka.topic"] == "srv"

    @pytest.mark.parametrize("value", ["false", "FALSE", "False"])
    def test_after_state_only_false_warns_about_smt(self, translator, value):
        cfg = {
            "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
            "database.server.name": "srv",
            "after.state.only": value,
        }
        _, warnings = translator.translate_sqlserver_v1_to_v2(cfg)
        assert any("ExtractNewRecordState" in w for w in warnings)

    def test_after_state_only_true_warns_about_deprecation_without_smt_advice(self, translator):
        cfg = {
            "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
            "database.server.name": "srv",
            "after.state.only": "true",
        }
        _, warnings = translator.translate_sqlserver_v1_to_v2(cfg)
        deprec = [w for w in warnings if "after.state.only" in w]
        assert deprec
        assert not any("ExtractNewRecordState" in w for w in deprec)

    def test_tombstones_default_change_warned_when_missing(self, translator):
        cfg = {
            "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
            "database.server.name": "srv",
        }
        _, warnings = translator.translate_sqlserver_v1_to_v2(cfg)
        assert any("tombstones.on.delete" in w for w in warnings)

    def test_tombstones_default_not_warned_when_explicitly_set(self, translator):
        cfg = {
            "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
            "database.server.name": "srv",
            "tombstones.on.delete": "false",
        }
        _, warnings = translator.translate_sqlserver_v1_to_v2(cfg)
        assert not any("tombstones.on.delete default changed" in w for w in warnings)

    def test_unrecognized_keys_copied_through(self, translator):
        cfg = {
            "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector",
            "database.server.name": "srv",
            "my.custom.key": "x",
        }
        translated, _ = translator.translate_sqlserver_v1_to_v2(cfg)
        assert translated["my.custom.key"] == "x"


# ---------------------------------------------------------------------------
# translate_mysql_v1_to_v2
# ---------------------------------------------------------------------------


class TestTranslateMysqlV1ToV2:
    @pytest.fixture
    def translator(self):
        return DebeziumV1ToV2Translator(logger=MagicMock())

    def test_class_becomes_v2_template_id(self, translator, sample_debezium_mysql_v1_config):
        translated, _ = translator.translate_mysql_v1_to_v2(sample_debezium_mysql_v1_config)
        assert translated["connector.class"] == "MySqlCdcSourceV2"

    def test_server_name_becomes_topic_prefix(self, translator, sample_debezium_mysql_v1_config):
        translated, _ = translator.translate_mysql_v1_to_v2(sample_debezium_mysql_v1_config)
        assert translated["topic.prefix"] == "mysqlcluster"

    def test_history_kafka_topic_renamed(self, translator, sample_debezium_mysql_v1_config):
        translated, _ = translator.translate_mysql_v1_to_v2(sample_debezium_mysql_v1_config)
        assert translated["schema.history.internal.kafka.topic"] == "schema-history"

    def test_history_bootstrap_servers_renamed(self, translator, sample_debezium_mysql_v1_config):
        translated, _ = translator.translate_mysql_v1_to_v2(sample_debezium_mysql_v1_config)
        assert translated["schema.history.internal.kafka.bootstrap.servers"] == "kafka:9092"

    @pytest.mark.parametrize("deprecated_key", ["binlog.filename.override", "binlog.row.in.binlog.file"])
    def test_deprecated_binlog_props_warned(self, translator, deprecated_key):
        cfg = {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "database.server.name": "srv",
            deprecated_key: "1",
        }
        _, warnings = translator.translate_mysql_v1_to_v2(cfg)
        assert any(deprecated_key in w and "DEPRECATED" in w for w in warnings)

    def test_after_state_only_false_warns_about_smt(self, translator, sample_debezium_mysql_v1_config):
        # The sample has after.state.only=false.
        _, warnings = translator.translate_mysql_v1_to_v2(sample_debezium_mysql_v1_config)
        assert any("ExtractNewRecordState" in w for w in warnings)

    def test_include_schema_changes_default_warned_when_missing(self, translator):
        cfg = {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "database.server.name": "srv",
        }
        _, warnings = translator.translate_mysql_v1_to_v2(cfg)
        assert any("include.schema.changes" in w for w in warnings)

    def test_include_schema_changes_default_not_warned_when_explicit(self, translator):
        cfg = {
            "connector.class": "io.debezium.connector.mysql.MySqlConnector",
            "database.server.name": "srv",
            "include.schema.changes": "false",
        }
        _, warnings = translator.translate_mysql_v1_to_v2(cfg)
        assert not any("include.schema.changes default changed" in w for w in warnings)


# ---------------------------------------------------------------------------
# translate_postgresql_v1_to_v2
# ---------------------------------------------------------------------------


class TestTranslatePostgresqlV1ToV2:
    @pytest.fixture
    def translator(self):
        return DebeziumV1ToV2Translator(logger=MagicMock())

    def test_class_becomes_v2_template_id(self, translator, sample_debezium_postgres_v1_config):
        translated, _, _ = translator.translate_postgresql_v1_to_v2(sample_debezium_postgres_v1_config)
        assert translated["connector.class"] == "PostgresCdcSourceV2"

    def test_server_name_becomes_topic_prefix(self, translator, sample_debezium_postgres_v1_config):
        translated, _, _ = translator.translate_postgresql_v1_to_v2(sample_debezium_postgres_v1_config)
        assert translated["topic.prefix"] == "pgcluster"

    def test_decoderbufs_plugin_rewritten_to_pgoutput(
        self, translator, sample_debezium_postgres_v1_config
    ):
        translated, warnings, _ = translator.translate_postgresql_v1_to_v2(
            sample_debezium_postgres_v1_config
        )
        assert translated["plugin.name"] == "pgoutput"
        assert any("decoderbufs" in w.lower() for w in warnings)

    def test_pgoutput_plugin_passed_through(self, translator):
        cfg = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "srv",
            "plugin.name": "pgoutput",
            "publication.name": "pub",
        }
        translated, _, _ = translator.translate_postgresql_v1_to_v2(cfg)
        assert translated["plugin.name"] == "pgoutput"

    def test_unknown_plugin_emits_warning_but_no_error(self, translator):
        cfg = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "srv",
            "plugin.name": "wal2json",
            "publication.name": "pub",
        }
        translated, warnings, errors = translator.translate_postgresql_v1_to_v2(cfg)
        assert any("Unrecognized plugin.name" in w for w in warnings)

    def test_missing_plugin_defaults_to_pgoutput(self, translator):
        cfg = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "srv",
            "publication.name": "pub",
        }
        translated, warnings, _ = translator.translate_postgresql_v1_to_v2(cfg)
        assert translated["plugin.name"] == "pgoutput"
        assert any("pgoutput" in w and "default" in w for w in warnings)

    def test_missing_publication_name_yields_error(self, translator):
        cfg = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "srv",
            "plugin.name": "pgoutput",
        }
        _, _, errors = translator.translate_postgresql_v1_to_v2(cfg)
        assert any("publication.name" in e and "REQUIRED" in e for e in errors)

    def test_after_state_only_false_warns_about_smt(self, translator):
        cfg = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "srv",
            "plugin.name": "pgoutput",
            "publication.name": "pub",
            "after.state.only": "false",
        }
        _, warnings, _ = translator.translate_postgresql_v1_to_v2(cfg)
        assert any("ExtractNewRecordState" in w for w in warnings)

    def test_schema_history_note_present(self, translator, sample_debezium_postgres_v1_config):
        _, warnings, _ = translator.translate_postgresql_v1_to_v2(sample_debezium_postgres_v1_config)
        assert any("schema history topic" in w.lower() for w in warnings)


# ---------------------------------------------------------------------------
# Dispatcher translate_v1_to_v2
# ---------------------------------------------------------------------------


class TestDispatcher:
    @pytest.fixture
    def translator(self):
        return DebeziumV1ToV2Translator(logger=MagicMock())

    def test_dispatches_to_sqlserver(self, translator, sample_debezium_sqlserver_v1_config):
        translated, _, errors = translator.translate_v1_to_v2(
            sample_debezium_sqlserver_v1_config["connector.class"], sample_debezium_sqlserver_v1_config
        )
        assert translated["connector.class"] == "SqlServerCdcSourceV2"
        assert errors == []

    def test_dispatches_to_mysql(self, translator, sample_debezium_mysql_v1_config):
        translated, _, errors = translator.translate_v1_to_v2(
            sample_debezium_mysql_v1_config["connector.class"], sample_debezium_mysql_v1_config
        )
        assert translated["connector.class"] == "MySqlCdcSourceV2"
        assert errors == []

    def test_dispatches_to_postgres_and_propagates_errors(self, translator):
        cfg = {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.server.name": "srv",
            "plugin.name": "pgoutput",
        }
        translated, _, errors = translator.translate_v1_to_v2(cfg["connector.class"], cfg)
        assert any("publication.name" in e for e in errors)

    def test_unknown_class_returns_config_with_error(self, translator):
        cfg = {"connector.class": "x.y.Z"}
        translated, warnings, errors = translator.translate_v1_to_v2("x.y.Z", cfg)
        assert translated == cfg
        assert any("Unknown connector class" in e for e in errors)
