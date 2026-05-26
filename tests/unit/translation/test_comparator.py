"""Tests for src/comparator/comparator.py.

The comparator is a single large class (4,108 lines) — exhaustive coverage of
every private method is deferred until the class is split into mixins (see
Task #3 in the refactor plan). This file covers:

  - construction and parameter validation
  - the static helpers (parse_connector_file, extract_transforms_config)
  - URL parsing (_parse_jdbc_url, _parse_mongodb_connection_string, _get_database_type)
  - simple classification helpers (_is_source_connector, connector_pack_type,
    _is_placeholder, _extract_placeholder_name)
  - public no-network helpers (encode_to_base64, extract_recommended_transform_types)
  - get_SM_template (with HTTP mocked)
"""

import json
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import responses

from translation.sm_to_fm_translator import ConnectorComparator


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def comparator(tmp_path):
    """Minimal in-memory ConnectorComparator (no worker, no templates on disk)."""
    return ConnectorComparator(
        input_file=tmp_path / "input.json",
        output_dir=tmp_path,
    )


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestConstruction:
    def test_minimum_args_succeeds(self, tmp_path):
        cmp = ConnectorComparator(input_file=tmp_path / "input.json", output_dir=tmp_path)
        assert cmp.input_file == tmp_path / "input.json"
        assert cmp.debezium_version == "v2"

    @pytest.mark.parametrize("debezium_version", ["v1", "v2"])
    def test_valid_debezium_versions_accepted(self, tmp_path, debezium_version):
        cmp = ConnectorComparator(
            input_file=tmp_path / "in", output_dir=tmp_path, debezium_version=debezium_version
        )
        assert cmp.debezium_version == debezium_version

    def test_invalid_debezium_version_defaults_to_v2(self, tmp_path):
        cmp = ConnectorComparator(
            input_file=tmp_path / "in", output_dir=tmp_path, debezium_version="v3"
        )
        assert cmp.debezium_version == "v2"

    def test_debezium_version_uppercased_normalized(self, tmp_path):
        cmp = ConnectorComparator(
            input_file=tmp_path / "in", output_dir=tmp_path, debezium_version="V1"
        )
        assert cmp.debezium_version == "v1"

    def test_basic_auth_set_with_both_creds(self, tmp_path):
        cmp = ConnectorComparator(
            input_file=tmp_path / "in",
            output_dir=tmp_path,
            worker_username="u",
            worker_password="p",
        )
        assert cmp.worker_auth is not None

    def test_basic_auth_skipped_with_partial_creds(self, tmp_path):
        cmp = ConnectorComparator(
            input_file=tmp_path / "in",
            output_dir=tmp_path,
            worker_username="u",  # no password
        )
        assert cmp.worker_auth is None

    def test_data_module_constants_are_used(self, comparator):
        # Spot check that the imported tables were assigned.
        assert "mysql" in comparator.jdbc_database_types
        assert "io.confluent.connect.jms.IbmMqSinkConnector" in comparator.premium_pack_connector_dict
        assert comparator.converter_to_format_mappings["io.confluent.connect.avro.AvroConverter"] == "AVRO"


# ---------------------------------------------------------------------------
# encode_to_base64
# ---------------------------------------------------------------------------


class TestEncodeToBase64:
    def test_round_trip(self, comparator):
        import base64
        out = comparator.encode_to_base64("hello")
        assert base64.b64decode(out).decode("utf-8") == "hello"

    def test_empty_string(self, comparator):
        assert comparator.encode_to_base64("") == ""


# ---------------------------------------------------------------------------
# extract_transforms_config (static)
# ---------------------------------------------------------------------------


class TestExtractTransformsConfig:
    def test_extracts_only_transforms_keys(self):
        cfg = {
            "transforms": "x",
            "transforms.x.type": "y",
            "name": "n",
            "tasks.max": 1,
        }
        out = ConnectorComparator.extract_transforms_config(cfg)
        assert out == {"transforms": "x", "transforms.x.type": "y"}

    def test_empty_dict_returns_empty(self):
        assert ConnectorComparator.extract_transforms_config({}) == {}

    def test_no_transforms_returns_empty(self):
        assert ConnectorComparator.extract_transforms_config({"name": "x"}) == {}

    def test_non_string_keys_skipped(self):
        # The implementation guards against non-string keys.
        assert ConnectorComparator.extract_transforms_config({1: "x", "transforms": "y"}) == {
            "transforms": "y"
        }


# ---------------------------------------------------------------------------
# extract_recommended_transform_types
# ---------------------------------------------------------------------------


class TestExtractRecommendedTransformTypes:
    def test_returns_recommendations_from_target_config(self, comparator):
        response = {
            "configs": [
                {"value": {"name": "other.config", "recommended_values": ["x"]}},
                {"value": {"name": "transforms.transform_0.type", "recommended_values": ["a", "b"]}},
            ]
        }
        assert comparator.extract_recommended_transform_types(response) == ["a", "b"]

    def test_returns_empty_when_target_missing(self, comparator):
        response = {"configs": [{"value": {"name": "other"}}]}
        assert comparator.extract_recommended_transform_types(response) == []

    def test_returns_empty_for_empty_configs(self, comparator):
        assert comparator.extract_recommended_transform_types({"configs": []}) == []

    def test_returns_empty_when_no_configs_key(self, comparator):
        assert comparator.extract_recommended_transform_types({}) == []


# ---------------------------------------------------------------------------
# _get_database_type
# ---------------------------------------------------------------------------


class TestGetDatabaseType:
    @pytest.mark.parametrize(
        "url,expected",
        [
            ("jdbc:mysql://host:3306/db", "mysql"),
            ("jdbc:mariadb://host:3306/db", "mysql"),  # mariadb is under mysql
            ("jdbc:postgresql://host:5432/db", "postgresql"),
            ("jdbc:postgres://host:5432/db", "postgresql"),
            ("jdbc:sqlserver://host:1433;databaseName=db", "sqlserver"),
            ("jdbc:mssql://host:1433", "sqlserver"),
            ("jdbc:oracle:thin:@host:1521:sid", "oracle"),
            ("jdbc:snowflake://acct.snowflakecomputing.com", "snowflake"),
        ],
    )
    def test_recognizes_well_known_urls(self, comparator, url, expected):
        assert comparator._get_database_type({"connection.url": url}) == expected

    def test_url_case_insensitive(self, comparator):
        assert comparator._get_database_type({"connection.url": "JDBC:MYSQL://host:3306/db"}) == "mysql"

    def test_unknown_url_returns_unknown(self, comparator):
        assert comparator._get_database_type({"connection.url": "jdbc:weirddb://host"}) == "unknown"

    def test_database_type_config_overrides_url_absence(self, comparator):
        assert comparator._get_database_type({"database.type": "Oracle"}) == "oracle"

    def test_returns_unknown_when_no_url_and_no_type(self, comparator):
        assert comparator._get_database_type({}) == "unknown"


# ---------------------------------------------------------------------------
# _parse_jdbc_url
# ---------------------------------------------------------------------------


class TestParseJdbcUrl:
    def test_simple_postgres_url(self, comparator):
        info = comparator._parse_jdbc_url("jdbc:postgresql://db.example.com:5432/sales")
        assert info["host"] == "db.example.com"
        assert info["port"] == "5432"
        assert info["db.name"] == "sales"

    def test_mysql_with_query_params(self, comparator):
        info = comparator._parse_jdbc_url(
            "jdbc:mysql://host:3306/mydb?user=admin&password=secret"
        )
        assert info["host"] == "host"
        assert info["port"] == "3306"
        assert info["db.name"] == "mydb"
        assert info["user"] == "admin"
        assert info["password"] == "secret"

    def test_oracle_description_block(self, comparator):
        url = (
            "jdbc:oracle:thin:@(DESCRIPTION="
            "(ADDRESS=(PROTOCOL=TCPS)(HOST=oracle.example.com)(PORT=1521))"
            "(CONNECT_DATA=(SERVICE_NAME=mypdb)))"
        )
        info = comparator._parse_jdbc_url(url)
        assert info["host"] == "oracle.example.com"
        assert info["port"] == "1521"
        # Casing is preserved from the original URL (regex uses original_url with IGNORECASE).
        assert info["db.connection.type"] == "SERVICE_NAME"
        assert info["db.name"] == "mypdb"

    def test_empty_dict_for_unparseable_url(self, comparator):
        info = comparator._parse_jdbc_url("notajdbcurl")
        assert info == {}


# ---------------------------------------------------------------------------
# _parse_mongodb_connection_string
# ---------------------------------------------------------------------------


class TestParseMongodbConnectionString:
    def test_mongodb_atlas_srv_format(self, comparator):
        info = comparator._parse_mongodb_connection_string(
            "mongodb+srv://user:pwd@cluster0.mongodb.net/sales?retryWrites=true"
        )
        assert info["host"] == "cluster0.mongodb.net"
        assert info["user"] == "user"
        assert info["password"] == "pwd"
        assert info["database"] == "sales"

    def test_plain_mongodb_format(self, comparator):
        info = comparator._parse_mongodb_connection_string("mongodb://u:p@h:27017/d")
        assert info["host"] == "h:27017"
        assert info["user"] == "u"
        assert info["password"] == "p"
        assert info["database"] == "d"

    def test_mongodb_without_creds(self, comparator):
        info = comparator._parse_mongodb_connection_string("mongodb://h:27017/d")
        assert info["host"] == "h:27017"
        assert info["database"] == "d"
        assert "user" not in info

    def test_unrecognized_format_returns_empty(self, comparator):
        info = comparator._parse_mongodb_connection_string("not-a-mongo-url")
        assert info == {}


# ---------------------------------------------------------------------------
# _is_source_connector
# ---------------------------------------------------------------------------


class TestIsSourceConnector:
    def test_explicit_source_in_template(self, comparator):
        assert comparator._is_source_connector({"connector_type": "SOURCE"}) is True

    def test_explicit_sink_in_template(self, comparator):
        assert comparator._is_source_connector({"connector_type": "SINK"}) is False

    def test_connector_type_inside_templates_array(self, comparator):
        assert comparator._is_source_connector({"templates": [{"connector_type": "SINK"}]}) is False

    def test_falls_back_to_source_indicators_in_class_name(self, comparator):
        for cls in ("MySqlSource", "OracleCdcSource", "OracleXStream"):
            assert comparator._is_source_connector({"connector.class": cls}) is True

    def test_falls_back_to_sink_indicators_in_class_name(self, comparator):
        assert comparator._is_source_connector({"connector.class": "S3Sink"}) is False

    def test_default_source_when_no_indicator(self, comparator):
        assert comparator._is_source_connector({"connector.class": "WeirdConnector"}) is True

    def test_default_source_for_empty_template(self, comparator):
        assert comparator._is_source_connector({}) is True

    def test_default_source_when_template_is_none(self, comparator):
        assert comparator._is_source_connector(None) is True


# ---------------------------------------------------------------------------
# connector_pack_type
# ---------------------------------------------------------------------------


class TestConnectorPackType:
    def test_premium(self, comparator):
        assert (
            comparator.connector_pack_type("io.confluent.connect.oracle.cdc.OracleCdcSourceConnector")
            == "premium_pack_connectors"
        )

    def test_commercial(self, comparator):
        assert (
            comparator.connector_pack_type("io.confluent.connect.cassandra.CassandraSinkConnector")
            == "commercial_pack_connectors"
        )

    def test_unknown_returns_unknown_pack(self, comparator):
        assert comparator.connector_pack_type("unknown") == "unknown_pack_connectors"

    def test_other_returns_non_commercial(self, comparator):
        assert comparator.connector_pack_type("io.confluent.connect.jdbc.JdbcSourceConnector") == "non_commercial_pack_connectors"


# ---------------------------------------------------------------------------
# parse_connector_file (static)
# ---------------------------------------------------------------------------


class TestParseConnectorFile:
    def test_raises_when_file_missing(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            ConnectorComparator.parse_connector_file(tmp_path / "missing.json", {})

    def test_parses_connectors_dict_structure(self, tmp_path):
        f = tmp_path / "x.json"
        f.write_text(json.dumps({"connectors": {"a": {"name": "a", "config": {"x": 1}}}}))
        out = {}
        ConnectorComparator.parse_connector_file(f, out)
        assert "a" in out

    def test_parses_list_of_connector_dicts(self, tmp_path):
        f = tmp_path / "x.json"
        f.write_text(json.dumps([{"name": "a", "config": {"x": 1}}]))
        out = {}
        ConnectorComparator.parse_connector_file(f, out)
        assert out["a"]["config"]["x"] == 1

    def test_parses_single_connector_object(self, tmp_path):
        f = tmp_path / "x.json"
        f.write_text(json.dumps({"name": "alpha", "config": {"k": "v"}}))
        out = {}
        ConnectorComparator.parse_connector_file(f, out)
        assert out["alpha"]["config"]["k"] == "v"

    def test_parses_dict_with_info_wrapper(self, tmp_path):
        # A common Confluent expand=info shape: {"<connector_name>": {"info": {"name":..., "config":...}, ...}}
        f = tmp_path / "x.json"
        f.write_text(
            json.dumps(
                {"alpha": {"info": {"name": "alpha", "config": {"k": "v"}}, "status": {}}}
            )
        )
        out = {}
        ConnectorComparator.parse_connector_file(f, out)
        assert out["alpha"]["config"]["k"] == "v"

    def test_swallows_unparseable_json(self, tmp_path):
        f = tmp_path / "bad.json"
        f.write_text("not json")
        out = {}
        # Should not raise; errors are logged.
        ConnectorComparator.parse_connector_file(f, out, logging.getLogger("test"))
        assert out == {}

    def test_skips_non_json_files(self, tmp_path):
        f = tmp_path / "thing.txt"
        f.write_text("ignored")
        out = {}
        ConnectorComparator.parse_connector_file(f, out)
        assert out == {}


# ---------------------------------------------------------------------------
# get_SM_template
# ---------------------------------------------------------------------------


class TestGetSMTemplate:
    def test_returns_empty_when_no_worker_url(self, comparator):
        assert comparator.get_SM_template("X") == {}

    @responses.activate
    def test_fetches_template_from_worker(self, comparator):
        responses.add(
            responses.PUT,
            "http://w:8083/connector-plugins/X/config/validate",
            json={"configs": [{"value": {"name": "foo"}}]},
            status=200,
        )
        out = comparator.get_SM_template("X", worker_url="http://w:8083")
        assert out["configs"][0]["value"]["name"] == "foo"

    @responses.activate
    def test_returns_empty_on_http_error(self, comparator):
        responses.add(
            responses.PUT, "http://w:8083/connector-plugins/X/config/validate", status=500
        )
        assert comparator.get_SM_template("X", worker_url="http://w:8083") == {}

    @responses.activate
    def test_adds_http_prefix_when_missing(self, comparator):
        responses.add(
            responses.PUT,
            "http://w:8083/connector-plugins/X/config/validate",
            json={"configs": []},
            status=200,
        )
        out = comparator.get_SM_template("X", worker_url="w:8083")
        assert "configs" in out


# ---------------------------------------------------------------------------
# Smoke test: instantiating with a real templates dir
# ---------------------------------------------------------------------------


class TestSmokeInstantiation:
    def test_with_existing_templates_directory(self, tmp_path):
        # Should not blow up even if the templates dir has unexpected JSON files
        # (the loader tolerates missing files).
        cmp = ConnectorComparator(input_file=tmp_path / "in", output_dir=tmp_path)
        assert isinstance(cmp.fm_templates, dict)
        assert isinstance(cmp.fm_transforms_fallback, dict)
        assert isinstance(cmp.connector_class_to_template, dict)
