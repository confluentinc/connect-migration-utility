"""Exhaustive tests for src/discovery/offset_manager.py."""

import logging
from unittest.mock import MagicMock, patch

import pytest

from data.offset_supported import OFFSET_SUPPORTED_SOURCE_CONNECTOR_TYPES
from discovery.offset_manager import OffsetManager


# ---------------------------------------------------------------------------
# Singleton
# ---------------------------------------------------------------------------


class TestSingleton:
    def test_first_instance_via_constructor_succeeds(self):
        OffsetManager._instance = None
        om = OffsetManager(logging.getLogger("t"))
        assert om is OffsetManager._instance

    def test_second_instance_via_constructor_raises(self):
        OffsetManager._instance = None
        OffsetManager(logging.getLogger("t"))
        with pytest.raises(Exception, match="singleton"):
            OffsetManager(logging.getLogger("t"))

    def test_get_instance_creates_and_caches(self):
        OffsetManager._instance = None
        a = OffsetManager.get_instance()
        b = OffsetManager.get_instance()
        assert a is b

    def test_get_instance_uses_provided_logger(self):
        OffsetManager._instance = None
        my_logger = logging.getLogger("mine")
        om = OffsetManager.get_instance(my_logger)
        assert om.logger is my_logger


# ---------------------------------------------------------------------------
# is_offset_supported_connector
# ---------------------------------------------------------------------------


class TestIsOffsetSupported:
    @pytest.fixture
    def om(self):
        OffsetManager._instance = None
        return OffsetManager.get_instance()

    @pytest.mark.parametrize("connector_type", ["sink", "SINK", "Sink"])
    def test_all_sinks_supported_regardless_of_class(self, om, connector_type):
        assert om.is_offset_supported_connector(connector_type, "any.class") is True

    @pytest.mark.parametrize(
        "connector_class",
        [
            "io.confluent.connect.s3.source.S3SourceConnector",
            "io.confluent.connect.kinesis.KinesisSourceConnector",
            "com.mongodb.kafka.connect.MongoSourceConnector",
            "io.confluent.connect.snowflake.jdbc.SnowflakeSourceConnector",
        ],
    )
    def test_source_with_supported_class_returns_true(self, om, connector_class):
        assert om.is_offset_supported_connector("source", connector_class) is True

    def test_source_with_unsupported_class_returns_false(self, om):
        assert om.is_offset_supported_connector("source", "x.y.UnknownSource") is False

    def test_source_with_empty_class_returns_false(self, om):
        assert om.is_offset_supported_connector("source", "") is False

    def test_unknown_type_returns_false(self, om):
        assert om.is_offset_supported_connector("transform", "any") is False

    def test_empty_type_returns_false(self, om):
        assert om.is_offset_supported_connector("", "any") is False

    def test_none_type_returns_false(self, om):
        assert om.is_offset_supported_connector(None, "any") is False


# ---------------------------------------------------------------------------
# get_offsets_of_connector
# ---------------------------------------------------------------------------


class TestGetOffsetsOfConnector:
    @pytest.fixture
    def om(self):
        OffsetManager._instance = None
        return OffsetManager.get_instance()

    def test_unsupported_connector_returns_none(self, om):
        cfg = {
            "name": "x",
            "type": "source",
            "config": {"connector.class": "unknown.Class"},
            "worker": "http://w:8083",
        }
        assert om.get_offsets_of_connector(cfg) is None

    def test_successful_offset_fetch(self, om):
        cfg = {
            "name": "snowflake",
            "type": "source",
            "config": {"connector.class": "io.confluent.connect.snowflake.jdbc.SnowflakeSourceConnector"},
            "worker": "http://w:8083",
        }
        with patch.object(
            __import__("discovery.config_discovery", fromlist=["ConfigDiscovery"]).ConfigDiscovery,
            "get_json_from_url",
            return_value={"offsets": [{"partition": {"a": 1}, "offset": {"x": 1}}]},
        ):
            out = om.get_offsets_of_connector(cfg)
        assert out == [{"partition": {"a": 1}, "offset": {"x": 1}}]

    def test_empty_offsets_returns_none(self, om):
        cfg = {
            "name": "snowflake",
            "type": "source",
            "config": {"connector.class": "io.confluent.connect.snowflake.jdbc.SnowflakeSourceConnector"},
            "worker": "http://w:8083",
        }
        with patch(
            "discovery.config_discovery.ConfigDiscovery.get_json_from_url",
            return_value={"offsets": []},
        ):
            assert om.get_offsets_of_connector(cfg) is None

    def test_none_response_returns_none(self, om):
        cfg = {
            "name": "snowflake",
            "type": "source",
            "config": {"connector.class": "io.confluent.connect.snowflake.jdbc.SnowflakeSourceConnector"},
            "worker": "http://w:8083",
        }
        with patch(
            "discovery.config_discovery.ConfigDiscovery.get_json_from_url",
            return_value=None,
        ):
            assert om.get_offsets_of_connector(cfg) is None

    def test_exception_returns_none(self, om):
        cfg = {
            "name": "snowflake",
            "type": "source",
            "config": {"connector.class": "io.confluent.connect.snowflake.jdbc.SnowflakeSourceConnector"},
            "worker": "http://w:8083",
        }
        with patch(
            "discovery.config_discovery.ConfigDiscovery.get_json_from_url",
            side_effect=RuntimeError("boom"),
        ):
            assert om.get_offsets_of_connector(cfg) is None

    def test_sink_always_supported_and_fetched(self, om):
        cfg = {
            "name": "any-sink",
            "type": "sink",
            "config": {"connector.class": "x.y.AnySink"},
            "worker": "http://w:8083",
        }
        with patch(
            "discovery.config_discovery.ConfigDiscovery.get_json_from_url",
            return_value={"offsets": [{"a": 1}]},
        ):
            assert om.get_offsets_of_connector(cfg) == [{"a": 1}]


# ---------------------------------------------------------------------------
# get_connector_configs_offsets
# ---------------------------------------------------------------------------


class TestGetConnectorConfigsOffsets:
    @pytest.fixture
    def om(self):
        OffsetManager._instance = None
        return OffsetManager.get_instance()

    def test_combines_configs_with_offsets(self, om):
        worker_configs = [
            {
                "name": "snowflake",
                "type": "source",
                "config": {"connector.class": "io.confluent.connect.snowflake.jdbc.SnowflakeSourceConnector"},
                "worker": "http://w:8083",
            },
        ]
        with patch(
            "discovery.config_discovery.ConfigDiscovery.get_connector_configs_from_worker",
            return_value=worker_configs,
        ), patch(
            "discovery.config_discovery.ConfigDiscovery.get_json_from_url",
            return_value={"offsets": [{"k": "v"}]},
        ):
            out = om.get_connector_configs_offsets(["http://w:8083"])
        assert len(out) == 1
        assert out[0]["offsets"] == [{"k": "v"}]

    def test_no_offsets_means_no_offsets_key(self, om):
        worker_configs = [
            {
                "name": "x",
                "type": "source",
                "config": {"connector.class": "unsupported.Class"},
                "worker": "http://w:8083",
            },
        ]
        with patch(
            "discovery.config_discovery.ConfigDiscovery.get_connector_configs_from_worker",
            return_value=worker_configs,
        ):
            out = om.get_connector_configs_offsets(["http://w:8083"])
        assert "offsets" not in out[0]
