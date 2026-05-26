"""Exhaustive tests for src/discovery/config_discovery.py."""

import json
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests
import responses

from discovery.config_discovery import ConfigDiscovery


@responses.activate
def _make_discovery(worker_urls="http://w1:8083", **kwargs) -> ConfigDiscovery:
    """Helper: build a ConfigDiscovery while stubbing /connectors as alive."""
    responses.add(
        responses.GET,
        "http://w1:8083/connectors",
        json={},
        status=200,
    )
    responses.add(
        responses.GET,
        "http://w2:8083/connectors",
        json={},
        status=200,
    )
    return ConfigDiscovery(worker_urls=worker_urls, **kwargs)


# ---------------------------------------------------------------------------
# Construction & worker URL parsing
# ---------------------------------------------------------------------------


class TestConstruction:
    def test_no_urls_or_file_raises(self):
        with pytest.raises(ValueError, match="Either worker_urls or worker_urls_file"):
            ConfigDiscovery()

    @responses.activate
    def test_basic_auth_set_when_both_username_and_password_provided(self):
        responses.add(responses.GET, "http://w1:8083/connectors", json={}, status=200)
        cd = ConfigDiscovery(
            worker_urls="http://w1:8083",
            worker_username="user",
            worker_password="pass",
        )
        assert cd.worker_auth is not None

    @responses.activate
    def test_basic_auth_disabled_when_only_username(self):
        responses.add(responses.GET, "http://w1:8083/connectors", json={}, status=200)
        cd = ConfigDiscovery(worker_urls="http://w1:8083", worker_username="user")
        assert cd.worker_auth is None

    @responses.activate
    def test_basic_auth_disabled_when_only_password(self):
        responses.add(responses.GET, "http://w1:8083/connectors", json={}, status=200)
        cd = ConfigDiscovery(worker_urls="http://w1:8083", worker_password="pass")
        assert cd.worker_auth is None

    @responses.activate
    def test_http_prefix_added_when_missing(self):
        responses.add(responses.GET, "http://w1:8083/connectors", json={}, status=200)
        cd = ConfigDiscovery(worker_urls="w1:8083")
        assert cd.worker_urls == ["http://w1:8083"]

    @responses.activate
    def test_trailing_slashes_stripped(self):
        responses.add(responses.GET, "http://w1:8083/connectors", json={}, status=200)
        cd = ConfigDiscovery(worker_urls="http://w1:8083/")
        assert cd.worker_urls == ["http://w1:8083"]

    @responses.activate
    def test_unreachable_workers_filtered_out(self):
        responses.add(responses.GET, "http://up:8083/connectors", json={}, status=200)
        responses.add(responses.GET, "http://down:8083/connectors", status=500)
        cd = ConfigDiscovery(worker_urls="http://up:8083,http://down:8083")
        assert cd.worker_urls == ["http://up:8083"]

    @responses.activate
    def test_all_unreachable_returns_empty_list(self):
        responses.add(responses.GET, "http://down:8083/connectors", status=500)
        cd = ConfigDiscovery(worker_urls="http://down:8083")
        assert cd.worker_urls == []

    @responses.activate
    def test_request_exception_during_alive_check_marks_unreachable(self):
        responses.add(
            responses.GET,
            "http://no:8083/connectors",
            body=requests.exceptions.ConnectionError("nope"),
        )
        cd = ConfigDiscovery(worker_urls="http://no:8083")
        assert cd.worker_urls == []

    @responses.activate
    def test_multiple_urls_comma_separated(self):
        responses.add(responses.GET, "http://w1:8083/connectors", json={}, status=200)
        responses.add(responses.GET, "http://w2:8083/connectors", json={}, status=200)
        cd = ConfigDiscovery(worker_urls="http://w1:8083,http://w2:8083")
        assert set(cd.worker_urls) == {"http://w1:8083", "http://w2:8083"}


# ---------------------------------------------------------------------------
# _extract_worker_urls_from_file
# ---------------------------------------------------------------------------


class TestExtractWorkerUrlsFromFile:
    @responses.activate
    def test_parses_cluster_urls(self, tmp_path):
        responses.add(responses.GET, "http://w1:8083/connectors", json={}, status=200)
        worker_file = tmp_path / "workers.properties"
        worker_file.write_text(
            "confluent.controlcenter.connect.first.cluster=http://w1:8083,http://w2:8083\n"
            "other.line=ignored\n"
        )
        responses.add(responses.GET, "http://w2:8083/connectors", json={}, status=200)
        cd = ConfigDiscovery(worker_urls_file=str(worker_file))
        assert set(cd.worker_urls) == {"http://w1:8083", "http://w2:8083"}

    @responses.activate
    def test_skips_non_http_urls(self, tmp_path):
        responses.add(responses.GET, "http://valid:8083/connectors", json={}, status=200)
        worker_file = tmp_path / "workers.properties"
        worker_file.write_text(
            "confluent.controlcenter.connect.a.cluster=ftp://x,http://valid:8083\n"
        )
        cd = ConfigDiscovery(worker_urls_file=str(worker_file))
        assert cd.worker_urls == ["http://valid:8083"]

    @responses.activate
    def test_missing_file_returns_empty(self, tmp_path):
        cd = ConfigDiscovery(worker_urls_file=str(tmp_path / "nonexistent.properties"))
        assert cd.worker_urls == []


# ---------------------------------------------------------------------------
# _sensitive_config
# ---------------------------------------------------------------------------


class TestSensitiveConfig:
    @pytest.fixture
    def cd(self):
        with patch("discovery.config_discovery.requests.get") as mock_get:
            mock_get.return_value.status_code = 200
            return ConfigDiscovery(worker_urls="http://w1:8083")

    @pytest.mark.parametrize(
        "key,sensitive",
        [
            ("database.password", True),
            ("DATABASE.PASSWORD", True),  # case-insensitive
            ("connection.user", False),  # not sensitive
            ("oauth2.client.secret", True),  # exact static match
            ("my.custom.password.here", True),  # contains "password" substring
            ("my.token.something", True),  # matches "token" pattern
            ("my.secret.x", True),  # matches "secret" pattern
            ("my.credential.thing", True),  # matches "credential" pattern
            ("name", False),
            ("topics", False),
            ("connector.class", False),
        ],
    )
    def test_classifies_keys(self, cd, key, sensitive):
        assert cd._sensitive_config(key) is sensitive

    def test_file_sensitive_keys_loaded(self, tmp_path):
        sf = tmp_path / "sensitive.txt"
        sf.write_text("# comment\nmy.custom.thing\nAnotherKey\n")
        with patch("discovery.config_discovery.requests.get") as mock_get:
            mock_get.return_value.status_code = 200
            cd = ConfigDiscovery(worker_urls="http://w1:8083", sensitive_file=str(sf))
        assert "my.custom.thing" in cd.file_sensitive_configs
        assert "anotherkey" in cd.file_sensitive_configs


# ---------------------------------------------------------------------------
# _redact_sensitive_info
# ---------------------------------------------------------------------------


class TestRedactSensitiveInfo:
    @pytest.fixture
    def cd(self):
        with patch("discovery.config_discovery.requests.get") as mock_get:
            mock_get.return_value.status_code = 200
            return ConfigDiscovery(worker_urls="http://w1:8083")

    def test_redacts_password_at_top_level(self, cd):
        out = cd._redact_sensitive_info({"connection.password": "shh", "name": "ok"})
        assert out["connection.password"] == "********"
        assert out["name"] == "ok"

    def test_recursively_redacts_nested_dict(self, cd):
        out = cd._redact_sensitive_info({"config": {"database.password": "p"}})
        assert out["config"]["database.password"] == "********"

    def test_recursively_redacts_lists_of_dicts(self, cd):
        out = cd._redact_sensitive_info({"items": [{"password": "x"}, {"name": "ok"}]})
        assert out["items"][0]["password"] == "********"
        assert out["items"][1]["name"] == "ok"

    def test_non_sensitive_passes_through_unchanged(self, cd):
        original = {"name": "x", "tasks.max": "5", "config": {"connector.class": "Foo"}}
        out = cd._redact_sensitive_info(original)
        assert out == original


# ---------------------------------------------------------------------------
# get_json_from_url
# ---------------------------------------------------------------------------


class TestGetJsonFromUrl:
    @responses.activate
    def test_returns_json_on_2xx(self):
        responses.add(responses.GET, "http://w/x", json={"foo": "bar"}, status=200)
        out = ConfigDiscovery.get_json_from_url("http://w/x")
        assert out == {"foo": "bar"}

    @responses.activate
    def test_returns_none_on_http_error(self):
        responses.add(responses.GET, "http://w/x", status=404)
        out = ConfigDiscovery.get_json_from_url("http://w/x")
        assert out is None

    @responses.activate
    def test_returns_none_on_invalid_json(self):
        responses.add(responses.GET, "http://w/x", body="not json", status=200)
        out = ConfigDiscovery.get_json_from_url("http://w/x")
        assert out is None

    @responses.activate
    def test_returns_none_on_connection_error(self):
        responses.add(
            responses.GET, "http://w/x", body=requests.exceptions.ConnectionError("no"),
        )
        out = ConfigDiscovery.get_json_from_url("http://w/x")
        assert out is None


# ---------------------------------------------------------------------------
# get_connector_configs_from_worker
# ---------------------------------------------------------------------------


class TestGetConnectorConfigsFromWorker:
    @responses.activate
    def test_happy_path(self, sample_worker_expand_info_response):
        responses.add(
            responses.GET,
            "http://w:8083/connectors?expand=info",
            json=sample_worker_expand_info_response,
            status=200,
            match_querystring=True,
        )
        out = ConfigDiscovery.get_connector_configs_from_worker("http://w:8083")
        assert len(out) == 2
        names = {c["name"] for c in out}
        assert names == {"mysql-source", "http-sink"}
        types = {c["type"] for c in out}
        assert types == {"source", "sink"}
        for c in out:
            assert c["worker"] == "http://w:8083"

    @responses.activate
    def test_empty_response_returns_empty(self):
        responses.add(
            responses.GET, "http://w:8083/connectors?expand=info", json={}, status=200,
            match_querystring=True,
        )
        out = ConfigDiscovery.get_connector_configs_from_worker("http://w:8083")
        # Empty dict is falsy -> early return [].
        assert out == []

    @responses.activate
    def test_http_error_returns_empty(self):
        responses.add(
            responses.GET, "http://w:8083/connectors?expand=info", status=500,
            match_querystring=True,
        )
        out = ConfigDiscovery.get_connector_configs_from_worker("http://w:8083")
        assert out == []


# ---------------------------------------------------------------------------
# get_connector_statuses_from_worker
# ---------------------------------------------------------------------------


class TestGetConnectorStatusesFromWorker:
    @responses.activate
    def test_happy_path(self, sample_worker_expand_status_response):
        responses.add(
            responses.GET,
            "http://w:8083/connectors?expand=status",
            json=sample_worker_expand_status_response,
            status=200,
            match_querystring=True,
        )
        out = ConfigDiscovery.get_connector_statuses_from_worker("http://w:8083")
        assert set(out.keys()) == {"mysql-source", "http-sink"}
        assert out["mysql-source"]["connector_status"]["state"] == "RUNNING"
        assert out["http-sink"]["tasks_status"][0]["state"] == "FAILED"

    @responses.activate
    def test_empty_returns_empty_dict(self):
        responses.add(
            responses.GET, "http://w:8083/connectors?expand=status", json={}, status=200,
            match_querystring=True,
        )
        out = ConfigDiscovery.get_connector_statuses_from_worker("http://w:8083")
        assert out == {}

    @responses.activate
    def test_404_returns_empty(self):
        responses.add(
            responses.GET, "http://w:8083/connectors?expand=status", status=404,
            match_querystring=True,
        )
        out = ConfigDiscovery.get_connector_statuses_from_worker("http://w:8083")
        assert out == {}


# ---------------------------------------------------------------------------
# _load_configs_from_file
# ---------------------------------------------------------------------------


class TestLoadConfigsFromFile:
    @pytest.fixture
    def cd(self):
        with patch("discovery.config_discovery.requests.get") as mock_get:
            mock_get.return_value.status_code = 200
            return ConfigDiscovery(worker_urls="http://w1:8083")

    def test_loads_simple_key_value(self, cd, tmp_path):
        f = tmp_path / "cfg.properties"
        f.write_text("a=1\nb=2\n")
        out = cd._load_configs_from_file(str(f))
        assert out == {"a": "1", "b": "2"}

    def test_skips_comments(self, cd, tmp_path):
        f = tmp_path / "cfg.properties"
        f.write_text("# a comment\na=1\n")
        out = cd._load_configs_from_file(str(f))
        assert out == {"a": "1"}

    def test_filters_by_allowed_prefix(self, cd, tmp_path):
        f = tmp_path / "cfg.properties"
        f.write_text("producer.x=1\nrandom.y=2\nconsumer.z=3\n")
        out = cd._load_configs_from_file(str(f), allowed_prefixes=["producer.", "consumer."])
        assert out == {"producer.x": "1", "consumer.z": "3"}

    def test_missing_file_returns_empty(self, cd, tmp_path):
        out = cd._load_configs_from_file(str(tmp_path / "missing"))
        assert out == {}


# ---------------------------------------------------------------------------
# discover_and_save
# ---------------------------------------------------------------------------


class TestDiscoverAndSave:
    @responses.activate
    def test_writes_output_file_with_connectors(
        self, sample_worker_expand_info_response, tmp_output_dir
    ):
        responses.add(responses.GET, "http://w:8083/connectors", json={}, status=200)
        responses.add(
            responses.GET,
            "http://w:8083/connectors?expand=info",
            json=sample_worker_expand_info_response,
            status=200,
            match_querystring=True,
        )
        cd = ConfigDiscovery(worker_urls="http://w:8083", output_dir=tmp_output_dir)
        out_file = cd.discover_and_save()
        assert out_file.exists()
        with open(out_file) as f:
            data = json.load(f)
        assert set(data["connectors"].keys()) == {"mysql-source", "http-sink"}
        assert data["worker_configs"] == {}

    @responses.activate
    def test_redaction_applied_when_enabled(
        self, sample_worker_expand_info_response, tmp_output_dir
    ):
        responses.add(responses.GET, "http://w:8083/connectors", json={}, status=200)
        responses.add(
            responses.GET,
            "http://w:8083/connectors?expand=info",
            json=sample_worker_expand_info_response,
            status=200,
            match_querystring=True,
        )
        cd = ConfigDiscovery(
            worker_urls="http://w:8083", redact=True, output_dir=tmp_output_dir
        )
        out_file = cd.discover_and_save()
        data = json.loads(out_file.read_text())
        password = data["connectors"]["mysql-source"]["config"]["connection.password"]
        assert password == "********"

    @responses.activate
    def test_worker_config_file_loaded(
        self, sample_worker_expand_info_response, tmp_output_dir, tmp_path
    ):
        responses.add(responses.GET, "http://w:8083/connectors", json={}, status=200)
        responses.add(
            responses.GET,
            "http://w:8083/connectors?expand=info",
            json=sample_worker_expand_info_response,
            status=200,
            match_querystring=True,
        )
        wc = tmp_path / "wc.properties"
        wc.write_text("producer.x=1\noffset.flush.timeout.ms=5000\n")
        cd = ConfigDiscovery(
            worker_urls="http://w:8083",
            output_dir=tmp_output_dir,
            worker_config_file=str(wc),
        )
        out_file = cd.discover_and_save()
        data = json.loads(out_file.read_text())
        assert data["worker_configs"] == {"producer.x": "1", "offset.flush.timeout.ms": "5000"}
