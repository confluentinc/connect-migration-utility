"""Tests for src/cli/migrate.py.

We test the KafkaAuth helper, the ConnectorCreator (with HTTP mocked), and
the main() control flow with the network and orchestration mocked out.
"""

import base64
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import responses

from cli import migrate as cli_migrate
from cli.migrate import ConnectorCreator, KafkaAuth


# ---------------------------------------------------------------------------
# KafkaAuth
# ---------------------------------------------------------------------------


class TestKafkaAuth:
    def test_api_key_mode_requires_key_and_secret(self):
        with pytest.raises(ValueError):
            KafkaAuth(api_key=None, api_secret=None, auth_mode="KAFKA_API_KEY").verify_kafka_auth()
        with pytest.raises(ValueError):
            KafkaAuth(api_key="k", api_secret=None, auth_mode="KAFKA_API_KEY").verify_kafka_auth()

    def test_api_key_mode_accepts_full_creds(self):
        KafkaAuth(api_key="k", api_secret="s", auth_mode="KAFKA_API_KEY").verify_kafka_auth()

    def test_service_account_mode_requires_id(self):
        with pytest.raises(ValueError):
            KafkaAuth(service_account_id=None, auth_mode="SERVICE_ACCOUNT").verify_kafka_auth()

    def test_service_account_mode_accepts_id(self):
        KafkaAuth(service_account_id="sa-1", auth_mode="SERVICE_ACCOUNT").verify_kafka_auth()

    def test_assign_api_key_mode_updates_config(self):
        cfg = {}
        KafkaAuth(api_key="k", api_secret="s", auth_mode="KAFKA_API_KEY").assign_kafka_auth_to_config(cfg)
        assert cfg["kafka.api.key"] == "k"
        assert cfg["kafka.api.secret"] == "s"
        assert cfg["kafka.auth.mode"] == "KAFKA_API_KEY"

    def test_assign_service_account_mode_updates_config(self):
        cfg = {}
        KafkaAuth(service_account_id="sa-1", auth_mode="SERVICE_ACCOUNT").assign_kafka_auth_to_config(cfg)
        assert cfg["kafka.service.account.id"] == "sa-1"
        assert cfg["kafka.auth.mode"] == "SERVICE_ACCOUNT"


# ---------------------------------------------------------------------------
# ConnectorCreator
# ---------------------------------------------------------------------------


class TestConnectorCreator:
    def test_unknown_environment_raises(self):
        with pytest.raises(ValueError, match="Unknown environment"):
            ConnectorCreator("staging")

    def test_prod_constructs_url_template(self):
        creator = ConnectorCreator("prod")
        assert "{environment_id}" in creator.url_template
        assert "{kafka_cluster_id}" in creator.url_template

    def test_encode_to_base64(self):
        out = ConnectorCreator.encode_to_base64("abc")
        assert base64.b64decode(out).decode("utf-8") == "abc"

    @responses.activate
    def test_stop_cp_connector_202(self):
        responses.add(responses.PUT, "http://w/connectors/foo/stop", status=202)
        creator = ConnectorCreator("prod")
        # Should not raise.
        creator.stop_cp_connector("http://w", "foo")

    @responses.activate
    def test_stop_cp_connector_400_raises(self):
        responses.add(responses.PUT, "http://w/connectors/foo/stop", status=400, body="nope")
        creator = ConnectorCreator("prod")
        with pytest.raises(RuntimeError):
            creator.stop_cp_connector("http://w", "foo")

    @responses.activate
    def test_create_connector_api_call_success(self):
        responses.add(
            responses.POST,
            "http://api/connect/v1/x",
            json={"id": "abc"},
            status=200,
        )
        creator = ConnectorCreator("prod")
        out = creator.create_connector_api_call(
            "http://api/connect/v1/x", "conn", {"name": "x", "config": {}}, {"Content-Type": "application/json"}
        )
        assert out == {"id": "abc"}

    @responses.activate
    def test_create_connector_api_call_failure_raises(self):
        responses.add(responses.POST, "http://api/x", status=500, body="boom")
        creator = ConnectorCreator("prod")
        with pytest.raises(RuntimeError):
            creator.create_connector_api_call("http://api/x", "conn", {}, {})

    @responses.activate
    def test_create_connector_from_config(self):
        responses.add(
            responses.POST,
            "https://api.confluent.cloud/connect/v1/environments/env-1/clusters/lkc-1/connectors",
            json={"name": "n"},
            status=200,
        )
        creator = ConnectorCreator("prod")
        kafka_auth = KafkaAuth(api_key="k", api_secret="s", auth_mode="KAFKA_API_KEY")
        out = creator.create_connector_from_config(
            environment_id="env-1",
            kafka_cluster_id="lkc-1",
            kafka_auth=kafka_auth,
            fm_config={"name": "n", "config": {"x": "y"}},
            bearer_token="tok",
        )
        assert out == {"name": "n"}

    def test_create_connector_from_config_with_none_config_raises_pre_existing_bug(self):
        """Pre-existing bug pinned here: when config is None the function calls
        ``list(config.keys())`` before the None-check, so an AttributeError is
        raised instead of returning None as the surrounding ``if`` suggests.
        """
        creator = ConnectorCreator("prod")
        kafka_auth = KafkaAuth(api_key="k", api_secret="s", auth_mode="KAFKA_API_KEY")
        with pytest.raises(AttributeError):
            creator.create_connector_from_config(
                environment_id="env-1",
                kafka_cluster_id="lkc-1",
                kafka_auth=kafka_auth,
                fm_config={"name": "n", "config": None},
                bearer_token="tok",
            )


# ---------------------------------------------------------------------------
# main() — argparse smoke
# ---------------------------------------------------------------------------


class TestMain:
    def test_main_create_mode_invokes_creator(self, monkeypatch, tmp_path):
        fm_dir = tmp_path / "fm"
        fm_dir.mkdir()
        (fm_dir / "x.json").write_text(json.dumps({"name": "x", "config": {}}))

        out_dir = tmp_path / "out"

        argv = [
            "migrate_connector_script.py",
            "--fm-config-dir", str(fm_dir),
            "--migration-output-dir", str(out_dir),
            "--bearer-token", "tok",
            "--kafka-api-key", "k",
            "--kafka-api-secret", "s",
            "--environment-id", "env-1",
            "--cluster-id", "lkc-1",
            "--migration-mode", "create",
        ]
        monkeypatch.setattr(sys, "argv", argv)
        with patch("cli.migrate.ConnectorCreator") as MockCreator:
            creator_instance = MagicMock()
            creator_instance.create_connector_from_json_file.return_value = [{"name": "x"}]
            MockCreator.return_value = creator_instance
            cli_migrate.main()
            creator_instance.create_connector_from_json_file.assert_called()

        # And output files should exist.
        assert (out_dir / "successful_migration.json").exists()
        assert (out_dir / "unsuccessful_migration.json").exists()

    def test_main_create_latest_offset_requires_worker_urls(self, monkeypatch, tmp_path):
        fm_dir = tmp_path / "fm"
        fm_dir.mkdir()
        (fm_dir / "x.json").write_text(json.dumps({"name": "x", "config": {}}))

        argv = [
            "migrate_connector_script.py",
            "--fm-config-dir", str(fm_dir),
            "--migration-output-dir", str(tmp_path / "out"),
            "--bearer-token", "tok",
            "--kafka-api-key", "k",
            "--kafka-api-secret", "s",
            "--environment-id", "env-1",
            "--cluster-id", "lkc-1",
            "--migration-mode", "create_latest_offset",
        ]
        monkeypatch.setattr(sys, "argv", argv)
        with pytest.raises(SystemExit):
            cli_migrate.main()
