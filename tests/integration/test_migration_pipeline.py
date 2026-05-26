"""End-to-end integration test for the migration pipeline.

The migrate CLI orchestrates: read FM config files -> assign Kafka auth
-> POST to Confluent Cloud API. We mock the Cloud API and a worker (for
offset retrieval) and assert the success/failure files are written.
"""

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest
import responses

from cli import migrate as cli_migrate
from cli.migrate import ConnectorCreator, KafkaAuth


CLOUD_API = "https://api.confluent.cloud/connect/v1/environments/env-1/clusters/lkc-1/connectors"


@pytest.fixture
def fm_config_dir(tmp_path):
    """Lay out FM connector configs the way the discovery pipeline produces them."""
    d = tmp_path / "fm_configs"
    d.mkdir()
    (d / "alpha.json").write_text(
        json.dumps(
            {
                "name": "alpha",
                "config": {"connector.class": "BigQueryStorageSink", "topics": "events"},
            }
        )
    )
    (d / "beta.json").write_text(
        json.dumps(
            {
                "name": "beta",
                "config": {"connector.class": "HttpSinkV2", "topics": "events"},
            }
        )
    )
    return d


class TestMigrationPipelineCreateMode:
    @pytest.mark.integration
    @responses.activate
    def test_create_two_connectors_emits_success_files(self, tmp_path, fm_config_dir):
        responses.add(responses.POST, CLOUD_API, json={"name": "alpha"}, status=200)
        responses.add(responses.POST, CLOUD_API, json={"name": "beta"}, status=200)

        out = tmp_path / "migration_output"
        argv = [
            "migrate_connector_script.py",
            "--fm-config-dir", str(fm_config_dir),
            "--migration-output-dir", str(out),
            "--bearer-token", "tok",
            "--kafka-api-key", "k",
            "--kafka-api-secret", "s",
            "--environment-id", "env-1",
            "--cluster-id", "lkc-1",
            "--migration-mode", "create",
        ]
        with patch.object(sys, "argv", argv):
            cli_migrate.main()

        successes = json.loads((out / "successful_migration.json").read_text())
        failures = json.loads((out / "unsuccessful_migration.json").read_text())
        assert len(successes) == 2
        assert {s["name"] for s in successes} == {"alpha", "beta"}
        assert failures == []

    @pytest.mark.integration
    @responses.activate
    def test_failed_api_call_lands_in_failures(self, tmp_path, fm_config_dir):
        responses.add(responses.POST, CLOUD_API, status=500, body="boom")
        # All connectors hit the same URL — one stub used for both calls.

        out = tmp_path / "migration_output"
        argv = [
            "migrate_connector_script.py",
            "--fm-config-dir", str(fm_config_dir),
            "--migration-output-dir", str(out),
            "--bearer-token", "tok",
            "--kafka-api-key", "k",
            "--kafka-api-secret", "s",
            "--environment-id", "env-1",
            "--cluster-id", "lkc-1",
            "--migration-mode", "create",
        ]
        with patch.object(sys, "argv", argv):
            cli_migrate.main()

        failures = json.loads((out / "unsuccessful_migration.json").read_text())
        assert failures, "Expected at least one failure when API returns 500"


class TestKafkaAuthIntegration:
    @pytest.mark.integration
    @responses.activate
    def test_service_account_mode_threads_through_to_request_body(self, tmp_path):
        captured = {}

        def capture(request):
            captured["body"] = json.loads(request.body)
            return (200, {}, json.dumps({"name": "ok"}))

        responses.add_callback(responses.POST, CLOUD_API, callback=capture, content_type="application/json")

        creator = ConnectorCreator("prod")
        kafka_auth = KafkaAuth(service_account_id="sa-7", auth_mode="SERVICE_ACCOUNT")
        creator.create_connector_from_config(
            environment_id="env-1",
            kafka_cluster_id="lkc-1",
            kafka_auth=kafka_auth,
            fm_config={"name": "ok", "config": {"connector.class": "HttpSinkV2"}},
            bearer_token="tok",
        )
        body = captured["body"]
        assert body["config"]["kafka.auth.mode"] == "SERVICE_ACCOUNT"
        assert body["config"]["kafka.service.account.id"] == "sa-7"
        assert "kafka.api.key" not in body["config"]
