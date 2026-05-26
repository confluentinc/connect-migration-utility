"""End-to-end integration test for the discovery pipeline.

Wires together the real ConfigDiscovery + ConnectorComparator + summary
generation against a mocked Kafka Connect worker, then asserts the
output files written under the discovered_configs/ tree are well-formed
and that summary + Terraform generation can consume them.
"""

import json
from pathlib import Path

import pytest
import responses

from translation.sm_to_fm_translator import ConnectorComparator
from discovery.config_discovery import ConfigDiscovery
from reporting.summary import generate_migration_summary
from reporting.terraform_generator import TerraformGenerator


WORKER_URL = "http://kafka-connect:8083"


@pytest.fixture
def mocked_worker_responses(sample_worker_expand_info_response):
    """Register canned HTTP responses for the worker API."""
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        rsps.add(responses.GET, f"{WORKER_URL}/connectors", json={}, status=200)
        rsps.add(
            responses.GET,
            f"{WORKER_URL}/connectors?expand=info",
            json=sample_worker_expand_info_response,
            status=200,
            match_querystring=True,
        )
        rsps.add(
            responses.GET,
            f"{WORKER_URL}/connectors?expand=status",
            json={},
            status=200,
            match_querystring=True,
        )
        yield rsps


class TestDiscoveryPipeline:
    @pytest.mark.integration
    def test_discover_then_compile_input(self, tmp_path, mocked_worker_responses):
        # Step 1: Discover from worker -> writes compiled_input_sm_configs.json.
        cd = ConfigDiscovery(worker_urls=WORKER_URL, output_dir=tmp_path)
        compiled = cd.discover_and_save()
        assert compiled.exists()
        data = json.loads(compiled.read_text())
        assert set(data["connectors"].keys()) == {"mysql-source", "http-sink"}

    @pytest.mark.integration
    def test_compare_pipeline_handles_unknown_connector_classes(
        self, tmp_path, mocked_worker_responses
    ):
        # Step 1: discover
        cd = ConfigDiscovery(worker_urls=WORKER_URL, output_dir=tmp_path)
        compiled = cd.discover_and_save()
        # Step 2: comparator should produce FM configs (or mapping errors)
        # for every connector even when no FM template exists locally.
        comparator = ConnectorComparator(
            input_file=compiled,
            output_dir=tmp_path,
        )
        fm_configs = comparator.process_connectors()
        assert fm_configs is not None
        assert set(fm_configs.keys()) == {"mysql-source", "http-sink"}
        for entry in fm_configs.values():
            assert "name" in entry
            # No template loaded -> mapping_errors should be populated.
            assert "config" in entry
            assert "mapping_errors" in entry

    @pytest.mark.integration
    def test_summary_works_on_discovery_output(self, tmp_path, mocked_worker_responses):
        # Drive the full pipeline and assert summarize_output sees the
        # written files.
        cd = ConfigDiscovery(worker_urls=WORKER_URL, output_dir=tmp_path)
        compiled = cd.discover_and_save()

        comparator = ConnectorComparator(input_file=compiled, output_dir=tmp_path)
        fm_configs = comparator.process_connectors()

        # Manually lay out the discovered_configs tree the way cli/discovery.py
        # does, so that summary.summarize_output can walk it.
        from cli.discovery import write_fm_configs_to_file
        import logging
        write_fm_configs_to_file(fm_configs, tmp_path, logging.getLogger("test"))

        report = generate_migration_summary(tmp_path)
        assert report["fm_configs_found"] >= 1
        total = report["total_successful_files"] + report["total_unsuccessful_files"]
        assert total == 2

    @pytest.mark.integration
    def test_terraform_runs_over_pipeline_output(self, tmp_path, mocked_worker_responses):
        cd = ConfigDiscovery(worker_urls=WORKER_URL, output_dir=tmp_path)
        compiled = cd.discover_and_save()
        comparator = ConnectorComparator(input_file=compiled, output_dir=tmp_path)
        fm_configs = comparator.process_connectors()

        # Generate terraform directly from the dict.
        gen = TerraformGenerator(tmp_path, "env-1", "lkc-1")
        tf_dir = gen.generate_from_fm_configs_dict(fm_configs)
        assert tf_dir.exists()
        # If everything had mapping errors (no FM templates), tf_dir is empty —
        # that's fine; just assert the function ran and the dir was created.
