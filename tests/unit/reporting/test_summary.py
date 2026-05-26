"""Exhaustive tests for src/reporting/summary.py."""

import json
from pathlib import Path

import pytest

from reporting.summary import (
    collect_mapping_errors_with_details,
    count_files,
    extract_config_name,
    extract_transform_name,
    generate_migration_summary,
    generate_tco_information_output,
    get_connector_type_counts,
    summarize_output,
)


# ---------------------------------------------------------------------------
# count_files
# ---------------------------------------------------------------------------


class TestCountFiles:
    def test_zero_when_path_missing(self, tmp_path):
        assert count_files(tmp_path / "nope") == 0

    def test_zero_when_empty_dir(self, tmp_path):
        assert count_files(tmp_path) == 0

    def test_counts_only_json_files(self, tmp_path):
        (tmp_path / "a.json").write_text("{}")
        (tmp_path / "b.json").write_text("{}")
        (tmp_path / "c.txt").write_text("nope")
        assert count_files(tmp_path) == 2

    def test_passes_path_as_str(self, tmp_path):
        (tmp_path / "x.json").write_text("{}")
        assert count_files(str(tmp_path)) == 1


# ---------------------------------------------------------------------------
# get_connector_type_counts
# ---------------------------------------------------------------------------


class TestGetConnectorTypeCounts:
    def test_empty_when_path_missing(self, tmp_path):
        out = get_connector_type_counts(tmp_path / "nope")
        assert dict(out) == {}

    def test_counts_config_section(self, tmp_path):
        (tmp_path / "a.json").write_text(json.dumps({"config": {"connector.class": "A"}}))
        (tmp_path / "b.json").write_text(json.dumps({"config": {"connector.class": "A"}}))
        (tmp_path / "c.json").write_text(json.dumps({"config": {"connector.class": "B"}}))
        out = get_connector_type_counts(tmp_path)
        assert dict(out) == {"A": 2, "B": 1}

    def test_counts_sm_config_list_section(self, tmp_path):
        (tmp_path / "x.json").write_text(json.dumps({"sm_config": [{"connector.class": "X"}]}))
        out = get_connector_type_counts(tmp_path)
        assert dict(out) == {"X": 1}

    def test_skips_files_without_connector_class(self, tmp_path):
        (tmp_path / "x.json").write_text(json.dumps({"config": {"name": "noclass"}}))
        out = get_connector_type_counts(tmp_path)
        assert dict(out) == {}

    def test_skips_non_json_files(self, tmp_path):
        (tmp_path / "x.txt").write_text("ignored")
        out = get_connector_type_counts(tmp_path)
        assert dict(out) == {}

    def test_swallows_unparseable_json(self, tmp_path):
        (tmp_path / "bad.json").write_text("not json")
        out = get_connector_type_counts(tmp_path)
        assert dict(out) == {}


# ---------------------------------------------------------------------------
# extract_config_name
# ---------------------------------------------------------------------------


class TestExtractConfigName:
    def test_from_config_section(self):
        assert extract_config_name({"config": {"name": "alpha"}}) == "alpha"

    def test_from_sm_config_list(self):
        assert extract_config_name({"sm_config": [{"name": "beta"}]}) == "beta"

    def test_default_when_unknown(self):
        assert extract_config_name({}) == "UNKNOWN_CONFIG"

    def test_sm_config_without_name_field(self):
        assert extract_config_name({"sm_config": [{}]}) == "UNKNOWN_CONFIG"


# ---------------------------------------------------------------------------
# extract_transform_name
# ---------------------------------------------------------------------------


class TestExtractTransformName:
    def test_extracts_quoted_transform(self):
        assert extract_transform_name("Transform 'router' failed") == "router"

    def test_unknown_for_unparseable(self):
        assert extract_transform_name("some other error") == "UNKNOWN_TRANSFORM"

    def test_empty_input(self):
        assert extract_transform_name("") == "UNKNOWN_TRANSFORM"


# ---------------------------------------------------------------------------
# collect_mapping_errors_with_details
# ---------------------------------------------------------------------------


class TestCollectMappingErrorsWithDetails:
    def test_empty_when_path_missing(self, tmp_path):
        out = collect_mapping_errors_with_details(tmp_path / "nope")
        assert dict(out) == {}

    def test_collects_top_level_mapping_errors(self, tmp_path):
        (tmp_path / "x.json").write_text(
            json.dumps(
                {
                    "config": {"name": "x"},
                    "mapping_errors": ["Transform 'router' failed", "Other error"],
                }
            )
        )
        out = collect_mapping_errors_with_details(tmp_path)
        assert out["Transform 'router' failed"]["count"] == 1
        assert ("x", "router") in out["Transform 'router' failed"]["occurrences"]

    def test_collects_nested_config_mapping_errors(self, tmp_path):
        (tmp_path / "x.json").write_text(
            json.dumps(
                {
                    "config": {"name": "x", "mapping_errors": ["err 1"]},
                }
            )
        )
        out = collect_mapping_errors_with_details(tmp_path)
        assert out["err 1"]["count"] == 1

    def test_swallows_bad_json(self, tmp_path):
        (tmp_path / "bad.json").write_text("xx")
        out = collect_mapping_errors_with_details(tmp_path)
        assert dict(out) == {}


# ---------------------------------------------------------------------------
# summarize_output + generate_migration_summary (end-to-end)
# ---------------------------------------------------------------------------


def _make_output_tree(base: Path, success_files=("c1.json",), fail_files=()):
    """Create a discovered_configs directory layout under base/."""
    discovered = base / "discovered_configs"
    success = discovered / "successful_configs"
    fail = discovered / "unsuccessful_configs_with_errors"
    success.mkdir(parents=True)
    fail.mkdir(parents=True)
    for name in success_files:
        (success / name).write_text(json.dumps({"config": {"connector.class": "ClassA", "name": name}}))
    for name in fail_files:
        (fail / name).write_text(
            json.dumps(
                {
                    "config": {"name": name},
                    "mapping_errors": ["Transform 'r' failed"],
                }
            )
        )


class TestSummarizeOutput:
    def test_single_cluster_success_only(self, tmp_path):
        _make_output_tree(tmp_path, success_files=("a.json", "b.json"))
        report = summarize_output(tmp_path)
        assert report["fm_configs_found"] == 1
        assert report["total_successful_files"] == 2
        assert report["total_unsuccessful_files"] == 0

    def test_with_failures_and_mapping_errors(self, tmp_path):
        _make_output_tree(tmp_path, success_files=("a.json",), fail_files=("f.json",))
        report = summarize_output(tmp_path)
        assert report["total_successful_files"] == 1
        assert report["total_unsuccessful_files"] == 1
        assert any("'r' failed" in err for err in report["global_mapping_errors"])

    def test_no_discovered_configs_dir_yields_zeros(self, tmp_path):
        report = summarize_output(tmp_path)
        assert report["fm_configs_found"] == 0
        assert report["total_successful_files"] == 0


class TestGenerateMigrationSummary:
    def test_writes_summary_file(self, tmp_path):
        _make_output_tree(tmp_path, success_files=("ok.json",))
        report = generate_migration_summary(tmp_path)
        summary_file = tmp_path / "summary.txt"
        assert summary_file.exists()
        content = summary_file.read_text()
        assert "Overall Summary" in content
        assert "1" in content  # one successful

    def test_returns_summary_dict(self, tmp_path):
        _make_output_tree(tmp_path, success_files=("ok.json",))
        report = generate_migration_summary(tmp_path)
        assert report["total_successful_files"] == 1


class TestGenerateTcoInformationOutput:
    def test_writes_tco_file_with_pack_summary(self, tmp_path):
        tco = {
            "total_connectors": 3,
            "total_tasks": 5,
            "worker_node_count": 2,
            "premium_pack_connectors": {"oracle": {"connector_count": 1}},
            "commercial_pack_connectors": {"salesforce": {"connector_count": 2}},
            "non_commercial_pack_connectors": 0,
            "unknown_pack_connectors": [],
            "worker_node_task_map": {"w1": {"task_count": 3, "task_list": ["t1", "t2"]}},
        }
        generate_tco_information_output(tco, str(tmp_path))
        out = (tmp_path / "tco_information.txt").read_text()
        assert "Premium Packs required" in out
        assert "Commercial Packs required" in out
        assert "w1" in out

    def test_silent_when_no_tco_info(self, tmp_path):
        # Should not write a file when input is empty/None.
        generate_tco_information_output({}, str(tmp_path))
        assert not (tmp_path / "tco_information.txt").exists()
