"""Tests for src/cli/discovery.py.

The discovery CLI's main() is large and orchestrational — we mock all heavy
dependencies (ConfigDiscovery, ConnectorComparator, TerraformGenerator,
generate_migration_summary) and verify the control flow under various flag
combinations.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from cli import discovery as cli_discovery


# ---------------------------------------------------------------------------
# write_fm_configs_to_file
# ---------------------------------------------------------------------------


class TestWriteFmConfigsToFile:
    def test_successful_and_unsuccessful_are_separated(self, tmp_path, mock_logger):
        fm_configs = {
            "good": {"config": {"x": 1}, "mapping_errors": []},
            "bad": {"config": {"y": 2}, "mapping_errors": ["err"]},
        }
        cli_discovery.write_fm_configs_to_file(fm_configs, tmp_path, mock_logger)

        success = tmp_path / "discovered_configs" / "successful_configs"
        fail = tmp_path / "discovered_configs" / "unsuccessful_configs_with_errors"
        assert (success / "good.json").exists()
        assert (fail / "bad.json").exists()
        assert (success / "fm_configs" / "fm_config_good.json").exists()
        assert (fail / "fm_configs" / "fm_config_bad.json").exists()

    def test_writes_compiled_output(self, tmp_path, mock_logger):
        cli_discovery.write_fm_configs_to_file({"x": {"config": {}}}, tmp_path, mock_logger)
        compiled = tmp_path / "discovered_configs" / "compiled_output_fm_configs.json"
        assert compiled.exists()


# ---------------------------------------------------------------------------
# main() — flag handling
# ---------------------------------------------------------------------------


class TestMainErrorPaths:
    def test_both_config_file_and_config_dir_exits(self, monkeypatch, tmp_path):
        argv = [
            "discovery_script.py",
            "--config-file", "x.json",
            "--config-dir", "y",
            "--output-dir", str(tmp_path),
        ]
        monkeypatch.setattr(sys, "argv", argv)
        with patch("cli.discovery.ConfigDiscovery"), patch("cli.discovery.ConnectorComparator"):
            with pytest.raises(SystemExit) as exc:
                cli_discovery.main()
            assert exc.value.code == 1

    def test_no_input_source_exits(self, monkeypatch, tmp_path):
        argv = ["discovery_script.py", "--output-dir", str(tmp_path)]
        monkeypatch.setattr(sys, "argv", argv)
        with pytest.raises(SystemExit) as exc:
            cli_discovery.main()
        assert exc.value.code == 1

    def test_nonexistent_config_file_raises(self, monkeypatch, tmp_path):
        argv = [
            "discovery_script.py",
            "--config-file", str(tmp_path / "does-not-exist.json"),
            "--output-dir", str(tmp_path),
        ]
        monkeypatch.setattr(sys, "argv", argv)
        # FileNotFoundError gets caught by the outer try/except and re-raised as SystemExit(1)
        with pytest.raises(SystemExit):
            cli_discovery.main()


class TestMainHappyPath:
    def test_runs_with_config_file(self, monkeypatch, tmp_path):
        # Build a minimal config file the parser will accept.
        cfg = tmp_path / "input.json"
        cfg.write_text('{"connectors": {}}')

        argv = [
            "discovery_script.py",
            "--config-file", str(cfg),
            "--output-dir", str(tmp_path),
        ]
        monkeypatch.setattr(sys, "argv", argv)
        with patch("cli.discovery.ConnectorComparator") as MockComp, patch(
            "cli.discovery.generate_migration_summary"
        ) as mock_sum:
            comparator_instance = MagicMock()
            comparator_instance.process_connectors.return_value = {
                "x": {"config": {"a": 1}, "mapping_errors": []}
            }
            # Avoid TCO call by setting worker_urls to empty.
            comparator_instance.worker_urls = []
            MockComp.return_value = comparator_instance
            cli_discovery.main()
            MockComp.assert_called_once()
            comparator_instance.process_connectors.assert_called_once()
            mock_sum.assert_called_once()

    def test_terraform_flag_invokes_generator(self, monkeypatch, tmp_path):
        cfg = tmp_path / "input.json"
        cfg.write_text('{"connectors": {}}')
        argv = [
            "discovery_script.py",
            "--config-file", str(cfg),
            "--output-dir", str(tmp_path),
            "--terraform",
        ]
        monkeypatch.setattr(sys, "argv", argv)
        with patch("cli.discovery.ConnectorComparator") as MockComp, patch(
            "cli.discovery.generate_migration_summary"
        ), patch("cli.discovery.TerraformGenerator") as MockTf:
            comparator_instance = MagicMock()
            comparator_instance.process_connectors.return_value = {}
            comparator_instance.worker_urls = []
            MockComp.return_value = comparator_instance
            tf_instance = MagicMock()
            MockTf.return_value = tf_instance
            cli_discovery.main()
            tf_instance.generate_from_successful_configs.assert_called_once()

    def test_no_terraform_flag_skips_generator(self, monkeypatch, tmp_path):
        cfg = tmp_path / "input.json"
        cfg.write_text('{"connectors": {}}')
        argv = [
            "discovery_script.py",
            "--config-file", str(cfg),
            "--output-dir", str(tmp_path),
        ]
        monkeypatch.setattr(sys, "argv", argv)
        with patch("cli.discovery.ConnectorComparator") as MockComp, patch(
            "cli.discovery.generate_migration_summary"
        ), patch("cli.discovery.TerraformGenerator") as MockTf:
            comparator_instance = MagicMock()
            comparator_instance.process_connectors.return_value = {}
            comparator_instance.worker_urls = []
            MockComp.return_value = comparator_instance
            cli_discovery.main()
            MockTf.assert_not_called()
