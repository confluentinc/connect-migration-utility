"""Exhaustive tests for src/reporting/terraform_generator.py."""

import json
import logging
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from reporting.terraform_generator import TerraformGenerator


# ---------------------------------------------------------------------------
# _sanitize_resource_name
# ---------------------------------------------------------------------------


class TestSanitizeResourceName:
    @pytest.fixture
    def gen(self, tmp_path):
        return TerraformGenerator(tmp_path, "env-1", "lkc-1", MagicMock())

    @pytest.mark.parametrize(
        "input_name,expected",
        [
            ("simple", "simple"),
            ("with-dash", "with-dash"),
            ("with_underscore", "with_underscore"),
            ("with space", "with_space"),
            ("with.dots", "with_dots"),
            ("with/slash", "with_slash"),
            ("UPPER_case", "UPPER_case"),
        ],
    )
    def test_sanitizes_known_characters(self, gen, input_name, expected):
        assert gen._sanitize_resource_name(input_name) == expected

    def test_underscores_added_when_name_starts_with_digit(self, gen):
        assert gen._sanitize_resource_name("9life")[0] == "_"

    def test_underscores_kept_when_name_starts_with_underscore(self, gen):
        assert gen._sanitize_resource_name("_already") == "_already"


# ---------------------------------------------------------------------------
# _escape_hcl_string
# ---------------------------------------------------------------------------


class TestEscapeHclString:
    @pytest.fixture
    def gen(self, tmp_path):
        return TerraformGenerator(tmp_path, "env-1", "lkc-1", MagicMock())

    def test_backslashes_escaped(self, gen):
        assert gen._escape_hcl_string("a\\b") == "a\\\\b"

    def test_quotes_escaped(self, gen):
        assert gen._escape_hcl_string('say "hi"') == 'say \\"hi\\"'

    def test_clean_string_unchanged(self, gen):
        assert gen._escape_hcl_string("abc 123") == "abc 123"


# ---------------------------------------------------------------------------
# _format_config_value
# ---------------------------------------------------------------------------


class TestFormatConfigValue:
    @pytest.fixture
    def gen(self, tmp_path):
        return TerraformGenerator(tmp_path, "env-1", "lkc-1", MagicMock())

    @pytest.mark.parametrize(
        "value,expected",
        [
            (True, "true"),
            (False, "false"),
            (1, "1"),
            (3.14, "3.14"),
            ("plain", '"plain"'),
            ("${var.x}", "${var.x}"),
            (["a", "b"], '["a", "b"]'),
        ],
    )
    def test_value_formatting(self, gen, value, expected):
        assert gen._format_config_value(value) == expected

    def test_string_with_quotes_escaped(self, gen):
        assert gen._format_config_value('"hi"') == '"\\"hi\\""'

    def test_object_falls_back_to_string(self, gen):
        out = gen._format_config_value({"a": 1})
        assert out.startswith('"') and out.endswith('"')


# ---------------------------------------------------------------------------
# _is_sensitive_field
# ---------------------------------------------------------------------------


class TestIsSensitiveField:
    @pytest.fixture
    def gen(self, tmp_path):
        return TerraformGenerator(tmp_path, "env-1", "lkc-1", MagicMock())

    @pytest.mark.parametrize(
        "key",
        ["password", "API_TOKEN", "client_secret", "private.key", "credentials.json"],
    )
    def test_sensitive_by_key_pattern(self, gen, key):
        assert gen._is_sensitive_field(key, "anything") is True

    def test_sensitive_when_value_uses_key_vault_reference(self, gen):
        assert gen._is_sensitive_field("any.key", "${keyVault:my-secret}") is True
        assert gen._is_sensitive_field("any.key", "${azurekeyvault:other}") is True

    def test_not_sensitive_for_plain_key_and_value(self, gen):
        assert gen._is_sensitive_field("name", "alpha") is False


# ---------------------------------------------------------------------------
# Block generators
# ---------------------------------------------------------------------------


class TestGenerateConnectorResource:
    @pytest.fixture
    def gen(self, tmp_path):
        return TerraformGenerator(tmp_path, "env-7", "lkc-7", MagicMock())

    def test_basic_resource_block_structure(self, gen):
        block = gen._generate_connector_resource(
            "my-conn", {"connector.class": "X", "tasks.max": 1}
        )
        assert 'resource "confluent_connector" "my-conn"' in block
        assert 'id = "env-7"' in block
        assert 'id = "lkc-7"' in block
        assert 'config_nonsensitive' in block
        assert '"connector.class" = "X"' in block

    def test_warnings_render_as_block_comment(self, gen):
        block = gen._generate_connector_resource(
            "my-conn",
            {"connector.class": "X"},
            warnings=[{"field": "f", "message": "m"}],
        )
        assert "/*" in block
        assert "- [f] m" in block

    def test_no_warnings_means_no_comment_block(self, gen):
        block = gen._generate_connector_resource("my-conn", {"connector.class": "X"})
        assert "/*" not in block.split("resource ")[0]


class TestGenerateProviderBlock:
    def test_provider_block_contains_required_providers(self, tmp_path):
        gen = TerraformGenerator(tmp_path, "env-1", "lkc-1", MagicMock())
        block = gen._generate_provider_block()
        assert "required_providers" in block
        assert "confluentinc/confluent" in block


class TestGenerateVariablesFile:
    def test_variables_file_declares_expected_variables(self, tmp_path):
        gen = TerraformGenerator(tmp_path, "env-1", "lkc-1", MagicMock())
        content = gen._generate_variables_file()
        for name in (
            "confluent_cloud_api_key",
            "confluent_cloud_api_secret",
            "environment_id",
            "kafka_cluster_id",
        ):
            assert f'variable "{name}"' in content


class TestGenerateOutputsFile:
    def test_outputs_lists_each_connector(self, tmp_path):
        gen = TerraformGenerator(tmp_path, "env-1", "lkc-1", MagicMock())
        content = gen._generate_outputs_file(["foo", "bar"])
        assert "connector_ids" in content
        assert "confluent_connector.foo.id" in content
        assert "confluent_connector.bar.id" in content


# ---------------------------------------------------------------------------
# End-to-end file generation
# ---------------------------------------------------------------------------


class TestGenerateFromSuccessfulConfigs:
    @pytest.fixture
    def successful_dir(self, tmp_path):
        d = tmp_path / "successful_configs"
        d.mkdir()
        (d / "a.json").write_text(
            json.dumps({"name": "alpha", "config": {"connector.class": "A"}})
        )
        (d / "b.json").write_text(
            json.dumps({"name": "beta", "config": {"connector.class": "B"}})
        )
        return d

    def test_generates_one_tf_file_per_connector(self, tmp_path, successful_dir):
        gen = TerraformGenerator(tmp_path, "env-1", "lkc-1", MagicMock())
        tf_dir = gen.generate_from_successful_configs(successful_dir)
        files = sorted(p.name for p in tf_dir.iterdir())
        assert "alpha-connector.tf" in files
        assert "beta-connector.tf" in files

    def test_skips_files_missing_name(self, tmp_path, successful_dir):
        bad = successful_dir / "noname.json"
        bad.write_text(json.dumps({"config": {"connector.class": "X"}}))
        gen = TerraformGenerator(tmp_path, "env-1", "lkc-1", MagicMock())
        tf_dir = gen.generate_from_successful_configs(successful_dir)
        files = {p.name for p in tf_dir.iterdir()}
        assert "noname-connector.tf" not in files

    def test_handles_empty_directory_gracefully(self, tmp_path):
        empty = tmp_path / "successful_configs"
        empty.mkdir()
        gen = TerraformGenerator(tmp_path, "env-1", "lkc-1", MagicMock())
        tf_dir = gen.generate_from_successful_configs(empty)
        assert tf_dir.exists()
        assert list(tf_dir.iterdir()) == []

    def test_handles_unparseable_json(self, tmp_path, successful_dir):
        (successful_dir / "broken.json").write_text("not-json")
        gen = TerraformGenerator(tmp_path, "env-1", "lkc-1", MagicMock())
        tf_dir = gen.generate_from_successful_configs(successful_dir)
        # Other valid files still get generated.
        assert (tf_dir / "alpha-connector.tf").exists()


class TestGenerateFromFmConfigsDict:
    def test_only_successful_configs_emitted(self, tmp_path):
        fm = {
            "good": {"config": {"connector.class": "X"}, "mapping_errors": []},
            "bad": {"config": {"connector.class": "Y"}, "mapping_errors": ["err"]},
        }
        gen = TerraformGenerator(tmp_path, "env-1", "lkc-1", MagicMock())
        tf_dir = gen.generate_from_fm_configs_dict(fm)
        files = {p.name for p in tf_dir.iterdir()}
        assert "good-connector.tf" in files
        assert "bad-connector.tf" not in files

    def test_empty_dict_handled(self, tmp_path):
        gen = TerraformGenerator(tmp_path, "env-1", "lkc-1", MagicMock())
        tf_dir = gen.generate_from_fm_configs_dict({})
        assert tf_dir.exists()
        assert list(tf_dir.iterdir()) == []
