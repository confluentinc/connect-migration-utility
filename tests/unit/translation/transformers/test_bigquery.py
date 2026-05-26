"""Exhaustive tests for src/transformers/bigquery.py."""

import logging
from unittest.mock import MagicMock

import pytest

from translation.transformers.bigquery import (
    BigQueryV1ToV2Transformer,
    _get_transformer,
    check_unsupported_configs,
    is_bigquery_v1_connector,
    is_bigquery_v2_connector,
    transform_v1_to_v2,
    transform_v1_to_v2_full,
)


# ---------------------------------------------------------------------------
# Detection — class & class-in-config
# ---------------------------------------------------------------------------


class TestIsBigqueryV1:
    @pytest.fixture
    def transformer(self):
        return BigQueryV1ToV2Transformer(logger=MagicMock())

    def test_v1_connector_class_is_detected(self, transformer):
        assert transformer.is_bigquery_v1("com.wepay.kafka.connect.bigquery.BigQuerySinkConnector") is True

    def test_v2_connector_class_is_not_v1(self, transformer):
        assert transformer.is_bigquery_v1("BigQueryStorageSink") is False

    def test_unknown_connector_class_is_not_v1(self, transformer):
        assert transformer.is_bigquery_v1("io.confluent.connect.jdbc.JdbcSinkConnector") is False

    @pytest.mark.parametrize("falsy", ["", None])
    def test_empty_or_none_is_not_v1(self, transformer, falsy):
        assert transformer.is_bigquery_v1(falsy) is False


class TestIsBigqueryV1Config:
    @pytest.fixture
    def transformer(self):
        return BigQueryV1ToV2Transformer(logger=MagicMock())

    def test_config_with_v1_connector_is_v1(self, transformer, sample_bigquery_v1_config):
        assert transformer.is_bigquery_v1_config(sample_bigquery_v1_config) is True

    def test_config_with_v2_class_is_not_v1(self, transformer):
        assert transformer.is_bigquery_v1_config({"connector.class": "BigQueryStorageSink"}) is False

    def test_config_missing_class_is_not_v1(self, transformer):
        assert transformer.is_bigquery_v1_config({}) is False


class TestIsBigqueryV2:
    @pytest.fixture
    def transformer(self):
        return BigQueryV1ToV2Transformer(logger=MagicMock())

    def test_v2_template_id_is_v2(self, transformer):
        assert transformer.is_bigquery_v2("BigQueryStorageSink") is True

    def test_v1_class_is_not_v2(self, transformer):
        assert transformer.is_bigquery_v2("com.wepay.kafka.connect.bigquery.BigQuerySinkConnector") is False

    @pytest.mark.parametrize("falsy", ["", None])
    def test_empty_or_none_is_not_v2(self, transformer, falsy):
        assert transformer.is_bigquery_v2(falsy) is False


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


class TestModuleLevelHelpers:
    def test_is_bigquery_v1_connector_module_helper(self, sample_bigquery_v1_config):
        assert is_bigquery_v1_connector(sample_bigquery_v1_config) is True

    def test_is_bigquery_v2_connector_module_helper(self):
        assert is_bigquery_v2_connector({"connector.class": "BigQueryStorageSink"}) is True
        assert is_bigquery_v2_connector({"connector.class": "X"}) is False

    def test_get_transformer_returns_singleton(self):
        first = _get_transformer()
        second = _get_transformer()
        assert first is second

    def test_check_unsupported_configs_returns_all_present(self):
        v1 = {
            "allow.schema.unionization": "true",
            "all.bq.fields.nullable": "true",
            "convert.double.special.values": "true",
            "allow.bigquery.required.field.relaxation": "true",
            "project": "p",
        }
        found = check_unsupported_configs(v1)
        assert set(found) == {
            "allow.schema.unionization",
            "all.bq.fields.nullable",
            "convert.double.special.values",
            "allow.bigquery.required.field.relaxation",
        }

    def test_check_unsupported_configs_returns_empty_for_clean_v1(self, sample_bigquery_v1_config):
        assert check_unsupported_configs(sample_bigquery_v1_config) == []


# ---------------------------------------------------------------------------
# translate_v1_to_v2 — every step
# ---------------------------------------------------------------------------


class TestTranslateV1ToV2:
    @pytest.fixture
    def transformer(self):
        return BigQueryV1ToV2Transformer(logger=MagicMock())

    def test_connector_class_changed_to_v2_template_id(self, transformer, sample_bigquery_v1_config):
        translated, _, _ = transformer.translate_v1_to_v2(sample_bigquery_v1_config)
        assert translated["connector.class"] == "BigQueryStorageSink"

    def test_missing_required_configs_generate_errors(self, transformer):
        translated, _, errors = transformer.translate_v1_to_v2({"connector.class": "x"})
        assert any("project" in e for e in errors)
        assert any("datasets" in e for e in errors)
        assert any("topics" in e for e in errors)

    def test_all_required_configs_present_yields_no_required_errors(
        self, transformer, sample_bigquery_v1_config
    ):
        _, _, errors = transformer.translate_v1_to_v2(sample_bigquery_v1_config)
        # The only errors should be about *missing required* fields.
        for e in errors:
            assert "Missing required" not in e

    @pytest.mark.parametrize(
        "unsupported_key",
        [
            "allow.schema.unionization",
            "all.bq.fields.nullable",
            "convert.double.special.values",
            "allow.bigquery.required.field.relaxation",
        ],
    )
    def test_unsupported_config_emits_warning(self, transformer, sample_bigquery_v1_config, unsupported_key):
        cfg = dict(sample_bigquery_v1_config)
        cfg[unsupported_key] = "true"
        _, warnings, _ = transformer.translate_v1_to_v2(cfg)
        assert any(unsupported_key in w and "UNSUPPORTED" in w for w in warnings)

    def test_unsupported_config_is_not_copied_into_output(self, transformer, sample_bigquery_v1_config):
        cfg = dict(sample_bigquery_v1_config)
        cfg["allow.schema.unionization"] = "true"
        translated, _, _ = transformer.translate_v1_to_v2(cfg)
        assert "allow.schema.unionization" not in translated

    @pytest.mark.parametrize(
        "breaking_change_key",
        ["TIMESTAMP", "DATE", "DATETIME_FORMAT", "DATA_TYPES", "INT8_INT16"],
    )
    def test_breaking_change_warnings_always_present(
        self, transformer, sample_bigquery_v1_config, breaking_change_key
    ):
        _, warnings, _ = transformer.translate_v1_to_v2(sample_bigquery_v1_config)
        assert any(breaking_change_key in w for w in warnings)

    # sanitize.field.names.in.array is intentionally excluded — step 11 of
    # translate_v1_to_v2 recomputes it from sanitize.field.names, so its value
    # is not what the V1_TO_V2_MAPPING passthrough would produce.
    @pytest.mark.parametrize(
        "v1_key,v2_key",
        [
            (v1, v2)
            for v1, v2 in BigQueryV1ToV2Transformer.V1_TO_V2_MAPPING.items()
            if v2 != "sanitize.field.names.in.array"
        ],
    )
    def test_every_v1_to_v2_mapping_key_is_translated(self, transformer, v1_key, v2_key):
        cfg = {
            "connector.class": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
            "project": "p",
            "datasets": "d",
            "topics": "t",
            v1_key: "value",
        }
        translated, _, _ = transformer.translate_v1_to_v2(cfg)
        assert translated[v2_key] == "value"

    def test_sanitize_field_names_in_array_is_recomputed_from_sanitize_field_names(self, transformer):
        # Document the override behavior: even if the user passes
        # sanitize.field.names.in.array, step 11 of translate_v1_to_v2 will
        # overwrite it based on the value of sanitize.field.names.
        cfg = {
            "connector.class": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
            "project": "p",
            "datasets": "d",
            "topics": "t",
            "sanitize.field.names": "true",
            "sanitize.field.names.in.array": "false",
        }
        translated, _, _ = transformer.translate_v1_to_v2(cfg)
        assert translated["sanitize.field.names.in.array"] == "true"

    def test_table_name_format_collapses_into_topic2table_map(self, transformer):
        # 'table.name.format' and 'topic2table.map' both map to 'topic2table.map'.
        cfg = {
            "connector.class": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
            "project": "p",
            "datasets": "d",
            "topics": "t",
            "table.name.format": "${topic}_v1",
            "topic2table.map": "events:events_table",
        }
        translated, _, _ = transformer.translate_v1_to_v2(cfg)
        assert "topic2table.map" in translated

    def test_tasks_max_defaults_to_one_when_missing(self, transformer):
        cfg = {
            "connector.class": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
            "project": "p",
            "datasets": "d",
            "topics": "t",
        }
        translated, _, _ = transformer.translate_v1_to_v2(cfg)
        assert translated["tasks.max"] == "1"

    def test_tasks_max_preserved_when_provided(self, transformer):
        cfg = {
            "connector.class": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
            "project": "p",
            "datasets": "d",
            "topics": "t",
            "tasks.max": "7",
        }
        translated, _, _ = transformer.translate_v1_to_v2(cfg)
        assert translated["tasks.max"] == "7"

    @pytest.mark.parametrize(
        "default_key,default_val",
        list(BigQueryV1ToV2Transformer.V2_DEFAULTS.items()),
    )
    def test_v2_defaults_applied_when_missing(self, transformer, default_key, default_val):
        cfg = {
            "connector.class": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
            "project": "p",
            "datasets": "d",
            "topics": "t",
        }
        translated, _, _ = transformer.translate_v1_to_v2(cfg)
        # For some keys the default may be derived (e.g. sanitize.field.names.in.array
        # is recomputed from sanitize.field.names), so just check presence.
        assert default_key in translated

    @pytest.mark.parametrize(
        "user_key",
        list(BigQueryV1ToV2Transformer.USER_CONFIGURABLE_DEFAULTS.keys()),
    )
    def test_user_configurable_default_added_with_warning(self, transformer, user_key):
        cfg = {
            "connector.class": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
            "project": "p",
            "datasets": "d",
            "topics": "t",
        }
        translated, warnings, _ = transformer.translate_v1_to_v2(cfg)
        expected_default = BigQueryV1ToV2Transformer.USER_CONFIGURABLE_DEFAULTS[user_key]["default"]
        assert translated[user_key] == expected_default
        assert any(user_key in w and "USER CONFIG REQUIRED" in w for w in warnings)

    def test_keyfile_sets_authentication_method(self, transformer):
        cfg = {
            "connector.class": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
            "project": "p",
            "datasets": "d",
            "topics": "t",
            "keyfile": "{}",
        }
        translated, _, _ = transformer.translate_v1_to_v2(cfg)
        assert translated["authentication.method"] == "Google cloud service account"
        assert translated["keyfile"] == "{}"

    def test_transforms_are_copied(self, transformer):
        cfg = {
            "connector.class": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
            "project": "p",
            "datasets": "d",
            "topics": "t",
            "transforms": "router",
            "transforms.router.type": "org.apache.kafka.connect.transforms.RegexRouter",
        }
        translated, _, _ = transformer.translate_v1_to_v2(cfg)
        assert translated["transforms"] == "router"
        assert translated["transforms.router.type"] == "org.apache.kafka.connect.transforms.RegexRouter"

    def test_unknown_config_keys_copied_through(self, transformer):
        cfg = {
            "connector.class": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
            "project": "p",
            "datasets": "d",
            "topics": "t",
            "my.custom.setting": "preserve.me",
        }
        translated, _, _ = transformer.translate_v1_to_v2(cfg)
        assert translated["my.custom.setting"] == "preserve.me"

    def test_sanitize_field_names_drives_in_array_value_true(self, transformer):
        cfg = {
            "connector.class": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
            "project": "p",
            "datasets": "d",
            "topics": "t",
            "sanitize.field.names": "true",
        }
        translated, _, _ = transformer.translate_v1_to_v2(cfg)
        assert translated["sanitize.field.names.in.array"] == "true"

    def test_sanitize_field_names_drives_in_array_value_false(self, transformer):
        cfg = {
            "connector.class": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
            "project": "p",
            "datasets": "d",
            "topics": "t",
            "sanitize.field.names": "false",
        }
        translated, _, _ = transformer.translate_v1_to_v2(cfg)
        assert translated["sanitize.field.names.in.array"] == "false"


# ---------------------------------------------------------------------------
# transform_v1_to_v2 + transform_v1_to_v2_full wrappers
# ---------------------------------------------------------------------------


class TestTransformV1ToV2Wrapper:
    def test_returns_v2_config_unchanged_when_already_v2(self):
        already_v2 = {"connector.class": "BigQueryStorageSink", "foo": "bar"}
        out = transform_v1_to_v2(already_v2)
        assert out == already_v2
        # And it should be a copy, not the same reference.
        assert out is not already_v2

    def test_returns_translated_for_v1(self, sample_bigquery_v1_config):
        out = transform_v1_to_v2(sample_bigquery_v1_config)
        assert out["connector.class"] == "BigQueryStorageSink"

    def test_missing_required_raises(self):
        with pytest.raises(Exception, match="BigQuery V1 to V2 transformation failed"):
            transform_v1_to_v2({"connector.class": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector"})


class TestTransformV1ToV2Full:
    def test_returns_unchanged_for_v2_with_empty_warnings_and_errors(self):
        already_v2 = {"connector.class": "BigQueryStorageSink"}
        out, warnings, errors = transform_v1_to_v2_full(already_v2)
        assert out == already_v2
        assert warnings == []
        assert errors == []

    def test_returns_warnings_and_errors_when_translating_v1(self, sample_bigquery_v1_config):
        out, warnings, errors = transform_v1_to_v2_full(sample_bigquery_v1_config)
        assert out["connector.class"] == "BigQueryStorageSink"
        assert warnings  # at minimum breaking-change warnings
        assert errors == []
