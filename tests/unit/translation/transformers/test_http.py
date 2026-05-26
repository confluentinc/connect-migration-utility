"""Exhaustive tests for src/transformers/http.py."""

from unittest.mock import MagicMock

import pytest

from translation.transformers.http import (
    HttpV1ToV2Transformer,
    _get_transformer,
    is_http_v1_connector,
    is_http_v2_connector,
    transform_v1_to_v2,
    transform_v1_to_v2_full,
)


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------


class TestDetection:
    @pytest.fixture
    def transformer(self):
        return HttpV1ToV2Transformer(logger=MagicMock())

    def test_v1_class_is_v1(self, transformer):
        assert transformer.is_http_v1("io.confluent.connect.http.HttpSinkConnector") is True

    @pytest.mark.parametrize("falsy", ["", None])
    def test_falsy_is_not_v1(self, transformer, falsy):
        assert transformer.is_http_v1(falsy) is False

    @pytest.mark.parametrize(
        "v2_class",
        [
            "io.confluent.connect.http.sink.GenericHttpSinkConnector",
            "HttpSinkV2",
        ],
    )
    def test_v2_class_is_v2(self, transformer, v2_class):
        assert transformer.is_http_v2(v2_class) is True

    def test_v1_class_is_not_v2(self, transformer):
        assert transformer.is_http_v2("io.confluent.connect.http.HttpSinkConnector") is False


class TestModuleLevelHelpers:
    def test_is_http_v1_connector(self, sample_http_v1_config):
        assert is_http_v1_connector(sample_http_v1_config) is True

    def test_is_http_v2_connector(self):
        assert is_http_v2_connector({"connector.class": "HttpSinkV2"}) is True
        assert is_http_v2_connector({"connector.class": "X"}) is False

    def test_get_transformer_returns_singleton(self):
        assert _get_transformer() is _get_transformer()


# ---------------------------------------------------------------------------
# URL parsing
# ---------------------------------------------------------------------------


class TestParseHttpApiUrl:
    @pytest.fixture
    def transformer(self):
        return HttpV1ToV2Transformer(logger=MagicMock())

    @pytest.mark.parametrize(
        "url,expected_base,expected_path",
        [
            ("https://api.example.com/v1/events", "https://api.example.com", "/v1/events"),
            ("https://api.example.com/", "https://api.example.com", "/"),
            ("https://api.example.com", "https://api.example.com", "/"),
            ("http://localhost:8080/path/to/x", "http://localhost:8080", "/path/to/x"),
            (
                "https://api.example.com/v1/events?source=kafka",
                "https://api.example.com",
                "/v1/events?source=kafka",
            ),
        ],
    )
    def test_parses_well_formed_urls(self, transformer, url, expected_base, expected_path):
        base, path = transformer.parse_http_api_url(url)
        assert base == expected_base
        assert path == expected_path

    def test_empty_url_returns_empty_tuple(self, transformer):
        assert transformer.parse_http_api_url("") == ("", "")

    def test_url_without_leading_slash_path_is_normalized(self, transformer):
        # urlparse handles malformed inputs gracefully; this asserts our wrapper logic.
        base, path = transformer.parse_http_api_url("https://example.com")
        assert path.startswith("/")


# ---------------------------------------------------------------------------
# transform_value
# ---------------------------------------------------------------------------


class TestTransformValue:
    @pytest.fixture
    def transformer(self):
        return HttpV1ToV2Transformer(logger=MagicMock())

    @pytest.mark.parametrize(
        "key,v1_value,expected_v2",
        [
            ("behavior.on.error", "ignore", "IGNORE"),
            ("behavior.on.error", "fail", "FAIL"),
            ("behavior.on.error", "log", "IGNORE"),
            ("behavior.on.null.values", "ignore", "IGNORE"),
            ("behavior.on.null.values", "delete", "DELETE"),
            ("behavior.on.null.values", "fail", "FAIL"),
            ("request.body.format", "string", "STRING"),
            ("request.body.format", "json", "JSON"),
            ("report.errors.as", "error_string", "Error string"),
            ("report.errors.as", "http_response", "Http response"),
        ],
    )
    def test_known_value_is_transformed(self, transformer, key, v1_value, expected_v2):
        new_value, warning = transformer.transform_value(key, v1_value)
        assert new_value == expected_v2
        if v1_value != expected_v2:
            assert warning is not None

    def test_unknown_key_passes_value_through(self, transformer):
        value, warning = transformer.transform_value("not.a.transformed.key", "anything")
        assert value == "anything"
        assert warning is None

    def test_none_value_passes_through(self, transformer):
        value, warning = transformer.transform_value("behavior.on.error", None)
        assert value is None
        assert warning is None


# ---------------------------------------------------------------------------
# translate_v1_to_v2
# ---------------------------------------------------------------------------


class TestTranslateV1ToV2:
    @pytest.fixture
    def transformer(self):
        return HttpV1ToV2Transformer(logger=MagicMock())

    def test_class_changed_to_template_id(self, transformer, sample_http_v1_config):
        translated, _, _ = transformer.translate_v1_to_v2(sample_http_v1_config)
        assert translated["connector.class"] == "HttpSinkV2"

    def test_api_url_split_into_base_url_and_path(self, transformer, sample_http_v1_config):
        translated, _, _ = transformer.translate_v1_to_v2(sample_http_v1_config)
        assert translated["http.api.base.url"] == "https://api.example.com"
        assert translated["api1.http.api.path"] == "/v1/events?source=kafka"

    def test_missing_http_api_url_produces_error(self, transformer):
        cfg = {"connector.class": "io.confluent.connect.http.HttpSinkConnector"}
        _, _, errors = transformer.translate_v1_to_v2(cfg)
        assert any("http.api.url" in e for e in errors)

    def test_apis_num_always_set_to_one(self, transformer, sample_http_v1_config):
        translated, _, _ = transformer.translate_v1_to_v2(sample_http_v1_config)
        assert translated["apis.num"] == "1"

    @pytest.mark.parametrize(
        "v1_key,v2_key",
        list(HttpV1ToV2Transformer.V1_TO_V2_MAPPING.items()),
    )
    def test_every_v1_to_v2_mapping_is_applied(self, transformer, v1_key, v2_key):
        cfg = {
            "connector.class": "io.confluent.connect.http.HttpSinkConnector",
            "http.api.url": "https://api.example.com/",
            v1_key: "value",
        }
        translated, _, _ = transformer.translate_v1_to_v2(cfg)
        assert v2_key in translated

    def test_value_transformation_warning_emitted(self, transformer):
        cfg = {
            "connector.class": "io.confluent.connect.http.HttpSinkConnector",
            "http.api.url": "https://api.example.com/",
            "behavior.on.error": "log",  # v1 'log' -> v2 'IGNORE' with warning
        }
        _, warnings, _ = transformer.translate_v1_to_v2(cfg)
        assert any("transformed" in w for w in warnings)

    @pytest.mark.parametrize("common", HttpV1ToV2Transformer.COMMON_CONFIGS)
    def test_common_configs_copied(self, transformer, common):
        cfg = {
            "connector.class": "io.confluent.connect.http.HttpSinkConnector",
            "http.api.url": "https://api.example.com/",
            common: "value",
        }
        translated, _, _ = transformer.translate_v1_to_v2(cfg)
        assert translated[common] == "value"

    def test_tasks_max_defaults_to_one_with_warning(self, transformer):
        cfg = {
            "connector.class": "io.confluent.connect.http.HttpSinkConnector",
            "http.api.url": "https://api.example.com/",
        }
        translated, warnings, _ = transformer.translate_v1_to_v2(cfg)
        assert translated["tasks.max"] == "1"
        assert any("tasks.max" in w for w in warnings)

    def test_tasks_max_preserved(self, transformer):
        cfg = {
            "connector.class": "io.confluent.connect.http.HttpSinkConnector",
            "http.api.url": "https://api.example.com/",
            "tasks.max": "5",
        }
        translated, _, _ = transformer.translate_v1_to_v2(cfg)
        assert translated["tasks.max"] == "5"

    @pytest.mark.parametrize("deprecated_key", HttpV1ToV2Transformer.V2_UNSUPPORTED_CONFIGS)
    def test_deprecated_keys_dropped_and_warned(self, transformer, deprecated_key):
        cfg = {
            "connector.class": "io.confluent.connect.http.HttpSinkConnector",
            "http.api.url": "https://api.example.com/",
            deprecated_key: "x",
        }
        translated, warnings, _ = transformer.translate_v1_to_v2(cfg)
        assert deprecated_key not in translated
        assert any(deprecated_key in w and "DEPRECATED" in w for w in warnings)

    def test_transforms_passed_through(self, transformer):
        cfg = {
            "connector.class": "io.confluent.connect.http.HttpSinkConnector",
            "http.api.url": "https://api.example.com/",
            "transforms": "x",
            "transforms.x.type": "org.apache.kafka.connect.transforms.RegexRouter",
        }
        translated, _, _ = transformer.translate_v1_to_v2(cfg)
        assert translated["transforms"] == "x"
        assert "transforms.x.type" in translated

    def test_unknown_key_copied_with_warning(self, transformer):
        cfg = {
            "connector.class": "io.confluent.connect.http.HttpSinkConnector",
            "http.api.url": "https://api.example.com/",
            "my.random.key": "rk",
        }
        translated, warnings, _ = transformer.translate_v1_to_v2(cfg)
        assert translated["my.random.key"] == "rk"
        assert any("Copied unrecognized config" in w for w in warnings)

    def test_behavior_on_error_default_warned_when_missing(self, transformer):
        cfg = {
            "connector.class": "io.confluent.connect.http.HttpSinkConnector",
            "http.api.url": "https://api.example.com/",
        }
        _, warnings, _ = transformer.translate_v1_to_v2(cfg)
        assert any("behavior.on.error" in w and "BEHAVIOR CHANGE" in w for w in warnings)

    def test_request_body_format_default_warned_when_missing(self, transformer):
        cfg = {
            "connector.class": "io.confluent.connect.http.HttpSinkConnector",
            "http.api.url": "https://api.example.com/",
        }
        _, warnings, _ = transformer.translate_v1_to_v2(cfg)
        assert any("request.body.format" in w and "BEHAVIOR CHANGE" in w for w in warnings)


class TestTransformV1ToV2Wrapper:
    def test_returns_unchanged_when_already_v2(self):
        v2 = {"connector.class": "HttpSinkV2"}
        out = transform_v1_to_v2(v2)
        assert out == v2

    def test_returns_translated_for_v1(self, sample_http_v1_config):
        out = transform_v1_to_v2(sample_http_v1_config)
        assert out["connector.class"] == "HttpSinkV2"
        assert out["http.api.base.url"] == "https://api.example.com"

    def test_missing_url_raises(self):
        with pytest.raises(Exception, match="HTTP V1 to V2 transformation failed"):
            transform_v1_to_v2({"connector.class": "io.confluent.connect.http.HttpSinkConnector"})


class TestTransformV1ToV2Full:
    def test_v2_input_returns_empty_warnings_errors(self):
        v2 = {"connector.class": "HttpSinkV2"}
        out, warnings, errors = transform_v1_to_v2_full(v2)
        assert out == v2
        assert warnings == []
        assert errors == []
