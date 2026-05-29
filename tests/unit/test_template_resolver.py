"""
Unit tests for TemplateResolverMixin (src/comparator/template_resolver.py).

These methods live on the mixin but are exercised here through a composed
ConnectorComparator instance built by the `make_comparator` fixture. All tests
are fully offline; any HTTP (`requests.put`) is monkeypatched.
"""

import json

import pytest

import comparator.template_resolver as tr_module
from comparator.template_resolver import TemplateResolverMixin


# --------------------------------------------------------------------------- #
# Fake HTTP response helper
# --------------------------------------------------------------------------- #
class FakeResponse:
    def __init__(self, status_code=200, json_data=None, text="", raise_exc=None):
        self.status_code = status_code
        self._json = json_data if json_data is not None else {}
        self.text = text
        self._raise_exc = raise_exc

    def json(self):
        if isinstance(self._json, Exception):
            raise self._json
        return self._json

    def raise_for_status(self):
        if self._raise_exc is not None:
            raise self._raise_exc


# =========================================================================== #
# _get_plugin_name_for_connector / _get_jdbc_plugin_name
# =========================================================================== #
class TestGetPluginNameForConnector:
    def test_jdbc_source_dispatches_to_jdbc_plugin(self, make_comparator, sample_jdbc_config):
        c = make_comparator()
        name = c._get_plugin_name_for_connector(
            "io.confluent.connect.jdbc.JdbcSourceConnector", sample_jdbc_config
        )
        assert name == "MySqlSource"

    def test_jdbc_sink_dispatches_to_jdbc_plugin(self, make_comparator):
        c = make_comparator()
        cfg = {"connection.url": "jdbc:postgresql://h:5432/db"}
        name = c._get_plugin_name_for_connector(
            "io.confluent.connect.jdbc.JdbcSinkConnector", cfg
        )
        assert name == "PostgresSink"

    def test_non_jdbc_dispatches_to_template_lookup(self, make_comparator, write_template,
                                                    template_factory):
        _t, _wrap = template_factory
        write_template("MyConn", _wrap(_t("MyConnTemplateId", "com.example.MyConnector")))
        c = make_comparator(fm_dir=write_template.dir)
        name = c._get_plugin_name_for_connector("com.example.MyConnector")
        assert name == "MyConnTemplateId"


class TestGetJdbcPluginName:
    def test_no_config_returns_none(self, make_comparator):
        c = make_comparator()
        assert c._get_jdbc_plugin_name("io.confluent.connect.jdbc.JdbcSourceConnector", None) is None

    def test_unknown_db_type_returns_none(self, make_comparator):
        c = make_comparator()
        cfg = {"connection.url": "jdbc:weirddb://host/db"}
        assert c._get_jdbc_plugin_name("io.confluent.connect.jdbc.JdbcSourceConnector", cfg) is None

    @pytest.mark.parametrize("url,is_source,expected", [
        ("jdbc:mysql://h:3306/d", True, "MySqlSource"),
        ("jdbc:mysql://h:3306/d", False, "MySqlSink"),
        ("jdbc:postgresql://h:5432/d", True, "PostgresSource"),
        ("jdbc:oracle:thin:@h:1521/d", True, "OracleDatabaseSource"),
        ("jdbc:sqlserver://h:1433;databaseName=d", True, "MicrosoftSqlServerSource"),
        ("jdbc:snowflake://acct.snowflakecomputing.com/", True, "SnowflakeSource"),
        ("jdbc:snowflake://acct.snowflakecomputing.com/", False, "SnowflakeSink"),
    ])
    def test_db_type_to_plugin_mapping(self, make_comparator, url, is_source, expected):
        c = make_comparator()
        cc = ("io.confluent.connect.jdbc.JdbcSourceConnector" if is_source
              else "io.confluent.connect.jdbc.JdbcSinkConnector")
        assert c._get_jdbc_plugin_name(cc, {"connection.url": url}) == expected


# =========================================================================== #
# _get_plugin_name_from_template
# =========================================================================== #
class TestGetPluginNameFromTemplate:
    def test_missing_dir_returns_none(self, make_comparator, tmp_path):
        c = make_comparator()
        c.fm_template_dir = tmp_path / "does_not_exist"
        assert c._get_plugin_name_from_template("com.example.X") is None

    def test_direct_connector_class_match(self, write_template, make_comparator):
        path = write_template("Direct", {
            "connector.class": "com.example.DirectConnector",
            "template_id": "DirectTemplate",
        })
        assert path.exists()
        c = make_comparator(fm_dir=write_template.dir)
        assert c._get_plugin_name_from_template("com.example.DirectConnector") == "DirectTemplate"

    def test_nested_templates_match(self, write_template, template_factory, make_comparator):
        _t, _wrap = template_factory
        write_template("Nested", _wrap(_t("NestedId", "com.example.NestedConnector")))
        c = make_comparator(fm_dir=write_template.dir)
        assert c._get_plugin_name_from_template("com.example.NestedConnector") == "NestedId"

    def test_no_match_returns_none(self, write_template, template_factory, make_comparator):
        _t, _wrap = template_factory
        write_template("X", _wrap(_t("XId", "com.example.Other")))
        c = make_comparator(fm_dir=write_template.dir)
        assert c._get_plugin_name_from_template("com.example.NotPresent") is None

    def test_match_but_no_template_id_returns_none(self, write_template, make_comparator):
        # direct connector.class match, but no template_id anywhere
        write_template("NoId", {"connector.class": "com.example.NoId"})
        c = make_comparator(fm_dir=write_template.dir)
        assert c._get_plugin_name_from_template("com.example.NoId") is None

    def test_debezium_v2_mapping_used_for_lookup(self, write_template, template_factory, make_comparator):
        _t, _wrap = template_factory
        # Template stores the v2 class; we look up using the v1 class with version v2.
        write_template("DebV2", _wrap(_t("MySqlCdcV2",
                                         "io.debezium.connector.v2.mysql.MySqlConnectorV2")))
        c = make_comparator(fm_dir=write_template.dir, debezium_version="v2")
        result = c._get_plugin_name_from_template("io.debezium.connector.mysql.MySqlConnector")
        assert result == "MySqlCdcV2"

    def test_skips_unreadable_json_file(self, write_template, make_comparator, template_factory):
        _t, _wrap = template_factory
        bad = write_template.dir / "bad.json"
        bad.write_text("{ not valid json ")
        write_template("Good", _wrap(_t("GoodId", "com.example.Good")))
        c = make_comparator(fm_dir=write_template.dir)
        assert c._get_plugin_name_from_template("com.example.Good") == "GoodId"


# =========================================================================== #
# _apply_debezium_v1_to_v2_if_needed
# =========================================================================== #
class TestApplyDebeziumV1ToV2:
    def test_no_translation_when_version_v2(self, make_comparator):
        c = make_comparator(debezium_version="v2")
        fm = {"connector.class": "x"}
        warnings, errors = [], []
        out_fm, out_w, out_e = c._apply_debezium_v1_to_v2_if_needed(
            "io.debezium.connector.mysql.MySqlConnector", fm, warnings, errors
        )
        assert out_fm is fm
        assert out_w == [] and out_e == []

    def test_no_translation_when_not_debezium_v1(self, make_comparator):
        c = make_comparator(debezium_version="v1")
        fm = {"connector.class": "x"}
        out_fm, out_w, out_e = c._apply_debezium_v1_to_v2_if_needed(
            "com.example.NotDebezium", fm, [], []
        )
        assert out_fm is fm
        assert out_w == [] and out_e == []

    def test_translation_applied_for_debezium_v1(self, make_comparator):
        c = make_comparator(debezium_version="v1")
        warnings, errors = [], []

        def fake_translate(connector_class, fm_configs):
            return {"connector.class": "translated"}, ["w1"], ["e1"]

        c.debezium_translator.translate_v1_to_v2 = fake_translate
        out_fm, out_w, out_e = c._apply_debezium_v1_to_v2_if_needed(
            "io.debezium.connector.mysql.MySqlConnector", {"a": 1}, warnings, errors
        )
        assert out_fm == {"connector.class": "translated"}
        assert out_w == ["[v1→v2 Translation] w1"]
        assert out_e == ["[v1→v2 Translation] e1"]


# =========================================================================== #
# _translate_connector_config_via_api
# =========================================================================== #
class TestTranslateConnectorConfigViaApi:
    def _creds_comparator(self, make_comparator, write_template, template_factory, **kw):
        _t, _wrap = template_factory
        write_template("MyConn", _wrap(_t("MyPlugin", "com.example.MyConnector")))
        return make_comparator(fm_dir=write_template.dir, env_id="env-1",
                               lkc_id="lkc-1", bearer_token="key:secret", **kw)

    def test_missing_credentials_returns_none(self, make_comparator):
        c = make_comparator()  # no env/lkc/token
        assert c._translate_connector_config_via_api("n", {"connector.class": "x"}) is None

    def test_missing_connector_class_returns_none(self, make_comparator):
        c = make_comparator(env_id="e", lkc_id="l", bearer_token="t")
        assert c._translate_connector_config_via_api("n", {}) is None

    def test_no_plugin_name_returns_none(self, make_comparator):
        # creds present, but no template matching connector.class -> plugin None
        c = make_comparator(env_id="e", lkc_id="l", bearer_token="t")
        assert c._translate_connector_config_via_api("n", {"connector.class": "com.unknown.X"}) is None

    def test_non_200_returns_none(self, make_comparator, write_template, template_factory, monkeypatch):
        c = self._creds_comparator(make_comparator, write_template, template_factory)
        monkeypatch.setattr(tr_module.requests, "put",
                            lambda *a, **k: FakeResponse(status_code=500, text="boom"))
        assert c._translate_connector_config_via_api(
            "n", {"connector.class": "com.example.MyConnector"}) is None

    def test_success_parses_config_warnings_errors(self, make_comparator, write_template,
                                                   template_factory, monkeypatch):
        c = self._creds_comparator(make_comparator, write_template, template_factory)
        payload = {
            "config": {"k": "v"},
            "warnings": [{"field": "f1", "message": "m1"}],
            "errors": [{"field": "f2", "message": "m2"}],
        }
        captured = {}

        def fake_put(url, json=None, headers=None, verify=None):
            captured["url"] = url
            captured["json"] = json
            captured["verify"] = verify
            return FakeResponse(status_code=200, json_data=payload)

        monkeypatch.setattr(tr_module.requests, "put", fake_put)
        result = c._translate_connector_config_via_api(
            "n", {"connector.class": "com.example.MyConnector"})
        assert result["config"] == {"k": "v"}
        assert result["warnings"] == ["[Translate API] f1: m1"]
        assert result["errors"] == ["[Translate API] f2: m2"]
        # plugin name was placed into the URL
        assert "MyPlugin" in captured["url"]
        assert captured["verify"] is True  # disable_ssl_verify defaults to False

    def test_success_with_no_warnings_or_errors(self, make_comparator, write_template,
                                                template_factory, monkeypatch):
        c = self._creds_comparator(make_comparator, write_template, template_factory)
        monkeypatch.setattr(tr_module.requests, "put",
                            lambda *a, **k: FakeResponse(status_code=200, json_data={"config": {}}))
        result = c._translate_connector_config_via_api(
            "n", {"connector.class": "com.example.MyConnector"})
        assert result == {"config": {}, "warnings": [], "errors": []}

    def test_request_exception_returns_none(self, make_comparator, write_template,
                                            template_factory, monkeypatch):
        c = self._creds_comparator(make_comparator, write_template, template_factory)

        def raiser(*a, **k):
            raise tr_module.requests.exceptions.RequestException("network down")

        monkeypatch.setattr(tr_module.requests, "put", raiser)
        assert c._translate_connector_config_via_api(
            "n", {"connector.class": "com.example.MyConnector"}) is None

    def test_disable_ssl_verify_passes_verify_false(self, make_comparator, write_template,
                                                    template_factory, monkeypatch):
        c = self._creds_comparator(make_comparator, write_template, template_factory,
                                   disable_ssl_verify=True)
        captured = {}

        def fake_put(url, json=None, headers=None, verify=None):
            captured["verify"] = verify
            return FakeResponse(status_code=200, json_data={"config": {}})

        monkeypatch.setattr(tr_module.requests, "put", fake_put)
        c._translate_connector_config_via_api("n", {"connector.class": "com.example.MyConnector"})
        assert captured["verify"] is False


# =========================================================================== #
# encode_to_base64
# =========================================================================== #
class TestEncodeToBase64:
    def test_round_trip(self, make_comparator):
        import base64
        c = make_comparator()
        encoded = c.encode_to_base64("hello:world")
        assert base64.b64decode(encoded).decode("utf-8") == "hello:world"

    def test_known_value(self, make_comparator):
        c = make_comparator()
        assert c.encode_to_base64("user:pass") == "dXNlcjpwYXNz"


# =========================================================================== #
# extract_transforms_config  (note: defined without `self`)
# =========================================================================== #
class TestExtractTransformsConfig:
    def test_extracts_only_transforms_keys(self):
        cfg = {
            "transforms": "a,b",
            "transforms.a.type": "T",
            "predicates": "p1",
            "connector.class": "x",
            42: "ignored-non-str-key",
        }
        out = TemplateResolverMixin.extract_transforms_config(cfg)
        assert out == {"transforms": "a,b", "transforms.a.type": "T"}

    def test_empty_when_no_transforms(self):
        assert TemplateResolverMixin.extract_transforms_config({"a": 1}) == {}


# =========================================================================== #
# extract_recommended_transform_types
# =========================================================================== #
class TestExtractRecommendedTransformTypes:
    def test_returns_recommended_values(self, make_comparator):
        c = make_comparator()
        resp = {"configs": [
            {"value": {"name": "something.else", "recommended_values": ["x"]}},
            {"value": {"name": "transforms.transform_0.type",
                       "recommended_values": ["TypeA", "TypeB"]}},
        ]}
        assert c.extract_recommended_transform_types(resp) == ["TypeA", "TypeB"]

    def test_returns_empty_when_not_found(self, make_comparator):
        c = make_comparator()
        assert c.extract_recommended_transform_types({"configs": []}) == []

    def test_returns_empty_when_no_configs_key(self, make_comparator):
        c = make_comparator()
        assert c.extract_recommended_transform_types({}) == []


# =========================================================================== #
# get_SM_template
# =========================================================================== #
class TestGetSMTemplate:
    def test_no_worker_url_returns_empty(self, make_comparator):
        c = make_comparator()
        assert c.get_SM_template("com.example.X") == {}

    def test_adds_http_protocol_and_succeeds(self, make_comparator, monkeypatch):
        c = make_comparator()
        captured = {}

        def fake_put(url, json=None, headers=None, verify=None, auth=None):
            captured["url"] = url
            return FakeResponse(status_code=200, json_data={"configs": [], "name": "X"})

        monkeypatch.setattr(tr_module.requests, "put", fake_put)
        result = c.get_SM_template("com.example.X", worker_url="localhost:8083")
        assert result == {"configs": [], "name": "X"}
        assert captured["url"].startswith("http://localhost:8083/")

    def test_keeps_existing_protocol(self, make_comparator, monkeypatch):
        c = make_comparator()
        captured = {}

        def fake_put(url, json=None, headers=None, verify=None, auth=None):
            captured["url"] = url
            return FakeResponse(status_code=200, json_data={"k": "v"})

        monkeypatch.setattr(tr_module.requests, "put", fake_put)
        c.get_SM_template("com.example.X", worker_url="https://w:8083")
        assert captured["url"].startswith("https://w:8083/")

    def test_request_exception_returns_empty(self, make_comparator, monkeypatch):
        c = make_comparator()

        def raiser(*a, **k):
            raise tr_module.requests.exceptions.RequestException("nope")

        monkeypatch.setattr(tr_module.requests, "put", raiser)
        assert c.get_SM_template("com.example.X", worker_url="http://w") == {}

    def test_raise_for_status_error_returns_empty(self, make_comparator, monkeypatch):
        c = make_comparator()
        exc = tr_module.requests.exceptions.HTTPError("403")
        monkeypatch.setattr(tr_module.requests, "put",
                            lambda *a, **k: FakeResponse(status_code=403, raise_exc=exc))
        assert c.get_SM_template("com.example.X", worker_url="http://w") == {}

    def test_handles_non_dict_response(self, make_comparator, monkeypatch):
        c = make_comparator()
        monkeypatch.setattr(tr_module.requests, "put",
                            lambda *a, **k: FakeResponse(status_code=200, json_data=["not", "dict"]))
        assert c.get_SM_template("com.example.X", worker_url="http://w") == ["not", "dict"]


# =========================================================================== #
# _load_fm_transforms_fallback
# =========================================================================== #
class TestLoadFmTransformsFallback:
    def test_loads_when_file_present(self, make_comparator, tmp_path, monkeypatch):
        c = make_comparator()
        f = tmp_path / "fm_transforms_list.json"
        f.write_text(json.dumps({"MySqlSource": ["TransformA"]}))
        monkeypatch.chdir(tmp_path)
        assert c._load_fm_transforms_fallback() == {"MySqlSource": ["TransformA"]}

    def test_returns_empty_when_file_missing(self, make_comparator, tmp_path, monkeypatch):
        c = make_comparator()
        monkeypatch.chdir(tmp_path)  # no fm_transforms_list.json here
        assert c._load_fm_transforms_fallback() == {}

    def test_returns_empty_on_bad_json(self, make_comparator, tmp_path, monkeypatch):
        c = make_comparator()
        (tmp_path / "fm_transforms_list.json").write_text("{bad json")
        monkeypatch.chdir(tmp_path)
        assert c._load_fm_transforms_fallback() == {}


# =========================================================================== #
# get_FM_SMT
# =========================================================================== #
class TestGetFmSmt:
    def test_uses_fallback_when_no_credentials(self, make_comparator):
        c = make_comparator()
        c.fm_transforms_fallback = {"MyPlugin": ["TransA", "TransB"]}
        assert c.get_FM_SMT("MyPlugin") == {"TransA", "TransB"}

    def test_no_credentials_no_fallback_returns_empty_set(self, make_comparator):
        c = make_comparator()
        c.fm_transforms_fallback = {}
        assert c.get_FM_SMT("Missing") == set()

    def test_http_success_returns_recommended(self, make_comparator, monkeypatch):
        c = make_comparator(env_id="e", lkc_id="l", bearer_token="t")
        payload = {"configs": [
            {"value": {"name": "transforms.transform_0.type",
                       "recommended_values": ["T1", "T2"]}}
        ]}
        monkeypatch.setattr(tr_module.requests, "put",
                            lambda *a, **k: FakeResponse(status_code=200, json_data=payload))
        assert c.get_FM_SMT("MyPlugin") == ["T1", "T2"]

    def test_http_failure_falls_back_to_file(self, make_comparator, monkeypatch):
        c = make_comparator(env_id="e", lkc_id="l", bearer_token="t")
        c.fm_transforms_fallback = {"MyPlugin": ["FB1"]}

        def raiser(*a, **k):
            raise tr_module.requests.exceptions.RequestException("down")

        monkeypatch.setattr(tr_module.requests, "put", raiser)
        assert c.get_FM_SMT("MyPlugin") == {"FB1"}

    def test_http_empty_recommended_falls_back_to_file(self, make_comparator, monkeypatch):
        c = make_comparator(env_id="e", lkc_id="l", bearer_token="t")
        c.fm_transforms_fallback = {"MyPlugin": ["FB1"]}
        monkeypatch.setattr(tr_module.requests, "put",
                            lambda *a, **k: FakeResponse(status_code=200, json_data={"configs": []}))
        assert c.get_FM_SMT("MyPlugin") == {"FB1"}


# =========================================================================== #
# get_transforms_config (delegates to get_FM_SMT + classify)
# =========================================================================== #
class TestGetTransformsConfig:
    def test_delegates_and_classifies(self, make_comparator):
        c = make_comparator()
        c.fm_transforms_fallback = {"MyPlugin": ["AllowedType"]}
        config = {
            "transforms": "a,b",
            "transforms.a.type": "AllowedType",
            "transforms.a.field": "x",
            "transforms.b.type": "DisallowedType",
        }
        result = c.get_transforms_config(config, "MyPlugin")
        assert result["allowed"]["transforms"] == "a"
        assert result["disallowed"]["transforms"] == "b"
        assert result["allowed"]["transforms.a.field"] == "x"


# =========================================================================== #
# classify_transform_configs_with_full_chain  (pure logic)
# =========================================================================== #
class TestClassifyTransformConfigs:
    def test_allowed_and_disallowed_split(self, make_comparator):
        c = make_comparator()
        config = {
            "transforms": "a, b",
            "transforms.a.type": "Good",
            "transforms.a.field": "f",
            "transforms.b.type": "Bad",
            "transforms.b.field": "g",
        }
        res = c.classify_transform_configs_with_full_chain(config, {"Good"})
        assert res["allowed"]["transforms"] == "a"
        assert res["allowed"]["transforms.a.field"] == "f"
        assert res["disallowed"]["transforms"] == "b"
        assert res["disallowed"]["transforms.b.field"] == "g"
        assert any("Bad" in m for m in res["mapping_errors"])

    def test_missing_type_goes_disallowed_with_error(self, make_comparator):
        c = make_comparator()
        config = {"transforms": "a", "transforms.a.field": "f"}
        res = c.classify_transform_configs_with_full_chain(config, {"Anything"})
        assert res["disallowed"]["transforms"] == "a"
        assert res["disallowed"]["transforms.a.field"] == "f"
        assert any("no type specified" in m for m in res["mapping_errors"])
        assert "allowed" not in res["allowed"].get("transforms", "a")

    def test_predicate_filtered_when_transform_disallowed(self, make_comparator):
        c = make_comparator()
        config = {
            "transforms": "a",
            "transforms.a.type": "Bad",
            "transforms.a.predicate": "p1",
            "predicates": "p1",
            "predicates.p1.type": "PredType",
        }
        res = c.classify_transform_configs_with_full_chain(config, set())
        # The predicate referenced by the disallowed transform is filtered out
        assert res["disallowed"]["predicates"] == "p1"
        assert res["disallowed"]["predicates.p1.type"] == "PredType"
        assert "predicates" not in res["allowed"]

    def test_predicate_allowed_when_not_referenced_by_disallowed(self, make_comparator):
        c = make_comparator()
        config = {
            "transforms": "a",
            "transforms.a.type": "Good",
            "predicates": "p1",
            "predicates.p1.type": "PredType",
        }
        res = c.classify_transform_configs_with_full_chain(config, {"Good"})
        assert res["allowed"]["predicates"] == "p1"
        assert res["allowed"]["predicates.p1.type"] == "PredType"

    def test_empty_chain_returns_empty_structure(self, make_comparator):
        c = make_comparator()
        res = c.classify_transform_configs_with_full_chain({}, {"X"})
        assert res == {"allowed": {}, "disallowed": {}, "mapping_errors": []}


# =========================================================================== #
# _build_connector_class_mapping
# =========================================================================== #
class TestBuildConnectorClassMapping:
    def test_maps_resolved_templates_files(self, make_comparator, write_template):
        write_template("MySqlSource_resolved_templates",
                       {"connector.class": "com.example.A"})
        write_template("PgSink_resolved_templates",
                       {"connector.class": "com.example.B"})
        # a non-resolved file is ignored
        write_template("ignored", {"connector.class": "com.example.C"})
        c = make_comparator(fm_dir=write_template.dir)
        mapping = c._build_connector_class_mapping()
        assert "com.example.A" in mapping
        assert "com.example.B" in mapping
        assert "com.example.C" not in mapping
        assert mapping["com.example.A"]["fm_templates"][0].endswith(
            "MySqlSource_resolved_templates.json")

    def test_empty_when_no_fm_template_dir(self, make_comparator):
        c = make_comparator()
        c.fm_template_dir = None
        assert c._build_connector_class_mapping() == {}


# =========================================================================== #
# _find_fm_template_by_connector_class
# =========================================================================== #
class TestFindFmTemplateByConnectorClass:
    def test_missing_dir_returns_none(self, make_comparator, tmp_path):
        c = make_comparator()
        c.fm_template_dir = tmp_path / "nope"
        assert c._find_fm_template_by_connector_class("com.example.X") is None

    def test_single_match_returned(self, make_comparator, write_template, template_factory):
        _t, _wrap = template_factory
        p = write_template("Only", _wrap(_t("OnlyId", "com.example.Only")))
        c = make_comparator(fm_dir=write_template.dir)
        assert c._find_fm_template_by_connector_class("com.example.Only") == str(p)

    def test_no_match_returns_none(self, make_comparator, write_template, template_factory):
        _t, _wrap = template_factory
        write_template("Other", _wrap(_t("OtherId", "com.example.Other")))
        c = make_comparator(fm_dir=write_template.dir)
        assert c._find_fm_template_by_connector_class("com.example.NotThere") is None

    def test_v2_mapping_then_fallback_to_original(self, make_comparator, write_template,
                                                  template_factory):
        # version v2, but only a v1 template exists; mapping target not found,
        # so it should fall back and find via the original connector class.
        _t, _wrap = template_factory
        p = write_template("MySqlV1", _wrap(_t("MySqlV1Id",
                                               "io.debezium.connector.mysql.MySqlConnector")))
        c = make_comparator(fm_dir=write_template.dir, debezium_version="v2")
        assert c._find_fm_template_by_connector_class(
            "io.debezium.connector.mysql.MySqlConnector") == str(p)

    def test_multiple_non_jdbc_invokes_user_selection(self, make_comparator, write_template,
                                                      template_factory, monkeypatch):
        _t, _wrap = template_factory
        write_template("First", _wrap(_t("FirstId", "com.example.Dup")))
        write_template("Second", _wrap(_t("SecondId", "com.example.Dup")))
        c = make_comparator(fm_dir=write_template.dir)
        # user picks option 2
        monkeypatch.setattr("builtins.input", lambda *_: "2")
        result = c._find_fm_template_by_connector_class("com.example.Dup", "myconn")
        assert result.endswith(".json")

    def test_multiple_jdbc_auto_selects(self, make_comparator, write_template,
                                        template_factory, sample_jdbc_config):
        _t, _wrap = template_factory
        write_template("MySqlSource", _wrap(
            _t("MySqlSource", "io.confluent.connect.jdbc.JdbcSourceConnector")))
        write_template("PostgresSource", _wrap(
            _t("PostgresSource", "io.confluent.connect.jdbc.JdbcSourceConnector")))
        c = make_comparator(fm_dir=write_template.dir)
        result = c._find_fm_template_by_connector_class(
            "io.confluent.connect.jdbc.JdbcSourceConnector", "jdbc1", sample_jdbc_config)
        assert result.endswith("MySqlSource.json")


# =========================================================================== #
# _auto_select_jdbc_template
# =========================================================================== #
class TestAutoSelectJdbcTemplate:
    def _info(self, write_template, *items):
        info = []
        for tid in items:
            p = write_template.dir / f"{tid}.json"
            p.write_text(json.dumps({"template_id": tid,
                                     "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector"}))
            info.append({"path": str(p), "template_id": tid, "filename": p.name})
        return info

    def test_selects_source_template_for_mysql(self, make_comparator, write_template,
                                               sample_jdbc_config):
        c = make_comparator(fm_dir=write_template.dir)
        info = self._info(write_template, "MySqlSource", "PostgresSource")
        result = c._auto_select_jdbc_template(
            "io.confluent.connect.jdbc.JdbcSourceConnector", info, sample_jdbc_config)
        assert result.endswith("MySqlSource.json")

    def test_partial_match_fallback(self, make_comparator, write_template):
        c = make_comparator(fm_dir=write_template.dir)
        # template id not in the exact expected list but contains 'mysql'
        info = self._info(write_template, "CustomMysqlThing")
        cfg = {"connection.url": "jdbc:mysql://h:3306/d"}
        result = c._auto_select_jdbc_template(
            "io.confluent.connect.jdbc.JdbcSourceConnector", info, cfg)
        assert result.endswith("CustomMysqlThing.json")

    def test_no_match_falls_back_to_user_selection(self, make_comparator, write_template,
                                                   monkeypatch):
        c = make_comparator(fm_dir=write_template.dir)
        info = self._info(write_template, "TotallyUnrelated")
        cfg = {"connection.url": "jdbc:mysql://h:3306/d"}
        monkeypatch.setattr("builtins.input", lambda *_: "1")
        result = c._auto_select_jdbc_template(
            "io.confluent.connect.jdbc.JdbcSourceConnector", info, cfg)
        assert result.endswith("TotallyUnrelated.json")

    def test_snowflake_searches_snowflake_specific_template(self, make_comparator,
                                                            write_template, template_factory):
        _t, _wrap = template_factory
        write_template("SnowflakeSrc", _wrap(_t(
            "SnowflakeJdbcSource",
            "io.confluent.connect.snowflake.jdbc.SnowflakeSourceConnector")))
        c = make_comparator(fm_dir=write_template.dir)
        cfg = {"connection.url": "jdbc:snowflake://acct.snowflakecomputing.com/"}
        # template_info here are the generic jdbc ones (empty is fine; snowflake search overrides)
        result = c._auto_select_jdbc_template(
            "io.confluent.connect.jdbc.JdbcSourceConnector", [], cfg)
        assert result.endswith("SnowflakeSrc.json")


# =========================================================================== #
# _filter_cdc_templates_by_version
# =========================================================================== #
class TestFilterCdcTemplatesByVersion:
    def test_non_debezium_returns_unchanged(self, make_comparator):
        c = make_comparator()
        info = [{"template_id": "X", "filename": "x.json", "path": "x.json"}]
        assert c._filter_cdc_templates_by_version("com.example.NotDebezium", info) == info

    def test_filters_to_v2(self, make_comparator, write_template):
        c = make_comparator(debezium_version="v2")
        v1 = write_template.dir / "v1.json"
        v1.write_text(json.dumps({"connector.class": "io.debezium.connector.mysql.MySqlConnector"}))
        v2 = write_template.dir / "v2file.json"
        v2.write_text(json.dumps({"connector.class": "io.debezium.connector.v2.mysql.MySqlConnectorV2"}))
        info = [
            {"template_id": "MySqlV1", "filename": "v1.json", "path": str(v1)},
            {"template_id": "MySqlV2", "filename": "v2file.json", "path": str(v2)},
        ]
        result = c._filter_cdc_templates_by_version(
            "io.debezium.connector.mysql.MySqlConnector", info)
        assert len(result) == 1
        assert result[0]["template_id"] == "MySqlV2"

    def test_filters_to_v1(self, make_comparator, write_template):
        c = make_comparator(debezium_version="v1")
        v1 = write_template.dir / "v1.json"
        v1.write_text(json.dumps({"connector.class": "io.debezium.connector.mysql.MySqlConnector"}))
        v2 = write_template.dir / "v2file.json"
        v2.write_text(json.dumps({"connector.class": "io.debezium.connector.v2.mysql.MySqlConnectorV2"}))
        info = [
            {"template_id": "MySqlV1", "filename": "v1.json", "path": str(v1)},
            {"template_id": "MySqlV2", "filename": "v2file.json", "path": str(v2)},
        ]
        result = c._filter_cdc_templates_by_version(
            "io.debezium.connector.mysql.MySqlConnector", info)
        assert len(result) == 1
        assert result[0]["template_id"] == "MySqlV1"

    def test_v2_requested_but_only_v1_present(self, make_comparator, write_template):
        c = make_comparator(debezium_version="v2")
        v1 = write_template.dir / "v1.json"
        v1.write_text(json.dumps({"connector.class": "io.debezium.connector.mysql.MySqlConnector"}))
        info = [{"template_id": "MySqlV1", "filename": "v1.json", "path": str(v1)}]
        result = c._filter_cdc_templates_by_version(
            "io.debezium.connector.mysql.MySqlConnector", info)
        assert result == info


# =========================================================================== #
# _get_user_template_selection
# =========================================================================== #
class TestGetUserTemplateSelection:
    def _info(self):
        return [
            {"template_id": "T1", "filename": "t1.json", "path": "/p/t1.json"},
            {"template_id": "T2", "filename": "t2.json", "path": "/p/t2.json"},
        ]

    def test_valid_choice(self, make_comparator, monkeypatch):
        c = make_comparator()
        monkeypatch.setattr("builtins.input", lambda *_: "2")
        assert c._get_user_template_selection("cc", self._info()) == "/p/t2.json"

    def test_retries_on_out_of_range_then_valid(self, make_comparator, monkeypatch):
        c = make_comparator()
        answers = iter(["5", "1"])
        monkeypatch.setattr("builtins.input", lambda *_: next(answers))
        assert c._get_user_template_selection("cc", self._info()) == "/p/t1.json"

    def test_retries_on_non_numeric_then_valid(self, make_comparator, monkeypatch):
        c = make_comparator()
        answers = iter(["abc", "1"])
        monkeypatch.setattr("builtins.input", lambda *_: next(answers))
        assert c._get_user_template_selection("cc", self._info()) == "/p/t1.json"


# =========================================================================== #
# _get_templates_for_connector
# =========================================================================== #
class TestGetTemplatesForConnector:
    def test_loads_fm_template_no_worker(self, make_comparator, write_template, template_factory):
        _t, _wrap = template_factory
        write_template("Conn", _wrap(_t("ConnId", "com.example.Conn")))
        c = make_comparator(fm_dir=write_template.dir)
        c.fm_templates = {}
        sm, fm = c._get_templates_for_connector("com.example.Conn", "myconn")
        assert sm == {}
        assert fm is not None
        assert "templates" in fm

    def test_missing_fm_returns_none(self, make_comparator, write_template, template_factory):
        _t, _wrap = template_factory
        write_template("Conn", _wrap(_t("ConnId", "com.example.Other")))
        c = make_comparator(fm_dir=write_template.dir)
        c.fm_templates = {}
        sm, fm = c._get_templates_for_connector("com.example.NotThere")
        assert sm == {}
        assert fm is None

    def test_uses_worker_url_from_config(self, make_comparator, write_template,
                                         template_factory, monkeypatch):
        _t, _wrap = template_factory
        write_template("Conn", _wrap(_t("ConnId", "com.example.Conn")))
        c = make_comparator(fm_dir=write_template.dir)
        c.fm_templates = {}
        captured = {}

        def fake_put(url, json=None, headers=None, verify=None, auth=None):
            captured["url"] = url
            return FakeResponse(status_code=200, json_data={"configs": []})

        monkeypatch.setattr(tr_module.requests, "put", fake_put)
        cfg = {"connector.class": "com.example.Conn", "worker": "http://worker:8083"}
        sm, fm = c._get_templates_for_connector("com.example.Conn", "myconn", cfg)
        assert sm == {"configs": []}
        assert "worker:8083" in captured["url"]
        assert fm is not None

    def test_sftp_special_mapping(self, make_comparator, write_template, template_factory):
        _t, _wrap = template_factory
        # No direct match for the SftpCsvSource class, but SftpSource_resolved_templates exists
        write_template("SftpSource_resolved_templates",
                       _wrap(_t("SftpSource", "io.confluent.connect.sftp.SftpSourceConnector")))
        c = make_comparator(fm_dir=write_template.dir)
        c.fm_templates = {}
        sm, fm = c._get_templates_for_connector(
            "io.confluent.connect.sftp.SftpCsvSourceConnector", "sftp1")
        assert fm is not None


# =========================================================================== #
# _load_templates
# =========================================================================== #
class TestLoadTemplates:
    def test_loads_all_json_keyed_by_stem(self, make_comparator, write_template, template_factory):
        _t, _wrap = template_factory
        write_template("alpha", _wrap(_t("AlphaId", "com.example.Alpha")))
        write_template("beta", _wrap(_t("BetaId", "com.example.Beta")))
        c = make_comparator(fm_dir=write_template.dir)
        templates = c._load_templates(write_template.dir)
        assert set(templates.keys()) == {"alpha", "beta"}
        assert templates["alpha"]["templates"][0]["template_id"] == "AlphaId"

    def test_nonexistent_dir_returns_empty(self, make_comparator, tmp_path):
        c = make_comparator()
        assert c._load_templates(tmp_path / "nope") == {}

    def test_skips_bad_json(self, make_comparator, write_template, template_factory):
        _t, _wrap = template_factory
        write_template("good", _wrap(_t("GoodId", "com.example.Good")))
        (write_template.dir / "bad.json").write_text("{ not json")
        c = make_comparator(fm_dir=write_template.dir)
        templates = c._load_templates(write_template.dir)
        assert "good" in templates
        assert "bad" not in templates