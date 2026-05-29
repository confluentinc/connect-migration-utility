"""
Unit tests for ConfigMapperMixin (src/comparator/config_mapper.py).

These methods are mostly pure functions over dicts/strings. They are invoked on
a composed ConnectorComparator instance built offline via the `make_comparator`
fixture. We assert on data mutations / return values, never on log text.
"""

import pytest


# --------------------------------------------------------------------------- #
# _get_database_type
# --------------------------------------------------------------------------- #

def test_get_database_type_precise_pattern(make_comparator):
    c = make_comparator()
    cfg = {"connection.url": "jdbc:mysql://host:3306/db"}
    assert c._get_database_type(cfg) == "mysql"


def test_get_database_type_postgres(make_comparator):
    c = make_comparator()
    assert c._get_database_type({"connection.url": "jdbc:postgresql://h:5432/d"}) == "postgresql"


def test_get_database_type_fallback_pattern(make_comparator):
    # No 'jdbc:<pattern>://' but the substring 'oracle' is present -> fallback.
    c = make_comparator()
    cfg = {"connection.url": "jdbc:oracle:thin:@host:1521:orcl"}
    assert c._get_database_type(cfg) == "oracle"


def test_get_database_type_from_config_key(make_comparator):
    c = make_comparator()
    # URL has no recognizable pattern, but database.type provided.
    cfg = {"connection.url": "jdbc:weirddb://h/d", "database.type": "MySQL"}
    assert c._get_database_type(cfg) == "mysql"


def test_get_database_type_unknown(make_comparator):
    c = make_comparator()
    assert c._get_database_type({"connection.url": "jdbc:weirddb://h/d"}) == "unknown"


def test_get_database_type_no_url_no_type(make_comparator):
    c = make_comparator()
    assert c._get_database_type({}) == "unknown"


def test_get_database_type_non_string_url(make_comparator):
    c = make_comparator()
    assert c._get_database_type({"connection.url": 123}) == "unknown"


# --------------------------------------------------------------------------- #
# _parse_jdbc_url - standard path
# --------------------------------------------------------------------------- #

def test_parse_jdbc_url_standard_full(make_comparator):
    c = make_comparator()
    url = "jdbc:mysql://db.example.com:3306/inventory?user=admin&password=secret"
    info = c._parse_jdbc_url(url)
    assert info["host"] == "db.example.com"
    assert info["port"] == "3306"
    assert info["db.name"] == "inventory"
    assert info["user"] == "admin"
    assert info["password"] == "secret"


def test_parse_jdbc_url_standard_minimal(make_comparator):
    c = make_comparator()
    info = c._parse_jdbc_url("jdbc:postgresql://localhost:5432/mydb")
    assert info["host"] == "localhost"
    assert info["port"] == "5432"
    assert info["db.name"] == "mydb"
    assert "user" not in info
    assert "password" not in info


def test_parse_jdbc_url_no_port(make_comparator):
    c = make_comparator()
    info = c._parse_jdbc_url("jdbc:postgresql://localhost/mydb")
    assert info["host"] == "localhost"
    assert "port" not in info
    assert info["db.name"] == "mydb"


# --------------------------------------------------------------------------- #
# _parse_jdbc_url - Oracle complex DESCRIPTION path
# --------------------------------------------------------------------------- #

def test_parse_jdbc_url_oracle_description(make_comparator):
    c = make_comparator()
    url = (
        'jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)'
        '(HOST=oracle.example.com)(PORT=2484))'
        '(CONNECT_DATA=(SERVICE_NAME=orclpdb))'
        '(SECURITY=(SSL_SERVER_CERT_DN="CN=oracle,O=acme")))'
    )
    info = c._parse_jdbc_url(url)
    assert info["host"] == "oracle.example.com"
    assert info["port"] == "2484"
    # Oracle branch parses original_url (not lowercased), so key keeps its case.
    assert info["db.connection.type"] == "SERVICE_NAME"
    assert info["db.name"] == "orclpdb"
    # Standard-path keys should NOT be present on the Oracle branch.
    assert "user" not in info


def test_parse_jdbc_url_oracle_sid(make_comparator):
    c = make_comparator()
    url = (
        'jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)'
        '(HOST=h1)(PORT=1521))(CONNECT_DATA=(SID=xe)))'
    )
    info = c._parse_jdbc_url(url)
    assert info["host"] == "h1"
    assert info["port"] == "1521"
    assert info["db.connection.type"] == "SID"
    assert info["db.name"] == "xe"
    assert "ssl.server.cert.dn" not in info


# --------------------------------------------------------------------------- #
# _parse_mongodb_connection_string
# --------------------------------------------------------------------------- #

def test_parse_mongodb_srv_with_credentials_and_db(make_comparator):
    c = make_comparator()
    url = "mongodb+srv://alice:pw123@cluster0.mongodb.net/mydb?retryWrites=true"
    info = c._parse_mongodb_connection_string(url)
    assert info["user"] == "alice"
    assert info["password"] == "pw123"
    assert info["host"] == "cluster0.mongodb.net"
    assert info["database"] == "mydb"


def test_parse_mongodb_srv_no_db(make_comparator):
    c = make_comparator()
    info = c._parse_mongodb_connection_string("mongodb+srv://u:p@host.mongodb.net")
    assert info["user"] == "u"
    assert info["password"] == "p"
    assert info["host"] == "host.mongodb.net"
    assert "database" not in info


def test_parse_mongodb_plain_with_credentials(make_comparator):
    c = make_comparator()
    info = c._parse_mongodb_connection_string("mongodb://bob:secret@h1:27017/orders?x=1")
    assert info["user"] == "bob"
    assert info["password"] == "secret"
    assert info["host"] == "h1:27017"
    assert info["database"] == "orders"


def test_parse_mongodb_plain_no_credentials(make_comparator):
    c = make_comparator()
    info = c._parse_mongodb_connection_string("mongodb://h1:27017/orders")
    assert "user" not in info
    assert "password" not in info
    assert info["host"] == "h1:27017"
    assert info["database"] == "orders"


def test_parse_mongodb_plain_no_credentials_no_db(make_comparator):
    c = make_comparator()
    info = c._parse_mongodb_connection_string("mongodb://h1:27017")
    assert info["host"] == "h1:27017"
    assert "database" not in info


def test_parse_mongodb_unrecognized_returns_empty(make_comparator):
    c = make_comparator()
    assert c._parse_mongodb_connection_string("postgres://x") == {}


# --------------------------------------------------------------------------- #
# _map_jdbc_properties
# --------------------------------------------------------------------------- #

def test_map_jdbc_properties_with_mappings(make_comparator):
    c = make_comparator()
    # Inject a property_mappings entry for mysql (fm_prop -> jdbc connection_info key).
    c.jdbc_database_types["mysql"]["property_mappings"] = {
        "connection.host": "host",
        "connection.port": "port",
        "db.name": "db.name",
        "connection.user": "user",
        "connection.password": "password",
        "missing.prop": "nonexistent",
    }
    cfg = {"connection.url": "jdbc:mysql://h1:3306/inv?user=a&password=b"}
    mapped = c._map_jdbc_properties(cfg, "mysql")
    assert mapped["connection.host"] == "h1"
    assert mapped["connection.port"] == "3306"
    assert mapped["db.name"] == "inv"
    assert mapped["connection.user"] == "a"
    assert mapped["connection.password"] == "b"
    # jdbc prop not present in connection_info should be skipped.
    assert "missing.prop" not in mapped


def test_map_jdbc_properties_no_mappings_for_type(make_comparator):
    c = make_comparator()
    # mysql has no property_mappings by default -> empty mapped config.
    mapped = c._map_jdbc_properties({"connection.url": "jdbc:mysql://h:3306/d"}, "mysql")
    assert mapped == {}


def test_map_jdbc_properties_non_jdbc_url(make_comparator):
    c = make_comparator()
    c.jdbc_database_types["mysql"]["property_mappings"] = {"connection.host": "host"}
    assert c._map_jdbc_properties({"connection.url": "mongodb://h/d"}, "mysql") == {}


def test_map_jdbc_properties_no_url(make_comparator):
    c = make_comparator()
    assert c._map_jdbc_properties({}, "mysql") == {}


# --------------------------------------------------------------------------- #
# _get_required_properties
# --------------------------------------------------------------------------- #

def test_get_required_properties_string_true(make_comparator, template_factory):
    _t, _wrap = template_factory
    c = make_comparator()
    tpl = _wrap(_t("t", "cls", config_defs=[
        {"name": "a", "required": "true"},
        {"name": "b", "required": "True"},
        {"name": "c", "required": "false"},
    ]))
    req = c._get_required_properties(tpl)
    assert set(req.keys()) == {"a", "b"}


def test_get_required_properties_bool(make_comparator, template_factory):
    _t, _wrap = template_factory
    c = make_comparator()
    tpl = _wrap(_t("t", "cls", config_defs=[
        {"name": "a", "required": True},
        {"name": "b", "required": False},
    ]))
    assert set(c._get_required_properties(tpl).keys()) == {"a"}


def test_get_required_properties_internal_skipped(make_comparator, template_factory):
    _t, _wrap = template_factory
    c = make_comparator()
    tpl = _wrap(_t("t", "cls", config_defs=[
        {"name": "a", "required": True, "internal": True},
        {"name": "b", "required": True},
    ]))
    assert set(c._get_required_properties(tpl).keys()) == {"b"}


def test_get_required_properties_non_bool_non_str(make_comparator, template_factory):
    _t, _wrap = template_factory
    c = make_comparator()
    tpl = _wrap(_t("t", "cls", config_defs=[{"name": "a", "required": 1}]))
    assert c._get_required_properties(tpl) == {}


def test_get_required_properties_no_templates(make_comparator):
    c = make_comparator()
    assert c._get_required_properties({}) == {}


# --------------------------------------------------------------------------- #
# _is_source_connector
# --------------------------------------------------------------------------- #

def test_is_source_none_template_defaults_true(make_comparator):
    c = make_comparator()
    assert c._is_source_connector(None) is True
    assert c._is_source_connector({}) is True  # empty falls through to default


def test_is_source_top_level_connector_type(make_comparator):
    c = make_comparator()
    assert c._is_source_connector({"connector_type": "SOURCE"}) is True
    assert c._is_source_connector({"connector_type": "SINK"}) is False


def test_is_source_templates_connector_type(make_comparator):
    c = make_comparator()
    tpl = {"templates": [{"connector_type": "SINK"}]}
    assert c._is_source_connector(tpl) is False
    tpl2 = {"templates": [{"connector_type": "SOURCE"}]}
    assert c._is_source_connector(tpl2) is True


def test_is_source_class_name_source_indicator(make_comparator):
    c = make_comparator()
    tpl = {"templates": [{"connector.class": "com.acme.FooSourceConnector"}]}
    assert c._is_source_connector(tpl) is True


def test_is_source_class_name_cdc_indicator(make_comparator):
    c = make_comparator()
    assert c._is_source_connector({"connector.class": "OracleCdcConnector"}) is True


def test_is_source_class_name_xstream_indicator(make_comparator):
    c = make_comparator()
    assert c._is_source_connector({"connector.class": "OracleXStreamConnector"}) is True


def test_is_source_class_name_sink_indicator(make_comparator):
    c = make_comparator()
    tpl = {"templates": [{"connector.class": "com.acme.FooSinkConnector"}]}
    assert c._is_source_connector(tpl) is False


def test_is_source_class_name_no_indicator_defaults_true(make_comparator):
    c = make_comparator()
    assert c._is_source_connector({"connector.class": "com.acme.Mystery"}) is True


# --------------------------------------------------------------------------- #
# _create_direct_mappings_from_template
# --------------------------------------------------------------------------- #

def test_create_direct_mappings_template_var(make_comparator, template_factory):
    _t, _wrap = template_factory
    c = make_comparator()
    tpl = _wrap(_t("t", "cls", connector_configs=[
        {"name": "connection.host", "value": "${connection.host}"},
    ]))
    m = c._create_direct_mappings_from_template(tpl)
    # sm property name -> extracted fm property name
    assert m["connection.host"] == "connection.host"


def test_create_direct_mappings_direct_value(make_comparator, template_factory):
    _t, _wrap = template_factory
    c = make_comparator()
    tpl = _wrap(_t("t", "cls", connector_configs=[
        {"name": "dialect.name", "value": "MySqlDatabaseDialect"},
    ]))
    m = c._create_direct_mappings_from_template(tpl)
    # direct value mapping: value -> name
    assert m["MySqlDatabaseDialect"] == "dialect.name"


def test_create_direct_mappings_switch(make_comparator, template_factory):
    _t, _wrap = template_factory
    c = make_comparator()
    tpl = _wrap(_t("t", "cls", connector_configs=[
        {"name": "ssl.mode", "switch": {
            "ssl.mode": {"on": "${ssl.mode}", "off": "disabled"}
        }},
    ]))
    m = c._create_direct_mappings_from_template(tpl)
    assert m["ssl.mode"] == "ssl.mode"


def test_create_direct_mappings_same_name(make_comparator, template_factory):
    _t, _wrap = template_factory
    c = make_comparator()
    tpl = _wrap(_t("t", "cls", connector_configs=[
        {"name": "tasks.max"},
    ]))
    m = c._create_direct_mappings_from_template(tpl)
    assert m["tasks.max"] == "tasks.max"


def test_create_direct_mappings_no_templates(make_comparator):
    c = make_comparator()
    assert c._create_direct_mappings_from_template({}) == {}
    assert c._create_direct_mappings_from_template(None) == {}


# --------------------------------------------------------------------------- #
# _map_using_template_direct_mappings
# --------------------------------------------------------------------------- #

def test_map_direct_fixed_value_override_error(make_comparator, template_factory):
    _t, _wrap = template_factory
    c = make_comparator()
    # connection.attempts has a fixed value '3'; user gives '5' -> override + error.
    tpl = _wrap(_t("t", "cls", connector_configs=[
        {"name": "connection.attempts", "value": "3"},
    ]))
    cfg = {"connection.attempts": "5"}
    mapped, errors = c._map_using_template_direct_mappings(cfg, tpl)
    # direct value mapping is {"3": "connection.attempts"}; SM key "3" is not in cfg
    # so the fixed-value override branch is not hit. But "connection.attempts" is in
    # direct_mappings.values() and present in cfg, so the same-name pass maps it.
    assert mapped == {"connection.attempts": "5"}
    assert errors == []


def test_map_direct_fixed_value_override_via_template_var(make_comparator, template_factory):
    _t, _wrap = template_factory
    c = make_comparator()
    # Template-var mapping so sm property 'x' -> fm 'x', PLUS a fixed value for 'x'.
    tpl = _wrap(_t("t", "cls", connector_configs=[
        {"name": "x", "value": "${x}"},
        {"name": "x", "value": "fixedval"},
    ]))
    # direct_mappings: from first config x->x ; fixed_values: from second config x->fixedval
    cfg = {"x": "userval"}
    mapped, errors = c._map_using_template_direct_mappings(cfg, tpl)
    assert mapped["x"] == "fixedval"
    assert any("overridden by template fixed value" in e for e in errors)


def test_map_direct_fixed_value_match(make_comparator, template_factory):
    _t, _wrap = template_factory
    c = make_comparator()
    tpl = _wrap(_t("t", "cls", connector_configs=[
        {"name": "x", "value": "${x}"},
        {"name": "x", "value": "same"},
    ]))
    mapped, errors = c._map_using_template_direct_mappings({"x": "same"}, tpl)
    assert mapped["x"] == "same"
    assert errors == []


def test_map_direct_recommended_values_valid(make_comparator, template_factory):
    _t, _wrap = template_factory
    c = make_comparator()
    tpl = _wrap(_t("t", "cls",
                   connector_configs=[{"name": "output.data.format", "value": "${output.data.format}"}],
                   config_defs=[{"name": "output.data.format",
                                 "recommended_values": ["AVRO", "JSON"]}]))
    mapped, errors = c._map_using_template_direct_mappings({"output.data.format": "AVRO"}, tpl)
    assert mapped["output.data.format"] == "AVRO"
    assert errors == []


def test_map_direct_recommended_values_invalid_skips(make_comparator, template_factory):
    _t, _wrap = template_factory
    c = make_comparator()
    tpl = _wrap(_t("t", "cls",
                   connector_configs=[{"name": "output.data.format", "value": "${output.data.format}"}],
                   config_defs=[{"name": "output.data.format",
                                 "recommended_values": ["AVRO", "JSON"]}]))
    mapped, errors = c._map_using_template_direct_mappings({"output.data.format": "CSV"}, tpl)
    # The direct-mapping pass records a validation error and `continue`s without
    # mapping, but the subsequent same-name pass still writes the (invalid) value
    # because the key is in direct_mappings.values(). Error is still recorded.
    assert mapped["output.data.format"] == "CSV"
    assert any("not in recommended values" in e for e in errors)


def test_map_direct_same_name_mapping(make_comparator, template_factory):
    _t, _wrap = template_factory
    c = make_comparator()
    # 'tasks.max' is a same-name mapping (no value/switch). direct_mappings values
    # contain 'tasks.max'; config has it -> mapped via same-name pass.
    tpl = _wrap(_t("t", "cls", connector_configs=[{"name": "tasks.max"}]))
    mapped, errors = c._map_using_template_direct_mappings({"tasks.max": "4"}, tpl)
    assert mapped["tasks.max"] == "4"
    assert errors == []


# --------------------------------------------------------------------------- #
# _get_fixed_values_from_template
# --------------------------------------------------------------------------- #

def test_get_fixed_values(make_comparator, template_factory):
    _t, _wrap = template_factory
    c = make_comparator()
    tpl = _wrap(_t("t", "cls", connector_configs=[
        {"name": "a", "value": "fixed"},
        {"name": "b", "value": "${b}"},  # template var -> skipped
        {"name": "c"},                    # no value -> skipped
    ]))
    fv = c._get_fixed_values_from_template(tpl)
    assert fv == {"a": "fixed"}


def test_get_fixed_values_no_templates(make_comparator):
    c = make_comparator()
    assert c._get_fixed_values_from_template({}) == {}
    assert c._get_fixed_values_from_template(None) == {}


# --------------------------------------------------------------------------- #
# _get_recommended_values_from_template
# --------------------------------------------------------------------------- #

def test_get_recommended_values(make_comparator, template_factory):
    _t, _wrap = template_factory
    c = make_comparator()
    tpl = _wrap(_t("t", "cls", config_defs=[
        {"name": "fmt", "recommended_values": ["AVRO", "JSON"]},
        {"name": "other"},
    ]))
    rv = c._get_recommended_values_from_template(tpl)
    assert rv == {"fmt": ["AVRO", "JSON"]}


def test_get_recommended_values_no_templates(make_comparator):
    c = make_comparator()
    assert c._get_recommended_values_from_template({}) == {}


# --------------------------------------------------------------------------- #
# _generate_fm_config (legacy / "not being used")
# --------------------------------------------------------------------------- #

def test_generate_fm_config_no_connector_class(make_comparator):
    c = make_comparator()
    connector = {"name": "c1", "config": {"tasks.max": "1"}}
    result = c._generate_fm_config(connector)
    assert result["name"] == "c1"
    assert result["config"] == {}
    assert result["mapping_errors"] == ["No connector.class found in config"]
    assert "tasks.max" in result["unmapped_configs"]


def test_generate_fm_config_jdbc_happy_path(make_comparator, template_factory):
    _t, _wrap = template_factory
    c = make_comparator()

    fm_template = _wrap(_t(
        "JdbcSourceV2", "io.confluent.connect.jdbc.JdbcSourceConnector", "SOURCE",
        connector_configs=[],
        config_defs=[
            {"name": "connection.url", "required": True},
            {"name": "tasks.max", "required": False},
            {"name": "output.data.format", "required": False},
        ],
    ))

    # Stub template resolution + transforms so the legacy path runs offline.
    c._get_templates_for_connector = lambda cls, name, cfg: ({}, fm_template)
    c.get_transforms_config = lambda config, plugin_type: {"allowed": {}, "mapping_errors": []}

    connector = {
        "name": "jdbc1",
        "config": {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "connection.url": "jdbc:mysql://h:3306/inv",
            "tasks.max": "2",
        },
    }
    result = c._generate_fm_config(connector)
    assert result["name"] == "jdbc1"
    # connector.class is updated to the template_id and is in config_defs? It's not
    # in config_defs, but it is set directly; filtering removes non-config_def keys.
    # tasks.max IS in config_defs and provided -> mapped.
    assert result["config"]["tasks.max"] == "2"
    assert "sm_config" in result


# --------------------------------------------------------------------------- #
# _extract_connector_config_defs
# --------------------------------------------------------------------------- #

def test_extract_connector_config_defs(make_comparator, template_factory):
    _t, _wrap = template_factory
    c = make_comparator()
    tpl = _wrap(
        _t("t1", "cls", connector_configs=[{"name": "a", "value": "1"}]),
        _t("t2", "cls", connector_configs=[{"name": "b", "value": "2"}]),
    )
    defs = c._extract_connector_config_defs(tpl)
    names = [d["name"] for d in defs]
    assert names == ["a", "b"]


def test_extract_connector_config_defs_no_templates(make_comparator):
    c = make_comparator()
    assert c._extract_connector_config_defs({}) == []


def test_extract_connector_config_defs_templates_not_list(make_comparator):
    c = make_comparator()
    assert c._extract_connector_config_defs({"templates": "notalist"}) == []


def test_extract_connector_config_defs_connector_configs_not_list(make_comparator):
    c = make_comparator()
    tpl = {"templates": [{"connector_configs": True}]}
    assert c._extract_connector_config_defs(tpl) == []


def test_extract_connector_config_defs_skips_non_dict_template(make_comparator):
    c = make_comparator()
    tpl = {"templates": ["notadict", {"connector_configs": [{"name": "a"}]}]}
    defs = c._extract_connector_config_defs(tpl)
    assert [d["name"] for d in defs] == ["a"]


# --------------------------------------------------------------------------- #
# _extract_template_config_defs (dedup, first wins)
# --------------------------------------------------------------------------- #

def test_extract_template_config_defs_dedup_first_wins(make_comparator, template_factory):
    _t, _wrap = template_factory
    c = make_comparator()
    tpl = _wrap(
        _t("t1", "cls", config_defs=[{"name": "dup", "type": "FIRST"}, {"name": "a"}]),
        _t("t2", "cls", config_defs=[{"name": "dup", "type": "SECOND"}, {"name": "b"}]),
    )
    defs = c._extract_template_config_defs(tpl)
    names = [d["name"] for d in defs]
    assert names == ["dup", "a", "b"]
    dup = next(d for d in defs if d["name"] == "dup")
    assert dup["type"] == "FIRST"


def test_extract_template_config_defs_skips_invalid(make_comparator):
    c = make_comparator()
    tpl = {"templates": [{"config_defs": ["notadict", {"noname": 1}, {"name": "ok"}]}]}
    defs = c._extract_template_config_defs(tpl)
    assert [d["name"] for d in defs] == ["ok"]


def test_extract_template_config_defs_no_templates(make_comparator):
    c = make_comparator()
    assert c._extract_template_config_defs({}) == []


def test_extract_template_config_defs_config_defs_not_list(make_comparator):
    c = make_comparator()
    assert c._extract_template_config_defs({"templates": [{"config_defs": True}]}) == []


# --------------------------------------------------------------------------- #
# _get_config_derivation_method
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("name", [
    "connection.url", "connection.host", "connection.port", "connection.user",
    "connection.password", "connection.database", "db.name", "db.connection.type",
    "ssl.server.cert.dn", "input.key.format", "input.data.format",
    "output.key.format", "output.data.format", "ssl.mode", "redis.hostname",
    "azure.servicebus.namespace", "subject.name.strategy",
    "value.converter.reference.subject.name.strategy",
])
def test_get_config_derivation_method_known(make_comparator, name):
    c = make_comparator()
    method = c._get_config_derivation_method(name, {})
    assert method is not None
    assert callable(method)


def test_get_config_derivation_method_unknown(make_comparator):
    c = make_comparator()
    assert c._get_config_derivation_method("totally.unknown.key", {}) is None


# --------------------------------------------------------------------------- #
# _process_value_case
# --------------------------------------------------------------------------- #

def test_process_value_case_non_string_bool(make_comparator):
    c = make_comparator()
    fm = {}
    c._process_value_case(
        {"name": "validate.non.null", "value": False},
        "ignored", [], fm, [], {}, set())
    assert fm["validate.non.null"] == "false"


def test_process_value_case_non_string_number(make_comparator):
    c = make_comparator()
    fm = {}
    c._process_value_case({"name": "n", "value": 5}, "ignored", [], fm, [], {}, set())
    assert fm["n"] == "5"


def test_process_value_case_internal_plainloginmodule(make_comparator):
    c = make_comparator()
    fm, warnings = {}, []
    val = "org.apache.kafka.common.security.plain.PlainLoginModule required;"
    c._process_value_case({"name": "sasl.jaas.config", "value": val},
                          "u", [], fm, warnings, {}, set())
    assert fm == {}
    assert any("internal" in w for w in warnings)


def test_process_value_case_internal_logical_cluster(make_comparator):
    c = make_comparator()
    fm, warnings = {}, []
    val = "prefix-{{.logicalClusterId}}"
    c._process_value_case({"name": "k", "value": val}, "u", [], fm, warnings, {}, set())
    assert fm == {}
    assert any("internal" in w for w in warnings)


def test_process_value_case_constant_match(make_comparator):
    c = make_comparator()
    fm, warnings = {}, []
    c._process_value_case({"name": "k", "value": "fixed"}, "fixed", [], fm, warnings, {}, set())
    assert fm["k"] == "fixed"
    assert warnings == []


def test_process_value_case_constant_mismatch_warns(make_comparator):
    c = make_comparator()
    fm, warnings = {}, []
    c._process_value_case({"name": "k", "value": "fixed"}, "other", [], fm, warnings, {}, set())
    assert "k" not in fm
    assert any("constant value" in w for w in warnings)


def test_process_value_case_referenced_key_user_has_value(make_comparator):
    c = make_comparator()
    fm, warnings = {}, []
    tdefs = [{"name": "ref.key"}]
    user_configs = {"ref.key": "uservalue"}
    c._process_value_case(
        {"name": "combo", "value": "${ref.key}"},
        "uservalue", tdefs, fm, warnings, user_configs, set())
    # user has the referenced key -> copy user_config_value to config_name.
    assert fm["combo"] == "uservalue"


def test_process_value_case_referenced_key_derivation_method_returns_early(make_comparator):
    c = make_comparator()
    fm, warnings = {}, []
    # connection.host has a derivation method -> early return, nothing written.
    tdefs = [{"name": "connection.host"}]
    c._process_value_case(
        {"name": "combo", "value": "${connection.host}"},
        "v", tdefs, fm, warnings, {}, set())
    assert fm == {}


def test_process_value_case_referenced_key_direct_ref_copy(make_comparator):
    c = make_comparator()
    fm, warnings = {}, []
    # referenced key has no derivation method, value is exactly ${ref} -> copy.
    tdefs = [{"name": "plain.ref"}]
    c._process_value_case(
        {"name": "combo", "value": "${plain.ref}"},
        "thevalue", tdefs, fm, warnings, {}, set())
    assert fm["plain.ref"] == "thevalue"


def test_process_value_case_referenced_key_internal_warns(make_comparator):
    c = make_comparator()
    fm, warnings = {}, []
    tdefs = [{"name": "int.ref", "internal": True}]
    c._process_value_case(
        {"name": "combo", "value": "${int.ref}"},
        "v", tdefs, fm, warnings, {}, set())
    assert fm == {}
    assert any("internal" in w for w in warnings)


def test_process_value_case_referenced_key_not_in_template_semantic(make_comparator):
    c = make_comparator()
    fm, warnings, sml = {}, [], set()
    # referenced key 'missing' is in the value but not in template_config_defs ->
    # _find_referenced_keys only returns keys present in high_level_keys, so to hit
    # the 'not present in template' branch we include it in tdefs names set but not
    # as a real def. Achieve this by having the def list NOT contain it while it's a
    # high level key.  high_level_keys is derived from tdefs names, so include a def
    # whose name is the referenced key but make find_template return None is not
    # possible. Instead test that an unknown ref simply yields no referenced keys.
    tdefs = [{"name": "other"}]
    c._process_value_case(
        {"name": "combo", "value": "${missing}"},
        "v", tdefs, fm, warnings, {}, sml)
    # 'missing' not a high-level key -> no referenced keys processed.
    assert fm == {}
    assert sml == set()


def test_process_value_case_referenced_key_semantic_when_def_none(make_comparator, monkeypatch):
    c = make_comparator()
    fm, warnings, sml = {}, [], set()
    # Force _find_template_config_def_by_name to return None so the else (semantic)
    # branch runs while the key is still recognized as a high-level key.
    tdefs = [{"name": "ref"}]
    monkeypatch.setattr(c, "_find_template_config_def_by_name", lambda name, defs: None)
    c._process_value_case(
        {"name": "combo", "value": "${ref}"},
        "v", tdefs, fm, warnings, {}, sml)
    assert "combo" in sml


# --------------------------------------------------------------------------- #
# _process_switch_case / _process_non_internal_switch_case
# --------------------------------------------------------------------------- #

def test_process_switch_case_internal_warns(make_comparator):
    c = make_comparator()
    fm, warnings, errors, sml = {}, [], [], set()
    ccd = {"name": "x", "switch": {"int.key": {"a": "1"}}}
    tdefs = [{"name": "int.key", "internal": True}]
    c._process_switch_case(ccd, {}, tdefs, fm, warnings, errors, sml)
    assert any("internal" in w for w in warnings)


def test_process_switch_case_key_not_in_template_logs_error(make_comparator):
    c = make_comparator()
    fm, warnings, errors, sml = {}, [], [], set()
    ccd = {"name": "x", "switch": {"unknown.key": {"a": "1"}}}
    c._process_switch_case(ccd, {}, [], fm, warnings, errors, sml)
    # template_config_def is None -> only an error log, no fm/warnings mutation.
    assert fm == {}
    assert warnings == []


def test_process_switch_case_already_in_fm_returns(make_comparator):
    c = make_comparator()
    fm = {"tk": "existing"}
    warnings, errors, sml = [], [], set()
    ccd = {"name": "x", "switch": {"tk": {"a": "1"}}}
    tdefs = [{"name": "tk"}]
    c._process_switch_case(ccd, {}, tdefs, fm, warnings, errors, sml)
    assert fm == {"tk": "existing"}


def test_process_non_internal_switch_reverse_match(make_comparator):
    c = make_comparator()
    fm, warnings, sml = {}, [], set()
    ccd = {"name": "ssl.enabled"}
    tdef = {"name": "ssl.mode.high"}
    switch_mapping = {"required": "true", "disabled": "false"}
    user_configs = {"ssl.enabled": "true"}
    c._process_non_internal_switch_case(ccd, tdef, switch_mapping, user_configs, fm, warnings, sml)
    # reverse switch: user 'true' -> key 'required'
    assert fm["ssl.mode.high"] == "required"


def test_process_non_internal_switch_no_match_warns(make_comparator):
    c = make_comparator()
    fm, warnings, sml = {}, [], set()
    ccd = {"name": "ssl.enabled"}
    tdef = {"name": "ssl.mode.high"}
    switch_mapping = {"required": "true"}
    user_configs = {"ssl.enabled": "maybe"}
    c._process_non_internal_switch_case(ccd, tdef, switch_mapping, user_configs, fm, warnings, sml)
    assert "ssl.mode.high" not in fm
    assert any("does not match" in w for w in warnings)


def test_process_non_internal_switch_has_matchers_with_derivation(make_comparator):
    c = make_comparator()
    fm, warnings, sml = {}, [], set()
    ccd = {"name": "x"}
    # template config name 'connection.host' has a derivation method -> early return.
    tdef = {"name": "connection.host"}
    switch_mapping = {"a": "${connection.host}"}
    c._process_non_internal_switch_case(ccd, tdef, switch_mapping, {}, fm, warnings, sml)
    assert fm == {}
    assert sml == set()


def test_process_non_internal_switch_has_matchers_no_derivation_semantic(make_comparator):
    c = make_comparator()
    fm, warnings, sml = {}, [], set()
    ccd = {"name": "x"}
    tdef = {"name": "no.derivation.key"}
    switch_mapping = {"a": "${no.derivation.key}"}
    c._process_non_internal_switch_case(ccd, tdef, switch_mapping, {}, fm, warnings, sml)
    assert "x" in sml


# --------------------------------------------------------------------------- #
# infer_dynamic_mappings
# --------------------------------------------------------------------------- #

def test_infer_dynamic_mappings_known(make_comparator):
    c = make_comparator()
    fn = "value.converter.reference.subject.name.strategy.mapper"
    assert c.infer_dynamic_mappings(
        fn, "io.confluent.kafka.serializers.subject.TopicNameStrategy") == "TopicNameStrategy"
    assert c.infer_dynamic_mappings(
        fn, "io.confluent.kafka.serializers.subject.RecordNameStrategy") == "RecordNameStrategy"
    assert c.infer_dynamic_mappings(
        fn, "io.confluent.kafka.serializers.subject.TopicRecordNameStrategy") == "TopicRecordNameStrategy"


def test_infer_dynamic_mappings_known_unmapped_value(make_comparator):
    c = make_comparator()
    fn = "value.converter.reference.subject.name.strategy.mapper"
    assert c.infer_dynamic_mappings(fn, "SomethingElse") is None


def test_infer_dynamic_mappings_unknown_function(make_comparator):
    c = make_comparator()
    assert c.infer_dynamic_mappings("unknown.mapper", "anything") is None


def test_infer_dynamic_mappings_empty_name(make_comparator):
    c = make_comparator()
    assert c.infer_dynamic_mappings("", "x") is None


# --------------------------------------------------------------------------- #
# _process_dynamic_mapper_case
# --------------------------------------------------------------------------- #

def test_process_dynamic_mapper_in_template(make_comparator):
    c = make_comparator()
    fm, warnings, sml = {}, [], set()
    ccd = {"name": "strat", "dynamic.mapper": {"name": "fn"}}
    tdefs = [{"name": "strat"}]
    c._process_dynamic_mapper_case(ccd, "uservalue", {}, tdefs, fm, warnings, sml)
    assert fm["strat"] == "uservalue"
    assert sml == set()


def test_process_dynamic_mapper_not_in_template_semantic(make_comparator):
    c = make_comparator()
    fm, warnings, sml = {}, [], set()
    ccd = {"name": "strat", "dynamic.mapper": {"name": "fn"}}
    c._process_dynamic_mapper_case(ccd, "uservalue", {}, [], fm, warnings, sml)
    # not in template defs at all -> semantic match list.
    assert "strat" in sml
    assert fm == {}


# --------------------------------------------------------------------------- #
# _process_null_value_case
# --------------------------------------------------------------------------- #

def test_process_null_value_case(make_comparator):
    c = make_comparator()
    fm = {}
    c._process_null_value_case({"name": "k"}, "v", [], fm, [])
    assert fm["k"] == "v"


# --------------------------------------------------------------------------- #
# _process_user_config_in_connector_config_def (dispatcher)
# --------------------------------------------------------------------------- #

def test_dispatch_value_case(make_comparator):
    c = make_comparator()
    fm = {}
    c._process_user_config_in_connector_config_def(
        {"name": "k", "value": "v"}, "v", [], fm, [], [], {}, set())
    assert fm["k"] == "v"


def test_dispatch_switch_case(make_comparator):
    c = make_comparator()
    fm, warnings, errors, sml = {}, [], [], set()
    ccd = {"name": "x", "switch": {"tk": {"req": "true"}}}
    tdefs = [{"name": "tk"}]
    user_configs = {"x": "true"}
    c._process_user_config_in_connector_config_def(
        ccd, "true", tdefs, fm, warnings, errors, user_configs, sml)
    assert fm["tk"] == "req"


def test_dispatch_dynamic_mapper_case(make_comparator):
    c = make_comparator()
    fm, sml = {}, set()
    ccd = {"name": "strat", "dynamic.mapper": {"name": "fn"}}
    tdefs = [{"name": "strat"}]
    c._process_user_config_in_connector_config_def(
        ccd, "uval", tdefs, fm, [], [], {}, sml)
    assert fm["strat"] == "uval"


def test_dispatch_null_value_case(make_comparator):
    c = make_comparator()
    fm = {}
    # no value/switch/dynamic.mapper -> null value case.
    c._process_user_config_in_connector_config_def(
        {"name": "k"}, "v", [], fm, [], [], {}, set())
    assert fm["k"] == "v"


# --------------------------------------------------------------------------- #
# _find_template_config_def_by_name
# --------------------------------------------------------------------------- #

def test_find_template_config_def_by_name_found(make_comparator):
    c = make_comparator()
    tdefs = [{"name": "a"}, {"name": "b"}]
    assert c._find_template_config_def_by_name("b", tdefs) == {"name": "b"}


def test_find_template_config_def_by_name_not_found(make_comparator):
    c = make_comparator()
    assert c._find_template_config_def_by_name("z", [{"name": "a"}]) is None


# --------------------------------------------------------------------------- #
# _find_referenced_keys
# --------------------------------------------------------------------------- #

def test_find_referenced_keys(make_comparator):
    c = make_comparator()
    keys = c._find_referenced_keys("${a}/${b}/${c}", {"a", "c"})
    assert keys == {"a", "c"}


def test_find_referenced_keys_none(make_comparator):
    c = make_comparator()
    assert c._find_referenced_keys("noreferences", {"a"}) == set()
