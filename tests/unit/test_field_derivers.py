from connect_migrate.mapper.jdbc.url_parser import JdbcUrlParser
from connect_migrate.mapper.properties.derivations import FieldDeriver
from connect_migrate.mapper.properties.derivations.connection_fields import (
    ConnectionFieldDeriver,
)


def make_field_deriver():
    return FieldDeriver(JdbcUrlParser())


class TestFieldDeriverFacade:
    def test_lookup_known_key_returns_callable(self):
        fd = make_field_deriver()
        m = fd.lookup("connection.host")
        assert m is not None
        assert m.__name__ == "_derive_connection_host"

    def test_lookup_unknown_key_returns_none(self):
        assert make_field_deriver().lookup("not.a.real.key") is None

    def test_derive_invokes_correct_method(self):
        fd = make_field_deriver()
        val = fd.derive(
            "connection.host",
            user_configs={"connection.url": "jdbc:mysql://db.example.com:3306/d"},
            fm_configs={},
        )
        assert val == "db.example.com"

    def test_subject_name_strategy_dispatches_for_multiple_keys(self):
        # 5 of the 7 subject-name-strategy dispatch entries map to the same
        # method — pin that all of them route there.
        fd = make_field_deriver()
        keys = [
            "key.converter.key.subject.name.strategy",
            "value.converter.value.subject.name.strategy",
            "key.subject.name.strategy",
            "subject.name.strategy",
            "value.subject.name.strategy",
        ]
        for k in keys:
            m = fd.lookup(k)
            assert m is not None, k
            assert m.__name__ == "_derive_subject_name_strategy"


class TestConnectionFieldDeriver:
    def test_host_from_jdbc(self):
        d = ConnectionFieldDeriver(JdbcUrlParser())
        out = d.derive(
            "connection.host",
            user_configs={"connection.url": "jdbc:postgresql://pg.local:5432/orders"},
            fm_configs={},
        )
        assert out == "pg.local"

    def test_port_from_jdbc(self):
        d = ConnectionFieldDeriver(JdbcUrlParser())
        out = d.derive(
            "connection.port",
            user_configs={"connection.url": "jdbc:postgresql://pg.local:5432/orders"},
            fm_configs={},
        )
        assert out == "5432"

    def test_no_connection_url_returns_none(self):
        d = ConnectionFieldDeriver(JdbcUrlParser())
        assert d.derive("connection.host", user_configs={}, fm_configs={}) is None


class TestDerivationBaseHelpers:
    """The placeholder helpers on the base class are used by many derivers."""

    def test_is_placeholder(self):
        d = ConnectionFieldDeriver(JdbcUrlParser())
        assert d._is_placeholder("${foo}")
        assert not d._is_placeholder("plain")

    def test_extract_placeholder_name(self):
        d = ConnectionFieldDeriver(JdbcUrlParser())
        assert d._extract_placeholder_name("${connection.host}") == "connection.host"
        assert d._extract_placeholder_name("plain") == "plain"

    def test_resolve_template_default_from_fm_configs(self):
        d = ConnectionFieldDeriver(JdbcUrlParser())
        assert d._resolve_template_default("${a}", {"a": "value"}) == "value"

    def test_resolve_template_default_passthrough(self):
        d = ConnectionFieldDeriver(JdbcUrlParser())
        assert d._resolve_template_default("literal", {}) == "literal"

    def test_get_template_default_value(self):
        d = ConnectionFieldDeriver(JdbcUrlParser())
        defs = [{"name": "x", "default_value": "default-x"}, {"name": "y"}]
        assert d._get_template_default_value(defs, "x") == "default-x"
        assert d._get_template_default_value(defs, "y") is None
        assert d._get_template_default_value(defs, "missing") is None
