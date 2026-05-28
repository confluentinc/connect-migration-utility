from connect_migrate.utils.sensitive_data_redactor import (
    REDACTED_PLACEHOLDER,
    SensitiveDataRedactor,
)


class TestIsSensitive:
    def test_static_key_exact_match(self):
        r = SensitiveDataRedactor()
        assert r.is_sensitive("password")
        assert r.is_sensitive("aws.access.key.id")
        assert r.is_sensitive("ssl.keystore.password")

    def test_pattern_match_password_substring(self):
        r = SensitiveDataRedactor()
        assert r.is_sensitive("some.thing.password.here")

    def test_pattern_match_token(self):
        r = SensitiveDataRedactor()
        assert r.is_sensitive("api.token")

    def test_case_insensitive(self):
        r = SensitiveDataRedactor()
        assert r.is_sensitive("CONNECTION.PASSWORD")
        assert r.is_sensitive("MyApiToken")

    def test_non_sensitive_key(self):
        r = SensitiveDataRedactor()
        assert not r.is_sensitive("topic")
        assert not r.is_sensitive("connection.host")


class TestRedact:
    def test_redacts_top_level_password(self):
        r = SensitiveDataRedactor()
        out = r.redact({"name": "c1", "password": "hunter2"})
        assert out["name"] == "c1"
        assert out["password"] == REDACTED_PLACEHOLDER

    def test_redacts_nested_dict(self):
        r = SensitiveDataRedactor()
        out = r.redact({"config": {"connection.password": "x", "topic": "t"}})
        assert out["config"]["connection.password"] == REDACTED_PLACEHOLDER
        assert out["config"]["topic"] == "t"

    def test_redacts_within_list_of_dicts(self):
        r = SensitiveDataRedactor()
        out = r.redact({"items": [{"password": "p"}, {"name": "n"}]})
        assert out["items"][0]["password"] == REDACTED_PLACEHOLDER
        assert out["items"][1]["name"] == "n"

    def test_non_dict_values_passthrough(self):
        r = SensitiveDataRedactor()
        out = r.redact({"count": 3, "names": ["a", "b"]})
        assert out["count"] == 3
        assert out["names"] == ["a", "b"]

    def test_redact_is_pure(self):
        # The redactor returns a new dict and must not mutate the input.
        r = SensitiveDataRedactor()
        original = {"password": "secret", "host": "x"}
        snapshot = dict(original)
        _ = r.redact(original)
        assert original == snapshot
