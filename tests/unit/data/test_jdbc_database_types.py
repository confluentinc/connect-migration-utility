"""Tests for src/data/jdbc_database_types.py."""

import pytest

from data.jdbc_database_types import JDBC_DATABASE_TYPES


REQUIRED_KEYS = ("url_patterns", "default_port")
EXPECTED_DB_TYPES = {"mysql", "oracle", "sqlserver", "postgresql", "snowflake"}


class TestJdbcDatabaseTypes:
    def test_is_a_dict(self):
        assert isinstance(JDBC_DATABASE_TYPES, dict)

    def test_has_all_five_expected_db_types(self):
        assert set(JDBC_DATABASE_TYPES.keys()) == EXPECTED_DB_TYPES

    @pytest.mark.parametrize("db_type", sorted(EXPECTED_DB_TYPES))
    def test_each_db_type_entry_has_required_keys(self, db_type):
        entry = JDBC_DATABASE_TYPES[db_type]
        for key in REQUIRED_KEYS:
            assert key in entry, f"{db_type} entry missing required key '{key}'"

    @pytest.mark.parametrize("db_type", sorted(EXPECTED_DB_TYPES))
    def test_url_patterns_is_non_empty_list_of_strings(self, db_type):
        patterns = JDBC_DATABASE_TYPES[db_type]["url_patterns"]
        assert isinstance(patterns, list)
        assert patterns, f"{db_type} has empty url_patterns"
        for p in patterns:
            assert isinstance(p, str)
            assert p

    @pytest.mark.parametrize("db_type", sorted(EXPECTED_DB_TYPES))
    def test_default_port_is_numeric_string(self, db_type):
        port = JDBC_DATABASE_TYPES[db_type]["default_port"]
        assert isinstance(port, str)
        assert port.isdigit(), f"{db_type} default_port should be numeric, got {port!r}"
        assert 1 <= int(port) <= 65535

    @pytest.mark.parametrize(
        "db_type,expected_port",
        [
            ("mysql", "3306"),
            ("oracle", "1521"),
            ("sqlserver", "1433"),
            ("postgresql", "5432"),
            ("snowflake", "443"),
        ],
    )
    def test_default_ports_match_known_defaults(self, db_type, expected_port):
        assert JDBC_DATABASE_TYPES[db_type]["default_port"] == expected_port

    @pytest.mark.parametrize(
        "db_type,expected_patterns",
        [
            ("mysql", ["mysql", "mariadb"]),
            ("oracle", ["oracle", "oracle:thin"]),
            ("sqlserver", ["sqlserver", "mssql"]),
            ("postgresql", ["postgresql", "postgres"]),
            ("snowflake", ["snowflake"]),
        ],
    )
    def test_url_patterns_match_known(self, db_type, expected_patterns):
        assert JDBC_DATABASE_TYPES[db_type]["url_patterns"] == expected_patterns

    def test_url_patterns_have_no_duplicates_within_a_type(self):
        for db_type, entry in JDBC_DATABASE_TYPES.items():
            patterns = entry["url_patterns"]
            assert len(patterns) == len(set(patterns)), f"{db_type} has duplicate url_patterns"
