"""Tests for src/data/debezium_mappings.py."""

import pytest

from data.debezium_mappings import DEBEZIUM_V1_TO_V2_MAPPING, DEBEZIUM_V2_TO_V1_MAPPING


class TestDebeziumV1ToV2Mapping:
    def test_mapping_is_a_dict(self):
        assert isinstance(DEBEZIUM_V1_TO_V2_MAPPING, dict)

    def test_mapping_has_expected_four_entries(self):
        # MySQL, Postgres, SqlServer, MariaDB.
        assert len(DEBEZIUM_V1_TO_V2_MAPPING) == 4

    @pytest.mark.parametrize(
        "v1_class,v2_class",
        [
            (
                "io.debezium.connector.mysql.MySqlConnector",
                "io.debezium.connector.v2.mysql.MySqlConnectorV2",
            ),
            (
                "io.debezium.connector.postgresql.PostgresConnector",
                "io.debezium.connector.v2.postgresql.PostgresConnectorV2",
            ),
            (
                "io.debezium.connector.sqlserver.SqlServerConnector",
                "io.debezium.connector.v2.sqlserver.SqlServerConnectorV2",
            ),
            (
                "io.debezium.connector.mariadb.MariaDbConnector",
                "io.debezium.connector.v2.mariadb.MariaDbConnector",
            ),
        ],
    )
    def test_each_v1_class_maps_to_expected_v2(self, v1_class, v2_class):
        assert DEBEZIUM_V1_TO_V2_MAPPING[v1_class] == v2_class

    def test_keys_are_unique_v1_class_names(self):
        # All keys distinct (dict guarantee, but assert anyway as a regression guard).
        assert len(set(DEBEZIUM_V1_TO_V2_MAPPING.keys())) == len(DEBEZIUM_V1_TO_V2_MAPPING)

    def test_values_are_unique_v2_class_names(self):
        # No two v1 classes should map to the same v2 class.
        assert len(set(DEBEZIUM_V1_TO_V2_MAPPING.values())) == len(DEBEZIUM_V1_TO_V2_MAPPING)

    def test_no_value_equals_its_key(self):
        for v1, v2 in DEBEZIUM_V1_TO_V2_MAPPING.items():
            assert v1 != v2

    def test_all_keys_and_values_are_strings(self):
        for v1, v2 in DEBEZIUM_V1_TO_V2_MAPPING.items():
            assert isinstance(v1, str)
            assert isinstance(v2, str)
            assert v1
            assert v2


class TestDebeziumV2ToV1Mapping:
    def test_v2_to_v1_is_inverse_of_v1_to_v2(self):
        for v1, v2 in DEBEZIUM_V1_TO_V2_MAPPING.items():
            assert DEBEZIUM_V2_TO_V1_MAPPING[v2] == v1

    def test_v2_to_v1_has_same_cardinality(self):
        assert len(DEBEZIUM_V2_TO_V1_MAPPING) == len(DEBEZIUM_V1_TO_V2_MAPPING)

    def test_v2_to_v1_keys_match_v1_to_v2_values(self):
        assert set(DEBEZIUM_V2_TO_V1_MAPPING.keys()) == set(DEBEZIUM_V1_TO_V2_MAPPING.values())
