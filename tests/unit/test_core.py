"""Unit tests for ConnectorComparator's own methods:

  * __init__ behavior (debezium version normalization, worker auth, mapping inverses)
  * connector_pack_type
  * parse_connector_file (staticmethod) across all input shapes
"""

import json
import logging
from pathlib import Path

import pytest
from requests.auth import HTTPBasicAuth

from connector_comparator import ConnectorComparator


# --------------------------------------------------------------------------- #
# __init__ behavior
# --------------------------------------------------------------------------- #

def test_init_invalid_debezium_version_defaults_to_v2(make_comparator):
    c = make_comparator(debezium_version="v9")
    assert c.debezium_version == "v2"


def test_init_debezium_version_v1_preserved(make_comparator):
    c = make_comparator(debezium_version="v1")
    assert c.debezium_version == "v1"


def test_init_debezium_version_uppercase_lowercased(make_comparator):
    c = make_comparator(debezium_version="V1")
    assert c.debezium_version == "v1"


def test_init_worker_auth_set_when_both_credentials(make_comparator):
    c = make_comparator(worker_username="user", worker_password="pass")
    assert c.worker_auth is not None
    assert isinstance(c.worker_auth, HTTPBasicAuth)
    assert c.worker_auth.username == "user"
    assert c.worker_auth.password == "pass"


def test_init_worker_auth_none_when_only_username(make_comparator):
    c = make_comparator(worker_username="user")
    assert c.worker_auth is None


def test_init_worker_auth_none_when_only_password(make_comparator):
    c = make_comparator(worker_password="pass")
    assert c.worker_auth is None


def test_init_debezium_mappings_are_inverses(make_comparator):
    c = make_comparator()
    # v2_to_v1 must be the exact inverse of v1_to_v2
    assert c.debezium_v2_to_v1_mapping == {v: k for k, v in c.debezium_v1_to_v2_mapping.items()}
    for v1_class, v2_class in c.debezium_v1_to_v2_mapping.items():
        assert c.debezium_v2_to_v1_mapping[v2_class] == v1_class


# --------------------------------------------------------------------------- #
# connector_pack_type
# --------------------------------------------------------------------------- #

def test_connector_pack_type_premium(make_comparator):
    c = make_comparator()
    premium_class = next(iter(c.premium_pack_connector_dict))
    assert c.connector_pack_type(premium_class) == "premium_pack_connectors"


def test_connector_pack_type_commercial(make_comparator):
    c = make_comparator()
    # pick a commercial class that is NOT also in the premium dict
    commercial_class = next(
        k for k in c.commercial_pack_connector_dict
        if k not in c.premium_pack_connector_dict
    )
    assert c.connector_pack_type(commercial_class) == "commercial_pack_connectors"


def test_connector_pack_type_unknown(make_comparator):
    c = make_comparator()
    assert c.connector_pack_type("unknown") == "unknown_pack_connectors"


def test_connector_pack_type_non_commercial(make_comparator):
    c = make_comparator()
    assert c.connector_pack_type("com.example.SomeRandomConnector") == "non_commercial_pack_connectors"


# --------------------------------------------------------------------------- #
# parse_connector_file (staticmethod)
# --------------------------------------------------------------------------- #

def _write_json(tmp_path: Path, name: str, payload) -> Path:
    path = tmp_path / name
    path.write_text(json.dumps(payload))
    return path


@pytest.fixture
def logger():
    return logging.getLogger("test_parse")


def test_parse_connectors_envelope(tmp_path, logger):
    payload = {
        "connectors": {
            "conn-a": {"name": "conn-a", "config": {"connector.class": "X"}},
            "conn-b": {"name": "conn-b", "config": {"connector.class": "Y"}},
        }
    }
    path = _write_json(tmp_path, "envelope.json", payload)
    out = {}
    ConnectorComparator.parse_connector_file(path, out, logger)
    assert set(out) == {"conn-a", "conn-b"}
    assert out["conn-a"]["config"]["connector.class"] == "X"


def test_parse_list_of_name_config(tmp_path, logger):
    payload = [
        {"name": "conn-1", "config": {"connector.class": "X"}},
        {"name": "conn-2", "config": {"connector.class": "Y"}},
    ]
    path = _write_json(tmp_path, "list.json", payload)
    out = {}
    ConnectorComparator.parse_connector_file(path, out, logger)
    assert set(out) == {"conn-1", "conn-2"}
    assert out["conn-2"]["config"]["connector.class"] == "Y"


def test_parse_single_name_config_dict(tmp_path, logger):
    payload = {"name": "solo", "config": {"connector.class": "Z"}}
    path = _write_json(tmp_path, "single.json", payload)
    out = {}
    ConnectorComparator.parse_connector_file(path, out, logger)
    assert set(out) == {"solo"}
    assert out["solo"]["config"]["connector.class"] == "Z"


def test_parse_keyed_dict_of_name_config(tmp_path, logger):
    payload = {
        "my-conn": {"name": "my-conn", "config": {"connector.class": "K"}},
    }
    path = _write_json(tmp_path, "keyed.json", payload)
    out = {}
    ConnectorComparator.parse_connector_file(path, out, logger)
    assert set(out) == {"my-conn"}
    assert out["my-conn"]["config"]["connector.class"] == "K"


def test_parse_dict_with_info_subdict(tmp_path, logger):
    # values contain an "info"/"INFO" sub-dict holding name + config
    payload = {
        "wrapped": {
            "status": {"state": "RUNNING"},
            "INFO": {"name": "wrapped", "config": {"connector.class": "I"}},
        }
    }
    path = _write_json(tmp_path, "info.json", payload)
    out = {}
    ConnectorComparator.parse_connector_file(path, out, logger)
    assert set(out) == {"wrapped"}
    assert out["wrapped"]["config"]["connector.class"] == "I"


def test_parse_nonexistent_file_raises(tmp_path, logger):
    missing = tmp_path / "does_not_exist.json"
    with pytest.raises(FileNotFoundError):
        ConnectorComparator.parse_connector_file(missing, {}, logger)


def test_parse_non_json_path_returns_silently(tmp_path, logger):
    path = tmp_path / "notjson.txt"
    path.write_text("name,config\nfoo,bar")
    out = {}
    # non-.json suffix -> returns without touching out_dict and without raising
    ConnectorComparator.parse_connector_file(path, out, logger)
    assert out == {}