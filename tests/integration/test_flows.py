"""End-to-end / integration tests for ConnectorComparator orchestration:

  * process_connectors
  * process_tco_information (ConfigDiscovery mocked)
  * transformSMToFm (template path, /translate API path, error path)

Everything runs offline: FM templates come from the write_template fixture and
the semantic matcher is stubbed by make_comparator.
"""

import json

import pytest


JDBC_CLASS = "io.confluent.connect.jdbc.JdbcSourceConnector"


# --------------------------------------------------------------------------- #
# transformSMToFm
# --------------------------------------------------------------------------- #

def test_transform_missing_connector_class_error(make_comparator):
    c = make_comparator()
    result = c.transformSMToFm("conn", {"tasks.max": "1"})
    assert result["errors"]  # non-empty
    assert any("connector.class" in e for e in result["errors"])


def test_transform_template_path(make_comparator, write_template, jdbc_source_template,
                                 sample_jdbc_config):
    fm_dir = write_template("MySqlSource_resolved_templates", jdbc_source_template).parent
    c = make_comparator(fm_dir=fm_dir)

    result = c.transformSMToFm("my-jdbc", sample_jdbc_config)

    assert set(["name", "fm_configs", "warnings", "errors"]).issubset(result.keys())
    # connector.class becomes the template_id from the matched template
    assert result["fm_configs"]["connector.class"] == "MySqlSource"
    assert result["fm_configs"]["name"] == "my-jdbc"
    assert result["name"] == "my-jdbc"


def test_transform_no_template_found(make_comparator):
    # empty fm dir -> no template -> basic structure with error
    c = make_comparator()
    result = c.transformSMToFm("orphan", {"connector.class": "com.example.Nope"})
    assert any("No FM template found" in e for e in result["errors"])
    assert result["fm_configs"]["connector.class"] == "com.example.Nope"
    assert result["fm_configs"]["name"] == "orphan"


def test_transform_translate_api_path(make_comparator):
    c = make_comparator(env_id="env-1", lkc_id="lkc-1", bearer_token="tok-1")

    def fake_translate(connector_name, config_dict):
        return {
            "config": {"connector.class": "FmJdbcSource", "extra": "from-api"},
            "warnings": ["api-warn"],
            "errors": [],
        }

    c._translate_connector_config_via_api = fake_translate

    result = c.transformSMToFm("api-conn", {"connector.class": JDBC_CLASS, "tasks.max": "1"})

    assert result["fm_configs"]["connector.class"] == "FmJdbcSource"
    assert result["fm_configs"]["extra"] == "from-api"
    # name injected by transformSMToFm when missing from API result
    assert result["fm_configs"]["name"] == "api-conn"
    assert result["name"] == "api-conn"
    assert "api-warn" in result["warnings"]


# --------------------------------------------------------------------------- #
# process_connectors
# --------------------------------------------------------------------------- #

def test_process_connectors_success(make_comparator, write_template, jdbc_source_template,
                                    sample_jdbc_config, tmp_path):
    fm_dir = write_template("MySqlSource_resolved_templates", jdbc_source_template).parent

    input_payload = {
        "connectors": {
            "my-jdbc": {"name": "my-jdbc", "config": sample_jdbc_config},
        }
    }
    input_file = tmp_path / "connectors_input.json"
    input_file.write_text(json.dumps(input_payload))

    c = make_comparator(fm_dir=fm_dir)
    c.input_file = input_file

    result = c.process_connectors()
    assert result is not None
    assert "my-jdbc" in result
    entry = result["my-jdbc"]
    assert "config" in entry
    assert "mapping_errors" in entry
    assert "mapping_warnings" in entry
    assert entry["config"]["connector.class"] == "MySqlSource"
    assert entry["config"]["name"] == "my-jdbc"


def test_process_connectors_empty_input_returns_none(make_comparator, tmp_path):
    input_file = tmp_path / "empty.json"
    input_file.write_text(json.dumps({}))
    c = make_comparator()
    c.input_file = input_file
    assert c.process_connectors() is None


def test_process_connectors_parse_fail_returns_none(make_comparator, tmp_path):
    # invalid JSON -> parse_connector_file swallows the exception, dict stays empty
    input_file = tmp_path / "broken.json"
    input_file.write_text("{ this is not valid json")
    c = make_comparator()
    c.input_file = input_file
    assert c.process_connectors() is None


# --------------------------------------------------------------------------- #
# process_tco_information
# --------------------------------------------------------------------------- #

def test_process_tco_information(make_comparator, monkeypatch):
    statuses = {
        "conn-a": {
            "tasks_status": [
                {"id": 0, "worker_id": "worker1:8083", "state": "RUNNING"},
                {"id": 1, "worker_id": "worker2:8083", "state": "RUNNING"},
            ]
        },
        "conn-b": {
            "tasks_status": [
                {"id": 0, "worker_id": "worker1:8083", "state": "RUNNING"},
            ]
        },
    }
    configs = [
        {"name": "conn-a", "type": "source",
         "config": {"connector.class": JDBC_CLASS}},
        {"name": "conn-b", "type": "sink",
         "config": {"connector.class": "com.example.RandomSink"}},
    ]

    monkeypatch.setattr(
        "connector_comparator.ConfigDiscovery.get_connector_statuses_from_worker",
        staticmethod(lambda *a, **k: statuses),
    )
    monkeypatch.setattr(
        "connector_comparator.ConfigDiscovery.get_connector_configs_from_worker",
        staticmethod(lambda *a, **k: configs),
    )

    c = make_comparator(worker_urls=["http://worker:8083"])
    tco = c.process_tco_information()

    assert tco["total_connectors"] == 2
    assert tco["total_tasks"] == 3
    # tasks landed on worker1 and worker2
    assert tco["worker_node_count"] == 2
    assert set(tco["worker_node_task_map"]) == {"worker1", "worker2"}

    # tco_info.json written to output_dir
    tco_file = c.output_dir / "tco_info.json"
    assert tco_file.is_file()
    written = json.loads(tco_file.read_text())
    assert written["total_connectors"] == 2
    assert written["total_tasks"] == 3