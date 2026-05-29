"""
Shared pytest fixtures for the connect-migration-utility test suite.

Key design points:
  * `src/` is put on sys.path so the flat top-level modules
    (`connector_comparator`, `comparator.*`, `semantic_matcher`, ...) import.
  * `make_comparator` builds a ConnectorComparator without any network or the
    optional sentence-transformer model. Its FM template directory points at a
    temp dir you control via `write_template`.
  * The semantic matcher is replaced with a stub by default so unit tests are
    deterministic and never touch the (optional) ML model.
"""

import json
import sys
from pathlib import Path

import pytest

SRC = Path(__file__).resolve().parent.parent / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from connector_comparator import ConnectorComparator  # noqa: E402


class StubMatchResult:
    """Mimics the result object returned by SemanticMatcher.find_best_match."""

    def __init__(self, matched_fm_property, match_type="semantic", similarity_score=0.99):
        self.matched_fm_property = matched_fm_property
        self.match_type = match_type
        self.similarity_score = similarity_score


class StubSemanticMatcher:
    """Deterministic stand-in for SemanticMatcher.

    By default it matches nothing (returns None). Tests can set
    `.forced_match` to a dict (an fm property def) to force a hit, or assign a
    custom callable to `.find_best_match`.
    """

    def __init__(self):
        self.forced_match = None
        self.calls = []

    def find_best_match(self, sm_prop, fm_properties_dict, semantic_threshold=0.7):
        self.calls.append((sm_prop, fm_properties_dict, semantic_threshold))
        if self.forced_match is not None:
            return StubMatchResult(self.forced_match)
        return None


def _template(template_id, connector_class, connector_type="SOURCE",
              connector_configs=None, config_defs=None):
    """Build one FM template dict (the inner item of the 'templates' list)."""
    return {
        "template_id": template_id,
        "connector_type": connector_type,
        "connector.class": connector_class,
        "connector_configs": connector_configs or [],
        "config_defs": config_defs or [],
    }


def _wrap(*templates):
    """Wrap inner template dicts in the top-level {'templates': [...]} envelope."""
    return {"templates": list(templates)}


@pytest.fixture
def template_factory():
    """Expose the template builders to tests."""
    return _template, _wrap


@pytest.fixture
def write_template(tmp_path):
    """Write a template dict (top-level envelope) to <fm_dir>/<name>.json.

    Returns the fm template directory Path. Call repeatedly to add files.
    """
    fm_dir = tmp_path / "fm_templates"
    fm_dir.mkdir(exist_ok=True)

    def _write(name, template_envelope):
        path = fm_dir / f"{name}.json"
        path.write_text(json.dumps(template_envelope))
        return path

    _write.dir = fm_dir
    return _write


@pytest.fixture
def make_comparator(tmp_path):
    """Factory that builds a ConnectorComparator wired for offline testing.

    Usage:
        c = make_comparator()                       # empty fm template dir
        c = make_comparator(fm_dir=write_template.dir, debezium_version='v1')
    """
    created = []

    def _make(fm_dir=None, stub_matcher=True, input_file=None, **kwargs):
        out = tmp_path / "out"
        out.mkdir(exist_ok=True)
        if fm_dir is None:
            fm_dir = tmp_path / "fm_templates"
            fm_dir.mkdir(exist_ok=True)
        comp = ConnectorComparator(
            input_file=input_file or (tmp_path / "in.json"),
            output_dir=out,
            **kwargs,
        )
        comp.fm_template_dir = Path(fm_dir)
        if stub_matcher:
            comp.semantic_matcher = StubSemanticMatcher()
        created.append(comp)
        return comp

    return _make


@pytest.fixture
def jdbc_source_template(template_factory):
    """A realistic-ish MySQL JDBC source template (single envelope)."""
    _t, _wrap = template_factory
    config_defs = [
        {"name": "connection.host", "type": "STRING", "required": True},
        {"name": "connection.port", "type": "INT", "required": True},
        {"name": "connection.user", "type": "STRING", "required": True},
        {"name": "connection.password", "type": "PASSWORD", "required": True},
        {"name": "db.name", "type": "STRING", "required": False},
        {"name": "output.data.format", "type": "STRING", "required": False,
         "recommended_values": ["AVRO", "JSON", "JSON_SR", "PROTOBUF"]},
        {"name": "tasks.max", "type": "INT", "required": False},
    ]
    connector_configs = [
        {"name": "connection.attempts", "value": "3"},
        {"name": "dialect.name", "value": "MySqlDatabaseDialect"},
        {"name": "connection.host", "value": "${connection.host}"},
        {"name": "connection.port", "value": "${connection.port}"},
    ]
    return _wrap(_t("MySqlSource", "io.confluent.connect.jdbc.JdbcSourceConnector",
                    "SOURCE", connector_configs, config_defs))


@pytest.fixture
def sample_jdbc_config():
    return {
        "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
        "connection.url": "jdbc:mysql://db.example.com:3306/inventory?user=admin&password=secret",
        "tasks.max": "2",
        "value.converter": "io.confluent.connect.avro.AvroConverter",
    }