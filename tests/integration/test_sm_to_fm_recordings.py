"""Recorded SM -> FM regression tests, adapted from PR #34.

Each JSON file under tests/fixtures/discovered_configs/ pairs a real-world
Self-Managed connector config with the Fully-Managed config the previous
implementation produced. We run the same SM config through the current
ConnectorMapper.transform_sm_to_fm() and assert the output still matches.

These fixtures were copied verbatim from PR confluentinc/connect-migration-utility#34
(branch refactor_code). The recordings were made before any of the review-driven
phases on this branch, so a clean pass means the refactor preserved behavior on
real-world inputs.
"""

import json
import os
from pathlib import Path

import pytest

from connect_migrate.mapper.connector_mapper import ConnectorMapper


REPO_ROOT = Path(__file__).resolve().parents[2]
RECORDINGS_DIR = REPO_ROOT / "tests" / "fixtures" / "discovered_configs"


def _recording_files():
    return sorted(p for p in RECORDINGS_DIR.glob("*.json") if p.is_file())


def _load_pair(path: Path):
    data = json.loads(path.read_text())
    return {
        "name": data["name"],
        "sm_config": data["sm_config"],
        "expected_fm_config": data["config"],
        "expected_mapping_errors": data.get("mapping_errors", []),
    }


@pytest.fixture(scope="module")
def chdir_repo_root():
    """ConnectorMapper loads templates from Path('templates/fm') relative to cwd."""
    prev = Path.cwd()
    os.chdir(REPO_ROOT)
    try:
        yield REPO_ROOT
    finally:
        os.chdir(prev)


@pytest.fixture(scope="module")
def mapper(chdir_repo_root, tmp_path_factory):
    """A single ConnectorMapper reused across all recordings — its __init__ does
    heavy work (template loader, semantic matcher, HTTP sessions) so per-test
    construction is wasteful."""
    tmp = tmp_path_factory.mktemp("recordings")
    return ConnectorMapper(
        input_file=tmp / "input.json",
        output_dir=tmp,
    )


@pytest.mark.parametrize(
    "recording_path",
    _recording_files(),
    ids=lambda p: p.stem,
)
class TestSmToFmRecordings:
    def test_fm_config_matches_recording(self, mapper, recording_path):
        pair = _load_pair(recording_path)
        result = mapper.transform_sm_to_fm(pair["name"], pair["sm_config"])
        actual = result["fm_configs"]
        expected = pair["expected_fm_config"]
        missing = set(expected) - set(actual)
        extra = set(actual) - set(expected)
        differing = {
            k: (expected[k], actual[k])
            for k in set(expected) & set(actual)
            if expected[k] != actual[k]
        }
        assert actual == expected, (
            f"FM config diverged from recorded file {recording_path.name}.\n"
            f"Missing keys: {missing}\n"
            f"Extra keys: {extra}\n"
            f"Differing values: {differing}"
        )

    def test_no_unexpected_mapping_errors(self, mapper, recording_path):
        pair = _load_pair(recording_path)
        result = mapper.transform_sm_to_fm(pair["name"], pair["sm_config"])
        assert result["errors"] == pair["expected_mapping_errors"], (
            f"For {recording_path.name}: expected mapping_errors "
            f"{pair['expected_mapping_errors']}, got {result['errors']}"
        )

    def test_connector_class_rewritten_to_template_id(self, mapper, recording_path):
        pair = _load_pair(recording_path)
        result = mapper.transform_sm_to_fm(pair["name"], pair["sm_config"])
        assert result["fm_configs"]["connector.class"] == pair["expected_fm_config"]["connector.class"]