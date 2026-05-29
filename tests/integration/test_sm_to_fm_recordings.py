"""End-to-end SM -> FM regression tests over recorded config files.

Pattern adapted from confluentinc/connect-migration-utility#36.

Each JSON file under tests/fixtures/ pairs a real-world Self-Managed connector
config (`sm_config`) with the Fully-Managed config the implementation produced
(`config`), plus the `mapping_errors` / `mapping_warnings` it emitted. We run
the same `sm_config` through the current ConnectorComparator.transformSMToFm()
and assert the output still matches the recording.

The recordings were captured on the pre-split implementation, so a clean pass
here means the mixin refactor preserved behavior on real connector inputs.
These run against the real FM templates in templates/fm/ (no network, no
credentials -> the template-based translation path) and need no ML model: none
of these fixtures depend on semantic matching.
"""

import json
import os
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC = REPO_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from connector_comparator import ConnectorComparator  # noqa: E402

FIXTURE_DIRS = [
    REPO_ROOT / "tests" / "fixtures" / "discovered_configs",
    REPO_ROOT / "tests" / "fixtures" / "sm_fm_pairs",
]


def _recording_files():
    files = []
    for d in FIXTURE_DIRS:
        files.extend(p for p in d.glob("*.json") if p.is_file())
    return sorted(files)


def _load_pair(path: Path):
    data = json.loads(path.read_text())
    return {
        "name": data["name"],
        "sm_config": data["sm_config"],
        "expected_fm_config": data["config"],
        "expected_mapping_errors": data.get("mapping_errors", []),
        "expected_mapping_warnings": data.get("mapping_warnings"),
    }


@pytest.fixture(scope="module")
def chdir_repo_root():
    """transformSMToFm loads templates from Path('templates/fm') relative to cwd."""
    prev = Path.cwd()
    os.chdir(REPO_ROOT)
    try:
        yield REPO_ROOT
    finally:
        os.chdir(prev)


@pytest.fixture(scope="module")
def comparator(chdir_repo_root, tmp_path_factory):
    """One ConnectorComparator reused across all recordings.

    No credentials are passed, so transformSMToFm takes the template-based
    translation path (never the Confluent Cloud /translate API). The real
    semantic matcher is used; with no local model present it returns no
    matches, which is exactly the condition under which these fixtures were
    recorded.
    """
    tmp = tmp_path_factory.mktemp("recordings")
    return ConnectorComparator(input_file=tmp / "input.json", output_dir=tmp)


# Guard against a silently-empty fixture directory turning the whole suite green.
def test_recordings_present():
    assert len(_recording_files()) >= 18


@pytest.mark.parametrize("recording_path", _recording_files(), ids=lambda p: p.stem)
class TestSmToFmRecordings:
    def test_fm_config_matches_recording(self, comparator, recording_path):
        pair = _load_pair(recording_path)
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
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
            f"Missing keys: {sorted(missing)}\n"
            f"Extra keys: {sorted(extra)}\n"
            f"Differing values: {differing}"
        )

    def test_connector_class_rewritten_to_template_id(self, comparator, recording_path):
        pair = _load_pair(recording_path)
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        assert (
            result["fm_configs"]["connector.class"]
            == pair["expected_fm_config"]["connector.class"]
        )

    def test_no_unexpected_mapping_errors(self, comparator, recording_path):
        pair = _load_pair(recording_path)
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        assert result["errors"] == pair["expected_mapping_errors"], (
            f"For {recording_path.name}: expected mapping_errors "
            f"{pair['expected_mapping_errors']}, got {result['errors']}"
        )

    def test_mapping_warnings_match_recording(self, comparator, recording_path):
        pair = _load_pair(recording_path)
        if pair["expected_mapping_warnings"] is None:
            pytest.skip("fixture has no recorded mapping_warnings")
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        assert result["warnings"] == pair["expected_mapping_warnings"]