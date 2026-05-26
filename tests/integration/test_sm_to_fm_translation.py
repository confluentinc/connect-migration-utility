"""Golden-file SM -> FM translation tests using real-world discovered configs.

Each JSON file under tests/fixtures/golden_discovered_configs/ contains both the
original Self-Managed (SM) connector config and the Fully-Managed (FM) config
that the comparator previously produced. We run the same SM config through
ConnectorComparator.transformSMToFm() and assert that the output still matches
the recorded golden FM config exactly.

This is a regression test: if any change to the comparator changes the output
for a real-world connector, one of these tests fails.

Coverage:
  - 9 MongoDB Atlas source connectors (from golden_discovered_configs/)
  - 9 MongoDB Atlas sink connectors (from golden_discovered_configs/)
  - 1 JDBC sink connector translated to OracleDatabaseSink (from golden_discovered_configs/)
  - 1 DatadogMetricsSink connector (from golden_sm_fm_pairs/)
  - 1 S3_SINK connector (from golden_sm_fm_pairs/)
"""

import json
import logging
import os
from pathlib import Path

import pytest

from translation.sm_to_fm_translator import ConnectorComparator


# Repo root — used so the comparator can resolve `templates/fm/` regardless of
# where pytest is invoked from.
REPO_ROOT = Path(__file__).resolve().parents[2]
GOLDEN_DIR = REPO_ROOT / "tests" / "fixtures" / "golden_discovered_configs"
FIXTURE_PAIRS_DIR = REPO_ROOT / "tests" / "fixtures" / "golden_sm_fm_pairs"


def _golden_files():
    """Discover every golden SM/FM pair."""
    return sorted(p for p in GOLDEN_DIR.glob("*.json") if p.is_file())


def _load_pair(path: Path):
    data = json.loads(path.read_text())
    return {
        "name": data["name"],
        "sm_config": data["sm_config"],
        "expected_fm_config": data["config"],
        "expected_mapping_errors": data.get("mapping_errors", []),
    }


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def chdir_repo_root():
    """ConnectorComparator loads templates from Path('templates/fm') relative
    to cwd, so we chdir to repo root for the duration of these tests."""
    prev = Path.cwd()
    os.chdir(REPO_ROOT)
    try:
        yield REPO_ROOT
    finally:
        os.chdir(prev)


@pytest.fixture
def comparator(chdir_repo_root, tmp_path):
    """A fresh ConnectorComparator. tmp_path is used for output_dir; input_file
    is a dummy because we call transformSMToFm directly (not process_connectors)."""
    return ConnectorComparator(
        input_file=tmp_path / "input.json",
        output_dir=tmp_path,
    )


@pytest.fixture
def golden_pair(request):
    """Parametrized fixture — `request.param` is a path to a golden file."""
    return _load_pair(request.param)


# ---------------------------------------------------------------------------
# Golden-file equality test
# ---------------------------------------------------------------------------


@pytest.mark.integration
@pytest.mark.parametrize(
    "golden_pair",
    _golden_files(),
    ids=lambda p: p.stem,
    indirect=True,
)
class TestSmToFmGolden:
    """One test class per golden file; each runs the full SM -> FM pipeline."""

    def test_no_unexpected_mapping_errors(self, comparator, golden_pair):
        result = comparator.transformSMToFm(golden_pair["name"], golden_pair["sm_config"])
        # The recorded examples are all from successful_configs/ — they had no
        # mapping_errors when produced. Make sure that still holds.
        assert result["errors"] == golden_pair["expected_mapping_errors"], (
            f"Expected mapping_errors {golden_pair['expected_mapping_errors']}, "
            f"got {result['errors']}"
        )

    def test_fm_config_matches_golden(self, comparator, golden_pair):
        result = comparator.transformSMToFm(golden_pair["name"], golden_pair["sm_config"])
        # Compare as sorted dicts so key-order differences don't trip the test.
        actual = result["fm_configs"]
        expected = golden_pair["expected_fm_config"]
        assert actual == expected, (
            "FM config diverged from golden file.\n"
            f"Missing keys: {set(expected) - set(actual)}\n"
            f"Extra keys: {set(actual) - set(expected)}\n"
            f"Differing values: "
            + str({
                k: (expected[k], actual[k])
                for k in set(expected) & set(actual)
                if expected[k] != actual[k]
            })
        )

    def test_connector_class_translated_to_fm_template_id(self, comparator, golden_pair):
        # Sanity invariant: connector.class is always rewritten from the SM
        # Java class to the FM template_id.
        result = comparator.transformSMToFm(golden_pair["name"], golden_pair["sm_config"])
        sm_class = golden_pair["sm_config"].get("connector.class")
        fm_class = result["fm_configs"].get("connector.class")
        assert fm_class != sm_class, "connector.class should be rewritten"
        assert fm_class == golden_pair["expected_fm_config"]["connector.class"]


# ---------------------------------------------------------------------------
# Spot tests with finer-grained assertions on a single representative example
# ---------------------------------------------------------------------------


MONGO_SOURCE_EXAMPLE = (
    GOLDEN_DIR / "mongo-source.search.consolidated-master-list.01.json"
)
MONGO_SINK_EXAMPLE = (
    GOLDEN_DIR / "mongo-sink.search.consolidated-master-list.01.json"
)
JDBC_ORACLE_SINK_EXAMPLE = (
    GOLDEN_DIR / "jdbc.sink.endeca.product-category-staging.05.json"
)


@pytest.mark.integration
class TestMongoSourceTranslation:
    """A representative SM -> FM translation for a MongoDB source connector."""

    @pytest.fixture
    def pair(self):
        return _load_pair(MONGO_SOURCE_EXAMPLE)

    def test_connector_class_becomes_mongodb_atlas_source(self, comparator, pair):
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        assert result["fm_configs"]["connector.class"] == "MongoDbAtlasSource"

    def test_connection_host_extracted_from_uri(self, comparator, pair):
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        # The SM config has connection.uri with a mongodb+srv:// URI; the FM
        # config splits this into connection.host + user + password.
        assert (
            result["fm_configs"]["connection.host"]
            == "panamax-ecom-dev-01-pl-0.t36a8.mongodb.net"
        )

    def test_value_converter_translated_to_output_data_format(self, comparator, pair):
        # SM 'value.converter' = StringConverter -> FM 'output.data.format' = STRING.
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        assert result["fm_configs"]["output.data.format"] == "STRING"
        # And the raw value.converter key should NOT appear in the FM config.
        assert "value.converter" not in result["fm_configs"]

    def test_key_converter_translated_to_output_key_format(self, comparator, pair):
        # SM 'key.converter' = JsonConverter -> FM 'output.key.format' = JSON.
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        assert result["fm_configs"]["output.key.format"] == "JSON"

    def test_mongodb_instance_type_added(self, comparator, pair):
        # The FM template adds a fixed `mongodb.instance.type: MONGODB_ATLAS`
        # when the source URI is mongodb+srv://.
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        assert result["fm_configs"]["mongodb.instance.type"] == "MONGODB_ATLAS"

    def test_database_and_collection_preserved(self, comparator, pair):
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        assert result["fm_configs"]["database"] == pair["sm_config"]["database"]
        assert result["fm_configs"]["collection"] == pair["sm_config"]["collection"]


@pytest.mark.integration
class TestMongoSinkTranslation:
    """A representative SM -> FM translation for a MongoDB sink connector."""

    @pytest.fixture
    def pair(self):
        return _load_pair(MONGO_SINK_EXAMPLE)

    def test_connector_class_becomes_mongodb_atlas_sink(self, comparator, pair):
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        assert result["fm_configs"]["connector.class"] == "MongoDbAtlasSink"

    def test_value_converter_translated_to_input_data_format(self, comparator, pair):
        # SINK direction: key/value.converter -> input.* (not output.*).
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        assert "input.data.format" in result["fm_configs"]
        assert "value.converter" not in result["fm_configs"]

    def test_no_mapping_errors_for_recorded_successful_sink(self, comparator, pair):
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        assert result["errors"] == []


@pytest.mark.integration
class TestJdbcOracleSinkTranslation:
    """A representative SM -> FM translation for a JDBC sink that should be
    auto-routed to OracleDatabaseSink based on the connection.url's Oracle
    DESCRIPTION block."""

    @pytest.fixture
    def pair(self):
        return _load_pair(JDBC_ORACLE_SINK_EXAMPLE)

    def test_connector_class_resolves_to_oracle_database_sink(self, comparator, pair):
        # Source class is the generic io.confluent.connect.jdbc.JdbcSinkConnector;
        # the comparator picks the FM template based on the Oracle URL.
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        assert pair["sm_config"]["connector.class"] == "io.confluent.connect.jdbc.JdbcSinkConnector"
        assert result["fm_configs"]["connector.class"] == "OracleDatabaseSink"

    def test_connection_host_extracted_from_oracle_description_block(self, comparator, pair):
        # The Oracle DESCRIPTION block contains (Host=hp887npc.main.usfood.com)(Port=1521).
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        assert result["fm_configs"]["connection.host"] == "hp887npc.main.usfood.com"
        assert result["fm_configs"]["connection.port"] == "1521"

    def test_db_name_extracted_from_service_name(self, comparator, pair):
        # The DESCRIPTION block has (CONNECT_DATA=(SERVICE_NAME=PIMDEV)) -> db.name=PIMDEV.
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        assert result["fm_configs"]["db.name"] == "PIMDEV"

    def test_value_converter_translated_to_input_data_format_avro(self, comparator, pair):
        # SINK: value.converter=AvroConverter -> input.data.format=AVRO.
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        assert result["fm_configs"]["input.data.format"] == "AVRO"
        assert "value.converter" not in result["fm_configs"]

    def test_key_converter_translated_to_input_key_format_avro(self, comparator, pair):
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        assert result["fm_configs"]["input.key.format"] == "AVRO"
        assert "key.converter" not in result["fm_configs"]

    def test_credentials_preserved(self, comparator, pair):
        # Connection user/password are passed through unchanged.
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        assert result["fm_configs"]["connection.user"] == pair["sm_config"]["connection.user"]
        assert result["fm_configs"]["connection.password"] == pair["sm_config"]["connection.password"]

    def test_table_name_format_preserved(self, comparator, pair):
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        assert result["fm_configs"]["table.name.format"] == "PRODUCT_CATEGORY_STAGING"

    def test_fm_template_defaults_are_applied(self, comparator, pair):
        # These keys aren't in the SM config; the FM template should supply
        # them as defaults.
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        assert result["fm_configs"]["ssl.mode"] == "verify-full"
        assert result["fm_configs"]["insert.mode"] == "INSERT"
        assert result["fm_configs"]["db.timezone"] == "UTC"
        assert result["fm_configs"]["timestamp.precision.mode"] == "microseconds"

    def test_schema_registry_credentials_not_carried_into_fm(self, comparator, pair):
        # The SM config has key.converter.schema.registry.* configs that FM
        # cloud handles externally — they should not appear in the FM output.
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        for unused_key in (
            "key.converter.schema.registry.url",
            "key.converter.schema.registry.basic.auth.user.info",
            "key.converter.schema.registry.basic.auth.credentials.source",
            "value.converter.schema.registry.url",
            "value.converter.schema.registry.basic.auth.user.info",
            "value.converter.schema.registry.basic.auth.credentials.source",
            "schemas.enable",
        ):
            assert unused_key not in result["fm_configs"]

    def test_no_mapping_errors(self, comparator, pair):
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        assert result["errors"] == []


@pytest.mark.integration
@pytest.mark.parametrize(
    "fixture_name",
    ["datadog_metrics_sink", "s3_sink"],
)
class TestFixtureGoldenPairs:
    """Byte-for-byte SM->FM equality for connectors not covered by golden_discovered_configs/."""

    def test_fm_config_matches_golden(self, comparator, fixture_name):
        path = FIXTURE_PAIRS_DIR / f"{fixture_name}.json"
        pair = _load_pair(path)
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        assert result["fm_configs"] == pair["expected_fm_config"]
        assert result["errors"] == pair["expected_mapping_errors"]


@pytest.mark.integration
class TestDatadogMetricsSinkTranslation:
    """Fine-grained checks for the Datadog Metrics sink translation."""

    @pytest.fixture
    def pair(self):
        return _load_pair(FIXTURE_PAIRS_DIR / "datadog_metrics_sink.json")

    def test_connector_class_becomes_datadog_metrics_sink(self, comparator, pair):
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        assert result["fm_configs"]["connector.class"] == "DatadogMetricsSink"

    def test_value_converter_avro_translated_to_input_data_format(self, comparator, pair):
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        assert result["fm_configs"]["input.data.format"] == "AVRO"
        assert "value.converter" not in result["fm_configs"]

    def test_datadog_specific_keys_preserved(self, comparator, pair):
        # Datadog-specific config keys should pass through unchanged.
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        for key in ("datadog.api.key", "datadog.site", "datadog.domain"):
            assert result["fm_configs"][key] == pair["sm_config"][key]

    def test_dlq_topic_and_retry_settings_preserved(self, comparator, pair):
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        assert (
            result["fm_configs"]["errors.deadletterqueue.topic.replication.factor"]
            == pair["sm_config"]["errors.deadletterqueue.topic.replication.factor"]
        )
        assert (
            result["fm_configs"]["errors.retry.timeout"]
            == pair["sm_config"]["errors.retry.timeout"]
        )

    def test_topics_preserved(self, comparator, pair):
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        assert result["fm_configs"]["topics"] == "rocks,stones"

    def test_no_mapping_errors(self, comparator, pair):
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        assert result["errors"] == []


@pytest.mark.integration
class TestS3SinkTranslation:
    """Fine-grained checks for the S3 sink translation.

    This is also notable because the SM config has several `transforms.*` SMT
    declarations that don't survive into the FM config — FM cloud connectors
    don't accept arbitrary SMTs the way Connect workers do, so the comparator
    should drop them rather than copy them through.
    """

    @pytest.fixture
    def pair(self):
        return _load_pair(FIXTURE_PAIRS_DIR / "s3_sink.json")

    def test_connector_class_becomes_s3_sink(self, comparator, pair):
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        assert result["fm_configs"]["connector.class"] == "S3_SINK"

    def test_s3_bucket_and_region_preserved(self, comparator, pair):
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        assert result["fm_configs"]["s3.bucket.name"] == "10x-analytics-bi-dashboard-zkkgq"
        assert result["fm_configs"]["s3.region"] == "eu-west-1"

    def test_value_converter_avro_translated_to_input_data_format(self, comparator, pair):
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        assert result["fm_configs"]["input.data.format"] == "AVRO"

    def test_topics_dir_preserved(self, comparator, pair):
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        assert result["fm_configs"]["topics.dir"] == "raw-avro"

    def test_partitioner_class_shortened_to_bare_name(self, comparator, pair):
        # SM uses the fully-qualified Java class name
        # 'io.confluent.connect.storage.partitioner.TimeBasedPartitioner';
        # FM cloud only accepts the bare class name. The comparator strips
        # the package prefix.
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        assert (
            pair["sm_config"]["partitioner.class"]
            == "io.confluent.connect.storage.partitioner.TimeBasedPartitioner"
        )
        assert result["fm_configs"]["partitioner.class"] == "TimeBasedPartitioner"

    def test_timezone_preserved(self, comparator, pair):
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        assert result["fm_configs"]["timezone"] == "GMT"

    def test_timestamp_extractor_dropped_from_fm(self, comparator, pair):
        # SM has 'timestamp.extractor: RecordField' but FM doesn't accept that
        # key — the comparator drops it and the FM template supplies its own
        # timestamp-related fields (timestamp.field, time.interval).
        assert "timestamp.extractor" in pair["sm_config"]
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        assert "timestamp.extractor" not in result["fm_configs"]

    def test_fm_derives_time_interval_from_partition_duration(self, comparator, pair):
        # SM provides partition.duration.ms=86400000 (one day). The FM template
        # turns this into a categorical time.interval=DAILY.
        assert pair["sm_config"]["partition.duration.ms"] == "86400000"
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        assert result["fm_configs"]["time.interval"] == "DAILY"

    def test_transforms_dropped_from_fm_output(self, comparator, pair):
        # SM declares transforms.ReplaceField/AddTimestampField/RenameField/
        # TombstoneHandler. None should survive — FM cloud S3_SINK doesn't
        # accept arbitrary SMTs.
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        for key in result["fm_configs"]:
            assert not key.startswith("transforms"), (
                f"FM config should not contain transform key {key!r}"
            )

    def test_schema_registry_url_dropped(self, comparator, pair):
        # value.converter.schema.registry.url is a Connect-worker config; in
        # FM cloud it's handled externally and shouldn't appear in the config.
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        assert "value.converter.schema.registry.url" not in result["fm_configs"]

    def test_no_mapping_errors(self, comparator, pair):
        result = comparator.transformSMToFm(pair["name"], pair["sm_config"])
        assert result["errors"] == []


# ---------------------------------------------------------------------------
# Smoke test: every golden file is at least loadable
# ---------------------------------------------------------------------------


class TestGoldenFilesShape:
    """Cheap pre-flight checks that don't require the comparator."""

    @pytest.mark.parametrize("golden_path", _golden_files(), ids=lambda p: p.stem)
    def test_pair_has_sm_and_fm_sides(self, golden_path):
        data = json.loads(golden_path.read_text())
        assert "name" in data
        assert "sm_config" in data
        assert "config" in data
        assert data["sm_config"].get("connector.class")
        assert data["config"].get("connector.class")
        assert data["sm_config"]["connector.class"] != data["config"]["connector.class"]
