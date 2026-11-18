"""Unit tests for OffsetManager's preparation of self-managed offsets for a fully-managed create request."""

import copy
import logging

import pytest

from offset_manager import OffsetManager

S3 = 'io.confluent.connect.s3.source.S3SourceConnector'
GCS = 'io.confluent.connect.gcs.GcsSourceConnector'
AZURE = 'io.confluent.connect.azure.blob.storage.AzureBlobStorageSourceConnector'
JDBC = 'io.confluent.connect.jdbc.JdbcSourceConnector'


@pytest.fixture
def manager():
    return OffsetManager.get_instance(logging.getLogger("test_offset_manager"))


def task_offset(task_id, record_num="8"):
    return {
        "partition": {"taskId": task_id},
        "offset": {"earliestIncomplete": "2026-03-01T00:00:00Z", "completedFiles": "[]", "recordNum": record_num},
    }


@pytest.mark.parametrize("connector_class", [S3, GCS, AZURE])
def test_object_store_task_ids_use_placeholder(manager, connector_class):
    offsets = [
        task_offset("s3-src-prod-0"),
        task_offset("s3-src-prod-0-in_progress", "7"),
        task_offset("s3-src-prod-12"),
    ]
    prepared = manager.prepare_offsets_for_fm("s3-src-prod", connector_class, offsets)

    assert [o["partition"]["taskId"] for o in prepared] == [
        "${connector}-0",
        "${connector}-0-in_progress",
        "${connector}-12",
    ]
    assert [o["offset"] for o in prepared] == [o["offset"] for o in offsets]


def test_input_is_not_modified(manager):
    offsets = [task_offset("s3-src-prod-0")]
    original = copy.deepcopy(offsets)
    manager.prepare_offsets_for_fm("s3-src-prod", S3, offsets)
    assert offsets == original


def test_other_connector_classes_are_unchanged(manager):
    offsets = [{"partition": {"protocol": "1", "table": "orders"}, "offset": {"incrementing": 42}},
               task_offset("jdbc-src-0")]
    assert manager.prepare_offsets_for_fm("jdbc-src", JDBC, offsets) is offsets


def test_only_task_ids_starting_with_the_connector_name_are_rewritten(manager):
    offsets = [task_offset("s3-src-prod-0"), task_offset("other-0"), task_offset("s3-src-prod")]
    prepared = manager.prepare_offsets_for_fm("s3-src-prod", S3, offsets)
    assert [o["partition"]["taskId"] for o in prepared] == ["${connector}-0", "other-0", "s3-src-prod"]


def test_partitions_without_task_id_are_unchanged(manager):
    # Non-generic modes key partitions by folder, not by connector name.
    offsets = [{"partition": {"folder": "topics/orders/partition=0/"}, "offset": {"lastFileRead": "x"}}]
    assert manager.prepare_offsets_for_fm("s3-src-prod", S3, offsets) == offsets


@pytest.mark.parametrize("offsets", [None, []])
def test_empty_offsets(manager, offsets):
    assert manager.prepare_offsets_for_fm("s3-src-prod", S3, offsets) == offsets


def test_offsets_use_connector_placeholder():
    assert OffsetManager.offsets_use_connector_placeholder([task_offset("${connector}-0")])
    assert not OffsetManager.offsets_use_connector_placeholder([task_offset("s3-src-prod-0")])
    assert not OffsetManager.offsets_use_connector_placeholder([{"partition": {"folder": "x"}}])
    assert not OffsetManager.offsets_use_connector_placeholder(None)


# Bodies of Confluent Cloud's create errors, as they appear in the exception raised by the create call.
PLACEHOLDER_NOT_ENABLED_ERROR = (
    "Failed to create connector 'my-s3-source': 400 {\"error_code\":400,\"message\":\"Invalid offsets: the "
    "${connector} placeholder in \\\"partition\\\" is not enabled for this connector.\"}")
STRICT_PARTITION_ERROR = (
    "Failed to create connector 'my-s3-source': 400 {\"error_code\":400,\"message\":\"Invalid offsets: "
    "offsets[1].partition {\\\"taskId\\\":\\\"other-0\\\"} must contain ${connector}.\"}")
VALIDATION_UNAVAILABLE_ERROR = (
    "Failed to create connector 'my-s3-source': 503 {\"error_code\":503,\"message\":\"Unable to validate "
    "initial offsets. Please retry after some time.\"}")


def test_hint_for_placeholder_not_enabled():
    hint = OffsetManager.placeholder_create_failure_hint(
        "my-s3-source", [task_offset("${connector}-0")], PLACEHOLDER_NOT_ENABLED_ERROR)
    assert hint is not None and "does not yet accept" in hint


def test_hint_for_validation_unavailable():
    hint = OffsetManager.placeholder_create_failure_hint(
        "my-s3-source", [task_offset("${connector}-0")], VALIDATION_UNAVAILABLE_ERROR)
    assert hint is not None and "temporary" in hint


def test_no_hint_for_strict_partition_error():
    # The strict check's 400 mentions the placeholder too, but the placeholder is enabled: the server's message
    # explains the problem, so no "not yet accepted" guidance is given.
    assert OffsetManager.placeholder_create_failure_hint(
        "my-s3-source", [task_offset("${connector}-0"), task_offset("other-0")], STRICT_PARTITION_ERROR) is None


def test_no_hint_without_placeholder_or_for_other_errors():
    assert OffsetManager.placeholder_create_failure_hint(
        "my-s3-source", [task_offset("my-s3-source-0")], PLACEHOLDER_NOT_ENABLED_ERROR) is None
    assert OffsetManager.placeholder_create_failure_hint("my-s3-source", None, PLACEHOLDER_NOT_ENABLED_ERROR) is None
    unauthorized = "Failed to create connector 'my-s3-source': 401 Unauthorized"
    assert OffsetManager.placeholder_create_failure_hint(
        "my-s3-source", [task_offset("${connector}-0")], unauthorized) is None


def test_warning_for_partitions_not_rewritten_mentions_strict_validation(manager, caplog):
    with caplog.at_level(logging.WARNING):
        manager.prepare_offsets_for_fm("s3-src-prod", S3, [task_offset("s3-src-prod-0"), task_offset("other-0")])
    assert "strict offset partition validation" in caplog.text
