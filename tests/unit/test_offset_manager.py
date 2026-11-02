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
