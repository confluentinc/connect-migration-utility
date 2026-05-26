"""Tests for src/data/format_mappings.py."""

import pytest

from data.format_mappings import (
    CONVERTER_TO_FORMAT_MAPPINGS,
    STATIC_PROPERTY_MAPPINGS_SINK,
    STATIC_PROPERTY_MAPPINGS_SOURCE,
)


class TestConverterToFormatMappings:
    @pytest.mark.parametrize(
        "converter_class,expected_format",
        [
            ("io.confluent.connect.avro.AvroConverter", "AVRO"),
            ("io.confluent.connect.json.JsonSchemaConverter", "JSON_SR"),
            ("io.confluent.connect.protobuf.ProtobufConverter", "PROTOBUF"),
            ("org.apache.kafka.connect.json.JsonConverter", "JSON"),
        ],
    )
    def test_each_converter_maps_to_expected_format(self, converter_class, expected_format):
        assert CONVERTER_TO_FORMAT_MAPPINGS[converter_class] == expected_format

    def test_mapping_has_exactly_four_entries(self):
        assert len(CONVERTER_TO_FORMAT_MAPPINGS) == 4

    def test_all_formats_are_unique(self):
        # AVRO, JSON_SR, PROTOBUF, JSON - each appears once.
        formats = list(CONVERTER_TO_FORMAT_MAPPINGS.values())
        assert len(formats) == len(set(formats))


class TestStaticPropertyMappings:
    def test_source_maps_converter_to_output_fields(self):
        assert STATIC_PROPERTY_MAPPINGS_SOURCE == {
            "key.converter": "output.key.format",
            "value.converter": "output.data.format",
        }

    def test_sink_maps_converter_to_input_fields(self):
        assert STATIC_PROPERTY_MAPPINGS_SINK == {
            "key.converter": "input.key.format",
            "value.converter": "input.data.format",
        }

    def test_source_and_sink_have_same_keys(self):
        assert set(STATIC_PROPERTY_MAPPINGS_SOURCE.keys()) == set(STATIC_PROPERTY_MAPPINGS_SINK.keys())

    def test_source_and_sink_values_are_disjoint(self):
        # input.* vs output.* should never collide.
        source_values = set(STATIC_PROPERTY_MAPPINGS_SOURCE.values())
        sink_values = set(STATIC_PROPERTY_MAPPINGS_SINK.values())
        assert not (source_values & sink_values)
