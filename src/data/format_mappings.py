"""Converter-class to wire-format mappings used during config translation."""

CONVERTER_TO_FORMAT_MAPPINGS = {
    "io.confluent.connect.avro.AvroConverter": "AVRO",
    "io.confluent.connect.json.JsonSchemaConverter": "JSON_SR",
    "io.confluent.connect.protobuf.ProtobufConverter": "PROTOBUF",
    "org.apache.kafka.connect.json.JsonConverter": "JSON",
}

STATIC_PROPERTY_MAPPINGS_SOURCE = {
    "key.converter": "output.key.format",
    "value.converter": "output.data.format",
}

STATIC_PROPERTY_MAPPINGS_SINK = {
    "key.converter": "input.key.format",
    "value.converter": "input.data.format",
}