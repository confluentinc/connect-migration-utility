from connect_migrate.mapper.properties.direct_mappings import DirectMappings


def make_mappings():
    return DirectMappings()


class TestCreateFromTemplate:
    def test_template_variable_mapping(self):
        # "${cleanup.policy}" means: when the SM config has 'cleanup.policy',
        # write it to the FM 'cleanup.policy'.
        template = {
            "templates": [
                {
                    "connector_configs": [
                        {"name": "cleanup.policy", "value": "${cleanup.policy}"}
                    ]
                }
            ]
        }
        result = make_mappings().create_from_template(template)
        assert result == {"cleanup.policy": "cleanup.policy"}

    def test_fixed_value_inverse_mapping(self):
        # A literal value (no ${}) means the FM config key maps from a
        # specific SM input value; the mapping is stored value->name.
        template = {
            "templates": [
                {"connector_configs": [{"name": "fm_key", "value": "fixed_val"}]}
            ]
        }
        result = make_mappings().create_from_template(template)
        assert result == {"fixed_val": "fm_key"}

    def test_same_name_mapping_when_no_value_or_switch(self):
        template = {"templates": [{"connector_configs": [{"name": "topic"}]}]}
        result = make_mappings().create_from_template(template)
        assert result == {"topic": "topic"}

    def test_empty_template(self):
        assert make_mappings().create_from_template({}) == {}


class TestGetFixedValues:
    def test_extracts_literal_values(self):
        template = {
            "templates": [
                {
                    "connector_configs": [
                        {"name": "input.format", "value": "AVRO"},
                        {"name": "cleanup.policy", "value": "${cleanup.policy}"},
                    ]
                }
            ]
        }
        # Template variables (${}) should NOT be counted as fixed values.
        assert make_mappings().get_fixed_values(template) == {"input.format": "AVRO"}


class TestGetRecommendedValues:
    def test_collects_from_all_templates(self):
        template = {
            "templates": [
                {
                    "config_defs": [
                        {"name": "input.format", "recommended_values": ["AVRO", "JSON"]},
                        {"name": "topic"},
                    ]
                }
            ]
        }
        assert make_mappings().get_recommended_values(template) == {
            "input.format": ["AVRO", "JSON"]
        }


class TestGetRequiredProperties:
    def test_required_true_string(self):
        template = {
            "templates": [
                {"config_defs": [{"name": "k", "required": "true"}]}
            ]
        }
        assert "k" in make_mappings().get_required_properties(template)

    def test_required_bool(self):
        template = {"templates": [{"config_defs": [{"name": "k", "required": True}]}]}
        assert "k" in make_mappings().get_required_properties(template)

    def test_internal_required_skipped(self):
        template = {
            "templates": [
                {"config_defs": [{"name": "k", "required": True, "internal": True}]}
            ]
        }
        assert make_mappings().get_required_properties(template) == {}

    def test_not_required_skipped(self):
        template = {"templates": [{"config_defs": [{"name": "k", "required": False}]}]}
        assert make_mappings().get_required_properties(template) == {}


class TestIsSourceConnector:
    def test_explicit_source(self):
        assert make_mappings().is_source_connector({"connector_type": "SOURCE"})

    def test_explicit_sink(self):
        assert not make_mappings().is_source_connector({"connector_type": "SINK"})

    def test_inferred_from_class_name_source(self):
        assert make_mappings().is_source_connector(
            {"connector.class": "io.example.MySqlSourceConnector"}
        )

    def test_inferred_from_class_name_sink(self):
        assert not make_mappings().is_source_connector(
            {"connector.class": "io.example.MySqlSinkConnector"}
        )

    def test_inferred_from_inner_template(self):
        assert make_mappings().is_source_connector(
            {"templates": [{"connector_type": "SOURCE"}]}
        )

    def test_default_true_when_unknown(self):
        # Documented fallback behavior — defaults to source.
        assert make_mappings().is_source_connector({"connector.class": "weird.Class"})

    def test_empty_template_true(self):
        # No template at all means assume source as a benign default.
        assert make_mappings().is_source_connector({})


class TestApplyToConfig:
    def test_fixed_value_override_records_error(self):
        template = {
            "templates": [
                {"connector_configs": [{"name": "fm_input", "value": "AVRO"}]}
            ]
        }
        # SM config supplies the "fixed value" name as a key, but with a
        # different value -> error.
        config = {"AVRO": "JSON"}
        mapped, errors = make_mappings().apply_to_config(config, template)
        assert mapped["fm_input"] == "AVRO"
        assert any("overridden" in e for e in errors)

    def test_value_outside_recommended_values_records_error(self):
        template = {
            "templates": [
                {
                    "connector_configs": [
                        {"name": "input.format", "value": "${input.format}"}
                    ],
                    "config_defs": [
                        {
                            "name": "input.format",
                            "recommended_values": ["AVRO", "JSON_SR"],
                        }
                    ],
                }
            ]
        }
        mapped, errors = make_mappings().apply_to_config({"input.format": "XML"}, template)
        # An error is recorded. The value is still present in mapped_config
        # because the original code writes it *before* checking recommended
        # values — see the comment "Don't map the property if value is invalid"
        # in apply_to_config, which is misleading: a `continue` skips later
        # processing but doesn't undo the earlier write. This test pins the
        # actual (latent-bug) behavior, not the intended one.
        assert any("not in recommended values" in e for e in errors)
        assert mapped.get("input.format") == "XML"
