from connect_migrate.mapper.properties.config_def_processor import ConfigDefProcessor


def no_resolver(name, config_def):
    """A derivation_resolver that never finds anything (used to test the fallback paths)."""
    return None


def make_processor(resolver=no_resolver):
    return ConfigDefProcessor(derivation_resolver=resolver)


class TestExtractConnectorConfigDefs:
    def test_flattens_across_templates(self):
        template = {
            "templates": [
                {"connector_configs": [{"name": "a"}]},
                {"connector_configs": [{"name": "b"}]},
            ]
        }
        result = make_processor().extract_connector_config_defs(template)
        assert [d["name"] for d in result] == ["a", "b"]

    def test_empty_when_no_templates(self):
        assert make_processor().extract_connector_config_defs({}) == []

    def test_skips_non_list_connector_configs(self):
        template = {
            "templates": [
                {"connector_configs": True},  # not a list
                {"connector_configs": [{"name": "ok"}]},
            ]
        }
        result = make_processor().extract_connector_config_defs(template)
        assert [d["name"] for d in result] == ["ok"]


class TestExtractTemplateConfigDefs:
    def test_first_definition_wins(self):
        template = {
            "templates": [
                {"config_defs": [{"name": "x", "v": 1}]},
                {"config_defs": [{"name": "x", "v": 2}, {"name": "y"}]},
            ]
        }
        result = make_processor().extract_template_config_defs(template)
        # 'x' appears in both — first wins
        assert {d["name"]: d.get("v") for d in result} == {"x": 1, "y": None}

    def test_skips_defs_without_name(self):
        template = {"templates": [{"config_defs": [{"name": "ok"}, {"no_name": True}]}]}
        result = make_processor().extract_template_config_defs(template)
        assert [d["name"] for d in result] == ["ok"]


class TestFindTemplateConfigDefByName:
    def test_finds_by_name(self):
        defs = [{"name": "a"}, {"name": "b", "x": 1}]
        out = make_processor().find_template_config_def_by_name("b", defs)
        assert out == {"name": "b", "x": 1}

    def test_returns_none_when_missing(self):
        assert make_processor().find_template_config_def_by_name("z", [{"name": "a"}]) is None


class TestProcessUserConfigCases:
    def test_constant_value_match_writes_through(self):
        # value is a constant matching the user-supplied value -> FM key written.
        cdef = {"name": "fm.key", "value": "AVRO"}
        fm_configs = {}
        warnings = []
        errors = []
        make_processor().process_user_config(
            connector_config_def=cdef,
            user_config_value="AVRO",
            template_config_defs=[],
            fm_configs=fm_configs,
            warnings=warnings,
            errors=errors,
            user_configs={},
            semantic_match_list=set(),
        )
        assert fm_configs == {"fm.key": "AVRO"}
        assert warnings == []

    def test_constant_value_mismatch_warns_and_ignores(self):
        cdef = {"name": "fm.key", "value": "AVRO"}
        fm_configs = {}
        warnings = []
        make_processor().process_user_config(
            connector_config_def=cdef,
            user_config_value="JSON",
            template_config_defs=[],
            fm_configs=fm_configs,
            warnings=warnings,
            errors=[],
            user_configs={},
            semantic_match_list=set(),
        )
        assert fm_configs == {}
        assert any("constant value 'AVRO'" in w for w in warnings)

    def test_non_string_value_is_stringified(self):
        cdef = {"name": "validate.non.null", "value": False}
        fm_configs = {}
        make_processor().process_user_config(
            connector_config_def=cdef,
            user_config_value="",
            template_config_defs=[],
            fm_configs=fm_configs,
            warnings=[],
            errors=[],
            user_configs={},
            semantic_match_list=set(),
        )
        # bool -> lowercased; numbers -> str()
        assert fm_configs["validate.non.null"] == "false"

    def test_null_value_case_writes_user_value(self):
        # No 'value' / 'switch' / 'dynamic.mapper' on the config def -> null
        # value case copies the user-supplied value as-is.
        cdef = {"name": "topic.prefix"}
        fm_configs = {}
        make_processor().process_user_config(
            connector_config_def=cdef,
            user_config_value="prod-",
            template_config_defs=[],
            fm_configs=fm_configs,
            warnings=[],
            errors=[],
            user_configs={},
            semantic_match_list=set(),
        )
        assert fm_configs == {"topic.prefix": "prod-"}


class TestInferDynamicMappings:
    def test_subject_name_strategy_known_class(self):
        p = make_processor()
        assert (
            p.infer_dynamic_mappings(
                "value.converter.reference.subject.name.strategy.mapper",
                "io.confluent.kafka.serializers.subject.TopicNameStrategy",
            )
            == "TopicNameStrategy"
        )

    def test_subject_name_strategy_unknown_class(self):
        p = make_processor()
        assert (
            p.infer_dynamic_mappings(
                "value.converter.reference.subject.name.strategy.mapper",
                "com.example.Custom",
            )
            is None
        )

    def test_unknown_mapper_fn(self):
        p = make_processor()
        assert p.infer_dynamic_mappings("unknown.mapper", "anything") is None
