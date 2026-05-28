from connect_migrate.mapper.smt.smt_classifier import SmtClassifier


def make_classifier():
    return SmtClassifier()


class TestClassify:
    def test_no_transforms_returns_empty(self):
        result = make_classifier().classify({}, set())
        assert result["allowed"] == {}
        assert result["disallowed"] == {}
        assert result["mapping_errors"] == []

    def test_allowed_transform(self):
        config = {
            "transforms": "drop_field",
            "transforms.drop_field.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
            "transforms.drop_field.blacklist": "internal",
        }
        allowed = {"org.apache.kafka.connect.transforms.ReplaceField$Value"}
        result = make_classifier().classify(config, allowed)
        assert "transforms.drop_field.type" in result["allowed"]
        assert result["allowed"]["transforms"] == "drop_field"
        assert result["disallowed"] == {}
        assert result["mapping_errors"] == []

    def test_disallowed_transform_records_error(self):
        config = {
            "transforms": "weird",
            "transforms.weird.type": "com.example.NotSupportedSmt",
        }
        result = make_classifier().classify(config, set())
        assert "transforms.weird.type" in result["disallowed"]
        assert any("not supported" in e for e in result["mapping_errors"])

    def test_predicate_filtered_with_unsupported_transform(self):
        config = {
            "transforms": "t1",
            "transforms.t1.type": "com.example.Bad",
            "transforms.t1.predicate": "p1",
            "predicates": "p1",
            "predicates.p1.type": "x",
        }
        result = make_classifier().classify(config, set())
        # p1 is associated with disallowed transform t1 and must be filtered out
        assert "predicates.p1.type" in result["disallowed"]
        assert any("predicate_0" not in e and "p1" in e for e in result["mapping_errors"])

    def test_independent_predicate_passes_through(self):
        config = {
            "transforms": "t1",
            "transforms.t1.type": "ok.Smt",
            "predicates": "p1",
            "predicates.p1.type": "ok.Predicate",
        }
        result = make_classifier().classify(config, {"ok.Smt"})
        assert "predicates.p1.type" in result["allowed"]
        assert result["allowed"]["predicates"] == "p1"


class TestExtractRecommendedTransformTypes:
    def test_finds_recommended_values(self):
        response = {
            "configs": [
                {"value": {"name": "other"}},
                {
                    "value": {
                        "name": "transforms.transform_0.type",
                        "recommended_values": ["a", "b", "c"],
                    }
                },
            ]
        }
        assert SmtClassifier.extract_recommended_transform_types(response) == ["a", "b", "c"]

    def test_missing_key_returns_empty(self):
        assert SmtClassifier.extract_recommended_transform_types({"configs": []}) == []
