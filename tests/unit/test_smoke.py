"""Foundation smoke test: the fixtures build a usable comparator offline."""


def test_make_comparator_builds(make_comparator):
    c = make_comparator()
    assert c.debezium_version == "v2"
    assert c.semantic_matcher.__class__.__name__ == "StubSemanticMatcher"


def test_write_template_and_find(make_comparator, write_template, jdbc_source_template):
    fm_dir = write_template("MySqlSource_resolved_templates", jdbc_source_template).parent
    c = make_comparator(fm_dir=fm_dir)
    path = c._find_fm_template_by_connector_class(
        "io.confluent.connect.jdbc.JdbcSourceConnector",
        config={"connection.url": "jdbc:mysql://h:3306/db"},
    )
    assert path is not None and path.endswith("MySqlSource_resolved_templates.json")