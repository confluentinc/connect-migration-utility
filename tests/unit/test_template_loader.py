import json

from connect_migrate.mapper.templates.template_loader import TemplateLoader
from connect_migrate.mapper.templates.connector_class_index import ConnectorClassIndex


def write_template(dir_path, name, data):
    """Drop a template JSON file into ``dir_path/<name>``."""
    path = dir_path / name
    path.write_text(json.dumps(data))
    return path


class TestTemplateLoader:
    def test_load_all_reads_every_json_in_dir(self, tmp_path):
        write_template(tmp_path, "a.json", {"key": "a"})
        write_template(tmp_path, "b.json", {"key": "b"})
        (tmp_path / "ignore.txt").write_text("nope")

        templates = TemplateLoader(tmp_path).load_all()
        assert set(templates.keys()) == {"a", "b"}
        assert templates["a"]["key"] == "a"
        assert templates["b"]["key"] == "b"

    def test_load_all_returns_empty_for_missing_dir(self, tmp_path):
        loader = TemplateLoader(tmp_path / "does_not_exist")
        assert loader.load_all() == {}

    def test_load_all_skips_invalid_json(self, tmp_path):
        write_template(tmp_path, "ok.json", {"key": "ok"})
        (tmp_path / "broken.json").write_text("{not json")

        templates = TemplateLoader(tmp_path).load_all()
        assert "ok" in templates
        # broken file is logged and skipped — not included in result
        assert "broken" not in templates

    def test_load_one_by_path(self, tmp_path):
        p = write_template(tmp_path, "x.json", {"hello": "world"})
        result = TemplateLoader(tmp_path).load_one(str(p))
        assert result == {"hello": "world"}

    def test_load_one_returns_none_on_error(self, tmp_path):
        result = TemplateLoader(tmp_path).load_one(str(tmp_path / "nope.json"))
        assert result is None


class TestConnectorClassIndex:
    def test_indexes_only_resolved_templates_suffix(self, tmp_path):
        # Only files ending with _resolved_templates.json are scanned.
        write_template(tmp_path, "X_resolved_templates.json",
                       {"connector.class": "io.example.X"})
        write_template(tmp_path, "Y.json",
                       {"connector.class": "io.example.Y"})

        index = ConnectorClassIndex(tmp_path).build()
        assert "io.example.X" in index
        assert "io.example.Y" not in index

    def test_groups_multiple_templates_per_class(self, tmp_path):
        write_template(tmp_path, "A_resolved_templates.json",
                       {"connector.class": "io.example.A"})
        write_template(tmp_path, "Av2_resolved_templates.json",
                       {"connector.class": "io.example.A"})

        index = ConnectorClassIndex(tmp_path).build()
        assert len(index["io.example.A"]["fm_templates"]) == 2

    def test_missing_connector_class_skipped(self, tmp_path):
        # Templates without a top-level connector.class key are not indexed.
        # (This is pre-existing behavior — the FM templates in this repo
        # use nested templates[].connector.class, so the index is sparse.)
        write_template(tmp_path, "weird_resolved_templates.json", {"name": "weird"})
        index = ConnectorClassIndex(tmp_path).build()
        assert index == {}
