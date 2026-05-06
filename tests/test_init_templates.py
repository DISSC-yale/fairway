"""Tests for the scaffold template strings."""
from __future__ import annotations

import yaml

from fairway import _init_templates as _t


def test_fairway_yaml_template_is_valid_yaml():
    parsed = yaml.safe_load(_t.FAIRWAY_YAML_TEMPLATE)
    assert isinstance(parsed, dict)


def test_fairway_yaml_template_has_no_apply_type_a():
    assert "apply_type_a" not in _t.FAIRWAY_YAML_TEMPLATE


def test_fairway_yaml_template_has_no_storage_raw():
    assert "storage_raw" not in _t.FAIRWAY_YAML_TEMPLATE


def test_table_schema_yaml_stub_points_at_discover():
    rendered = _t.TABLE_SCHEMA_YAML_STUB.format(name="x")
    assert "fairway discover" in rendered
    assert "columns: []" in rendered


def test_table_transform_py_template_defines_transform():
    rendered = _t.TABLE_TRANSFORM_PY_TEMPLATE.format(name="x")
    assert "def transform(con, ctx):" in rendered


def test_gitignore_includes_state_dir():
    body = _t.GITIGNORE_TEMPLATE
    for needed in ("data/", "build/", "tables/*/manifest.json", "tables/*/_state/"):
        assert needed in body


def test_table_config_yaml_template_is_valid(tmp_path):
    rendered = _t.TABLE_CONFIG_YAML_TEMPLATE.format(name="sales")
    parsed = yaml.safe_load(rendered)
    assert parsed["partition_by"] == ["state", "year"]
    assert parsed["source_format"] == "delimited"
