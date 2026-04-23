"""Focused tests for CLI config auto-discovery."""
from __future__ import annotations

import os

import click
import pytest

from fairway.cli import discover_config


def test_discover_config_errors_when_config_dir_missing(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(click.ClickException, match="No config/ directory"):
        discover_config()


def test_discover_config_errors_when_config_dir_empty(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config").mkdir()
    with pytest.raises(click.ClickException, match="No config files"):
        discover_config()


def test_discover_config_returns_single_config(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "fairway.yaml").write_text("dataset_name: test\n")
    assert discover_config() == os.path.join("config", "fairway.yaml")


def test_discover_config_errors_when_multiple_configs_present(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "a.yaml").write_text("dataset_name: a\n")
    (config_dir / "b.yaml").write_text("dataset_name: b\n")
    with pytest.raises(click.ClickException, match="Multiple"):
        discover_config()


def test_discover_config_ignores_schema_and_spark_yaml(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "fairway.yaml").write_text("dataset_name: test\n")
    (config_dir / "test_schema.yaml").write_text("columns: {}\n")
    (config_dir / "spark.yaml").write_text("account: demo\n")
    assert discover_config() == os.path.join("config", "fairway.yaml")


def test_discover_config_ignores_non_yaml_files(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "fairway.yaml").write_text("dataset_name: test\n")
    (config_dir / "README.md").write_text("# Documentation\n")
    (config_dir / ".DS_Store").write_text("binary garbage")
    assert discover_config() == os.path.join("config", "fairway.yaml")
