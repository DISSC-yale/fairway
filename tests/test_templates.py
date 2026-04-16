"""Tests for template loading."""
import pytest
from unittest.mock import patch, MagicMock
import fairway.templates as templates


class TestReadDataFile:
    """Tests for _read_data_file."""

    def test_loads_known_template(self):
        """Known templates load successfully."""
        result = templates._read_data_file('fairway.yaml')
        assert isinstance(result, str)
        assert len(result) > 0

    def test_missing_file_raises(self):
        """Missing template raises FileNotFoundError."""
        with pytest.raises(Exception):
            templates._read_data_file('nonexistent_file.txt')

    def test_fallback_path_when_pkg_resources_fails(self, tmp_path):
        """Falls back to direct file read when importlib.resources fails."""
        import os
        # Create a fake data file in the fallback location
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "test.txt").write_text("fallback content")

        with patch('importlib.resources.files', side_effect=Exception("no pkg")):
            with patch('os.path.dirname', return_value=str(tmp_path)):
                result = templates._read_data_file('test.txt')
                assert result == "fallback content"

    def test_fallback_missing_also_raises(self):
        """If both pkg_resources and fallback fail, original exception propagates."""
        with patch('importlib.resources.files', side_effect=FileNotFoundError("no pkg")):
            with patch('os.path.exists', return_value=False):
                with pytest.raises(FileNotFoundError):
                    templates._read_data_file('nonexistent.txt')


class TestTemplateConstants:
    """Verify all template constants are loaded."""

    @pytest.mark.parametrize("attr", [
        "HPC_SCRIPT", "APPTAINER_DEF", "DOCKERFILE_TEMPLATE",
        "DOCKERIGNORE", "MAKEFILE_TEMPLATE", "CONFIG_TEMPLATE",
        "SPARK_YAML_TEMPLATE", "TRANSFORM_TEMPLATE", "DRIVER_TEMPLATE",
        "DRIVER_SCHEMA_TEMPLATE", "SPARK_START_TEMPLATE",
        "README_TEMPLATE", "DOCS_TEMPLATE",
    ])
    def test_template_loaded(self, attr):
        """Each template constant is a non-empty string."""
        value = getattr(templates, attr)
        assert isinstance(value, str)
        assert len(value) > 0
