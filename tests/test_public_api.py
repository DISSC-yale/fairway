"""Smoke tests for the public fairway package API.

Guards against regressions where someone deletes __version__ or rearranges
subpackages such that `from fairway.validations import Validator` stops
working for downstream users.
"""


def test_fairway_exposes_version():
    import fairway
    assert hasattr(fairway, "__version__")
    assert isinstance(fairway.__version__, str)
    assert fairway.__version__


def test_fairway_version_matches_pyproject():
    import fairway
    from pathlib import Path

    try:
        import tomllib  # Python 3.11+
    except ImportError:
        try:
            import tomli as tomllib  # backport for 3.10
        except ImportError:
            import pytest
            pytest.skip("no tomllib/tomli available to parse pyproject.toml")

    project_root = Path(__file__).parent.parent
    with open(project_root / "pyproject.toml", "rb") as f:
        data = tomllib.load(f)
    expected = data["project"]["version"]
    if fairway.__version__ == "unknown":
        return
    assert fairway.__version__ == expected, (
        f"fairway.__version__ ({fairway.__version__!r}) does not match "
        f"pyproject.toml version ({expected!r})"
    )


def test_validator_importable_from_subpackage():
    from fairway.validations import Validator
    assert Validator is not None
    assert hasattr(Validator, "run_all")
