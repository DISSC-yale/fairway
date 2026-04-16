import pytest


@pytest.mark.local
def test_spark_conf_shell_injection_blocked():
    """Malicious spark.yaml conf values must not be embedded as raw shell.

    The critical injection vector: a double-quote breaks out of the shell
    string, allowing arbitrary command execution.
    """
    from fairway.engines.slurm_cluster import _sanitize_spark_conf_value

    with pytest.raises(ValueError, match="shell-unsafe"):
        _sanitize_spark_conf_value('8g"; touch /tmp/fairway_pwned #')


@pytest.mark.local
def test_spark_conf_safe_values_allowed():
    """Normal spark conf values must pass sanitization without error."""
    from fairway.engines.slurm_cluster import _sanitize_spark_conf_value

    safe_values = [
        "8g",
        "4",
        "true",
        "false",
        "spark://host:7077",
        "100m",
        "local[*]",
        "some.class.Name",
        "value-with-dashes",
        "value_with_underscores",
        "path/to/file",
        "key=value",
        "user@host",
    ]
    for val in safe_values:
        result = _sanitize_spark_conf_value(val)
        assert result == val


@pytest.mark.local
def test_spark_conf_shell_dangerous_chars_blocked():
    """Characters that are dangerous inside a double-quoted bash string are rejected.

    Inside a double-quoted string, only these chars are interpreted by the shell:
    $  (variable expansion), ` (command substitution), " (ends the string),
    \\ (escape).  Control characters (including newlines) would also break the line.
    """
    from fairway.engines.slurm_cluster import _sanitize_spark_conf_value

    dangerous = [
        # Quote terminates the string -> command injection
        '8g"; touch /tmp/pwned #',
        # Variable/command expansion inside double quotes
        "$(rm -rf /)",
        "`id`",
        "$HOME",
        "${PATH}",
        # Backslash can escape the closing quote
        "value\\",
        # Newline breaks the shell line
        "value\nnewline",
        # Null byte
        "value\x00null",
        # Other control characters
        "value\x1bESC",
    ]
    for injection in dangerous:
        with pytest.raises(ValueError, match="shell-unsafe"):
            _sanitize_spark_conf_value(injection)
