"""Engine registry.

Owns the canonical set of engine names and their user-facing aliases.
Callers pass user-supplied names through `normalize_engine_name` and
then validate against `VALID_ENGINES`.
"""

VALID_ENGINES = frozenset({"duckdb", "pyspark"})
ENGINE_ALIASES = {"spark": "pyspark"}


def normalize_engine_name(engine):
    """Return the canonical engine name for an input string.

    - Alias lookup: 'spark' -> 'pyspark' (case-insensitive, whitespace-stripped)
    - Canonical names pass through unchanged.
    - None / non-string pass through so the caller can validate with
      VALID_ENGINES membership instead of crashing here.
    - Whitespace-only collapses to "" — caller treats empty as missing.

    Does NOT validate; callers check membership in VALID_ENGINES.
    """
    if engine is None:
        return None
    if not isinstance(engine, str):
        return engine
    key = engine.strip().lower()
    if not key:
        return key
    return ENGINE_ALIASES.get(key, key)
