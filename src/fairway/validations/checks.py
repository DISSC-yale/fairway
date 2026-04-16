import logging
import re
from fairway.validations.result import ValidationResult

logger = logging.getLogger("fairway.validations")

# Known validation keys and their expected types.
# Used by _validate_validations_block() to reject typos and bad types.
KNOWN_VALIDATION_KEYS = {
    "min_rows": int,
    "max_rows": int,
    "check_nulls": list,
    "expected_columns": (list, dict),
    "check_range": dict,
    "check_values": dict,
    "check_pattern": dict,
    "check_unique": list,
    "check_custom": (str, list),
}

# Keys that indicate legacy level1/level2 nesting
LEGACY_LEVEL_KEYS = {"level1", "level2"}


class Validator:

    @staticmethod
    def _normalize_validation_config(raw):
        """Normalize validation config: accept flat keys OR legacy level1/level2 nesting.

        Returns a flat dict with all validation keys at the top level.
        """
        if not raw:
            return {}

        has_legacy = bool(LEGACY_LEVEL_KEYS & set(raw.keys()))
        has_flat = bool(set(raw.keys()) - LEGACY_LEVEL_KEYS)

        if has_legacy and has_flat:
            raise ValueError(
                "Cannot mix flat validation keys with level1/level2 nesting. "
                "Use one format or the other."
            )

        if has_legacy:
            flat = {}
            for level_key in LEGACY_LEVEL_KEYS:
                level_dict = raw.get(level_key)
                if isinstance(level_dict, dict):
                    flat.update(level_dict)
            return flat

        return dict(raw)

    @staticmethod
    def _validate_validations_block(config):
        """Validate a normalized (flat) validation config block.

        Returns a list of error strings. Empty list means valid.
        """
        errors = []
        for key, value in config.items():
            if key not in KNOWN_VALIDATION_KEYS:
                errors.append(f"Unknown validation key: '{key}'")
                continue

            expected = KNOWN_VALIDATION_KEYS[key]
            if not isinstance(value, expected):
                type_name = expected.__name__ if isinstance(expected, type) else str(expected)
                errors.append(
                    f"Validation key '{key}' must be {type_name}, got {type(value).__name__}"
                )
                continue

            # Additional type-specific checks
            if key == "min_rows" and value <= 0:
                errors.append(f"Validation key 'min_rows' must be a positive integer, got {value}")
            elif key == "max_rows" and value <= 0:
                errors.append(f"Validation key 'max_rows' must be a positive integer, got {value}")
            elif key == "check_pattern" and isinstance(value, dict):
                for col_name, pattern in value.items():
                    if not isinstance(pattern, str):
                        errors.append(f"check_pattern['{col_name}'] must be a string, got {type(pattern).__name__}")
                    else:
                        try:
                            re.compile(pattern)
                        except re.error as e:
                            errors.append(f"check_pattern['{col_name}'] has invalid regex: {e}")

        return errors

    @staticmethod
    def run_all(df, config, is_spark=False):
        """Unified entry point: normalize config, run all checks, return ValidationResult.

        Args:
            df: Pandas DataFrame or Spark DataFrame.
            config: Raw validation config (flat or legacy format).
            is_spark: If True, use Spark-native check variants.

        Returns:
            ValidationResult with all findings.
        """
        result = ValidationResult()
        flat = Validator._normalize_validation_config(config)

        # Validate config schema — fail fast on typos/bad types
        schema_errors = Validator._validate_validations_block(flat)
        if schema_errors:
            for err in schema_errors:
                result.add_finding({
                    "column": None,
                    "check": "config_validation",
                    "message": err,
                    "severity": "error",
                    "failed_count": 1,
                    "total_count": 1,
                })
            return result

        # Guard unimplemented checks
        if flat.get("check_custom"):
            raise NotImplementedError("check_custom is not yet implemented")
        if flat.get("check_unique"):
            raise NotImplementedError("check_unique is not yet implemented")

        # min_rows check
        min_rows = flat.get("min_rows")
        if min_rows is not None:
            if is_spark:
                sample_count = df.limit(min_rows).count()
                row_count_ok = sample_count >= min_rows
                actual = f"fewer than {min_rows}" if not row_count_ok else None
            else:
                actual_count = len(df)
                row_count_ok = actual_count >= min_rows
                actual = str(actual_count) if not row_count_ok else None

            if not row_count_ok:
                result.add_finding({
                    "column": None,
                    "check": "min_rows",
                    "message": f"Row count {actual} is less than minimum {min_rows}",
                    "severity": "error",
                    "failed_count": 1,
                    "total_count": 1,
                })

        # max_rows check
        # RULE-103: avoid df.count() on the full frame when we only need to
        # know whether it exceeds a threshold. df.limit(max_rows + 1).count()
        # short-circuits as soon as one row over the limit is materialized.
        max_rows = flat.get("max_rows")
        if max_rows is not None:
            if is_spark:
                over_limit = df.limit(max_rows + 1).count()
                exceeded = over_limit > max_rows
                reported = f">{max_rows}" if exceeded else str(over_limit)
            else:
                row_count = len(df)
                exceeded = row_count > max_rows
                reported = str(row_count)
            if exceeded:
                result.add_finding({
                    "column": None,
                    "check": "max_rows",
                    "message": f"Row count {reported} exceeds maximum {max_rows}",
                    "severity": "error",
                    "failed_count": 1,
                    "total_count": 1,
                })

        # expected_columns check
        expected_columns = flat.get("expected_columns")
        if expected_columns:
            strict = False
            if isinstance(expected_columns, dict):
                strict = expected_columns.get("strict", False)
                expected_columns = expected_columns.get("columns", [])

            actual_cols = set(df.columns)

            expected_set = set(expected_columns)
            missing = expected_set - actual_cols
            if missing:
                result.add_finding({
                    "column": None,
                    "check": "expected_columns",
                    "message": f"Missing expected columns: {sorted(missing)}",
                    "severity": "error",
                    "failed_count": len(missing),
                    "total_count": len(expected_set),
                })
            if strict:
                extra = actual_cols - expected_set
                if extra:
                    result.add_finding({
                        "column": None,
                        "check": "expected_columns",
                        "message": f"Unexpected extra columns (strict mode): {sorted(extra)}",
                        "severity": "error",
                        "failed_count": len(extra),
                        "total_count": len(actual_cols),
                    })

        # check_nulls
        # RULE-103: for Spark, combine per-column null counts plus the total
        # row count into a single aggregation pass instead of (N + 1) full
        # scans (one df.count() plus N df.filter(...).count() calls).
        check_nulls = flat.get("check_nulls")
        if check_nulls:
            if is_spark:
                from pyspark.sql import functions as F
                present_cols = [c for c in check_nulls if c in df.columns]
                if present_cols:
                    agg_exprs = [F.count(F.lit(1)).alias("_total_count")]
                    for c in present_cols:
                        agg_exprs.append(
                            F.count(F.when(F.col(c).isNull(), 1)).alias(f"__null__{c}")
                        )
                    row = df.agg(*agg_exprs).collect()[0]
                    total = row["_total_count"]
                    for col_name in present_cols:
                        null_count = row[f"__null__{col_name}"]
                        if null_count > 0:
                            result.add_finding({
                                "column": col_name,
                                "check": "check_nulls",
                                "message": f"Null values found in mandatory column: {col_name} (count: {null_count})",
                                "severity": "error",
                                "failed_count": null_count,
                                "total_count": total,
                            })
            else:
                for col_name in check_nulls:
                    if col_name in df.columns and df[col_name].isnull().any():
                        null_count = int(df[col_name].isnull().sum())
                        result.add_finding({
                            "column": col_name,
                            "check": "check_nulls",
                            "message": f"Null values found in mandatory column: {col_name} (count: {null_count})",
                            "severity": "error",
                            "failed_count": null_count,
                            "total_count": len(df),
                        })

        # check_range — min/max bounds for numeric columns (nulls excluded)
        check_range = flat.get("check_range")
        if check_range:
            for col_name, bounds in check_range.items():
                if is_spark:
                    if col_name not in df.columns:
                        result.add_finding({
                            "column": col_name,
                            "check": "check_range",
                            "message": f"Column '{col_name}' not found, skipping range check",
                            "severity": "warn",
                            "failed_count": 0,
                            "total_count": 0,
                        })
                        continue
                    from pyspark.sql.functions import col as spark_col
                    non_null = df.filter(spark_col(col_name).isNotNull())
                    total = non_null.count()
                    if total == 0:
                        continue
                    conditions = []
                    if "min" in bounds:
                        conditions.append(spark_col(col_name) < bounds["min"])
                    if "max" in bounds:
                        conditions.append(spark_col(col_name) > bounds["max"])
                    if conditions:
                        from functools import reduce
                        from operator import or_
                        combined = reduce(or_, conditions)
                        failed = non_null.filter(combined).count()
                        if failed > 0:
                            result.add_finding({
                                "column": col_name,
                                "check": "check_range",
                                "message": f"{failed} values in '{col_name}' outside range [{bounds.get('min')}, {bounds.get('max')}]",
                                "severity": "error",
                                "failed_count": failed,
                                "total_count": total,
                            })
                else:
                    if col_name not in df.columns:
                        result.add_finding({
                            "column": col_name,
                            "check": "check_range",
                            "message": f"Column '{col_name}' not found, skipping range check",
                            "severity": "warn",
                            "failed_count": 0,
                            "total_count": 0,
                        })
                        continue
                    series = df[col_name].dropna()
                    if len(series) == 0:
                        continue
                    violations = series.isna()  # all False since we already dropped NAs
                    if "min" in bounds:
                        violations = violations | (series < bounds["min"])
                    if "max" in bounds:
                        violations = violations | (series > bounds["max"])
                    failed = int(violations.sum())
                    if failed > 0:
                        result.add_finding({
                            "column": col_name,
                            "check": "check_range",
                            "message": f"{failed} values in '{col_name}' outside range [{bounds.get('min')}, {bounds.get('max')}]",
                            "severity": "error",
                            "failed_count": failed,
                            "total_count": len(series),
                        })

        # check_values — allowed-value list (enum check)
        check_values = flat.get("check_values")
        if check_values:
            for col_name, allowed in check_values.items():
                if is_spark:
                    if col_name not in df.columns:
                        result.add_finding({
                            "column": col_name,
                            "check": "check_values",
                            "message": f"Column '{col_name}' not found, skipping values check",
                            "severity": "warn",
                            "failed_count": 0,
                            "total_count": 0,
                        })
                        continue
                    from pyspark.sql.functions import col as spark_col
                    non_null = df.filter(spark_col(col_name).isNotNull())
                    total = non_null.count()
                    failed = non_null.filter(~spark_col(col_name).isin(allowed)).count()
                    if failed > 0:
                        result.add_finding({
                            "column": col_name,
                            "check": "check_values",
                            "message": f"{failed} values in '{col_name}' not in allowed set",
                            "severity": "error",
                            "failed_count": failed,
                            "total_count": total,
                        })
                else:
                    if col_name not in df.columns:
                        result.add_finding({
                            "column": col_name,
                            "check": "check_values",
                            "message": f"Column '{col_name}' not found, skipping values check",
                            "severity": "warn",
                            "failed_count": 0,
                            "total_count": 0,
                        })
                        continue
                    series = df[col_name].dropna()
                    if len(series) == 0:
                        continue
                    invalid = ~series.isin(allowed)
                    failed = int(invalid.sum())
                    if failed > 0:
                        result.add_finding({
                            "column": col_name,
                            "check": "check_values",
                            "message": f"{failed} values in '{col_name}' not in allowed set",
                            "severity": "error",
                            "failed_count": failed,
                            "total_count": len(series),
                        })

        # check_pattern — regex match on string columns (nulls excluded)
        check_pattern = flat.get("check_pattern")
        if check_pattern:
            for col_name, pattern in check_pattern.items():
                if is_spark:
                    if col_name not in df.columns:
                        result.add_finding({
                            "column": col_name,
                            "check": "check_pattern",
                            "message": f"Column '{col_name}' not found, skipping pattern check",
                            "severity": "warn",
                            "failed_count": 0,
                            "total_count": 0,
                        })
                        continue
                    from pyspark.sql.functions import col as spark_col
                    non_null = df.filter(spark_col(col_name).isNotNull())
                    total = non_null.count()
                    if total == 0:
                        continue
                    failed = non_null.filter(~spark_col(col_name).rlike(pattern)).count()
                    if failed > 0:
                        result.add_finding({
                            "column": col_name,
                            "check": "check_pattern",
                            "message": f"{failed} values in '{col_name}' do not match pattern '{pattern}'",
                            "severity": "error",
                            "failed_count": failed,
                            "total_count": total,
                        })
                else:
                    if col_name not in df.columns:
                        result.add_finding({
                            "column": col_name,
                            "check": "check_pattern",
                            "message": f"Column '{col_name}' not found, skipping pattern check",
                            "severity": "warn",
                            "failed_count": 0,
                            "total_count": 0,
                        })
                        continue
                    series = df[col_name].dropna().astype(str)
                    if len(series) == 0:
                        continue
                    matches = series.str.fullmatch(pattern)
                    failed = int((~matches).sum())
                    if failed > 0:
                        result.add_finding({
                            "column": col_name,
                            "check": "check_pattern",
                            "message": f"{failed} values in '{col_name}' do not match pattern '{pattern}'",
                            "severity": "error",
                            "failed_count": failed,
                            "total_count": len(series),
                        })

        return result

    @staticmethod
    def level1_check(df, config):
        """Basic sanity checks: row count minimum.

        Accepts both legacy format (config["level1"]["min_rows"]) and
        flat format (config["min_rows"]).
        """
        # Support both legacy nested and flat config
        checks = config.get('level1', {})
        min_rows = checks.get('min_rows') or config.get('min_rows')

        results = {"passed": True, "errors": []}

        if min_rows and len(df) < min_rows:
            results['passed'] = False
            results['errors'].append(f"Row count {len(df)} is less than minimum {min_rows}")

        return results

    @staticmethod
    def level2_check(df, config):
        """Schema and distribution checks: null detection.

        Accepts both legacy format (config["level2"]["check_nulls"]) and
        flat format (config["check_nulls"]).
        """
        # Support both legacy nested and flat config
        checks = config.get('level2') or {}
        check_nulls = checks.get('check_nulls') or config.get('check_nulls')

        results = {"passed": True, "errors": []}

        if check_nulls:
            for col in check_nulls:
                if col in df.columns and df[col].isnull().any():
                    results['passed'] = False
                    results['errors'].append(f"Null values found in mandatory column: {col}")

        return results

    @staticmethod
    def level1_check_spark(df, config):
        """Spark-native Level 1 checks.

        Uses limit() optimization for min_rows check to avoid full table scan.
        Only scans enough rows to verify the threshold.
        """
        checks = config.get('level1', {})
        min_rows = checks.get('min_rows') or config.get('min_rows')

        results = {"passed": True, "errors": []}

        if min_rows:
            logger.info("Validating minimum row count (threshold: %d)...", min_rows)
            sample_count = df.limit(min_rows).count()
            if sample_count < min_rows:
                results['passed'] = False
                results['errors'].append(f"Row count below minimum {min_rows} (found fewer than {min_rows} rows)")
            else:
                logger.info("Row count validation passed (>= %d rows)", min_rows)

        return results

    @staticmethod
    def level2_check_spark(df, config):
        """Spark-native Level 2 checks."""
        from pyspark.sql.functions import col as spark_col

        checks = config.get('level2') or {}
        check_nulls = checks.get('check_nulls') or config.get('check_nulls')

        results = {"passed": True, "errors": []}

        if check_nulls:
            logger.info("Checking nulls in %d columns...", len(check_nulls))
            for column in check_nulls:
                if column in df.columns:
                    logger.info("Checking nulls in column '%s'...", column)
                    null_count = df.filter(spark_col(column).isNull()).count()
                    if null_count > 0:
                        logger.warning("Column '%s' has %d null values", column, null_count)
                        results['passed'] = False
                        results['errors'].append(f"Null values found in mandatory column: {column} (count: {null_count})")

        return results
