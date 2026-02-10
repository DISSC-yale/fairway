import logging

logger = logging.getLogger("fairway.validations")


class Validator:
    @staticmethod
    def level1_check(df, config):
        """
        Basic sanity checks: column counts, minimum rows.
        """
        checks = config.get('level1', {})
        results = {"passed": True, "errors": []}

        if checks.get('min_rows') and len(df) < checks['min_rows']:
            results['passed'] = False
            results['errors'].append(f"Row count {len(df)} is less than minimum {checks['min_rows']}")

        return results

    def level2_check(df, config):
        """
        Schema and distribution checks.
        """
        checks = config.get('level2') or {}
        results = {"passed": True, "errors": []}

        if checks.get('check_nulls'):
            for col in checks['check_nulls']:
                if col in df.columns and df[col].isnull().any():
                    results['passed'] = False
                    results['errors'].append(f"Null values found in mandatory column: {col}")

        return results

    @staticmethod
    def level1_check_spark(df, config):
        """
        Spark-native Level 1 checks.

        Uses limit() optimization for min_rows check to avoid full table scan.
        Only scans enough rows to verify the threshold.
        """
        checks = config.get('level1', {})
        results = {"passed": True, "errors": []}

        min_rows = checks.get('min_rows')
        if min_rows:
            logger.info("Validating minimum row count (threshold: %d)...", min_rows)
            # Optimization: Use limit() to avoid full scan
            # Only count up to min_rows — if we get fewer, validation fails
            sample_count = df.limit(min_rows).count()
            if sample_count < min_rows:
                results['passed'] = False
                results['errors'].append(f"Row count below minimum {min_rows} (found fewer than {min_rows} rows)")
            else:
                logger.info("Row count validation passed (>= %d rows)", min_rows)

        return results

    @staticmethod
    def level2_check_spark(df, config):
        """
        Spark-native Level 2 checks.
        """
        from pyspark.sql.functions import col
        checks = config.get('level2') or {}
        results = {"passed": True, "errors": []}

        columns_to_check = checks.get('check_nulls', [])
        if columns_to_check:
            logger.info("Checking nulls in %d columns...", len(columns_to_check))
            for column in columns_to_check:
                # Spark check: filter where column is null, count result
                # Note: This triggers a job per column
                if column in df.columns:
                    logger.info("Checking nulls in column '%s'...", column)
                    null_count = df.filter(col(column).isNull()).count()
                    if null_count > 0:
                        logger.warning("Column '%s' has %d null values", column, null_count)
                        results['passed'] = False
                        results['errors'].append(f"Null values found in mandatory column: {column} (count: {null_count})")

        return results
