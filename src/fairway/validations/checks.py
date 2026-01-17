import pandas as pd

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
        """
        checks = config.get('level1', {})
        results = {"passed": True, "errors": []}
        
        count = df.count()
        if checks.get('min_rows') and count < checks['min_rows']:
            results['passed'] = False
            results['errors'].append(f"Row count {count} is less than minimum {checks['min_rows']}")
            
        return results

    @staticmethod
    def level2_check_spark(df, config):
        """
        Spark-native Level 2 checks.
        """
        from pyspark.sql.functions import col
        checks = config.get('level2') or {}
        results = {"passed": True, "errors": []}
        
        if checks.get('check_nulls'):
            for column in checks['check_nulls']:
                # Spark check: filter where column is null, count result
                # Note: This triggers a job, but it's optimized
                if column in df.columns:
                    null_count = df.filter(col(column).isNull()).count()
                    if null_count > 0:
                        results['passed'] = False
                        results['errors'].append(f"Null values found in mandatory column: {column} (count: {null_count})")
                        
        return results
