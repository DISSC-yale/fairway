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

    @staticmethod
    def level2_check(df, config):
        """
        Schema and distribution checks.
        """
        checks = config.get('level2', {})
        results = {"passed": True, "errors": []}
        
        if checks.get('check_nulls'):
            for col in checks['check_nulls']:
                if col in df.columns and df[col].isnull().any():
                    results['passed'] = False
                    results['errors'].append(f"Null values found in mandatory column: {col}")
                    
        return results
