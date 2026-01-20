from fairway.transformations.base import BaseTransformer
import pandas as pd

try:
    from pyspark.sql import DataFrame as SparkDataFrame
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    SparkDataFrame = type(None) # Dummy for isinstance check

class ExampleTransformer(BaseTransformer):
    """
    An example transformer that works with both DuckDB (Pandas) and PySpark engines.
    """
    
    def transform(self):
        if isinstance(self.df, pd.DataFrame):
            return self._transform_pandas()
        elif SPARK_AVAILABLE and isinstance(self.df, SparkDataFrame):
            return self._transform_spark()
        else:
            print(f"Warning: Unknown DataFrame type {type(self.df)}")
            return self.df

    def _transform_pandas(self):
        """Transformation logic for Pandas (DuckDB engine)"""
        print("Running Pandas transformation...")
        
        # 1. Simple Filtering
        if "amount" in self.df.columns:
            self.df = self.df[self.df["amount"] > 0].copy()
            
        # 2. Column Operations
        if "amount" in self.df.columns:
            self.df["amount_doubled"] = self.df["amount"] * 2
            
            # 3. Complex Logic (apply)
            import math
            def complex_math(x):
                # Handle nulls/errors
                return math.log(x + 1) if pd.notnull(x) and x > -1 else 0
                
            self.df["log_amount"] = self.df["amount"].apply(complex_math)
            
        return self.df

    def _transform_spark(self):
        """Transformation logic for PySpark (Spark engine)"""
        print("Running Spark transformation...")
        
        # 1. Native Spark Filtering
        if "amount" in self.df.columns:
            df_clean = self.df.filter(F.col("amount") > 0)
        else:
            df_clean = self.df
        
        # 2. Native Spark Column Operations
        if "amount" in df_clean.columns:
            df_clean = df_clean.withColumn("amount_doubled", F.col("amount") * 2)
        
        # 3. Complex Python Logic (via Vectorized UDF)
        @F.pandas_udf(DoubleType())
        def complex_math(amount_series):
            import math
            return amount_series.apply(lambda x: math.log(x + 1) if x and x > -1 else 0)

        if "amount" in df_clean.columns:
            df_enriched = df_clean.withColumn("log_amount", complex_math(F.col("amount")))
        else:
            df_enriched = df_clean
        
        return df_enriched
