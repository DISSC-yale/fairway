from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

class ExampleTransformer:
    def __init__(self, df):
        self.df = df

    def transform(self):
        """
        Example transformation that runs on Spark (Lazily).
        Input: self.df is a pyspark.sql.DataFrame
        Output: Must return a pyspark.sql.DataFrame
        """
        
        # 1. Native Spark Filtering (Pushed down to reader!)
        # This is extremely fast because it happens at the storage layer
        # Example: Filter for positive amounts if column exists
        if "amount" in self.df.columns:
            df_clean = self.df.filter(F.col("amount") > 0)
        else:
            df_clean = self.df
        
        # 2. Native Spark Column Operations
        if "amount" in df_clean.columns:
            df_clean = df_clean.withColumn("amount_doubled", F.col("amount") * 2)
        
        # 3. Complex Python Logic (via Vectorized UDF)
        # Use this when native Spark functions aren't enough (e.g. using a library)
        @F.pandas_udf(DoubleType())
        def complex_math(amount_series):
            # This function runs on workers, receiving a batch (pd.Series)
            import math
            # Handle potential nulls or errors gracefully in real code
            return amount_series.apply(lambda x: math.log(x + 1) if x and x > -1 else 0)

        if "amount" in df_clean.columns:
            df_enriched = df_clean.withColumn("log_amount", complex_math(F.col("amount")))
        else:
            df_enriched = df_clean
        
        return df_enriched
