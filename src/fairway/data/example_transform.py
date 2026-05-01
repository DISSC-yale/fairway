from fairway.transformations.base import BaseTransformer
import pandas as pd


class ExampleTransformer(BaseTransformer):
    """
    An example transformer that works with the DuckDB engine (Pandas DataFrames).
    """

    def transform(self):
        if isinstance(self.df, pd.DataFrame):
            return self._transform_pandas()
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
