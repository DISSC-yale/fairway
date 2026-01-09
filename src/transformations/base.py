import pandas as pd

class BaseTransformer:
    """Base class for dataset-specific transformations."""
    
    def __init__(self, df: pd.DataFrame):
        self.df = df
    
    def transform(self) -> pd.DataFrame:
        """Override this method in subclasses."""
        return self.df
    
    def rename_columns(self, mapping: dict):
        """Standard helper for column renaming."""
        self.df.rename(columns=mapping, inplace=True)
        return self
    
    def cast_types(self, schema: dict):
        """Standard helper for type casting."""
        for col, dtype in schema.items():
            if col in self.df.columns:
                self.df[col] = self.df[col].astype(dtype)
        return self

    def clean_strings(self, columns: list):
        """Strip whitespace and lowercase strings."""
        for col in columns:
            if col in self.df.columns:
                self.df[col] = self.df[col].str.strip().str.lower()
        return self
