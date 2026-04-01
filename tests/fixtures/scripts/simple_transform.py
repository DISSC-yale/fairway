"""
Fixture transform script.
Adds a 'processed' column set to True on every row.
Proves that fairway's transformation script execution path runs.

Fairway's TransformationRegistry loads class-based transformers (classes
ending in 'Transformer' that extend BaseTransformer).
"""
from fairway.transformations.base import BaseTransformer


class SimpleTransformer(BaseTransformer):
    """Add 'processed=True' column to every row."""

    def transform(self):
        self.df["processed"] = True
        return self.df
