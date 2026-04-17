import abc

from typing import Any


class BaseTransformer(abc.ABC):
    """Base class for dataset-specific transformations.

    Subclasses must override ``transform()`` to return the transformed
    DataFrame. The helper methods (``rename_columns``, ``cast_types``,
    ``clean_strings``) mutate ``self.df`` in place and return ``self`` for
    chaining — they are building blocks for ``transform()`` implementations.
    """

    def __init__(self, df: Any):
        self.df = df

    @abc.abstractmethod
    def transform(self) -> Any:
        """Return the transformed DataFrame. Must be overridden."""

    def rename_columns(self, mapping: dict):
        self.df.rename(columns=mapping, inplace=True)
        return self

    def cast_types(self, schema: dict):
        for col, dtype in schema.items():
            if col in self.df.columns:
                self.df[col] = self.df[col].astype(dtype)
        return self

    def clean_strings(self, columns: list):
        for col in columns:
            if col in self.df.columns:
                self.df[col] = self.df[col].str.strip().str.lower()
        return self
