"""
Fixture transform script.
Adds a 'processed' column set to True on every row.
Proves that fairway's transformation script execution path runs.
The transform() function is called by fairway's TransformationRegistry.
"""


def transform(df):
    """Add 'processed=True' column to every row."""
    df["processed"] = True
    return df
