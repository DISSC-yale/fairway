# Custom Transformations

While **fairway** handles the heavy lifting of ingestion and validation, datasets often require unique reshaping logic (e.g., long-to-wide transformations or complex aggregations).

## Writing a Transformer

To create a custom transformer:
1.  Create a new Python file in `src/transformations/`.
2.  Define a class that takes a DataFrame (Pandas or Spark, depending on the engine) and implements a `transform()` method.

### Example: `my_transform.py`

```python
class MyTransformer:
    def __init__(self, df):
        self.df = df

    def transform(self):
        # Your custom logic here
        self.df['new_column'] = self.df['existing_column'] * 2
        return self.df
```

## Registering the Transformer

Link your transformer to a specific source in the YAML config. You can assign different transformers to different files.

```yaml
sources:
  - name: "sales_data"
    path: "data/raw/sales.csv"
    transformation: "src/transformations/sales_cleaner.py"

  - name: "customer_data"
    path: "data/raw/customers.json"
    transformation: "src/transformations/customer_flattener.py"
```

Fairway will dynamically load the specified script for each source.

## Data Flow & State Preservation

Fairway implements a "Researcher Data Flow" that preserves data at each stage:

1.  **Raw**: Your original files in `data/raw/`.
2.  **Ingested (Faithful)**: A direct Parquet conversion of the raw data, stored in `data/intermediate/{name}.parquet`. This is **always** created and preserved.
3.  **Transformed (Processed)**: If a transformation script is provided, Fairway applies it to the Ingested data and saves the result to `data/intermediate/{name}_processed.parquet`.
4.  **Final**: The validated dataset (Transformed or Ingested) is promoted to `data/final/`.

This ensures you can always inspect the "Ingested" state to verify that the raw data was read correctly before any custom logic was applied.

## Transformation Lineage

Fairway tracks the lineage of transformed data products, ensuring you can always trace a final table back to the specific raw source file and transformation script version used to create it.
