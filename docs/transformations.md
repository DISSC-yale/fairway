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

Link your transformer to a dataset in the YAML config:

```yaml
data:
  transformation: "my_transform"
```

fairway will dynamically load `MyTransformer` from `src/transformations/my_transform.py` and apply it to the data during Phase III of the pipeline.

## Transformation Lineage

fairway tracks the lineage of transformed data products, ensuring you can always trace a final table back to the specific raw source file and transformation script version used to create it.
