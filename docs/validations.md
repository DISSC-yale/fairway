# Validations

Data quality is a core pillar of **fairway**. The framework provides built-in validation levels that can be configured per dataset.

## Level 1: Sanity Checks

Level 1 checks are basic sanity checks that ensure the data "looks" right at a high level.

*   **Config Options**:
    *   `min_rows`: Fails the pipeline if the row count is below this threshold.
*   **Example**:
    ```yaml
    validations:
      level1:
        min_rows: 1000
    ```

## Level 2: Schema & Distribution Checks

Level 2 checks look deeper into the content of the data.

*   **Config Options**:
    *   `check_nulls`: A list of columns that must not contain any null values.
*   **Example**:
    ```yaml
    validations:
      level2:
        check_nulls:
          - "user_id"
          - "transaction_date"
    ```

## Validation Reports

When a pipeline runs, fairway generates a markdown report for each source file in `data/final/`. This report includes:

*   Status of all validation checks.
*   Summary statistics (row counts, etc.).
*   Error messages for failed checks.

If any validation fails, the file is marked as `failed` in the manifest and will need to be addressed before subsequent phases.
