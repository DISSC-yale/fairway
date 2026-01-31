# Fixed-Width Format Test Fixtures

These fixtures are prepared for Chunk F (Fixed-Width Support).
The reader implementation is not yet complete.

## File Format

Fixed-width files have columns at specific character positions:

```
001Alice               030
002Bob                 025
003Carol               028
```

## Spec Format

Column specifications in `simple_spec.yaml` define:

| Field | Description |
|-------|-------------|
| `name` | Column name |
| `start` | 0-based character position |
| `length` | Number of characters |
| `type` | Data type (INTEGER, STRING, etc.) |

## Example Spec

```yaml
columns:
  - name: id
    start: 0
    length: 3
    type: INTEGER
  - name: name
    start: 3
    length: 20
    type: STRING
  - name: age
    start: 23
    length: 3
    type: INTEGER
```

## Notes

- Columns are padded with spaces to fill their width
- No delimiter between columns
- Parser must strip whitespace from STRING fields
- INTEGER fields should be parsed after stripping leading zeros
