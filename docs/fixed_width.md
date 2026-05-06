# Fixed-width tables

Fixed-width source files (no delimiter, no header — columns defined by
character offsets) are supported by fairway via the unified
`tables/<t>/schema.yaml`. There is no separate spec-file format; every
column lists its `name`, `type`, and a `physical:` block.

## When to choose `fixed_width`

Set `source_format: fixed_width` in `tables/<t>/config.yaml`. Discovery
(`fairway discover <t>`) will refuse — fixed-width files have no
headers, so there is nothing to infer. You author `schema.yaml` by
hand.

## schema.yaml layout

```yaml
on_drift: strict
record_type_filter: {position: 0, length: 1, value: "H"}   # optional
columns:
  - name: id
    type: INTEGER
    physical: {start: 0, length: 5, trim: true}
  - name: name
    type: VARCHAR
    physical: {start: 5, length: 30, trim: true}
  - name: amount
    type: DECIMAL(10,2)
    physical: {start: 35, length: 12, trim: true}
```

Validation rules enforced at submit time:

- Every column must have a `physical:` block when
  `source_format: fixed_width`.
- The `physical` block requires both `start` and `length`.
- `record_type_filter` is allowed only for `source_format: fixed_width`.

## on_drift

The same three-axis `on_drift` model applies as for delimited tables —
see the in-repo plan (`notes/plans/2026-05-05-tables-layout-and-manifest-sot-v2.md`)
or `schema.yaml` comments. Strict default aborts on extra columns or
uncastable values; `lenient` includes extras and nulls cast failures.
