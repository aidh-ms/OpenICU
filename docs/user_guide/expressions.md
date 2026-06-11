# Expression language

Wherever extraction and concept configurations compute, filter, or reshape values, they use a small, safe expression language. Expressions are written as strings in the YAML and compiled to [Polars](https://pola.rs) expressions — they execute inside Polars' lazy streaming engine, so they are fast and memory-efficient.

```yaml
callbacks:
  - add_offset(admission_timestamp, labresultoffset, output=event_timestamp)
filters:
  - col(numeric_value) > 0
transformations:
  - split_explode(drugname, ";")
```

## Syntax

The language is a restricted subset of Python expression syntax:

- **Bare names are column references**: `charttime`, `valuenum`. The explicit form `col(charttime)` is equivalent and used throughout the bundled configs for clarity.
- **Literals**: numbers, strings, booleans, `None`, lists, and tuples.
- **Operators**: arithmetic (`+`, `-`, `*`, `/`), comparisons (`==`, `!=`, `<`, `<=`, `>`, `>=`), and boolean logic (`&`/`and`, `|`/`or`, `~`/`not`). Example: `col(weight) / (col(height) * col(height))`.
- **Callback calls**: `name(arg, ..., key=value)` invokes a registered callback. Calls nest arbitrarily: `replace(col(temp_unit) == "F", (col(temp) - 32) / 1.8, col(temp), output=temp_c)`.
- Most callbacks accept an `output=<column>` keyword that names the resulting column; without it, the expression result replaces/feeds whatever context it is used in.

Arbitrary Python (attribute access, imports, comprehensions, chained comparisons) is intentionally **not** supported — configurations stay declarative and auditable.

## Where expressions run

Configs use expressions in three contexts with different expectations:

| Context | Applied with | Must produce |
| --- | --- | --- |
| `callbacks` (and `pre_`/`post_join_` variants), column mappings | `with_columns` | a column expression |
| `filters` (and variants) | `filter` | a boolean expression |
| `transformations` | replaces the frame | a new frame (e.g. `split_explode`) |

## Built-in callbacks

### Selection and constants

| Callback | Description |
| --- | --- |
| `col(name, output=...)` | Reference a column (or wrap any value as an expression) |
| `const(value, output=...)` | A constant value, e.g. `const(kg)` as a code part or `const(1)` as a flag |
| `first_not_null(a, b, ..., output=...)` | Row-wise first non-null value (coalesce) — useful for schema harmonisation |
| `max(a, b, ..., output=...)` | Row-wise maximum across columns |
| `cast(value, dtype, strict=False, output=...)` | Cast to a dtype (`int64`, `float32`, `string`, `datetime`, …) |
| `replace(condition, then_value, else_value, output=...)` | Conditional value (`when/then/otherwise`) |

### Date and time

| Callback | Description |
| --- | --- |
| `to_datetime(year, month, day, time, offset=None, offset_unit="minutes", output=...)` | Build a timestamp from components, optionally shifted by an offset column — the key tool for datasets like eICU that only store relative offsets |
| `add_offset(datetime, offset, offset_unit="minutes", output=...)` | Shift a timestamp by an offset column (`weeks` … `nanoseconds`) |
| `set_time(datetime, hours, minutes, seconds, output=...)` | Replace the time-of-day of a timestamp |

### Arithmetic

`add`, `subtract`, `multiply`, `divide`, `floor_divide`, `modulo`, `pow`, `root`, plus variadic `sum(...)` and `product(...)`. The infix operators `+ - * /` compile to the same callbacks.

### Comparison and logic

`equal`, `not_equal`, `greater_than`, `greater_equal`, `less_than`, `less_equal`, `and`, `or`, `not` — usually written with operators (`col(rate) > 0 and col(rate) < 100`).

### Filtering helpers

| Callback | Description |
| --- | --- |
| `drop_na(column)` | Keep rows where the column is not null |
| `drop_if(condition)` | Drop rows where the condition is true |
| `first_distinct(col_a, col_b, ...)` | Keep only the first row per distinct combination of the given columns |

### Frame transformations

| Callback | Description |
| --- | --- |
| `split_explode(column, separator=",", strip=True)` | Split a delimited string column and explode it into one row per element |

## Custom callbacks

The callback set is extensible from Python. A callback is a class with an `__init__` that captures the (already parsed) arguments and a `__call__` that receives the `LazyFrame` and returns a Polars expression (or a `LazyFrame`, for transformations). Registering it makes it available in YAML under the snake_case version of the class name:

```python
import polars as pl
from polars import LazyFrame

from open_icu.callbacks import register_callback_cls
from open_icu.callbacks.proto import AstValue, CallbackResult, to_expr


@register_callback_cls
class FahrenheitToCelsius:                      # YAML name: fahrenheit_to_celsius
    def __init__(self, value: AstValue, output: str | None = None) -> None:
        self.value = value
        self.output = output

    def __call__(self, lf: LazyFrame) -> CallbackResult:
        expr = (to_expr(lf, self.value) - 32) / 1.8
        return expr if self.output is None else expr.alias(self.output)
```

```yaml
callbacks:
  - fahrenheit_to_celsius(temperature, output=temperature_c)
```

Import the module that defines your callback before running the step so the registration executes. Use `to_expr` to accept columns, literals, and nested callbacks interchangeably — the same convention the built-ins follow.
