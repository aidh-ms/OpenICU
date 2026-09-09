# Sharding

The sharding step rewrites the long-format output of the concept step into
subject-oriented Parquet shard files. The output remains in long format.

## Step configuration

A sharding configuration can look like this:

```yaml
name: Sharding
version: 1.0.0

config:
  concept_step: Concept
  datasets: []
  concepts: []
  subjects: []
  subjects_per_shard: 1000
```

The available options are:

- `concept_step`: Name of the concept step whose output is used as input.
- `datasets`: Optional list of datasets to include. An empty list includes all available datasets.
- `concepts`: Optional list of concepts to include. An empty list includes all available concepts.
- `subjects`: Optional list of subjects to include. An empty list includes all available subjects.
- `subjects_per_shard`: Maximum number of subjects written to each shard.
- `event_order_config`: Optional path to a custom event-order configuration. If omitted, OpenICU uses its built-in default event order.

Relative `event_order_config` paths are resolved relative to the sharding
configuration file.

## Event ordering

Events are sorted within each shard by:

1. subject
2. timestamp
3. semantic event order
4. code

The semantic event order only affects events belonging to the same subject and
having the same timestamp. It does not change timestamps or move events across
different points in time.

This allows source measurements to appear before derived concepts when several
events occur at the same timestamp. For example:

```text
gcs_eye
gcs_motor
gcs_verbal
gcs_total
sofa_cns
sofa
```

The event-order value is used only as a temporary sorting key and is not written
to the output Parquet files.

## Custom event order

A custom event-order configuration can be provided as YAML:

```yaml
default_order: 30
unassigned: warn

groups:
  demographics:
    order: 20
    concepts:
      - patient_age
      - patient_sex
      - patient_height
      - patient_weight

  derived_first_level:
    order: 40
    concepts:
      - gcs_total

  derived_components:
    order: 50
    concepts:
      - sofa_cns

  derived_aggregates:
    order: 60
    concepts:
      - sofa
```

Lower order values are written before higher order values when the subject and
timestamp are identical.

Concepts that are not assigned to a group receive `default_order`.

The `unassigned` setting controls how OpenICU handles concepts that use the
default order:

- `ignore`: do not report unassigned concepts.
- `warn`: log a warning and continue.
- `error`: raise an error and stop the sharding step.

A concept can only be assigned to one event-order group.

To use the custom configuration, reference it from the sharding configuration:

```yaml
name: Sharding
version: 1.0.0

config:
  concept_step: Concept
  subjects_per_shard: 1000
  event_order_config: event_order.yml
```

If `event_order_config` is omitted, the built-in OpenICU default configuration
is used.

## Output

The step produces subject-oriented files such as:

```text
shard_00000.parquet
shard_00001.parquet
...
```

The persisted output schema is unchanged by event ordering. The ordering value
exists only during sorting and is removed before the shard is written.
