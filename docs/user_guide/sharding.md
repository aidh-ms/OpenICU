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

## Event ordering

OpenICU uses the global event-order configuration in
`configs/event_order/default.yml`. Extraction, concept processing, and sharding
therefore use the same ordering rules.

Events are sorted by subject, timestamp, semantic group order, explicit
within-group order, and finally `code`.

Groups are matched with regular expressions against the first component of the
MEDS code (before `//`). Lower `order` values come first. Within a group,
entries listed in `explicit_order` come first in the configured order; remaining
events are ordered alphabetically by `code`.

```yaml
default_group_order: 50
unassigned: warn

groups:
  labs:
    order: 30
    patterns:
      - '^LAB$'
      - '^(albumin|creatinine|glucose|sodium)$'

  derived:
    order: 60
    patterns:
      - '^(gcs_|sofa($|_))'
    explicit_order:
      - gcs_eye
      - gcs_motor
      - gcs_verbal
      - gcs_total
      - sofa_cns
      - sofa
```

Events that do not match a group use `default_group_order`. During sharding,
`unassigned` controls whether unmatched selected concepts are ignored, warned
about, or treated as an error. Temporary ordering columns are removed before
Parquet output is written.

## Output

The step produces subject-oriented files such as:

```text
shard_00000.parquet
shard_00001.parquet
...
```

The persisted output schema is unchanged by event ordering. The ordering value
exists only during sorting and is removed before the shard is written.
