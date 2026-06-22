# Projects and the pipeline

OpenICU organises all processing as a sequence of **steps** that run inside a **project**. This page explains both, and the conventions they share.

## The project

`OpenICUProject` manages an output directory with three areas:

```
<project>/
├── configs/      # snapshot of every configuration used by any step
├── workspace/    # intermediate files, one subdirectory per step
└── datasets/     # final MEDS datasets, one per step
```

```python
from pathlib import Path
from open_icu import OpenICUProject

with OpenICUProject(Path("output/project")) as project:
    ...
```

Passing `overwrite=True` removes and recreates the whole project directory; the default is to reuse what is there.

The `configs/` snapshot is what makes runs reproducible: every step writes the merged set of configurations it actually used (after resolving `includes`/`excludes`) back into the project, so the exact extraction logic is preserved alongside the data.

## Steps

A step is loaded from a YAML file and executed with `run()`:

```python
from open_icu import ExtractionStep

step = ExtractionStep.load(project, Path("config/extraction.yml"))
step.run()
```

Two steps are currently available, with a third in development:

| Step | Input | Output |
| --- | --- | --- |
| `ExtractionStep` | Raw source tables (CSV/CSV.GZ) | Dataset-level MEDS events |
| `ConceptStep` | An extraction step's output | Harmonised, dataset-agnostic concept events |
| `ShardingStep` *(in development)* | A concept step's output | Analysis-ready data shards |

Every step's `run()` follows the same lifecycle:

1. **Load configurations** — each entry in the step's `config_files` list is read recursively from disk into the step's configuration registry, honouring `includes`/`excludes` and `overwrite`. The merged registry is saved to `<project>/configs/`.
2. **Set up directories** — a workspace directory (`workspace/<step name>`) and a MEDS dataset (`datasets/<step name>`) are created.
3. **Extract** — the step's core logic writes Parquet files into its workspace.
4. **Collect** — the workspace's Parquet files are copied into `datasets/<step name>/data/`, and MEDS metadata is generated: `metadata/dataset.json` (dataset metadata plus ETL/MEDS version info) and `metadata/codes.parquet` (the vocabulary of all distinct codes in the output).

### Skipping and overwriting

If a step's workspace **and** dataset already exist and the step config has `overwrite: false` (the default), the step is skipped entirely. Set `overwrite: true` in the step YAML to force re-computation. This makes pipeline scripts safely re-runnable.

### Common step configuration

All step YAML files share this structure:

```yaml
name: Extraction          # step name; lowercased, it names the output directories
version: 1.0.0
overwrite: false          # re-run even if output exists

config_files:             # configurations to load into the step's registry
  - path: /path/to/configs/
    overwrite: false      # replace configs already registered under the same identifier
    includes: []          # optional: only load these config identifiers
    excludes: []          # optional: skip these config identifiers

dataset:                  # optional metadata written to the output dataset.json
  metadata:
    dataset_name: my_dataset
    dataset_version: "1.0"

config:                   # step-specific settings, see the respective guide
  ...
```

## Configuration identifiers

Every configuration object (table, concept, …) has a `name` and a `version` and derives a stable, hierarchical identifier from them:

```
openicu.config.<type>.<...>.<name|version>
```

For example, the bundled MIMIC-IV labevents table config is `openicu.config.table.mimic-iv.3.1.labevents`, and the heart rate concept is `openicu.config.concept.heart_rate.1.0.0`. These identifiers are what you list in `includes`/`excludes`, and they determine the file layout when configs are snapshotted into the project. A deterministic UUID is derived from each identifier as well.

For dataset-bound configs the name, dataset, and version are inferred from the file's location (`config/datasets/<dataset>/<version>/tables/<name>.yml`), so the YAML files themselves stay minimal.

## The MEDS output

Each step's `datasets/<step name>/` directory is a self-contained [MEDS](https://github.com/Medical-Event-Data-Standard/meds) dataset:

- `data/**/*.parquet` — event streams with the columns `subject_id` (int64), `time` (datetime, microseconds), `code` (string), `numeric_value` (float32), `text_value` (string), plus any configured extension columns (e.g. `hadm_id`, `stay_id`, `available_time`).
- `metadata/dataset.json` — dataset metadata, validated against the MEDS schema, including the OpenICU ETL version and creation timestamp.
- `metadata/codes.parquet` — every distinct code in the dataset. Browse this file to discover what was extracted; it is also the natural starting point for writing [concept mappings](concepts.md).

## Logging

OpenICU uses standard Python logging under the `open_icu` logger:

```python
from open_icu.logging import configure_logging, set_log_level

configure_logging(level="INFO")   # console logging
set_log_level("DEBUG")            # change verbosity later
```

`DEBUG` traces every table, join, event, and written file — useful when developing new configurations.
