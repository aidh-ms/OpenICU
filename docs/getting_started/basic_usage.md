# Basic Usage

This walkthrough extracts MIMIC-IV into MEDS format and harmonises it into clinical concepts. It assumes you have [installed OpenICU](installation.md), cloned the repository (for the bundled configs in `config/`), and downloaded MIMIC-IV 3.1 from PhysioNet.

A complete, runnable version of this walkthrough is in [`example/pipeline.ipynb`](https://github.com/aidh-ms/OpenICU/blob/main/example/pipeline.ipynb).

## 1. Configure the extraction step

Create `config/extraction.yml` in your working directory. It tells OpenICU which table configurations to load and where your raw data lives:

```yaml
name: Extraction
version: 1.0.0

config_files:
  - path: /path/to/OpenICU/config/dataset/mimic-iv/3.1/dataset/

config:
  data:
    - name: mimic-iv
      path: /path/to/physionet.org/files/mimiciv/3.1
```

- `config_files` points at directories of table configurations (here: the bundled MIMIC-IV ones). You can list several directories to process multiple datasets in one run, and restrict which configs are loaded with `includes:` / `excludes:` lists of config identifiers.
- `config.data` maps each dataset name to the local root of its raw files. The name must match the `dataset` referenced by the table configs (the bundled ones use `mimic-iv`, `eicu-crd`, `nwicu`).

## 2. Configure the concept step

Create `config/concept.yml`:

```yaml
name: Concept
version: 1.0.0

config_files:
  - path: /path/to/OpenICU/config/concept

config:
  extraction_step: Extraction
  dataset_configs:
    - name: mimic-iv
      path: /path/to/OpenICU/config/dataset/mimic-iv/3.1/concept/
```

- `config_files` points at the dataset-agnostic concept dictionary.
- `extraction_step` names the extraction step whose output should be used as input.
- `dataset_configs` points at the per-dataset concept mappings.

## 3. Run the pipeline

```python
from pathlib import Path

from open_icu import OpenICUProject, ExtractionStep, ConceptStep

config_path = Path.cwd() / "config"
project_path = Path.cwd() / "output" / "project"

with OpenICUProject(project_path) as project:
    extraction_step = ExtractionStep.load(project, config_path / "extraction.yml")
    extraction_step.run()

    concept_step = ConceptStep.load(project, config_path / "concept.yml")
    concept_step.run()
```

By default a step is **skipped** if its output already exists, so re-running the script is cheap. Set `overwrite: true` in a step's YAML to force re-computation.

To see what is happening during a run, enable logging:

```python
from open_icu.logging import configure_logging

configure_logging(level="INFO")  # or "DEBUG"
```

## 4. Explore the output

The project directory now contains one MEDS dataset per step:

```
output/project/
├── configs/        # snapshot of every config used in this project
├── workspace/      # intermediate per-step files
└── datasets/
    ├── extraction/
    │   ├── data/mimic-iv/3.1/<table>/<EVENT>.parquet
    │   └── metadata/{dataset.json, codes.parquet}
    └── concept/
        ├── data/<concept>/<version>/<dataset>.parquet
        └── metadata/{dataset.json, codes.parquet}
```

All data files share the MEDS event schema — `subject_id`, `time`, `code`, `numeric_value`, `text_value` — plus any extension columns the configs define (e.g. `hadm_id`, `stay_id`). Read them with any Parquet-capable tool:

```python
import polars as pl

heart_rate = pl.scan_parquet(
    project_path / "datasets" / "concept" / "data" / "heart_rate" / "1.0.0" / "mimic-iv.parquet"
).collect()
```

`metadata/codes.parquet` lists every distinct code in a dataset — useful for discovering what was extracted and for building concept mappings of your own.

## Next steps

- [The pipeline in depth](../user_guide/pipeline.md) — projects, steps, registries, and the output layout
- [Extraction configuration](../user_guide/extraction.md) — add or adapt source tables and events
- [Concept configuration](../user_guide/concepts.md) — define and map clinical concepts
- [Expression language](../user_guide/expressions.md) — the callback expressions used throughout the configs
