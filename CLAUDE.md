# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

OpenICU is an open-source Python framework for extracting and harmonising ICU time series data from heterogeneous datasets (MIMIC-IV, eICU-CRD, NWICU, custom institutional data) into the standardized MEDS (Medical Event Data Standard) format. It is a Python successor to R tools like `ricu`: clinical concepts are defined once and mapped per dataset via declarative YAML, so the same analysis code runs against any supported dataset.

**Key goals:** multiple data sources, declarative YAML configuration, MEDS-compliant output, fully offline operation (medical data privacy), laptop-scale performance via Polars lazy streaming.

## Development Commands

```bash
uv sync --all-groups          # install all dependencies (dev container has them already)

uv run pytest                 # all tests with coverage
uv run pytest --no-cov        # without coverage
uv run pytest tests/test_example.py::test_function_name

uv run ruff format            # format
uv run ruff check --fix       # lint (line length 120)
uv run ty check .             # type checking (ty, used in CI; a [tool.mypy] config also exists)

uv run mkdocs serve           # docs locally
uv run jupyter lab            # example notebook: example/pipeline.ipynb
```

## Architecture

The pipeline is **project + steps**, all processing on **Polars LazyFrames** (streaming `sink_parquet`), all configuration in **Pydantic v2** models loaded from YAML.

Public API (`src/open_icu/__init__.py`): `OpenICUProject`, `ExtractionStep`, `ConceptStep`.

```python
with OpenICUProject(project_path) as project:
    ExtractionStep.load(project, config_path / "extraction.yml").run()
    ConceptStep.load(project, config_path / "concept.yml").run()
```

### Modules (`src/open_icu/`)

- **`config/`** — `BaseConfig` (name + version → deterministic identifier `openicu.config.<type>.<...>` and uuid5; YAML load/save) and `BaseConfigRegistry` (generic registry with `includes`/`excludes` filtering, recursive directory loading).
- **`storage/`** — `OpenICUProject` (manages `datasets/`, `workspace/`, `configs/` directories), `WorkspaceDir`, `MEDSDataset` (writes `metadata/dataset.json` validated against the MEDS schema and `metadata/codes.parquet` code vocabulary).
- **`steps/base/`** — `ConfigurableBaseStep.run()` lifecycle: `setup_config` (load YAML into the step's singleton registry, snapshot merged configs to `<project>/configs/`) → `setup_project` → `extract` (abstract) → `hooks` (TODO, no-op) → `collect` (copy workspace parquet to `datasets/<step>/data/` + write metadata). Steps are **skipped** when `overwrite: false` and outputs exist.
- **`steps/extraction/`** — reads raw CSV/CSV.GZ tables per `TableConfig`: typed `scan_csv`, datetime parsing, lookup joins, then per event: column mapping to MEDS, code assembly as `dataset//table//code_prefix//code parts//code_suffix`, casts (`subject_id` Int64, `time` Datetime[us], `code` Str, `numeric_value` Float32, `text_value` Str + extension columns), sink to `workspace/<step>/<dataset>/<version>/<table>/<EVENT>.parquet` (appends if file exists). Callback/filter hook order: table `pre_callbacks` → `pre_filters` → datetime → `callbacks` → `filters`; per join `post_join_*`; table `post_join_*` → `transformations`; per event `pre_callbacks` → mapping → `callbacks` → `filters` → `transformations` → select/cast → `output_filters`.
- **`steps/concept/`** — harmonises extraction output into dataset-agnostic concepts. `ConceptConfig` (name, version, unit → output code `name//unit`) + per-dataset configs discovered by matching filename in `dataset_configs` paths. Three types (pydantic discriminated union on `type`): `simple` (regex on code column per mapping pattern), `derived` (depends on other concepts; dependency graph resolved with `graphlib.TopologicalSorter`; joins concept parquets), `complex` (dotted-path Python `ConceptTransformer` called with the project). Output: `workspace/<step>/<concept name>/<version>/<dataset>.parquet`.
- **`steps/sharding/`** — **work in progress**: `extract()` is a no-op, `ShardingConfig` is a sketch; not exported from the package.
- **`callbacks/`** — the YAML expression DSL. `interpreter.py` parses Python-syntax expression strings with `ast` (no eval): bare names = column refs, literals, operators (`+ - * /`, comparisons, `& |`/`and or not`), calls = registered callbacks. Callback classes implement `__init__(args) / __call__(lf) -> pl.Expr` (or LazyFrame for transformations), registered via `@register_callback_cls` under snake_case of the class name (`AddOffset` → `add_offset`). Built-ins in `_callbacks/`: col, const, cast, replace, to_datetime, add_offset, set_time, first_not_null, max, drop_na, drop_if, first_distinct, split_explode, arithmetic/comparison/logic.
- **`utils/`** — dotted-path importer, camel→snake, generic-type introspection. **`logging.py`** — `configure_logging`/`get_logger` under the `open_icu` logger.

### Configuration layout (`config/`)

```
config/
├── concepts/<category>/<name>.yml                     # dataset-agnostic concept (name, version, unit)
└── datasets/<dataset>/<version>/
    ├── extends.yml                                    # optional: inherit from a reference version
    ├── tables/<table>.yml                            # extraction table configs
    └── mappings/<name>.yml                            # per-dataset concept mappings (type: simple|derived|complex)
```

Dataset, version, and name are inferred from file paths for dataset-bound configs. A version dir with an `extends.yml` (keys: `dataset`, `version`) inherits all configs from the referenced version: files deep-merge (dicts merge, lists/scalars replace), `deleted: true` tombstones an inherited config, chains resolve recursively, and identity always comes from the extending version's directory (`src/open_icu/config/inheritance.py`; hooks in `registry.load_configs` and `ConceptConfig.load`). Diffs stack forward in time: the oldest fully-specified version is the reference (mimic-iv 2.2 is the reference; 3.1 and mimic-iv-demo 2.2 are marker-only extends; eicu-demo 2.0 extends eicu-crd 2.0 with one path override). A version dir may contain *only* an `extends.yml`. Bundled: mimic-iv 3.1 (most complete), eicu-crd 2.0, eicu-demo 2.0, nwicu 0.1.0; ~90 shared concepts. Step configs (see `example/config/{extraction,concept}.yml`) select `config_files` (with optional `includes`/`excludes` by identifier) and dataset data paths.

## Important Conventions

- Python 3.13+, line length 120 (Ruff), typed defs required, `uv` for everything.
- Conventional Commits: `feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`, `infra:`.
- Tests in `tests/` (`test_*.py`), importlib import mode, coverage on `src/` with an enforced `--cov-fail-under` floor. The suite covers the expression DSL, callbacks, config models/registries, storage, end-to-end step runs on synthetic fixtures (`tests/steps/conftest.py`), and validation of every shipped YAML in `config/` (`tests/test_shipped_configs.py`). Step tests must use the `clean_registries` autouse fixture pattern — the config registries are global singletons.
- Docs: README + `docs/` (MkDocs: getting_started/, user_guide/, arc42 in arc/). Keep README, docs/user_guide, and this file in sync with architecture changes.

## Common Workflows

**Add a dataset:** create `config/datasets/<name>/<version>/tables/*.yml` table configs (pure YAML, no Python); reference from an extraction step config under `config_files` + `config.data`; verify via `datasets/extraction/metadata/codes.parquet`.

**Add a concept:** define `config/concepts/<category>/<name>.yml`, then add a same-named mapping YAML per dataset under `config/datasets/<dataset>/<version>/mappings/`.

**Add a callback:** new class in `src/open_icu/callbacks/_callbacks/`, decorate with `@register_callback_cls`, export in `callbacks/__init__.py`; it becomes available in YAML as snake_case.

## Notes

- Active development, pre-1.0; config formats may still change.
- The sharding step and step `hooks()` are open work.
- The docs site builds with `uv run mkdocs build`; API reference pages are generated by `docs/scripts/gen_ref_pages.py`.
