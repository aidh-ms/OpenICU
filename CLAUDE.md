# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

OpenICU is an open-source Python framework for extracting, preprocessing, and analyzing ICU time series data from diverse datasets (MIMIC, eICU, etc.). The project converts heterogeneous ICU data into the standardized MEDS (Medical Event Data Standard) format, enabling reproducible research workflows.

**Key Goals:**
- Support multiple ICU data sources (public datasets like MIMIC, eICU, and custom institutional data)
- Extract medical concepts using declarative YAML configurations
- Export to MEDS format for standardized analysis
- Operate fully offline for medical data privacy compliance

## Development Commands

### Environment Setup
```bash
# The project uses uv for dependency management
# Dependencies are already installed in the dev container

# Install dependencies manually if needed:
uv sync
uv sync --all-groups  # Install with dev and docs groups
```

### Testing
```bash
# Run all tests with coverage
uv run pytest

# Run tests without coverage reports
uv run pytest --no-cov

# Run a specific test file
uv run pytest tests/test_example.py

# Run a specific test
uv run pytest tests/test_example.py::test_function_name
```

### Code Quality
```bash
# Format code with ruff
uv run ruff format

# Lint code with ruff
uv run ruff check

# Lint with auto-fix
uv run ruff check --fix

# Type checking with mypy
uv run mypy src/
```

### Documentation
```bash
# Serve documentation locally
uv run mkdocs serve

# Build documentation
uv run mkdocs build
```

### Jupyter Notebooks
```bash
# Launch JupyterLab for examples
uv run jupyter lab

# Example notebooks are in example/ directory
# - example_mimic.ipynb: Full MIMIC-IV extraction workflow
# - example_eicu.ipynb: eICU extraction workflow
```

## Architecture

### Core Components

**1. Configuration System (`src/open_icu/config/`)**
   - `source.py`: Defines data source configurations (SourceConfig, TableConfig, EventConfig)
   - `concept.py`: Medical concept definitions (currently a placeholder)
   - `utils.py`: YAML config loading utilities
   - Configuration files live in `configs/source/` (e.g., `mimic.yml`, `eicu.yml`)

**2. MEDS Processing (`src/open_icu/meds/`)**
   - `processor.py`: Core ETL logic - reads source data, performs joins, transforms to MEDS format
   - `processor_rs.py`: Rust-based processor (untracked file, likely in development)
   - `schema.py`: PyArrow schema definitions for MEDS data validation
   - `project.py`: MEDSProject class for managing output directory structure

### Configuration-Driven Architecture

The system uses a declarative YAML configuration approach:

1. **Source Configurations** (`configs/source/`): Define how to map raw ICU data tables to MEDS events
   - Specify tables, fields, data types, and joins
   - Define events to extract (e.g., medications, chart events, ICU stays)
   - Map source columns to MEDS schema fields (subject_id, time, code, numeric_value, text_value)
   - Support for calculated datetime fields and field constants

2. **Processing Flow**:
   - Load source config from YAML
   - Read source data tables using Dask (for scalability)
   - Apply field type conversions and datetime calculations
   - Perform table joins (e.g., joining item IDs with labels)
   - Extract events per configuration
   - Concatenate multi-field codes (e.g., "label//unit")
   - Write to Parquet files in MEDS format
   - Generate metadata (codes.parquet, dataset.json)

3. **MEDS Schema**:
   - Standard fields: subject_id, time, code, numeric_value, text_value
   - Extension fields: hadm_id, stay_id (hospital admission ID, ICU stay ID)
   - All data validated against PyArrow schema

### Key Design Patterns

- **Pydantic Models**: All configs use Pydantic for validation with computed fields
- **Dask DataFrames**: Used for parallel, out-of-core processing of large datasets
- **PyArrow Backend**: String data uses PyArrow for memory efficiency
- **Partition-based Processing**: map_partitions used for custom transformations
- **MEDS Compliance**: Outputs conform to MEDS v0.4.0+ standard

## Data Flow Example

For MIMIC-IV medications table:
1. Read `icu/inputevents.csv` (starttime, endtime, amount, rate, itemid)
2. Join with `icu/d_items.csv` (itemid → label)
3. Extract two events:
   - "dosage" event: endtime + label//amountuom → numeric_value=amount
   - "rate" event: starttime + label//rateuom → numeric_value=rate
4. Write to `output/data/mimic_medications_{event}_{partition}.parquet`
5. Accumulate unique codes to `output/metadata/codes.parquet`

## Important Conventions

### Code Style
- Line length: 120 characters (Ruff configured)
- Python version: 3.13+ (specified in .python-version)
- Use Ruff for formatting and linting (follows Black-compatible style)
- Type hints required: mypy strict mode (`disallow_untyped_defs = true`)
- String storage: Use PyArrow backend for pandas/dask string columns

### Testing
- Test discovery: `test_*.py` or `*_test.py` pattern
- Coverage required on src/ directory
- Deprecation warnings are filtered in pytest config
- Import mode: importlib (not prepend/append)

### Commit Convention
Follow Conventional Commits specification:
- `feat:` for new features
- `fix:` for bug fixes
- `docs:` for documentation changes
- `refactor:` for code refactoring
- `test:` for test additions/corrections
- `chore:` for maintenance tasks
- `infra:` for infrastructure changes

### File Organization
- Source code: `src/open_icu/`
- Tests: `tests/` (mirrors src structure)
- Configs: `configs/source/` (YAML)
- Examples: `example/` (Jupyter notebooks)
- Docs: `docs/` (MkDocs with arc42 structure)
- MIMIC data example: `example/data/uncompressed/mimiciv/3.1/`

## Common Workflows

### Adding Support for a New Data Source

1. Create YAML config in `configs/source/your_source.yml`
2. Define tables, fields, join relationships, and events
3. Ensure datetime fields are properly configured (use CalcDatetimeFieldConfig if needed)
4. Test extraction with a Jupyter notebook in `example/`
5. Verify output schema matches MEDS format

### Extending the MEDS Schema

1. Modify `OpenICUMEDSData` class in `src/open_icu/meds/schema.py`
2. Add new extension fields with Optional() PyArrow types
3. Update `MEDSFieldsConfig.extension` in source configs
4. Update processor to handle new fields in column_order

### Running End-to-End Extraction

```python
from pathlib import Path
from open_icu.config.utils import load_yaml_configs
from open_icu.config.source import SourceConfig
from open_icu.meds.processor import process_table
from open_icu.meds.project import MEDSProject

# Load configurations
configs = load_yaml_configs(Path("configs/source"), SourceConfig)

# Create output project
project = MEDSProject(project_path=Path("output"), overwrite=False)

# Process each table
for config in configs:
    for table in config.tables:
        process_table(
            table=table,
            path=Path("data/source"),  # Path to source data
            output_path=project.project_path,
            src=config.name
        )

# Write metadata
project.write_metadata({"dataset_name": "my_dataset", "dataset_version": "1.0"})
```

## Dependencies

Key external libraries:
- **dask[complete]**: Parallel dataframe processing
- **duckdb**: SQL analytics engine (potential future use)
- **meds**: MEDS standard library and schema validation
- **pandas**: Data manipulation (with pyarrow backend)
- **polars**: Alternative dataframe library (not yet utilized)
- **pydantic**: Configuration validation
- **pyarrow**: Columnar data format and type system
- **pyyaml**: YAML config parsing

## Arc42 Documentation

The project follows arc42 architecture documentation in `docs/arc/`:
- Requirements are tracked by ID (F1-F9 for functional, Q1-Q7 for quality)
- Target users: Clinical researchers and data engineers
- Key quality goals: Reproducibility, usability, privacy, extensibility
- System must operate fully offline (no data leaves secure perimeter)

## Notes

- The project is in active development - some architecture docs are incomplete (building_block.md, strategy.md)
- Rust processor (`processor_rs.py`) appears to be a performance optimization effort
- Large datasets (MIMIC-IV) are processed with 6+ Dask workers for parallelism
- The codebase is ~500 lines of core Python code (excluding tests/examples)
