# Extraction configuration

The extraction step turns raw source tables into MEDS event streams. It is driven by two kinds of YAML:

1. a **step config** that selects which table configs to load and where the raw data lives, and
2. one **table config** per source table that describes columns, joins, and the events to emit.

OpenICU ships table configs for MIMIC-IV 3.1, eICU-CRD 2.0, and NWICU 0.1.0 under `configs/datasets/<dataset>/<version>/tables/`. This page explains how they work, so you can adapt them or add your own datasets.

## Step configuration

```yaml
name: Extraction
version: 1.0.0

config_files:
  - path: /path/to/configs/datasets/mimic-iv/3.1/tables/
    # includes:
    #   - openicu.config.table.mimic-iv.3.1.labevents   # restrict to specific tables

config:
  data:
    - name: mimic-iv         # must match the dataset of the loaded table configs
      path: /path/to/physionet.org/files/mimiciv/3.1
```

`config.data` maps dataset names to local data roots. Table configs whose dataset has no entry are skipped with a warning, as are tables whose source file does not exist — so a partial download still processes whatever is available.

## Table configuration

A table config describes one source file and the events to extract from it. The dataset, version, and table name are inferred from the file's location, so the YAML starts directly with the table definition. A trimmed version of the bundled `labevents.yml`:

```yaml
path: hosp/labevents.csv.gz        # relative to the dataset root

columns:                           # only listed columns are read, with these types
  - name: subject_id
    type: int64
  - name: charttime
    type: datetime
    params:
      format: "%Y-%m-%d %H:%M:%S"  # passed to the datetime parser
  - name: itemid
    type: int64
  - name: valuenum
    type: float32
  - name: value
    type: string
  - name: valueuom
    type: string

join:                              # lookup tables joined before event extraction
  - path: hosp/d_labitems.csv.gz
    columns:
      - name: itemid
        type: int64
      - name: label
        type: string
    both_on: [itemid]              # or left_on/right_on; how: left (default)

event_defaults:                    # shared column mappings for all events
  subject_id: col(subject_id)

events:
  - name: LAB
    columns:
      time: col(charttime)
      code:                        # code parts, joined with "//"
        - col(itemid)
        - col(label)
        - col(valueuom)
      numeric_value: col(valuenum)
      text_value: col(value)
      extension:                   # extra output columns beyond the MEDS standard
        ref_range_lower: col(ref_range_lower)
```

### File format

The source file format is set with `type` (`parquet`, `csv`, or `csvgz`). You usually omit it: when `type` is not given it is inferred from the `path` extension (`.parquet`/`.pq` → parquet, `.csv.gz` → csvgz, `.csv` → csv), and any path without a recognised extension defaults to **parquet**. The bundled PhysioNet datasets are gzipped CSV (`.csv.gz`), so they read as `csvgz`; an OMOP export of Parquet files reads as `parquet` with no extra configuration.

### Columns and types

Only the columns listed under `columns` are read from the source file. Available types: `str`/`string`, `int`/`int8`–`int64`, `uint8`–`uint64`, `float`/`float32`/`float64`, `decimal`, `bool`/`boolean`, and `datetime`. For CSV sources every column is read as text and cast to the declared type; Parquet columns are cast from their stored types. Datetime columns are parsed with the `params` you provide (e.g. `format`) when stored as strings, while native Parquet timestamp/date columns are used directly.

### Joins

Each entry under `join` is itself a table definition (with its own `path`, `columns`, callbacks, and filters) plus join keys: `both_on` for same-named keys, or `left_on`/`right_on`, and `how` (default `left`). Joins typically attach human-readable labels (like MIMIC's `d_items`) or admission times needed to reconstruct timestamps (like eICU's `patient` table).

### Events

Each event produces one output file. The `columns` block maps source columns (or [expressions](expressions.md)) onto the MEDS schema:

- `subject_id`, `time` — required (directly or via `event_defaults`).
- `numeric_value`, `text_value` — optional; missing ones are filled with nulls.
- `code` — a list of code *parts*. The final code is assembled as

    ```
    <dataset>//<table>//<code_prefix parts>//<code parts>//<code_suffix parts>
    ```

    The dataset and table prefixes are added automatically, so every event code is traceable to its source. Typical code parts are the item ID, its label, and the unit of measurement: `mimic-iv//labevents//50912//Creatinine//mg/dL`.

- `extension` — a mapping of additional output columns, preserved alongside the MEDS standard columns (IDs like `hadm_id`/`stay_id`, data-availability timestamps, reference ranges, …).

`event_defaults` at the table level provides defaults for all events of the table; individual events override them (an explicit empty list overrides a default, too). One table can emit several events — for example, the bundled `inputevents.yml` produces `INFUSION_START`, `INFUSION_END`, and `SUBJECT_WEIGHT_AT_INFUSION` events from a single read.

### Callbacks, filters, and transformations

Tables, joins, and events all accept lists of [expressions](expressions.md) that hook into the processing at defined points:

- **callbacks** add or modify columns (`add_offset(admission_timestamp, labresultoffset, output=event_timestamp)`),
- **filters** keep only matching rows (`drop_na(valuenum)`, `col(rate) > 0`),
- **transformations** reshape the whole frame (`split_explode(drugname, ";")`).

Processing order:

1. *Per table and per join table:* `pre_callbacks` → `pre_filters` → datetime parsing → `callbacks` → `filters`
2. *After each join:* the join's `post_join_callbacks` → `post_join_filters`
3. *After all joins:* the table's `post_join_callbacks` → `post_join_filters` → `transformations`
4. *Per event:* `pre_callbacks` → column mapping and code construction → `callbacks` → `filters` → `transformations` → cast to MEDS schema → `output_filters`

A practical example from eICU, where timestamps are stored as minute offsets from ICU admission and have to be reconstructed:

```yaml
join:
  - path: patient.csv.gz
    both_on: [patientunitstayid]
    columns: [...]
    callbacks:
      - to_datetime(hospitaldischargeyear, 1, 1, hospitaladmittime24, hospitaladmitoffset, "minutes", output=admission_timestamp)

post_join_callbacks:
  - add_offset(admission_timestamp, labresultoffset, output=event_timestamp)
```

## Output layout

Within the extraction step's output dataset, files are organised by provenance:

```
datasets/extraction/data/<dataset>/<version>/<table>/<EVENT>.parquet
```

If the same event file already exists (e.g. when several table configs write to the same event), new rows are appended.

## Adding a new dataset

> For a new *version* of an already-supported dataset, or a *variant* like a demo subset, don't copy the configs — declare the differences against a reference version with `extends.yml`. See [dataset versions and variants](versioning.md).

1. Create `configs/datasets/<your-dataset>/<version>/tables/` and add one YAML per source table, as above.
2. Reference the directory from your extraction step's `config_files` and add the dataset's name and data path under `config.data`.
3. Run the extraction step and inspect `metadata/codes.parquet` to verify the extracted codes.
4. Optionally, add [concept mappings](concepts.md) so the shared concept dictionary covers your dataset.

No Python code is required for any of this.
