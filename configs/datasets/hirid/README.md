# HiRID

Configurations for [HiRID](https://physionet.org/content/hirid/1.1.1/), the
high time-resolution ICU dataset from Bern University Hospital (requires
credentialed PhysioNet access). Unlike [AmsterdamUMCdb](../aumc/), HiRID is read
in its **native long format**, not through an OMOP export — so it ships its own
table configs rather than extending the [`omop` reference](../omop/).

## Versions

- `1.1.1/` — current configuration: 3 table configs (`tables/`) and the
  per-dataset concept `mappings/`.

## Configured tables

HiRID stores everything as long, ID-keyed event tables. Each event's MEDS code is
built as `hirid // <table> // <id> // <Variable Name> // <Unit>` — the human-
readable name and unit are joined in from `hirid_variable_reference.csv` (the same
reference covers both observation and pharma variables), so the codes are
self-documenting and harmonisation can key on the leading numeric id.

| HiRID table     | Source                          | Events emitted                                  |
| --------------- | ------------------------------- | ----------------------------------------------- |
| `observations`  | `observation_tables/` (`variableid`) | `OBSERVATION`                              |
| `pharma`        | `pharma_records/` (`pharmaid`)  | `PHARMA`                                         |
| `general`       | `general_table.csv`             | `SEX`, `AGE`, `MORTALITY`, `ICU_ADMISSION`, `MEDS_BIRTH` |

## Concept mappings

The shared concepts in `configs/concepts/` are ricu-derived, and so are these
mappings: the HiRID `variableid`s and unit conversions are taken from
[`ricu`](https://github.com/eth-mds/ricu)'s `hirid` entries in
`concept-dict.json`. A `simple` mapping matches codes of the form
`hirid//observations//<variableid>`, with the unit conversion applied inline so
values come out in each concept's target unit, e.g. creatinine (µmol/L → mg/dL):

```yaml
type: simple
mappings:
  - pattern:
      table: observations
      event: OBSERVATION
      code: "(20000600)(//.*)?$"   # ricu crea, HiRID variableid 20000600
    columns:
      numeric_value: col(numeric_value) * 0.011312
      text_value: col(text_value)
```

The id pattern is anchored with `(//.*)?$` so a short id (e.g. `200`) does not
match a longer one (`2000`). Every conversion factor was checked against the
concept's declared `unit` before being applied.

### Coverage

Currently mapped (all `simple`): the laboratory panels (blood gas, chemistry,
haematology) from `observations`, vital signs, GCS components (eye/motor/verbal)
and RASS, height/weight, and the demographics from `general` (`patient_sex`,
`patient_age`, `in_hospital_mortality`).

Not yet mapped / not cleanly mappable as `simple`:
- **Drug rates / durations** (vasopressors, antibiotics, corticosteroids,
  insulin, dextrose): live in `pharma` and need `derived`/`complex` handling of
  HiRID's per-record dosing.
- **Urine output**: HiRID records a cumulative signal that ricu de-accumulates
  (`hirid_urine`) — needs a `complex` transform.
- **GCS total** and **lymphocytes**: derived (sum of GCS components / blood-cell
  ratio against the white-cell count).
- **Ventilation** windows / start / end / tracheostomy and **length-of-stay**
  outcomes: `derived`/`complex`.
- Concepts with **no HiRID source** in ricu (not invented here): `troponin_I`,
  `hematocrit`, `basophils`, `eosinophils`, `erythrocyte_distribution_width`,
  `prothrombine_time`, `red_blood_cell_count`, `Hemoglobin_A1C`, `totcal_CO2`,
  `patient_admission_type`.

## Conventions

- HiRID is distributed as many partitioned parquet parts, so `observations` and
  `pharma` use a **glob path** (`observation_tables/parquet/*.parquet`) that the
  extraction step expands and reads as one frame; `general` is a single CSV. All
  paths are relative to the dataset root and assume the standard PhysioNet
  extraction layout — adjust them in the table configs if you extracted the
  tarballs differently (e.g. under a `reference_data/` subdirectory).
- `entertime` (observations) and `enteredentryat` (pharma) are genuine store
  times, so they populate the `available_time` extension. HiRID has no recorded
  date of birth, so `MEDS_BIRTH` is derived as `admissiontime` minus `age` years.
- `patientid` is used as the MEDS `subject_id`.
- The `ordinal_vars_ref.csv` reference (which decodes coded ordinal/categorical
  observation values into text) is not wired in yet — it is only needed for the
  deferred categorical concepts (e.g. ventilation mode), so it will land with
  that batch.

## Layout

```
<version>/
├── tables/<table>.yml   # how to read each HiRID table and which MEDS events to emit
└── mappings/<name>.yml  # how shared concepts map onto HiRID variableids
```
