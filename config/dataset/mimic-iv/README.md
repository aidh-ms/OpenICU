# MIMIC-IV

Configurations for [MIMIC-IV](https://physionet.org/content/mimiciv/), the
critical care database from the Beth Israel Deaconess Medical Center
(requires credentialed PhysioNet access).

## Versions

- `2.2/` — **reference configuration**: 19 table configs (`dataset/`,
  covering the `icu` and `hosp` modules) and the per-dataset concept
  mappings (`concept/`) for the shared dictionary in `config/concept/`.
  This is the most complete dataset configuration in OpenICU.
- `3.1/` — **extends `2.2`** (see `3.1/extends.yml`) with no overrides:
  v3.0/v3.1 only extended the data (stays through 2022) without schema
  changes, and v3.1's `labevents` itemids are consistent with v2.2.

The openly available 100-patient demo is configured in `../mimic-iv-demo/`
as a diff of `2.2`.

## Layout

```
<version>/
├── dataset/<table>.yml   # how to read each source table and which MEDS events to emit
└── concept/<name>.yml    # how shared concepts map onto this dataset's codes
```

New MIMIC-IV versions should extend the closest existing version via an
`extends.yml` marker and state only their differences — see
`docs/user_guide/versioning.md`.
