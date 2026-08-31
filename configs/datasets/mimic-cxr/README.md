# MIMIC-CXR

Configurations for [MIMIC-CXR](https://physionet.org/content/mimic-cxr/), the
critical care database from the Beth Israel Deaconess Medical Center containing
Chest X-Rays
(requires credentialed PhysioNet access).

## Versions

- `2.0/` — **reference configuration**: 2 table configs (`tables/`,
  covering notes) and the per-dataset concept
  mappings (`mappings/`) for the shared dictionary in `configs/concepts/`.
  This is the most complete dataset configuration in OpenICU.
- `2.1/` — **extends `2.0`** (see `3.1/extends.yml`) with one table addition:
  v2.1 add the provider table to the dataset

## Layout

```
<version>/
├── tables/<table>.yml   # how to read each source table and which MEDS events to emit
└── mappings/<name>.yml  # how shared concepts map onto this dataset's codes
```

New MIMIC-IV versions should extend the closest existing version via an
`extends.yml` marker and state only their differences — see
`docs/user_guide/versioning.md`.
