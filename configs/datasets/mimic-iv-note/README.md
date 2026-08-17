# MIMIC-IV-Note

Configurations for [MIMIC-IV](https://physionet.org/content/mimic-iv-note/), the
critical care database from the Beth Israel Deaconess Medical Center
(requires credentialed PhysioNet access).

## Versions

- `2.2/` — **reference configuration**: 4 table configs (`tables/`,
  covering notes) and the per-dataset concept
  mappings (`mappings/`) for the shared dictionary in `configs/concepts/`.
  This is the most complete dataset configuration in OpenICU.

## Layout

```
<version>/
├── tables/<table>.yml   # how to read each source table and which MEDS events to emit
└── mappings/<name>.yml  # how shared concepts map onto this dataset's codes
```

New MIMIC-IV versions should extend the closest existing version via an
`extends.yml` marker and state only their differences — see
`docs/user_guide/versioning.md`.
