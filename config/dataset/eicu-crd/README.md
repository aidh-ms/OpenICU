# eICU-CRD

Configurations for the [eICU Collaborative Research Database](https://physionet.org/content/eicu-crd/),
a multi-center ICU database (requires credentialed PhysioNet access).

## Versions

- `2.0/` — current configuration: 14 table configs (`dataset/`) and the
  beginnings of the per-dataset concept mappings (`concept/`, currently only
  `antibiotics`). Contributions of further concept mappings are very welcome —
  see `docs/user_guide/concepts.md`.

## Layout

```
<version>/
├── dataset/<table>.yml   # how to read each source table and which MEDS events to emit
└── concept/<name>.yml    # how shared concepts map onto this dataset's codes
```

Note that eICU stores timestamps as minute offsets from ICU admission; the
table configs reconstruct absolute timestamps by joining the `patient` table
(see the `to_datetime`/`add_offset` callbacks in e.g. `2.0/dataset/lab.yml`).

The freely available demo subset is configured in `../eicu-demo/` as a diff
of this dataset via `extends.yml`.
