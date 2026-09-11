# Medical Information Mart for Intensive Care Brazil

Configurations for [MIMIC-BR](https://physionet.org/content/mimic-br/1.0.1/), the
critical care database from the Einstein Hospital Israelita
(requires credentialed PhysioNet access).

## Versions

- `1.0.1/` — **reference configuration**: TODO

## Layout

```
<version>/
├── tables/<table>.yml   # how to read each source table and which MEDS events to emit
└── mappings/<name>.yml  # how shared concepts map onto this dataset's codes
```

New MIMIC-BR versions should extend the closest existing version via an
`extends.yml` marker and state only their differences — see
`docs/user_guide/versioning.md`.
