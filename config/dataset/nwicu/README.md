# NWICU

Configurations for the [Northwestern ICU database (NWICU)](https://physionet.org/content/nwicu/),
a MIMIC-style ICU database from Northwestern Medicine (requires credentialed
PhysioNet access).

## Versions

- `0.1.0/` — current configuration: 9 table configs (`dataset/`). Concept
  mappings (`concept/`) are not yet written — contributions welcome, see
  `docs/user_guide/concepts.md`. Since NWICU follows the MIMIC schema
  closely, the MIMIC-IV mappings in `config/dataset/mimic-iv/3.1/concept/`
  are a good starting point.

## Layout

```
<version>/
├── dataset/<table>.yml   # how to read each source table and which MEDS events to emit
└── concept/<name>.yml    # how shared concepts map onto this dataset's codes
```
