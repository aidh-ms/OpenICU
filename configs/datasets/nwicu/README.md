# NWICU

Configurations for the [Northwestern ICU database (NWICU)](https://physionet.org/content/nwicu/),
a MIMIC-style ICU database from Northwestern Medicine (requires credentialed
PhysioNet access).

## Versions

- `0.1.0/` — current configuration with 9 table configs (`tables/`) and
  concept mappings (`mappings/`). Only concepts with corresponding NWICU
  source data are included.

## Layout

```
<version>/
├── tables/<table>.yml   # how to read each source table and which MEDS events to emit
└── mappings/<name>.yml  # how shared concepts map onto this dataset's codes
```
