# YAIB export example

Use `01_export_yaib_dynamic_tables.ipynb` to generate YAIB/RICU-like dynamic wide tables from OpenICU concept parquets.

The notebook contains two main runs:

- `openicu_dyn_14d.parquet`: capped to 14 days (`max_hours = 336`)
- `openicu_dyn_all.parquet`: uncapped (`max_hours = null`)

It also includes validation and debugging cells corresponding to the earlier notebooks:

- `00_main.ipynb`
- `01_openicu_to_yaib_dyn.ipynb`
- `01_openicu_to_yaib_dyn_all.ipynb`
- `check_14_unequall_stays.ipynb`
