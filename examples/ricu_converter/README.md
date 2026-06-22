# RICU converter example

Use `01_generate_ricu_configs.ipynb` to generate OpenICU concept configs for the RICU source keys `miiv`, `mimic_demo`, `eicu` and `eicu_demo`.

The converter writes two kinds of files:

- global concept configs under `concept/<category>/<concept>.yml`
- dataset-specific mappings under `dataset/<dataset>/<version>/concept/<concept>.yml`

When multiple RICU sources generate the same global concept config, identical global configs are kept once. Dataset-specific configs remain distinct per dataset/version. Compatible simple dataset mapping collisions are merged by concatenating mappings; incompatible file-path collisions raise an error instead of silently overwriting.
