# eICU-CRD demo

Configurations for the [eICU Collaborative Research Database demo](https://physionet.org/content/eicu-crd-demo/),
the freely accessible ~2,500-patient subset of eICU-CRD (no credentialing
required — useful for trying OpenICU end-to-end).

## Versions

- `2.0/` — **extends `eicu-crd/2.0`** (see `2.0/extends.yml`): all table
  configs and concept mappings of the full eICU-CRD are inherited
  automatically, since the demo contains the same tables with the same
  schema. Only the differences are spelled out:

    - `dataset/infusiondrug.yml` — the demo names the file
      `infusiondrug.csv.gz` (lowercase), unlike the full dataset's
      `infusionDrug.csv.gz`.

## Working with inherited configs

To override an inherited table or mapping, add a YAML file with the same name
containing only the keys to change (mappings merge recursively, lists are
replaced wholesale). A file containing `deleted: true` removes an inherited
config. See `docs/user_guide/versioning.md` for the full mechanism.
