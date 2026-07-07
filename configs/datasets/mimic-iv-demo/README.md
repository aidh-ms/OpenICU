# MIMIC-IV demo

Configurations for the [MIMIC-IV Clinical Database Demo](https://physionet.org/content/mimic-iv-demo/),
the openly available 100-patient subset of MIMIC-IV (no credentialing
required — useful for trying OpenICU end-to-end).

## Versions

- `2.2/` — **extends `mimic-iv/2.2`** (see `2.2/extends.yml`), the reference
  configuration. The demo ships all tables referenced by the configurations
  with the same schema and file names, so table configs and concept mappings
  are inherited. One export quirk requires an override:

    - `tables/procedureevents.yml` — the demo ships the
      `originalamount`/`originalrate` headers in UPPERCASE, unlike every
      full MIMIC-IV release.

## Working with inherited configs

To override an inherited table or mapping, add a YAML file with the same name
containing only the keys to change (mappings merge recursively, lists are
replaced wholesale). A file containing `deleted: true` removes an inherited
config. See `docs/user_guide/versioning.md` for the full mechanism.
