# Dataset versions and variants

Dataset configurations live under `config/dataset/<dataset>/<version>/`. Without further measures, supporting a new dataset *version* (MIMIC-IV 3.0 → 3.1) or a *variant* (the eICU demo subset) would mean copying every table and concept YAML — and keeping the copies in sync forever.

OpenICU avoids this with **configuration inheritance**: a version can declare a reference version and spell out only its differences, like a diff applied on top of a base.

## Declaring a base version

Place an `extends.yml` marker in the version directory:

```
config/dataset/eicu-demo/2.0/
├── extends.yml          # <- the marker
├── dataset/
│   └── infusiondrug.yml # only the differences
└── concept/
```

```yaml
# extends.yml
dataset: eicu-crd
version: "2.0"
```

The marker applies to all config subdirectories of the version (`dataset/` and `concept/` alike).

## Resolution rules

When a version with an `extends.yml` is loaded, its effective configuration is built from the base upwards:

1. **Inherit** — files that exist only in the base are used unchanged.
2. **Merge** — files that exist in both are deep-merged: mappings are merged recursively with the extending file taking precedence, while **lists and scalars are replaced wholesale**. To change one column of a table, restate the whole `columns` list; to change just the file `path`, a one-line file suffices.
3. **Delete** — a file containing `deleted: true` removes the inherited config entirely (e.g. for tables excluded from a demo distribution, like the free-text notes excluded from MIMIC demos).

Bases may themselves extend other versions — chains resolve recursively, and cycles are rejected with an error. Diffs stack forward in time: the oldest fully-specified version is the reference, and each newer version (or variant) states only its changes — e.g. `mimic-iv/3.1` extends the `mimic-iv/2.2` reference, and `mimic-iv-demo/2.2` extends it too.

**Identity always comes from the extending version's directory**: a table inherited by `eicu-demo/2.0` is registered as `openicu.config.dataset.eicu-demo.2.0.<table>`, produces event codes prefixed `eicu-demo//…`, and inherited concept mappings match against those codes automatically. Nothing about the base leaks into the output.

## Example: the eICU demo

The eICU demo distribution contains all tables of the full eICU-CRD with the same schema (just ~2,500 patients instead of ~200,000). Its complete configuration is therefore:

```yaml
# config/dataset/eicu-demo/2.0/extends.yml
dataset: eicu-crd
version: "2.0"
```

```yaml
# config/dataset/eicu-demo/2.0/dataset/infusiondrug.yml
# The demo names this file in lowercase, unlike the full dataset's
# infusionDrug.csv.gz.
path: infusiondrug.csv.gz
```

Two small files instead of fourteen copied table configs — and when the eICU-CRD configs are improved, the demo picks the changes up automatically.

## Example: a version bump

For a hypothetical MIMIC-IV 3.1 → 4.0 upgrade where one table gained a column and another was renamed:

```
config/dataset/mimic-iv/4.0/
├── extends.yml                  # dataset: mimic-iv / version: "3.1"
└── dataset/
    ├── labevents.yml            # restated columns list (lists replace wholesale)
    └── old_table.yml            # deleted: true
```

Everything else — all unchanged tables and concept mappings — carries over.

## Notes

- The shipped-config test suite (`tests/test_shipped_configs.py`) validates *effective* configurations, so inherited and merged configs are checked exactly like physical files.
- Overrides are matched by file name (e.g. `dataset/labevents.yml` overrides the base's `dataset/labevents.yml`), regardless of which version of the chain a file physically lives in.
- A missing base version or a malformed marker fails loudly at load time rather than silently extracting nothing.
