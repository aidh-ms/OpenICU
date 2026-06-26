# AmsterdamUMCdb (AUMCdb)

Configurations for [AmsterdamUMCdb](https://github.com/AmsterdamUMC/AmsterdamUMCdb),
the freely-accessible ICU database from Amsterdam UMC, after conversion to the
OMOP CDM 5.4 by the [AMSTEL ETL](https://github.com/AmsterdamUMC/AMSTEL).

## Versions

- `1.5.0/` — **extends `omop` 5.4** (see `1.5.0/extends.yml`). AUMCdb is read
  through its OMOP CDM 5.4 export, so all table configs are inherited unchanged
  from the [`omop` reference](../omop/); this directory adds only the
  AUMCdb-specific concept `mappings/`.

## Concept mappings

The shared concepts in `config/concepts/` are ricu-derived. Because AUMCdb is
read as OMOP, each mapping keys on the **OMOP `*_concept_id`** that AMSTEL
assigned to the corresponding AUMCdb item — taken from AMSTEL's Usagi mapping
files (`data/mappings/*.usagi.csv`). A `simple` mapping therefore matches codes
of the form `aumc//<table>//<concept_id>`, e.g. heart rate:

```yaml
type: simple
mappings:
  - pattern:
      table: measurement
      event: MEASUREMENT
      code: "(3027018|21490872)(//.*)?$"   # OMOP "Heart rate" concept_ids
    columns:
      numeric_value: col(numeric_value)
      text_value: col(text_value)
```

Each file's header comment lists the OMOP concept names behind the opaque ids.
For analytes with several targets (blood vs urine/CSF/body-fluid), only the
blood/serum/plasma `concept_id`s are selected.

### Coverage

Currently mapped (all `simple`, all from the OMOP `measurement` table): the
laboratory panels (blood gas, chemistry, haematology), vital signs, the core
respiratory numerics (SpO2, respiratory rate, FiO2) and urine output.

Not yet mapped / not cleanly mappable from OMOP:
- **Drug infusion rates** (`*_rate`, `*_duration`): AMSTEL stores the rate in
  the free-text `drug_exposure.sig` field, so a numeric rate cannot be recovered
  from the OMOP export.
- **Outcomes / length-of-stay** and **ventilation windows**: derived from
  `visit_occurrence`/`death`; need `derived`/`complex` mappings.
- **GCS / RASS**, demographics, drug *presence*, and microbiology: mappable from
  `listitems`/`person`/`drug_exposure`/`specimen`, pending.
- **troponin_I**, **erythrocyte_distribution_width**, **totcal_CO2**: no
  approved AMSTEL target in AUMCdb.
