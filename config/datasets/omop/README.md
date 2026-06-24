# OMOP CDM

Configurations for datasets stored in the [OHDSI OMOP Common Data
Model](https://ohdsi.github.io/CommonDataModel/). Unlike the source-specific
configs (MIMIC-IV, eICU, NWICU), this is a *model* configuration: it reads the
standardised OMOP CDM tables directly, so it applies to any dataset that has
been ETL'd into OMOP — for example
[AmsterdamUMCdb via AMSTEL](https://github.com/AmsterdamUMC/AMSTEL).

## Versions

- `5.4/` — **reference configuration** for OMOP CDM v5.4. Eleven table configs
  (`tables/`) covering the clinical, demographic and provenance tables that the
  AMSTEL ETL populates from AmsterdamUMCdb.

No per-dataset concept mappings (`mappings/`) ship yet — the table configs only
extract the raw OMOP events into MEDS. Concepts are mapped on the standardised
`*_concept_id` codes and can be added later under `5.4/mappings/`.

## Configured tables

Each event's MEDS code is built as `omop // <table> // <concept_id> //
<source_value> [...]`, so harmonisation can key on the OMOP `*_concept_id`.

| OMOP table             | Events emitted                                   |
| ---------------------- | ------------------------------------------------ |
| `person`               | `MEDS_BIRTH`, `GENDER`, `RACE`, `ETHNICITY`      |
| `death`                | `MEDS_DEATH`                                      |
| `visit_occurrence`     | `VISIT_START`, `VISIT_END`                        |
| `observation_period`   | `OBSERVATION_PERIOD_START`, `OBSERVATION_PERIOD_END` |
| `condition_occurrence` | `CONDITION`                                       |
| `drug_exposure`        | `DRUG_START`, `DRUG_END`                          |
| `procedure_occurrence` | `PROCEDURE`                                       |
| `measurement`          | `MEASUREMENT`                                     |
| `observation`          | `OBSERVATION`                                     |
| `device_exposure`      | `DEVICE_START`, `DEVICE_END`                      |
| `specimen`             | `SPECIMEN`                                        |

These mirror the 11 clinical/demographic OMOP tables filled by AMSTEL's
`insert_*.sql` scripts. The remaining AMSTEL targets are intentionally left out
of the extraction configs: the dimension tables `location`, `care_site`,
`provider` and the metadata table `cdm_source` carry no subject/time events,
and the derived `condition_era` / `drug_era` tables duplicate
`condition_occurrence` / `drug_exposure`.

## Conventions

- Each table is read from a single Parquet file (`<table>.parquet`). Parquet is
  the default format, so no `type` is set; point `path` at a `.csv`/`.csv.gz`
  export (and the type is inferred from the extension) if your data is not
  Parquet.
- OMOP `*_datetime` columns are nullable while the matching `*_date` is
  mandatory, so the event timestamp is `first_not_null(<x>_datetime, <x>_date)`.
  Columns stored as native Parquet timestamps/dates are used as-is; if your
  export stores them as strings they are parsed with the per-column
  `format`/`strict` params (`%Y-%m-%d %H:%M:%S` for datetimes, `%Y-%m-%d` for
  dates).
- `person_id` is used as the MEDS `subject_id`.
- No `available_time` extension is emitted. The OMOP CDM records no
  entry/result/store time, so there is no honest value for *when an event became
  knowable*; setting it to the event time would assert a zero latency we cannot
  justify (and risk leakage). It is left unset rather than fabricated — a
  consumer that wants the optimistic assumption can coalesce it to `time`
  downstream.

## Layout

```
<version>/
└── tables/<table>.yml   # how to read each OMOP table and which MEDS events to emit
```
