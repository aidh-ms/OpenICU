# SICdb

Configurations for the [Salzburg Intensive Care database
(SICdb)](https://physionet.org/content/sicdb/1.0.6/), a single-centre ICU database
from Salzburg (requires credentialed PhysioNet access). Like [HiRID](../hirid/) it is
read in its **native long format** (not OMOP), and its variable IDs + unit conversions
are taken from [`ricu`](https://github.com/eth-mds/ricu)'s `sic` entries in
`concept-dict.json`.

## Versions

- `1.0.6/` — current configuration: 4 table configs (`tables/`) and the per-dataset
  concept `mappings/`.

## Configured tables

SICdb stores measurements as long, ID-keyed CSV tables. Each event's MEDS code is built
as `sic // <table> // <id> // <ReferenceName> // <ReferenceUnit>` — the human-readable
name and unit are joined in from `d_references.csv` (the unified dictionary, keyed on
`ReferenceGlobalID`), so the codes are self-documenting and harmonisation can key on the
leading numeric id.

| SICdb table     | Source / key                     | Events emitted                                       |
| --------------- | -------------------------------- | ---------------------------------------------------- |
| `laboratory`    | `laboratory.csv` (`LaboratoryID`)| `LAB`                                                |
| `data_float_h`  | `data_float_h.csv` (`DataID`)    | `OBSERVATION`                                        |
| `cases`         | `cases.csv`                      | `SEX`, `AGE`, `HEIGHT`, `WEIGHT`, `ICU_ADMISSION`, `MEDS_BIRTH` |
| `medication`    | `medication.csv` (`DrugID`)      | `MEDICATION`                                         |

## Concept mappings

The shared concepts in `config/concepts/` are ricu-derived, and so are these mappings:
the SICdb `LaboratoryID`/`DataID`s and unit conversions come from ricu's `sic` source
entries. A `simple` mapping matches codes of the form `sic//laboratory//<id>`, with the
unit conversion applied inline so values come out in each concept's target unit, e.g.
blood urea nitrogen (SICdb stores urea, converted to BUN mg/dL):

```yaml
type: simple
mappings:
  - pattern:
      table: laboratory
      event: LAB
      code: "(355)(//.*)?$"   # ricu bun, SICdb LaboratoryID 355
    columns:
      numeric_value: col(numeric_value) * 0.467
      text_value: col(text_value)
```

The id pattern is anchored with `(//.*)?$` so a short id does not match a longer one.
Every conversion factor (bun `* 0.467`, ca `* 4.008`, mg `* 2.431`, phos `* 3.097521`,
tnt `/ 1000`, weight `* 0.001` g→kg) was checked against the concept's declared `unit`
before being applied.

### Coverage

Currently mapped (all `simple`): the laboratory panels (blood gas, chemistry,
haematology) from `laboratory`, the continuous vital signs from `data_float_h`
(heart rate, blood pressures, respiratory rate, temperature, FiO2, SpO2, urine output),
and the demographics from `cases` (`patient_sex`, `patient_age`, `patient_height`,
`patient_weight`).

Not yet mapped / not cleanly mappable as `simple`:
- **Drug rates / durations** (vasopressors, antibiotics): live in `medication` and need
  `derived`/`complex` handling of SICdb's per-record dosing.
- **in_hospital_mortality / outcomes**: from `cases.DischargeState`/`OffsetOfDeath`,
  whose code semantics still need decoding.
- Concepts with **no SICdb source** in ricu are simply absent (e.g. `tri`/triglycerides
  has a SICdb id but no OpenICU concept, so it is skipped).

## Conventions

- SICdb is fully de-identified: only the admission **year** is known and all times are
  integer **second-offsets** (a measurement's `Offset` is "seconds from that case's
  admission"). Times are reconstructed à la eICU. The MEDS subject is the **patient**
  (`PatientID`); a patient's stays are placed on one timeline anchored at the patient's
  **first** admission year, with `OffsetAfterFirstAdmission` spacing each readmission:
  `time = Jan1(first_admission_year) + OffsetAfterFirstAdmission + Offset` (seconds).
  The first-admission year is fetched via a join to `cases` filtered to
  `OffsetAfterFirstAdmission == 0`. `CaseID` is carried as an extension.
- No `available_time` extension — SICdb records no separate result/store time, so there
  is no honest value for *when an event became knowable* (see the convention used by the
  [`omop`](../omop/) configs).
- `data_float_h` is emitted one MEDS event per row from the validated value `Val` at its
  `Offset` (as ricu does). The row-level high-frequency detail SICdb also stores —
  `cnt` (number of raw samples) and `rawdata` (the packed sub-row waveform) — is **not**
  decoded; expanding the true high-frequency signal would be a separate feature.

## Layout

```
<version>/
├── tables/<table>.yml   # how to read each SICdb table and which MEDS events to emit
└── mappings/<name>.yml  # how shared concepts map onto SICdb LaboratoryID/DataIDs
```
