# Package overview

OpenICU is an open-source Python framework for extracting, preprocessing, and harmonising intensive care unit (ICU) time series data from diverse sources. It converts heterogeneous ICU datasets — public ones such as [MIMIC-IV](https://physionet.org/content/mimiciv/) and [eICU-CRD](https://physionet.org/content/eicu-crd/), as well as custom institutional data — into the standardised [MEDS](https://github.com/Medical-Event-Data-Standard/meds) (Medical Event Data Standard) format.

## The problem

ICU research routinely needs the same clinical variables — heart rate, creatinine, norepinephrine doses, ventilation episodes — but every dataset stores them differently: different table layouts, item identifiers, units, and timestamp conventions. Studies therefore accumulate one-off, dataset-specific extraction scripts that are hard to validate, hard to reuse, and hard to reproduce. Tools like the R package [`ricu`](https://github.com/eth-mds/ricu) showed that a shared, dataset-agnostic concept dictionary solves this; OpenICU brings that approach to the Python ecosystem and combines it with the MEDS output standard.

## The approach

OpenICU separates *what* to extract from *how* to extract it:

- **Declarative YAML configurations** describe source tables, joins, events, and clinical concepts. They are versioned, carry stable identifiers, and are snapshotted into every output project for reproducibility.
- **A processing pipeline** of steps executes those configurations:
    1. The **extraction step** turns raw source tables into MEDS event streams, with codes that preserve full provenance (`mimic-iv//chartevents//220045//Heart Rate//bpm`).
    2. The **concept step** maps those dataset-specific codes onto shared clinical concepts (`heart_rate//bpm`), so downstream code is identical for every dataset.
- **MEDS-compliant output**: every step writes a self-contained MEDS dataset (Parquet data plus `dataset.json` and `codes.parquet` metadata).

See the [user guide](../user_guide/pipeline.md) for a detailed walk through the pipeline.

## Key properties

| Property | How OpenICU achieves it |
| --- | --- |
| Reproducibility | Versioned configs with deterministic identifiers; merged config snapshot stored in each project |
| Privacy | Fully offline operation; no data ever leaves your machine |
| Performance | Polars lazy/streaming execution; full MIMIC-IV is processable on a 16–32 GB laptop |
| Extensibility | New datasets are pure YAML; custom transformation callbacks and complex concepts plug in via Python |
| Interoperability | MEDS v0.4+ output consumable by the wider MEDS tool ecosystem |

## Bundled configurations

The repository ships curated configurations under `configs/`:

- **Extraction configs** for MIMIC-IV 3.1, eICU-CRD 2.0 (full and demo), and NWICU 0.1.0.
- **A concept dictionary** of ~90 concepts across vital signs, blood gas, clinical chemistry, hematology, medications, neurological scores, respiratory parameters, fluid output, and demographics, with per-dataset mappings (most complete for MIMIC-IV).

# License

```
MIT License

Copyright (c) 2023 AIDH MS

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
