# OpenICU

<p style="text-align:center;" markdown="1">
_A Python framework for extracting and harmonising ICU data into the MEDS standard._ <br>
[![Coverage Status](https://coveralls.io/repos/github/aidh-ms/OpenICU/badge.svg?branch=main)](https://coveralls.io/github/aidh-ms/OpenICU?branch=main)
</p>

---

**Source Code:** [https://github.com/aidh-ms/OpenICU](https://github.com/aidh-ms/OpenICU) <br>

---

OpenICU converts heterogeneous intensive care datasets — MIMIC-IV, eICU-CRD, NWICU, or your own institutional exports — into the standardised [MEDS](https://github.com/Medical-Event-Data-Standard/meds) (Medical Event Data Standard) format using declarative, versioned YAML configurations.

- **One concept, many datasets** — define clinical concepts once, map them per dataset, and run the same study code everywhere.
- **MEDS-native output** — Parquet event streams with full metadata, ready for the MEDS ecosystem.
- **Fully offline** — designed for sensitive medical data; nothing leaves your secure perimeter.
- **Fast and lightweight** — built on Polars lazy streaming; processes full MIMIC-IV on a laptop.

New here? Start with the [package overview](getting_started/overview.md), then follow [installation](getting_started/installation.md) and [basic usage](getting_started/basic_usage.md).
