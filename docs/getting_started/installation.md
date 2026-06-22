# Installing

## Requirements

The supported Python versions for this Python library are the following:

* 3.13 or newer

## Installing from Source

OpenICU has not been published to PyPI yet. Until the first release, install it directly from GitHub.

**With pip**
```bash
pip install git+https://github.com/aidh-ms/OpenICU
```

**With uv**
```bash
uv add git+https://github.com/aidh-ms/OpenICU
```

## Installing for development

Clone the repository and either open it in the included dev container (recommended, see [contributing](contributing.md)) or set up the environment with [uv](https://docs.astral.sh/uv/):

```bash
git clone https://github.com/aidh-ms/OpenICU.git
cd OpenICU
uv sync --all-groups
```

Cloning the repository is also currently the easiest way to get the bundled dataset and concept configurations in `config/`, which you reference by path from your pipeline configuration.

## Dependencies

OpenICU builds on a small set of core libraries, installed automatically:

| Package | Role |
| --- | --- |
| [polars](https://pola.rs) | Lazy, streaming dataframe engine used for all data processing |
| [pydantic](https://docs.pydantic.dev) | Validation of all YAML configurations |
| [meds](https://github.com/Medical-Event-Data-Standard/meds) | MEDS schema definitions and metadata validation |
| [pyarrow](https://arrow.apache.org/docs/python/) | Parquet/columnar interchange |
| [pandas](https://pandas.pydata.org) | Interoperability with pandas-based workflows |
| [duckdb](https://duckdb.org) | SQL analytics engine |
| [pyyaml](https://pyyaml.org) | YAML parsing |

## Getting the data

OpenICU operates on locally downloaded dataset files (CSV/CSV.GZ) — no database setup is required. The public ICU datasets are distributed via [PhysioNet](https://physionet.org/) and require credentialed access:

- [MIMIC-IV](https://physionet.org/content/mimiciv/3.1/)
- [eICU-CRD](https://physionet.org/content/eicu-crd/2.0/) (a freely accessible [demo subset](https://physionet.org/content/eicu-crd-demo/2.0/) is also available)
- [NWICU](https://physionet.org/content/nwicu/0.1.0/)

Download the files and keep PhysioNet's directory layout (e.g. `physionet.org/files/mimiciv/3.1/hosp/labevents.csv.gz`); the bundled table configurations reference files relative to the dataset root.
