"""Input helpers for OpenICU concept parquets and MIMIC-IV ICU stays."""

from __future__ import annotations

from pathlib import Path

import polars as pl


def find_concept_file(
    concept_root: str | Path,
    openicu_concept: str,
    *,
    dataset: str = "mimic-iv",
    version: str | None = None,
) -> Path | None:
    """Find one OpenICU concept parquet.

    Expected common layout:
        <concept_root>/<concept>/<version>/<dataset>.parquet

    The function also falls back to a recursive search for '<dataset>.parquet'
    below the concept folder.
    """
    root = Path(concept_root)
    concept_dir = root / openicu_concept
    candidates: list[Path] = []

    if version is not None:
        candidates.append(concept_dir / version / f"{dataset}.parquet")
        candidates.append(concept_dir / version / f"{dataset.replace('-', '_')}.parquet")

    candidates.append(concept_dir / f"{dataset}.parquet")
    candidates.append(concept_dir / f"{dataset.replace('-', '_')}.parquet")

    for candidate in candidates:
        if candidate.is_file():
            return candidate

    if concept_dir.is_dir():
        recursive = sorted(concept_dir.rglob(f"{dataset}.parquet"))
        if recursive:
            return recursive[0]
        recursive = sorted(concept_dir.rglob(f"{dataset.replace('-', '_')}.parquet"))
        if recursive:
            return recursive[0]

    return None


def scan_openicu_concept(path: str | Path) -> pl.LazyFrame:
    """Scan an OpenICU MEDS-like concept parquet.

    Required columns after normalization:
        subject_id, time, numeric_value
    """
    lf = pl.scan_parquet(path)
    schema = lf.collect_schema()
    required = {"subject_id", "time", "numeric_value"}
    missing = sorted(required - set(schema.names()))
    if missing:
        raise ValueError(f"Concept parquet {path} is missing columns: {missing}")
    return lf.select([
        pl.col("subject_id").cast(pl.Int64),
        pl.col("time"),
        pl.col("numeric_value").cast(pl.Float64),
    ])


def scan_mimic_icustays(path: str | Path) -> pl.LazyFrame:
    """Scan MIMIC-IV icustays.csv.gz and normalize dtypes."""
    lf = pl.scan_csv(path, try_parse_dates=True)
    schema = lf.collect_schema()
    required = {"subject_id", "hadm_id", "stay_id", "intime", "outtime"}
    missing = sorted(required - set(schema.names()))
    if missing:
        raise ValueError(f"ICU stays file {path} is missing columns: {missing}")

    return lf.select([
        pl.col("subject_id").cast(pl.Int64),
        pl.col("hadm_id").cast(pl.Int64),
        pl.col("stay_id").cast(pl.Int64),
        pl.col("intime").cast(pl.Datetime),
        pl.col("outtime").cast(pl.Datetime),
    ])
