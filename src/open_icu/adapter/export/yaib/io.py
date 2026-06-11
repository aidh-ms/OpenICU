"""Input/output helpers."""

from __future__ import annotations

from pathlib import Path

import polars as pl


def find_concept_file(
    concept_root: str | Path,
    openicu_concept: str,
    dataset: str = "mimic-iv",
    version: str | None = None,
) -> Path | None:
    """Find one OpenICU concept parquet file.

    Expected layout: ``<concept_root>/<concept>/<version>/<dataset>.parquet``.
    If ``version`` is omitted, the lexicographically latest version directory is used.
    """
    root = Path(concept_root)
    concept_dir = root / openicu_concept
    if not concept_dir.exists():
        return None

    if version is not None:
        candidate = concept_dir / version / f"{dataset}.parquet"
        return candidate if candidate.exists() else None

    candidates = sorted(concept_dir.glob(f"*/{dataset}.parquet"))
    if not candidates:
        return None
    return candidates[-1]


def scan_openicu_concept(path: str | Path) -> pl.LazyFrame:
    """Scan one OpenICU MEDS-like concept parquet.

    The converter currently needs ``subject_id``, ``time`` and ``numeric_value``.
    Extension columns like ``dataset`` and ``table`` are intentionally ignored here.
    """
    lf = pl.scan_parquet(path)
    required = {"subject_id", "time", "numeric_value"}
    missing = required - set(lf.collect_schema().names())
    if missing:
        raise ValueError(f"Concept file {path} is missing columns: {sorted(missing)}")

    return lf.select(
        pl.col("subject_id").cast(pl.Int64),
        pl.col("time").cast(pl.Datetime),
        pl.col("numeric_value").cast(pl.Float64),
    )


def scan_mimic_icustays(path: str | Path) -> pl.LazyFrame:
    """Scan MIMIC-IV ``icu/icustays.csv.gz``.

    Expected columns: ``subject_id``, ``stay_id``, ``intime``, ``outtime``.
    """
    lf = pl.scan_csv(path, try_parse_dates=True)
    required = {"subject_id", "stay_id", "intime", "outtime"}
    missing = required - set(lf.collect_schema().names())
    if missing:
        raise ValueError(f"ICU stays file {path} is missing columns: {sorted(missing)}")

    return lf.select(
        pl.col("subject_id").cast(pl.Int64),
        pl.col("stay_id").cast(pl.Int64),
        pl.col("intime").cast(pl.Datetime),
        pl.col("outtime").cast(pl.Datetime),
    )
