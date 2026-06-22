"""Validation helpers for OpenICU -> YAIB/RICU dynamic tables."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from .concepts import DYNAMIC_VARS, RICU_TO_OPENICU
from .io import find_concept_file, scan_mimic_icustays, scan_openicu_concept
from .ricu_meta import RicuConceptMeta
from .transform import map_events_to_stays


def scan_dynamic(path: str | Path) -> pl.LazyFrame:
    return pl.scan_parquet(path)


def table_summary(df: pl.LazyFrame) -> pl.DataFrame:
    return df.select([
        pl.col("stay_id").n_unique().alias("n_stays"),
        pl.len().alias("n_rows"),
        pl.col("time").min().alias("min_time"),
        pl.col("time").max().alias("max_time"),
    ]).collect()


def validate_dynamic_columns(df: pl.LazyFrame, dynamic_vars: list[str] | None = None) -> dict[str, list[str]]:
    vars_ = dynamic_vars or DYNAMIC_VARS
    expected_cols = ["stay_id", "time"] + vars_
    actual_cols = df.collect_schema().names()
    return {
        "missing_cols": [c for c in expected_cols if c not in actual_cols],
        "unexpected_cols": [c for c in actual_cols if c not in expected_cols],
    }


def concept_coverage(df: pl.LazyFrame, dynamic_vars: list[str] | None = None) -> pl.DataFrame:
    vars_ = dynamic_vars or DYNAMIC_VARS
    actual_cols = set(df.collect_schema().names())
    present_vars = [c for c in vars_ if c in actual_cols]
    n_rows = df.select(pl.len()).collect().item()
    if not present_vars:
        return pl.DataFrame({"concept": [], "n_non_null": [], "percent_non_null": []})
    return (
        df.select([pl.col(c).is_not_null().sum().alias(c) for c in present_vars])
        .collect()
        .transpose(include_header=True, header_name="concept", column_names=["n_non_null"])
        .with_columns((pl.col("n_non_null") / n_rows * 100).alias("percent_non_null"))
        .sort("n_non_null")
    )


def ricu_range_report(df: pl.LazyFrame, ricu_meta: RicuConceptMeta, dynamic_vars: list[str] | None = None) -> pl.DataFrame:
    vars_ = dynamic_vars or DYNAMIC_VARS
    actual_cols = set(df.collect_schema().names())
    rows: list[pl.DataFrame] = []
    for concept in vars_:
        if concept not in actual_cols:
            continue
        lower, upper = ricu_meta.range_for(concept)
        rows.append(
            df.select([
                pl.lit(concept).alias("concept"),
                pl.col(concept).is_not_null().sum().alias("n_non_null"),
                pl.col(concept).min().cast(pl.Float64).alias("observed_min"),
                pl.col(concept).max().cast(pl.Float64).alias("observed_max"),
                pl.lit(None if lower is None else float(lower)).cast(pl.Float64).alias("ricu_min"),
                pl.lit(None if upper is None else float(upper)).cast(pl.Float64).alias("ricu_max"),
            ]).collect()
        )
    if not rows:
        return pl.DataFrame()
    return (
        pl.concat(rows, how="vertical")
        .with_columns([
            (pl.col("ricu_min").is_not_null() & pl.col("observed_min").is_not_null() & (pl.col("observed_min") < pl.col("ricu_min"))).alias("below_ricu_min"),
            (pl.col("ricu_max").is_not_null() & pl.col("observed_max").is_not_null() & (pl.col("observed_max") > pl.col("ricu_max"))).alias("above_ricu_max"),
        ])
        .sort("concept")
    )


def stay_windows_from_dyn(df: pl.LazyFrame) -> pl.DataFrame:
    """Return observed start/end time per stay in the final dynamic output."""
    return (
        df.group_by("stay_id")
        .agg([
            pl.col("time").min().alias("dyn_start"),
            pl.col("time").max().alias("dyn_end"),
            pl.col("time").n_unique().alias("dyn_n_timepoints"),
        ])
        .with_columns([
            (pl.col("dyn_end") - pl.col("dyn_start") + 1).alias("expected_n_timepoints"),
        ])
        .with_columns((pl.col("dyn_n_timepoints") - pl.col("expected_n_timepoints")).alias("missing_grid_points"))
        .sort("stay_id")
        .collect()
    )


def compare_dyn_to_icustays(
    dyn: pl.LazyFrame,
    icustays_csv: str | Path,
    *,
    end_rounding: str = "floor",
) -> pl.DataFrame:
    """Compare final dyn start/end per stay with MIMIC-IV icustays-derived windows."""
    if end_rounding not in {"floor", "ceil"}:
        raise ValueError("end_rounding must be 'floor' or 'ceil'.")
    stays = scan_mimic_icustays(icustays_csv)
    end_expr = pl.col("los_hours").floor() if end_rounding == "floor" else pl.col("los_hours").ceil()
    stay_windows = (
        stays.with_columns(((pl.col("outtime") - pl.col("intime")).dt.total_seconds().cast(pl.Float64) / 3600.0).alias("los_hours"))
        .with_columns(end_expr.cast(pl.Int64).alias("expected_end"))
        .select([
            "subject_id", "hadm_id", "stay_id", "intime", "outtime", "los_hours",
            pl.lit(0).cast(pl.Int64).alias("expected_start"),
            "expected_end",
        ])
    )
    dyn_windows = (
        dyn.group_by("stay_id")
        .agg([
            pl.col("time").min().alias("dyn_start"),
            pl.col("time").max().alias("dyn_end"),
            pl.col("time").n_unique().alias("dyn_n_timepoints"),
        ])
    )
    return (
        stay_windows.join(dyn_windows, on="stay_id", how="left")
        .with_columns([
            (pl.col("dyn_start") - pl.col("expected_start")).alias("diff_start"),
            (pl.col("dyn_end") - pl.col("expected_end")).alias("diff_end"),
        ])
        .sort("stay_id")
        .collect()
    )


def _range_filter_expr(ricu_meta: RicuConceptMeta, concept: str) -> pl.Expr:
    lower, upper = ricu_meta.range_for(concept)
    expr = pl.col("numeric_value").is_not_null()
    if lower is not None:
        expr = expr & (pl.col("numeric_value") >= float(lower))
    if upper is not None:
        expr = expr & (pl.col("numeric_value") <= float(upper))
    return expr


def debug_concept_against_output(
    *,
    concept: str,
    output_dyn: str | Path,
    concept_root: str | Path,
    icustays_csv: str | Path,
    ricu_meta: RicuConceptMeta,
    dataset: str = "mimic-iv",
    version: str | None = "1.0.0",
    concept_mapping: dict[str, str] | None = None,
    aggregate: str = "mean",
    filter_to_icu_window: bool = True,
) -> dict[str, Any]:
    """Trace one concept from raw OpenICU parquet to final wide output."""
    mapping = concept_mapping or RICU_TO_OPENICU
    if concept not in mapping:
        raise KeyError(f"No OpenICU concept mapping available for {concept!r}.")
    openicu_concept = mapping[concept]
    concept_file = find_concept_file(concept_root, openicu_concept, dataset=dataset, version=version)
    if concept_file is None:
        raise FileNotFoundError(f"Could not find parquet for {concept!r} / {openicu_concept!r}.")

    raw = scan_openicu_concept(concept_file)
    stays = scan_mimic_icustays(icustays_csv)
    filtered = raw.filter(_range_filter_expr(ricu_meta, concept))
    mapped = map_events_to_stays(filtered, stays, filter_to_icu_window=filter_to_icu_window)

    value = pl.col("numeric_value")
    aggregate_lower = aggregate.lower()
    if aggregate_lower == "mean":
        agg_expr = value.mean().alias(concept)
    elif aggregate_lower == "median":
        agg_expr = value.median().alias(concept)
    elif aggregate_lower == "sum":
        agg_expr = value.sum().alias(concept)
    elif aggregate_lower == "min":
        agg_expr = value.min().alias(concept)
    elif aggregate_lower == "max":
        agg_expr = value.max().alias(concept)
    else:
        raise ValueError(f"Unsupported debug aggregation: {aggregate!r}")

    manual = mapped.group_by(["stay_id", "time"]).agg(agg_expr).sort(["stay_id", "time"])
    final = (
        pl.scan_parquet(output_dyn)
        .select(["stay_id", "time", concept])
        .filter(pl.col(concept).is_not_null())
        .sort(["stay_id", "time"])
    )
    comparison = manual.join(final, on=["stay_id", "time"], how="inner", suffix="_final").with_columns(
        (pl.col(concept) - pl.col(f"{concept}_final")).abs().alias("abs_diff")
    )

    return {
        "concept": concept,
        "openicu_concept": openicu_concept,
        "concept_file": str(concept_file),
        "raw_summary": raw.select([
            pl.len().alias("rows"),
            pl.col("subject_id").n_unique().alias("subjects"),
            pl.col("time").min().alias("min_time"),
            pl.col("time").max().alias("max_time"),
            pl.col("numeric_value").min().alias("min_value"),
            pl.col("numeric_value").max().alias("max_value"),
        ]).collect(),
        "filtered_summary": filtered.select([
            pl.len().alias("rows"),
            pl.col("numeric_value").min().alias("min_value"),
            pl.col("numeric_value").max().alias("max_value"),
        ]).collect(),
        "mapped_summary": mapped.select([
            pl.len().alias("rows"),
            pl.col("stay_id").n_unique().alias("stays"),
            pl.col("time").min().alias("min_time"),
            pl.col("time").max().alias("max_time"),
        ]).collect(),
        "comparison_summary": comparison.select([
            pl.len().alias("n_compared"),
            pl.col("abs_diff").max().alias("max_abs_diff"),
            pl.col("abs_diff").mean().alias("mean_abs_diff"),
        ]).collect(),
        "largest_differences": comparison.sort("abs_diff", descending=True).limit(20).collect(),
    }
